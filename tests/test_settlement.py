"""日终结算服务测试：唯一批次、可恢复续办、幂等、封账后事件、来源区分。"""

import pytest
from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.trading.simulation_adapter import SimulationAdapter
from app.trading.base import Order, OrderSide, OrderType, OrderStatus
from app.services.settlement_service import SettlementService, SettlementException
from app.entities.settlement import Base, SettlementBatch, SettlementPosition
from app.mappers.settlement_mapper import SettlementMapper


ACCOUNT = "ACC_TEST"


@pytest.fixture
def session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'settlement_test.db'}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    yield factory
    engine.dispose()


@pytest.fixture
def svc(session_factory):
    return SettlementService(session_factory=session_factory)


@pytest.fixture
def adapter():
    adapter = SimulationAdapter(
        {"initial_cash": 100000, "account_id": ACCOUNT}
    )
    adapter.connect()
    return adapter


def _buy(adapter, code="000001", qty=1000, price=10.0, quote=None):
    adapter.set_quote(code, quote if quote is not None else price)
    return adapter.buy(code, qty, price=price)


class TestBasicSeal:
    """封账数字：现金、市值、未实现盈亏、费用、下一交易日可用额度。"""

    def test_buy_then_seal(self, svc, adapter):
        _buy(adapter)
        result = svc.run_settlement(
            adapter,
            "2026-10-01",
            closing_prices={"000001": 10.5},
            next_trading_date="2026-10-02",
        )

        assert result["status"] == "sealed"
        assert result["source"] == "sealed_batch"
        assert result["values_source"] == "settlement:sealed_batch"
        assert result["sealed"] is True
        assert result["trade_date"] == "2026-10-01"
        assert result["next_trading_date"] == "2026-10-02"
        assert result["batch_no"].startswith("STL20261001-")
        assert result["snapshot_hash"]

        # 1000*10 买入，佣金 max(3, 5)=5；收盘 10.5
        assert result["cash"] == pytest.approx(89995.0)
        assert result["market_value"] == pytest.approx(10500.0)
        assert result["total_assets"] == pytest.approx(100495.0)
        assert result["unrealized_pnl"] == pytest.approx(500.0)
        assert result["total_fees"] == pytest.approx(5.0)
        assert result["next_available_cash"] == pytest.approx(89995.0)

        pos = result["positions"][0]
        assert pos["quantity"] == 1000
        assert pos["avg_cost"] == pytest.approx(10.0)
        assert pos["close_price"] == pytest.approx(10.5)
        assert pos["price_source"] == "settlement_close"

        quote = result["quotes"][0]
        assert quote["stock_code"] == "000001"
        assert quote["source"] == "closing_input"

    def test_sell_fees_and_realized_pnl(self, svc, adapter):
        _buy(adapter, price=10.0)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )

        adapter.set_quote("000001", 11.0)
        adapter.sell("000001", 400, price=11.0)
        result = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 11.0}
        )

        # 卖出 4400，佣金 max(1.32,5)=5，印花税 4.4
        assert result["total_fees"] == pytest.approx(9.4)
        assert result["realized_pnl"] == pytest.approx(400.0)
        assert result["cash"] == pytest.approx(89995.0 + 4400.0 - 9.4)
        assert result["positions"][0]["quantity"] == 600

    def test_uniqueness_one_batch_per_day(self, svc, adapter, session_factory):
        _buy(adapter)
        svc.run_settlement(adapter, "2026-10-01", closing_prices={"000001": 10.0})

        session = session_factory()
        try:
            count = (
                session.query(SettlementBatch)
                .filter_by(account_id=ACCOUNT, trade_date="2026-10-01")
                .count()
            )
            assert count == 1
        finally:
            session.close()


class TestIdempotentRerun:
    """重复触发结算不得重复入账。"""

    def test_rerun_returns_same_batch(self, svc, adapter):
        _buy(adapter)
        first = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.5}
        )
        second = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.5}
        )

        assert second["batch_no"] == first["batch_no"]
        assert second["cash"] == first["cash"]
        assert second["total_assets"] == first["total_assets"]
        assert second["snapshot_hash"] == first["snapshot_hash"]
        assert second["positions"][0]["quantity"] == 1000

    def test_fixed_close_price_cannot_change(self, svc, adapter):
        _buy(adapter)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.5}
        )
        with pytest.raises(SettlementException) as exc:
            svc.run_settlement(
                adapter, "2026-10-01", closing_prices={"000001": 99.0}
            )
        assert exc.value.status_code == 409

    def test_multi_day_no_double_posting(self, svc, adapter):
        _buy(adapter, price=10.0)
        d1 = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 10.2}
        )
        # 无新成交：现金不变，仅市值重估
        assert d2["cash"] == d1["cash"]
        assert d2["positions"][0]["quantity"] == 1000
        assert d2["market_value"] == pytest.approx(10200.0)


class TestUnfinishedOrders:
    """未完成订单跨日跟踪，成交后只过账增量。"""

    def _open_order(self, adapter):
        order = Order(
            order_id="ORD_OPEN1",
            stock_code="000002",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=100,
            price=Decimal("100"),
        )
        order.status = OrderStatus.SUBMITTED
        adapter._orders["ORD_OPEN1"] = order
        adapter.set_quote("000002", 5.0)
        return order

    def test_open_order_carried_across_days(self, svc, adapter):
        _buy(adapter)
        self._open_order(adapter)

        d1 = svc.run_settlement(
            adapter,
            "2026-10-01",
            closing_prices={"000001": 10.0, "000002": 5.0},
        )
        assert d1["unfinished_order_count"] == 1
        assert d1["unfinished_orders"][0]["order_id"] == "ORD_OPEN1"

        d2 = svc.run_settlement(
            adapter,
            "2026-10-02",
            closing_prices={"000001": 10.2, "000002": 5.1},
        )
        assert d2["unfinished_order_count"] == 1
        # 未成交不应产生资金变动
        assert d2["cash"] == d1["cash"]

    def test_open_order_filled_later_posts_delta_once(self, svc, adapter):
        _buy(adapter)
        order = self._open_order(adapter)
        d1 = svc.run_settlement(
            adapter,
            "2026-10-01",
            closing_prices={"000001": 10.0, "000002": 5.0},
        )

        order.price = Decimal("5.1")
        adapter._execute_fill(order, Decimal("5.1"))
        d2 = svc.run_settlement(
            adapter,
            "2026-10-02",
            closing_prices={"000001": 10.2, "000002": 5.2},
        )

        posted = {
            o["order_id"]: o["posted_quantity"] for o in d2["orders"]
        }
        assert posted["ORD_OPEN1"] == 100
        assert d2["unfinished_order_count"] == 0
        assert d2["cash"] == pytest.approx(d1["cash"] - 510.0 - 5.0)

        # 再次封账查询不重复入账
        d2_again = svc.run_settlement(
            adapter,
            "2026-10-02",
            closing_prices={"000001": 10.2, "000002": 5.2},
        )
        assert d2_again["cash"] == d2["cash"]


class TestCrashRecovery:
    """封账中断后从快照断点安全续办，结果与一次完成一致。"""

    def test_replay_after_sealing_crash(self, svc, adapter, session_factory):
        _buy(adapter)
        d1 = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.5}
        )

        adapter.set_quote("000001", 10.6)
        adapter.sell("000001", 200, price=10.6)
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 10.8}
        )

        # 模拟在 sealing 后、封账完成前崩溃：状态回退、持仓行丢失
        session = session_factory()
        try:
            mapper = SettlementMapper(session)
            batch = mapper.get_batch(ACCOUNT, "2026-10-02")
            batch.status = "sealing"
            batch.sealed_at = None
            session.query(SettlementPosition).filter_by(
                batch_id=batch.id
            ).delete()
            session.commit()
        finally:
            session.close()

        resumed = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 10.8}
        )
        assert resumed["status"] == "sealed"
        assert resumed["batch_no"] == d2["batch_no"]
        assert resumed["cash"] == d2["cash"]
        assert resumed["total_assets"] == d2["total_assets"]
        assert resumed["snapshot_hash"] == d2["snapshot_hash"]
        assert resumed["positions"][0]["quantity"] == 800

    def test_missing_quote_marks_failed_then_resume(self, svc, adapter):
        _buy(adapter)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )

        # 重启替身：持仓/行情内存丢失，又不给收盘价
        class RestartedAdapter:
            def __init__(self, real):
                self._real = real
                self.config = real.config

            def get_account(self):
                return self._real.get_account()

            def get_positions(self):
                return []

            def get_orders(self):
                return self._real.get_orders()

            def get_quote(self, code):
                return None

        restarted = RestartedAdapter(adapter)
        with pytest.raises(SettlementException):
            svc.run_settlement(restarted, "2026-10-02")

        failed = svc.get_settlement(ACCOUNT, "2026-10-02")
        assert failed["status"] == "failed"
        assert failed["error_message"]

        # 补齐收盘价后续办成功
        resumed = svc.run_settlement(
            restarted, "2026-10-02", closing_prices={"000001": 10.8}
        )
        assert resumed["status"] == "sealed"
        assert resumed["positions"][0]["quantity"] == 1000
        assert resumed["positions"][0]["close_price"] == pytest.approx(10.8)

        # 续办后再查结果一致
        assert (
            svc.run_settlement(restarted, "2026-10-02")["cash"]
            == resumed["cash"]
        )


class TestLateFill:
    """封账后迟到成交：不改历史，由后续批次消费，且不重复。"""

    def test_late_fill_flow(self, svc, adapter):
        _buy(adapter, price=10.0)
        d1 = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )

        registered = svc.record_late_fill(
            ACCOUNT,
            "2026-10-01",
            order_id="ORD_LATE1",
            stock_code="000001",
            side="sell",
            quantity=400,
            filled_price=10.6,
        )
        assert registered["status"] == "pending"
        assert registered["duplicated"] is False

        # 重复登记同一事件 -> 幂等返回原记录
        again = svc.record_late_fill(
            ACCOUNT,
            "2026-10-01",
            order_id="ORD_LATE1",
            stock_code="000001",
            side="sell",
            quantity=400,
            filled_price=10.6,
        )
        assert again["duplicated"] is True
        assert again["id"] == registered["id"]

        # 历史封账数字不变
        assert (
            svc.get_settlement(ACCOUNT, "2026-10-01")["total_assets"]
            == d1["total_assets"]
        )

        # 下一交易日批次消费迟到成交（适配器内存仍持有 1000 股）
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 11.0}
        )
        assert d2["positions"][0]["quantity"] == 600
        # 收回 4240，佣金 5 + 印花税 4.24，实现盈亏 (10.6-10)*400=240
        assert d2["cash"] == pytest.approx(d1["cash"] + 4240 - 9.24)
        assert d2["realized_pnl"] == pytest.approx(240.0)

        # 调整记录已消费
        adjustments = [
            a
            for a in svc.list_adjustments(ACCOUNT)
            if a["adjustment_type"] == "late_fill"
        ]
        assert all(a["status"] == "applied" for a in adjustments)

        # 日2重跑不重复入账
        d2b = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 11.0}
        )
        assert d2b["cash"] == d2["cash"]
        assert d2b["positions"][0]["quantity"] == 600
        assert d2b["realized_pnl"] == d2["realized_pnl"]

    def test_late_fill_before_seal_rejected(self, svc, adapter):
        _buy(adapter)
        with pytest.raises(SettlementException) as exc:
            svc.record_late_fill(
                ACCOUNT,
                "2026-10-01",
                order_id="ORD_X",
                stock_code="000001",
                side="sell",
                quantity=100,
                filled_price=11.0,
            )
        assert exc.value.status_code == 422

    def test_late_fill_synced_into_adapter_not_double_posted(self, svc, adapter):
        _buy(adapter, price=10.0)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )
        svc.record_late_fill(
            ACCOUNT,
            "2026-10-01",
            order_id="ORD_LATE2",
            stock_code="000001",
            side="sell",
            quantity=400,
            filled_price=10.6,
        )
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 11.0}
        )

        # 之后迟到成交同步进了适配器（持仓修正为 600，订单出现）
        pos = adapter.get_position("000001")
        pos.quantity = 600
        pos.available_quantity = 600
        late_order = Order(
            order_id="ORD_LATE2",
            stock_code="000001",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=400,
            price=Decimal("10.6"),
            status=OrderStatus.FILLED,
            filled_quantity=400,
            filled_price=Decimal("10.6"),
        )
        adapter._orders["ORD_LATE2"] = late_order

        d3 = svc.run_settlement(
            adapter, "2026-10-03", closing_prices={"000001": 11.2}
        )
        # 该订单已在日2台账中过账 400 股，日3增量为 0，现金/持仓不变
        assert d3["cash"] == d2["cash"]
        assert d3["positions"][0]["quantity"] == 600
        # 日3无成交，当日已实现盈亏为 0（日2的 240 不会被重复计入）
        assert d3["realized_pnl"] == 0


class TestRestatement:
    """重新估值只留痕、永不改账。"""

    def test_restatement_rejected_and_immutable(self, svc, adapter):
        _buy(adapter)
        d1 = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.5}
        )

        result = svc.register_restatement(
            ACCOUNT,
            "2026-10-01",
            reason="行情源收盘价修正",
            corrected_quotes={"000001": 10.9},
        )
        assert result["status"] == "rejected"
        assert "不可变" in result["note"]

        # 重复登记幂等
        assert svc.register_restatement(
            ACCOUNT,
            "2026-10-01",
            reason="行情源收盘价修正",
            corrected_quotes={"000001": 10.9},
        )["duplicated"] is True

        # 封账数字不变
        again = svc.get_settlement(ACCOUNT, "2026-10-01")
        assert again["total_assets"] == d1["total_assets"]
        assert again["positions"][0]["close_price"] == pytest.approx(10.5)

    def test_restatement_requires_sealed_batch(self, svc, adapter):
        _buy(adapter)
        with pytest.raises(SettlementException):
            svc.register_restatement(ACCOUNT, "2026-10-01", reason="x")


class TestCorporateActions:
    """企业行动：现金分红、送股/拆股，幂等应用与确定性重放。"""

    def test_cash_dividend(self, svc, adapter):
        _buy(adapter, price=10.0)
        d1 = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )
        svc.register_corporate_action(
            ACCOUNT,
            "000001",
            "cash_dividend",
            "2026-10-02",
            cash_per_share=0.5,
        )
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 10.2}
        )
        assert d2["corporate_actions"][0]["effect_cash"] == pytest.approx(500.0)
        assert d2["cash"] == pytest.approx(d1["cash"] + 500.0)
        assert d2["positions"][0]["quantity"] == 1000

    def test_stock_dividend_dilutes_cost(self, svc, adapter, session_factory):
        _buy(adapter, price=10.0)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )
        svc.register_corporate_action(
            ACCOUNT, "000001", "stock_dividend", "2026-10-02", ratio=1.0
        )
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 6.0}
        )
        assert d2["positions"][0]["quantity"] == 2000
        assert d2["positions"][0]["avg_cost"] == pytest.approx(5.0)

        # 强制重放（模拟封账中断续办）后送股不重复
        session = session_factory()
        try:
            mapper = SettlementMapper(session)
            batch = mapper.get_batch(ACCOUNT, "2026-10-02")
            batch.status = "sealing"
            session.query(SettlementPosition).filter_by(
                batch_id=batch.id
            ).delete()
            session.commit()
        finally:
            session.close()
        resumed = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 6.0}
        )
        assert resumed["positions"][0]["quantity"] == 2000
        assert resumed["positions"][0]["avg_cost"] == pytest.approx(5.0)

    def test_split(self, svc, adapter):
        _buy(adapter, price=20.0, qty=100)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 20.0}
        )
        svc.register_corporate_action(
            ACCOUNT, "000001", "split", "2026-10-02", ratio=2.0
        )
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 10.5}
        )
        assert d2["positions"][0]["quantity"] == 200
        assert d2["positions"][0]["avg_cost"] == pytest.approx(10.0)

    def test_invalid_action_type(self, svc, adapter):
        with pytest.raises(SettlementException):
            svc.register_corporate_action(
                ACCOUNT, "000001", "mystery", "2026-10-02"
            )


class TestValuationAndSources:
    """接口区分临时估值与已封账数字，并给出来源。"""

    def test_provisional_valuation(self, svc, adapter):
        _buy(adapter, price=10.0)
        valuation = svc.get_valuation(
            adapter, "2026-10-01", quotes={"000001": 10.5}
        )
        assert valuation["source"] == "provisional"
        assert valuation["values_source"] == "provisional:live_adapter"
        assert valuation["sealed"] is False
        assert valuation["market_value"] == pytest.approx(10500.0)
        assert valuation["positions"][0]["price_source"] == "valuation_input"

    def test_account_view_prefers_sealed(self, svc, adapter):
        _buy(adapter, price=10.0)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.5}
        )

        sealed_view = svc.get_account_view(adapter, "2026-10-01")
        assert sealed_view["source"] == "sealed_batch"
        assert sealed_view["view"] == "settlement"

        future_view = svc.get_account_view(adapter, "2026-10-08")
        assert future_view["source"] == "provisional"
        assert future_view["view"] == "valuation"

    def test_in_progress_batch_exposed_in_view(self, svc, adapter):
        _buy(adapter, price=10.0)
        # 制造一个 failed 批次
        class RestartedAdapter:
            def __init__(self, real):
                self._real = real
                self.config = real.config

            def get_account(self):
                return self._real.get_account()

            def get_positions(self):
                return []

            def get_orders(self):
                return self._real.get_orders()

            def get_quote(self, code):
                return None

        with pytest.raises(SettlementException):
            svc.run_settlement(RestartedAdapter(adapter), "2026-10-02")

        view = svc.get_account_view(RestartedAdapter(adapter), "2026-10-02")
        assert view["source"] == "provisional"
        assert view["in_progress_status"] == "failed"
        assert view["in_progress_batch_no"]


class TestHistoryAndRates:
    """历史查询与费率快照。"""

    def test_list_settlements_ordering(self, svc, adapter):
        _buy(adapter, price=10.0)
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )
        svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 10.1}
        )
        svc.run_settlement(
            adapter, "2026-10-03", closing_prices={"000001": 10.2}
        )

        batches = svc.list_settlements(ACCOUNT)
        assert [b["trade_date"] for b in batches] == [
            "2026-10-03",
            "2026-10-02",
            "2026-10-01",
        ]
        assert all(b["status"] == "sealed" for b in batches)

        sealed_only = svc.list_settlements(ACCOUNT, status="sealed")
        assert len(sealed_only) == 3

    def test_rate_snapshot_frozen_per_batch(self, svc, adapter):
        _buy(adapter, price=10.0)
        d1 = svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )
        # 日2改用更高费率后卖出
        adapter.config["commission_rate"] = "0.001"
        adapter.config["min_commission"] = "10"
        adapter.set_quote("000001", 10.5)
        adapter.sell("000001", 100, price=10.5)
        d2 = svc.run_settlement(
            adapter, "2026-10-02", closing_prices={"000001": 10.5}
        )
        # 卖出 1050：佣金 max(1.05,10)=10，印花税 1.05
        assert d2["cash"] == pytest.approx(d1["cash"] + 1050 - 11.05)

        # 历史批次按当时费率，数字不变
        history = svc.get_settlement(ACCOUNT, "2026-10-01")
        assert history["cash"] == d1["cash"]

    def test_invalid_date_raises(self, svc, adapter):
        with pytest.raises(SettlementException):
            svc.run_settlement(adapter, "2026/10/01")

    def test_must_settle_in_date_order(self, svc, adapter):
        _buy(adapter, price=10.0)
        # 日1封账
        svc.run_settlement(
            adapter, "2026-10-01", closing_prices={"000001": 10.0}
        )

        # 重启替身导致日2失败
        class RestartedAdapter:
            def __init__(self, real):
                self._real = real
                self.config = real.config

            def get_account(self):
                return self._real.get_account()

            def get_positions(self):
                return []

            def get_orders(self):
                return self._real.get_orders()

            def get_quote(self, code):
                return None

        with pytest.raises(SettlementException):
            svc.run_settlement(RestartedAdapter(adapter), "2026-10-02")

        # 日3必须等日2续办完成
        with pytest.raises(SettlementException) as exc:
            svc.run_settlement(
                RestartedAdapter(adapter),
                "2026-10-03",
                closing_prices={"000001": 10.5},
            )
        assert exc.value.status_code == 409
        assert exc.value.details["open_trade_dates"] == ["2026-10-02"]

        # 续办日2后可正常封日3
        svc.run_settlement(
            RestartedAdapter(adapter),
            "2026-10-02",
            closing_prices={"000001": 10.2},
        )
        d3 = svc.run_settlement(
            RestartedAdapter(adapter),
            "2026-10-03",
            closing_prices={"000001": 10.3},
        )
        assert d3["status"] == "sealed"
