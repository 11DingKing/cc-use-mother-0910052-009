"""日终结算服务测试。

覆盖：唯一批次、断点续办、重启可恢复、跨日持仓不重复入账、
未完成订单结转、迟到成交、企业行动、临时估值与封账分离、历史查询。
"""

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool

from app.trading.simulation_adapter import SimulationAdapter
from app.trading.base import Order, OrderSide, OrderType, OrderStatus
from app.services.settlement_store import SettlementStore
from app.services.settlement_service import (
    SettlementService,
    SettlementException,
)
from app.services.trading_service import TradingService
from decimal import Decimal


@pytest.fixture
def store():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    return SettlementStore(engine=engine)


@pytest.fixture
def service(store):
    return SettlementService(store)


@pytest.fixture
def adapter():
    ad = SimulationAdapter({"initial_cash": 100000})
    ad.connect()
    return ad


def _buy(adapter, code, qty, price):
    adapter.set_quote(code, price)
    return adapter.buy(code, qty, price, order_type=OrderType.LIMIT)


def _sell(adapter, code, qty, price):
    adapter.set_quote(code, price)
    return adapter.sell(code, qty, price, order_type=OrderType.LIMIT)


class TestFirstDaySettlement:
    """首日封账。"""

    def test_basic_buy_seal_numbers(self, service, adapter):
        _buy(adapter, "000001", 1000, 10.0)  # 成交额 10000，佣金 5

        result = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )

        assert result["status"] == "sealed"
        assert result["sealed"] is True
        assert result["reused"] is False
        assert result["batch_no"] == f"STL_{adapter.get_account().account_id}_2026-10-07"

        # 现金 = 100000 - 10000 - 5
        assert result["account"]["cash"] == 89995.0
        # 市值按封账收盘价 10.5
        assert result["account"]["market_value"] == 10500.0
        assert result["account"]["total_assets"] == 100495.0
        # 未实现盈亏 500，已实现 0，费用 5
        assert result["account"]["unrealized_pnl"] == 500.0
        assert result["realized_pnl"] == 0.0
        assert result["fees_total"] == 5.0
        # 当日盈亏 = 500 - 5
        assert result["day_pnl"] == 495.0
        # 无未完成订单，下一交易日可用 = 现金
        assert result["next_day"]["available_cash"] == 89995.0
        assert result["next_day"]["frozen_cash"] == 0.0

        assert len(result["positions"]) == 1
        pos = result["positions"][0]
        assert pos["quantity"] == 1000
        assert pos["available_quantity"] == 1000  # T+1 次日全部可卖
        assert pos["avg_cost"] == 10.0
        assert pos["close_price"] == 10.5

        # 流水：opening + 买入本金 + 费用
        types = [e["entry_type"] for e in result["ledger"]]
        assert types == ["opening", "trade", "fee"]

    def test_idempotent_rerun(self, service, adapter):
        _buy(adapter, "000001", 1000, 10.0)
        first = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )
        first_ledger_len = len(first["ledger"])

        second = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )

        assert second["reused"] is True
        assert second["status"] == "sealed"
        assert second["account"]["total_assets"] == first["account"]["total_assets"]
        assert len(second["ledger"]) == first_ledger_len  # 没有重复入账
        assert second["batch_no"] == first["batch_no"]

    def test_revaluation_with_new_quotes_keeps_sealed(self, service, adapter):
        """封账后重新估值（不同收盘价）不得改动已封账数字。"""
        _buy(adapter, "000001", 1000, 10.0)
        first = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )

        second = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 99.0}
        )
        assert second["reused"] is True
        assert second["input_conflict"] is True
        assert second["account"]["market_value"] == 10500.0
        assert second["account"]["total_assets"] == first["account"]["total_assets"]

    def test_unique_batch_per_account_date(self, service, adapter, store):
        _buy(adapter, "000001", 100, 10.0)
        service.run_settlement(adapter, "2026-10-07", quotes={"000001": 10.0})
        account_id = adapter.get_account().account_id
        with pytest.raises(IntegrityError):
            store.create_batch(
                account_id=account_id,
                trade_date="2026-10-07",
                beginning_cash=Decimal("0"),
                beginning_equity=Decimal("0"),
                beginning_positions={},
                quotes_snapshot={},
                orders_snapshot=[],
                pending_snapshot=[],
                fees_snapshot={},
                corporate_actions_snapshot=[],
                input_hash="x",
            )


class TestResumeAfterFailure:
    """异常中断后续办。"""

    def test_resume_after_missing_quote(self, service, adapter, store):
        _buy(adapter, "000001", 1000, 10.0)

        # 清掉适配器行情并禁止默认补价，确保无任何收盘价可用
        adapter._quotes.pop("000001", None)
        adapter.default_quote_price = Decimal("0")

        # 缺持仓收盘价 -> 估值阶段失败，但成交流水已落库
        with pytest.raises(SettlementException) as exc:
            service.run_settlement(adapter, "2026-10-07", quotes={})
        assert "收盘价" in str(exc.value.message)

        account_id = adapter.get_account().account_id
        failed = store.get_batch(account_id, "2026-10-07")
        assert failed.status == "failed"
        assert failed.attempts == 1
        partial_keys = store.existing_ledger_keys(failed.id)
        assert any(":trade:" in k for k in partial_keys)

        # 补齐行情续办：成功封账且无重复流水
        result = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )
        assert result["status"] == "sealed"
        assert result["attempts"] == 2
        types = [e["entry_type"] for e in result["ledger"]]
        assert types.count("trade") == 1
        assert types.count("fee") == 1
        assert result["account"]["cash"] == 89995.0

    def test_recover_after_restart(self, store, service, adapter):
        """模拟进程重启：用同一数据库新建服务实例查询封账结果。"""
        _buy(adapter, "000001", 1000, 10.0)
        service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )
        account_id = adapter.get_account().account_id

        restarted = SettlementService(store)
        history = restarted.list_settlements(account_id)
        assert len(history) == 1
        assert history[0]["status"] == "sealed"
        assert history[0]["account"]["total_assets"] == 100495.0

        detail = restarted.get_settlement(account_id, "2026-10-07")
        assert detail["source"] == "sealed"
        assert detail["batch_no"]
        assert detail["positions"][0]["quantity"] == 1000

        ledger = restarted.get_ledger(account_id, "2026-10-07")
        assert len(ledger) == 3

    def test_recover_from_file_db_new_connection(self, tmp_path):
        """真实文件库 + 全新连接恢复：重启进程后数字一致并可继续次日结算。"""
        db_path = tmp_path / "settle.db"
        engine1 = create_engine(f"sqlite:///{db_path}")
        store1 = SettlementStore(engine=engine1)
        service1 = SettlementService(store1)
        adapter = SimulationAdapter(
            {"initial_cash": 100000, "account_id": "ACCT_FIXED"}
        )
        adapter.connect()
        _buy(adapter, "000001", 1000, 10.0)
        service1.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )
        engine1.dispose()

        # “重启”：新引擎、新服务、新适配器（同一固定账户）
        engine2 = create_engine(f"sqlite:///{db_path}")
        service2 = SettlementService(SettlementStore(engine=engine2))
        history = service2.list_settlements("ACCT_FIXED")
        assert len(history) == 1
        assert history[0]["account"]["total_assets"] == 100495.0

        adapter2 = SimulationAdapter(
            {"initial_cash": 0, "account_id": "ACCT_FIXED"}
        )
        adapter2.connect()
        sealed = service2.get_settlement("ACCT_FIXED", "2026-10-07")
        adapter2.restore_from_settlement(sealed)
        assert adapter2.get_account().available_cash == Decimal("89995.00")
        assert adapter2.get_position("000001").quantity == 1000

        # 恢复后继续第二日卖出
        _sell(adapter2, "000001", 500, 11.0)
        d2 = service2.run_settlement(
            adapter2, "2026-10-08", quotes={"000001": 11.0}
        )
        assert d2["account"]["cash"] == 95484.5
        assert d2["positions"][0]["quantity"] == 500
        engine2.dispose()


class TestCrossDayReplay:
    """跨日持仓与历史成交不重复入账。"""

    def test_two_days_sell_partial(self, service, adapter):
        # ---- 第一天 ----
        _buy(adapter, "000001", 1000, 10.0)
        d1 = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )
        assert d1["account"]["cash"] == 89995.0

        # ---- 第二天：卖出 500 @11 ----
        _sell(adapter, "000001", 500, 11.0)
        d2 = service.run_settlement(
            adapter, "2026-10-08", quotes={"000001": 11.0}
        )

        assert d2["status"] == "sealed"
        # 历史买单不再入账；仅卖出 5500 - 佣金5 - 印花税5.5
        # cash = 89995 + 5500 - 10.5
        assert d2["account"]["cash"] == 95484.5
        assert d2["realized_pnl"] == 500.0  # (11-10)*500
        assert d2["fees_total"] == 10.5
        # 剩余 500 股，市值 5500，未实现 500
        assert d2["account"]["market_value"] == 5500.0
        assert d2["account"]["unrealized_pnl"] == 500.0
        assert d2["account"]["total_assets"] == 100984.5
        # 当日盈亏相对上一封账权益 100495：已实现500 - 费用10.5
        assert d2["day_pnl"] == 489.5
        assert len(d2["positions"]) == 1
        assert d2["positions"][0]["quantity"] == 500
        assert d2["positions"][0]["avg_cost"] == 10.0

        # 流水只有 opening + sell + fee（无历史买单）
        types = [e["entry_type"] for e in d2["ledger"]]
        assert types == ["opening", "trade", "fee"]

    def test_pending_order_carried_and_frozen(self, service, adapter):
        """未完成买单跨日结转并冻结下一交易日额度。"""
        _buy(adapter, "000001", 1000, 10.0)
        # 低于市价的限价买单不会成交
        order = Order(
            order_id="PEND001",
            stock_code="000002",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=1000,
            price=Decimal("8.0"),
        )
        adapter.place_order(order)
        assert order.status == OrderStatus.SUBMITTED

        d1 = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.0, "000002": 9.0}
        )
        assert d1["pending_order_count"] == 1
        pending = d1["pending_orders"][0]
        assert pending["order_id"] == "PEND001"
        assert pending["remaining_quantity"] == 1000
        assert pending["frozen_amount"] == 8000.0
        # 下一交易日可用 = 现金 89995 - 冻结 8000
        assert d1["next_day"]["available_cash"] == 81995.0
        assert d1["next_day"]["frozen_cash"] == 8000.0

        # 续办视图
        carry = service.get_carry_forward(adapter.get_account().account_id)
        assert carry["latest_sealed_date"] == "2026-10-07"
        assert len(carry["pending_orders"]) == 1
        assert carry["pending_orders"][0]["order_id"] == "PEND001"

    def test_sell_overshoot_replay_rejected(self, service, adapter):
        """快照成交与持仓不一致（跨日卖出超量）时落 failed。"""
        _buy(adapter, "000001", 100, 10.0)
        service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.0}
        )

        # 第二天直接在订单簿塞一条超量卖出成交（绕过适配器前置校验）
        bad = Order(
            order_id="BADSELL",
            stock_code="000001",
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=99999,
            price=Decimal("10.0"),
            status=OrderStatus.FILLED,
            filled_quantity=99999,
            filled_price=Decimal("10.0"),
        )
        adapter._orders["BADSELL"] = bad

        with pytest.raises(SettlementException) as exc:
            service.run_settlement(
                adapter, "2026-10-08", quotes={"000001": 10.0}
            )
        assert "超过重放持仓" in str(exc.value.message)
        account_id = adapter.get_account().account_id
        assert store_like_status(service, account_id, "2026-10-08") == "failed"


def store_like_status(service, account_id, trade_date):
    return service.store.get_batch(account_id, trade_date).status


class TestLateTrades:
    """封账后迟到成交。"""

    def test_late_trade_booked_once_next_day(self, service, adapter):
        # Day1：买入 1000 股并封账
        _buy(adapter, "000001", 1000, 10.0)
        d1 = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.0}
        )
        account_id = adapter.get_account().account_id

        # 封账后到达一笔属于 Day1 的卖出 500@10
        reg = service.record_late_trade(
            {
                "account_id": account_id,
                "order_id": "LATE001",
                "intended_date": "2026-10-07",
                "stock_code": "000001",
                "side": "sell",
                "quantity": 500,
                "price": 10.0,
                "commission": 5.0,
            }
        )
        assert reg["registered"] is True
        assert reg["intended_date_sealed"] is True

        # Day1 封账数字绝不改变
        assert service.get_settlement(account_id, "2026-10-07")[
            "account"
        ]["cash"] == d1["account"]["cash"]

        # 重复上报安全
        again = service.record_late_trade(
            {
                "account_id": account_id,
                "order_id": "LATE001",
                "intended_date": "2026-10-07",
                "stock_code": "000001",
                "side": "sell",
                "quantity": 500,
                "price": 10.0,
            }
        )
        assert again["registered"] is False

        # Day2 结算时补入一次
        d2 = service.run_settlement(
            adapter, "2026-10-08", quotes={"000001": 10.0}
        )
        late_entries = [e for e in d2["ledger"] if e["entry_type"] == "late_trade"]
        assert len(late_entries) == 1
        assert late_entries[0]["ref_id"] == "LATE001"
        # cash = 89995 + 5000 - 5 = 94990
        assert d2["account"]["cash"] == 94990.0
        assert d2["positions"][0]["quantity"] == 500

        # 续办/重跑不重复补入
        d2_again = service.run_settlement(
            adapter, "2026-10-08", quotes={"000001": 10.0}
        )
        assert d2_again["reused"] is True
        late2 = [e for e in d2_again["ledger"] if e["entry_type"] == "late_trade"]
        assert len(late2) == 1

        # 已应用的迟到成交不再出现在续办视图
        carry = service.get_carry_forward(account_id)
        assert carry["pending_late_trades"] == []


class TestCorporateActions:
    """分红与拆送股。"""

    def test_dividend_and_split(self, service, adapter):
        _buy(adapter, "000001", 1000, 10.0)
        result = service.run_settlement(
            adapter,
            "2026-10-07",
            quotes={"000001": 10.0},
            corporate_actions=[
                {"type": "dividend", "stock_code": "000001", "cash_per_share": 0.5},
                {"type": "split", "stock_code": "000001", "ratio": 2},
            ],
        )
        # 分红 500 元入账
        assert result["dividend_total"] == 500.0
        assert any(e["entry_type"] == "dividend" for e in result["ledger"])
        assert any(e["entry_type"] == "split" for e in result["ledger"])

        pos = result["positions"][0]
        # 10送10：1000 -> 2000 股，成本 10 -> 5
        assert pos["quantity"] == 2000
        assert pos["avg_cost"] == 5.0
        # cash = 89995 + 500 = 90495；市值 2000*10
        assert result["account"]["cash"] == 90495.0
        assert result["account"]["market_value"] == 20000.0
        applied = pos["corporate_actions_applied"]
        assert {a["type"] for a in applied} == {"dividend", "split"}

    def test_invalid_action_rejected(self, service, adapter):
        _buy(adapter, "000001", 100, 10.0)
        with pytest.raises(SettlementException):
            service.run_settlement(
                adapter,
                "2026-10-07",
                quotes={"000001": 10.0},
                corporate_actions=[
                    {"type": "split", "stock_code": "000001", "ratio": 0}
                ],
            )


class TestProvisionalVsSealed:
    """临时估值与已封账数字区分来源。"""

    def test_view_before_and_after_seal(self, service, adapter):
        _buy(adapter, "000001", 1000, 10.0)

        view = service.get_account_view(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )
        assert view["source"] == "provisional"
        assert view["sealed"] is False
        assert view["total_assets"] == 100495.0
        assert view["note"]

        # 临时估值不产生任何批次/流水
        account_id = adapter.get_account().account_id
        assert service.get_settlement(account_id, "2026-10-07") is None

        sealed = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.5}
        )
        view2 = service.get_account_view(
            adapter, "2026-10-07", quotes={"000001": 99.0}
        )
        assert view2["source"] == "sealed"
        assert view2["sealed"] is True
        assert view2["batch_no"] == sealed["batch_no"]
        # 即使传入新行情，封账视图仍为封账数字
        assert view2["account"]["market_value"] == 10500.0

    def test_provisional_repeatable_no_ledger(self, service, adapter):
        _buy(adapter, "000001", 100, 10.0)
        v1 = service.provisional_valuation(
            adapter, "2026-10-07", quotes={"000001": 11.0}
        )
        v2 = service.provisional_valuation(
            adapter, "2026-10-07", quotes={"000001": 11.0}
        )
        assert v1["basis_hash"] == v2["basis_hash"]
        assert v1["total_assets"] == v2["total_assets"]
        account_id = adapter.get_account().account_id
        assert service.get_settlement(account_id, "2026-10-07") is None


class TestHistoryQueries:
    """历史查询与校验。"""

    def test_history_and_404(self, service, adapter):
        _buy(adapter, "000001", 100, 10.0)
        service.run_settlement(adapter, "2026-10-07", quotes={"000001": 10.0})
        account_id = adapter.get_account().account_id

        positions = service.get_settlement_positions(account_id, "2026-10-07")
        assert positions[0]["stock_code"] == "000001"

        with pytest.raises(SettlementException) as exc:
            service.get_settlement_positions(account_id, "2099-01-01")
        assert exc.value.status_code == 404

    def test_bad_trade_date(self, service, adapter):
        with pytest.raises(SettlementException):
            service.run_settlement(adapter, "2026/10/07")

    def test_backfill_earlier_date_rejected(self, service, adapter):
        """已有封账历史后，不允许乱序补做更早交易日。"""
        _buy(adapter, "000001", 100, 10.0)
        service.run_settlement(
            adapter, "2026-10-08", quotes={"000001": 10.0}
        )
        with pytest.raises(SettlementException) as exc:
            service.run_settlement(
                adapter, "2026-10-07", quotes={"000001": 10.0}
            )
        assert exc.value.status_code == 409

    def test_missing_late_trade_fields(self, service):
        with pytest.raises(SettlementException):
            service.record_late_trade({"order_id": "X"})


class TestCrashBetweenPhases:
    """阶段间异常中断（模拟在封账写结果前崩溃）。"""

    def test_crash_after_positions_before_seal(self, service, adapter, store):
        _buy(adapter, "000001", 1000, 10.0)

        original_seal = store.seal_batch
        calls = {"n": 0}

        def flaky_seal(batch_id, results):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("模拟进程在封账阶段崩溃")
            return original_seal(batch_id, results)

        store.seal_batch = flaky_seal

        with pytest.raises(SettlementException):
            service.run_settlement(
                adapter, "2026-10-07", quotes={"000001": 10.0}
            )

        account_id = adapter.get_account().account_id
        batch = store.get_batch(account_id, "2026-10-07")
        assert batch.status == "failed"
        # 持仓已写入，流水已存在
        assert len(store.list_positions(batch.id)) == 1

        # 恢复 store 后续办：数字正确，流水不重复
        store.seal_batch = original_seal
        result = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.0}
        )
        assert result["status"] == "sealed"
        assert result["attempts"] == 2
        assert result["account"]["cash"] == 89995.0
        types = [e["entry_type"] for e in result["ledger"]]
        assert types.count("trade") == 1


class TestPartialFillCrossDay:
    """部分成交订单跨日补量，只入新增部分。"""

    def test_partial_then_completed_next_day(self, service, adapter):
        # Day1：一笔 1000 股买单只成交 400
        partial = Order(
            order_id="PART001",
            stock_code="000001",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=1000,
            price=Decimal("10.0"),
            status=OrderStatus.PARTIAL_FILLED,
            filled_quantity=400,
            filled_price=Decimal("10.0"),
            commission=Decimal("5"),
        )
        adapter._orders["PART001"] = partial
        adapter._positions["000001"] = _position("000001", 400, Decimal("10.0"))
        # 与 400 股成交一致的账户现金：100000 - 4000 - 摊算费用 2
        adapter._account.available_cash = Decimal("95998")

        d1 = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 10.0}
        )
        assert d1["positions"][0]["quantity"] == 400
        assert d1["account"]["cash"] == 95998.0

        # Day2：同一订单剩余 600 股成交，价格 10.0
        partial.status = OrderStatus.FILLED
        partial.filled_quantity = 1000
        partial.commission = Decimal("5")
        adapter._positions["000001"] = _position("000001", 1000, Decimal("10.0"))

        d2 = service.run_settlement(
            adapter, "2026-10-08", quotes={"000001": 10.0}
        )
        # Day2 只入新增 600 股：本金 6000，费用 3
        trades = [e for e in d2["ledger"] if e["entry_type"] == "trade"]
        assert len(trades) == 1
        assert trades[0]["quantity"] == 600
        assert trades[0]["amount"] == -6000.0
        pos = d2["positions"][0]
        assert pos["quantity"] == 1000
        assert pos["avg_cost"] == 10.0


def _position(code, qty, price):
    from app.trading.base import Position

    return Position(
        stock_code=code,
        stock_name=code,
        quantity=qty,
        available_quantity=qty,
        avg_cost=price,
        current_price=price,
        market_value=price * qty,
        profit_loss=Decimal("0"),
        profit_loss_ratio=0.0,
    )


class TestMultiDayChain:
    """连续三日完整链路。"""

    def test_three_day_chain(self, service, adapter):
        _buy(adapter, "000001", 1000, 10.0)
        d1 = service.run_settlement(
            adapter, "2026-10-07", quotes={"000001": 11.0}
        )
        # cash 89995, mv 11000, equity 100995
        assert d1["account"]["total_assets"] == 100995.0

        _buy(adapter, "000002", 500, 20.0)  # 10000 + 5
        d2 = service.run_settlement(
            adapter, "2026-10-08",
            quotes={"000001": 11.0, "000002": 19.0},
        )
        # cash 79990; mv 11000 + 9500 = 20500; equity 100490
        assert d2["account"]["cash"] == 79990.0
        assert d2["account"]["market_value"] == 20500.0
        assert d2["account"]["total_assets"] == 100490.0
        assert d2["day_pnl"] == -505.0  # 相对 100995：费用 -5，000002 浮亏 -500

        _sell(adapter, "000001", 1000, 11.0)  # 11000 - 5 - 11
        d3 = service.run_settlement(
            adapter, "2026-10-09",
            quotes={"000001": 11.0, "000002": 21.0},
        )
        # cash 79990 + 10984 = 90974; mv 10500; equity 101474
        assert d3["account"]["cash"] == 90974.0
        assert d3["account"]["market_value"] == 10500.0
        assert d3["realized_pnl"] == 1000.0  # (11-10)*1000
        # day pnl vs 100490：000001 浮盈 1000 转为已实现（费用 -16），
        # 000002 浮盈 +1000，合计 +984
        assert d3["day_pnl"] == 984.0
        assert d3["sealed_position_count"] == 1

        history = service.list_settlements(
            adapter.get_account().account_id,
            start_date="2026-10-07",
            end_date="2026-10-09",
        )
        assert [h["trade_date"] for h in history] == [
            "2026-10-09",
            "2026-10-08",
            "2026-10-07",
        ]


class TestTradingServiceWiring:
    """TradingService 委托结算服务的接线。"""

    def test_service_delegation(self, store, adapter):
        trading = TradingService(adapter)
        trading.settlement = SettlementService(store)

        adapter.set_quote("000001", 10.0)
        trading.buy("000001", 1000, 10.0)

        view = trading.get_account_view("2026-10-07", {"000001": 10.2})
        assert view["source"] == "provisional"

        sealed = trading.run_settlement("2026-10-07", {"000001": 10.2})
        assert sealed["status"] == "sealed"

        fetched = trading.get_settlement("2026-10-07")
        assert fetched["batch_no"] == sealed["batch_no"]
        assert len(trading.get_settlement_ledger("2026-10-07")) == 3
