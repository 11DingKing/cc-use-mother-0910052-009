"""日终结算 API 集成测试：路由、来源标注、幂等与封账后事件。"""

import pytest
from fastapi.testclient import TestClient

import app.config as config
from app.main import app


@pytest.fixture
def client(tmp_path):
    # 使用独立临时数据库，避免污染默认 chan_trading.db
    original_url = config.DATABASE_URL
    original_engine = config._engine
    original_session = config._SessionLocal
    config.DATABASE_URL = f"sqlite:///{tmp_path / 'api_settlement.db'}"
    config._engine = None
    config._SessionLocal = None
    config.init_database()

    with TestClient(app) as c:
        connected = c.post(
            "/api/trading/connect",
            json={
                "adapter_type": "simulation",
                "config": {
                    "initial_cash": 100000,
                    "account_id": "API_ACC",
                },
            },
        )
        assert connected.status_code == 200
        yield c

    config.DATABASE_URL = original_url
    config._engine = original_engine
    config._SessionLocal = original_session


class TestSettlementAPI:
    def test_full_settlement_flow(self, client):
        # 买入
        buy = client.post(
            "/api/trading/buy",
            json={
                "stock_code": "000001",
                "quantity": 1000,
                "price": 10.0,
            },
        )
        assert buy.status_code == 200
        assert buy.json()["status"] == "filled"

        # 执行日终结算
        run = client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 10.5},
                "next_trading_date": "2026-10-02",
            },
        )
        assert run.status_code == 200
        sealed = run.json()
        assert sealed["status"] == "sealed"
        assert sealed["source"] == "sealed_batch"
        assert sealed["values_source"] == "settlement:sealed_batch"
        assert sealed["cash"] == pytest.approx(89995.0)
        assert sealed["market_value"] == pytest.approx(10500.0)
        assert sealed["total_assets"] == pytest.approx(100495.0)
        assert sealed["unrealized_pnl"] == pytest.approx(500.0)
        assert sealed["next_available_cash"] == pytest.approx(89995.0)
        assert sealed["positions"][0]["quantity"] == 1000
        batch_no = sealed["batch_no"]

        # 重复触发 -> 同一批次、同一数字
        rerun = client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 10.5},
            },
        )
        assert rerun.status_code == 200
        assert rerun.json()["batch_no"] == batch_no
        assert rerun.json()["total_assets"] == sealed["total_assets"]

        # 用冲突收盘价重封 -> 409
        conflict = client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 99.0},
            },
        )
        assert conflict.status_code == 409

    def test_get_settlement_and_history(self, client):
        client.post(
            "/api/trading/buy",
            json={"stock_code": "000001", "quantity": 1000, "price": 10.0},
        )
        client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 10.0},
            },
        )

        detail = client.get("/api/trading/settlements/2026-10-01")
        assert detail.status_code == 200
        assert detail.json()["status"] == "sealed"

        # 含订单明细
        with_orders = client.get(
            "/api/trading/settlements/2026-10-01?include_orders=true"
        )
        assert len(with_orders.json()["orders"]) == 1

        missing = client.get("/api/trading/settlements/1999-01-01")
        assert missing.status_code == 200
        assert missing.json()["status"] == "not_found"
        assert missing.json()["source"] == "none"

        listed = client.get("/api/trading/settlements")
        assert listed.status_code == 200
        dates = [b["trade_date"] for b in listed.json()["settlements"]]
        assert "2026-10-01" in dates

    def test_account_view_distinguishes_sources(self, client):
        client.post(
            "/api/trading/buy",
            json={"stock_code": "000001", "quantity": 1000, "price": 10.0},
        )
        client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 10.5},
            },
        )

        # 已封账日：封账数字
        sealed_view = client.get("/api/trading/account-view/2026-10-01")
        assert sealed_view.status_code == 200
        body = sealed_view.json()
        assert body["source"] == "sealed_batch"
        assert body["view"] == "settlement"
        assert body["sealed"] is True
        assert body["total_assets"] == pytest.approx(100495.0)

        # 未来日：临时估值
        prov = client.get("/api/trading/account-view/2026-10-08")
        assert prov.status_code == 200
        prov_body = prov.json()
        assert prov_body["source"] == "provisional"
        assert prov_body["view"] == "valuation"
        assert prov_body["sealed"] is False
        assert "note" in prov_body

        # 独立临时估值端点
        valuation = client.get("/api/trading/valuation?trade_date=2026-10-08")
        assert valuation.json()["source"] == "provisional"

    def test_late_fill_consumed_by_next_batch(self, client):
        client.post(
            "/api/trading/buy",
            json={"stock_code": "000001", "quantity": 1000, "price": 10.0},
        )
        d1 = client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 10.0},
            },
        ).json()

        # 封账后迟到成交登记
        late = client.post(
            "/api/trading/settlements/late-fills",
            json={
                "trade_date": "2026-10-01",
                "order_id": "ORD_LATE_API",
                "stock_code": "000001",
                "side": "sell",
                "quantity": 400,
                "filled_price": 10.6,
            },
        )
        assert late.status_code == 200
        assert late.json()["status"] == "pending"

        # 重复登记幂等
        late_again = client.post(
            "/api/trading/settlements/late-fills",
            json={
                "trade_date": "2026-10-01",
                "order_id": "ORD_LATE_API",
                "stock_code": "000001",
                "side": "sell",
                "quantity": 400,
                "filled_price": 10.6,
            },
        )
        assert late_again.json()["duplicated"] is True

        # 历史批次数字不变
        unchanged = client.get("/api/trading/settlements/2026-10-01").json()
        assert unchanged["total_assets"] == d1["total_assets"]

        # 下一交易日批次消费
        d2 = client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-02",
                "closing_prices": {"000001": 11.0},
            },
        ).json()
        assert d2["positions"][0]["quantity"] == 600
        assert d2["realized_pnl"] == pytest.approx(240.0)

        adjustments = client.get(
            "/api/trading/post-seal-adjustments?trade_date=2026-10-01"
        ).json()["adjustments"]
        assert all(a["status"] == "applied" for a in adjustments)

    def test_late_fill_before_seal_is_422(self, client):
        resp = client.post(
            "/api/trading/settlements/late-fills",
            json={
                "trade_date": "2026-10-01",
                "order_id": "X",
                "stock_code": "000001",
                "side": "sell",
                "quantity": 100,
                "filled_price": 11.0,
            },
        )
        assert resp.status_code == 422

    def test_restatement_is_rejected_trace_only(self, client):
        client.post(
            "/api/trading/buy",
            json={"stock_code": "000001", "quantity": 1000, "price": 10.0},
        )
        client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 10.5},
            },
        )
        resp = client.post(
            "/api/trading/settlements/restatements",
            json={
                "trade_date": "2026-10-01",
                "reason": "行情源修正",
                "corrected_quotes": {"000001": 10.9},
            },
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "rejected"

        # 封账数字不受影响
        sealed = client.get("/api/trading/settlements/2026-10-01").json()
        assert sealed["positions"][0]["close_price"] == pytest.approx(10.5)

    def test_corporate_action_dividend_applied(self, client):
        client.post(
            "/api/trading/buy",
            json={"stock_code": "000001", "quantity": 1000, "price": 10.0},
        )
        d1 = client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-01",
                "closing_prices": {"000001": 10.0},
            },
        ).json()

        ca = client.post(
            "/api/trading/corporate-actions",
            json={
                "stock_code": "000001",
                "action_type": "cash_dividend",
                "ex_date": "2026-10-02",
                "cash_per_share": 0.5,
            },
        )
        assert ca.status_code == 200

        d2 = client.post(
            "/api/trading/settlements/run",
            json={
                "trade_date": "2026-10-02",
                "closing_prices": {"000001": 10.2},
            },
        ).json()
        assert d2["corporate_actions"][0]["effect_cash"] == pytest.approx(500.0)
        assert d2["cash"] == pytest.approx(d1["cash"] + 500.0)

    def test_invalid_date_returns_400(self, client):
        resp = client.post(
            "/api/trading/settlements/run",
            json={"trade_date": "2026/10/01"},
        )
        assert resp.status_code == 400
