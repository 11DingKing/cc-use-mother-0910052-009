"""日终结算接口集成测试：临时估值与封账来源区分、幂等、迟到成交。"""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from app.main import app
from app.config import init_database
from app.services.settlement_store import SettlementStore
from app.services.settlement_service import SettlementService
from app.controllers import trading_controller


@pytest.fixture
def client():
    init_database()
    # 结算存储替换为内存库，与文件库隔离
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    trading_controller.trading_service.settlement = SettlementService(
        SettlementStore(engine=engine)
    )

    with TestClient(app) as c:
        yield c


def _connect_and_buy(client, qty=1000, price=10.0):
    resp = client.post(
        "/api/trading/connect",
        json={"adapter_type": "simulation", "config": {"initial_cash": 100000}},
    )
    assert resp.status_code == 200
    resp = client.post(
        "/api/trading/buy",
        json={"stock_code": "000001", "quantity": qty, "price": price},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "filled"


class TestSettlementAPI:
    """结算主链路接口。"""

    def test_provisional_then_sealed_view(self, client):
        _connect_and_buy(client)

        # 封账前：临时估值
        resp = client.get(
            "/api/trading/account/view", params={"trade_date": "2026-10-07"}
        )
        assert resp.status_code == 200
        view = resp.json()
        assert view["source"] == "provisional"
        assert view["sealed"] is False
        assert "total_assets" in view

        # 显式临时估值端点
        resp = client.get(
            "/api/trading/settlement/valuation",
            params={"trade_date": "2026-10-07"},
        )
        assert resp.status_code == 200
        assert resp.json()["source"] == "provisional"

        # 执行封账
        resp = client.post(
            "/api/trading/settlement/run",
            json={
                "trade_date": "2026-10-07",
                "quotes": {"000001": 10.5},
            },
        )
        assert resp.status_code == 200
        sealed = resp.json()
        assert sealed["status"] == "sealed"
        assert sealed["source"] == "sealed"
        assert sealed["batch_no"]
        assert sealed["account"]["unrealized_pnl"] == 500.0
        assert sealed["snapshots"]["input_hash"]

        # 视图切换为封账口径（即使给新价格也不变）
        resp = client.get(
            "/api/trading/account/view", params={"trade_date": "2026-10-07"}
        )
        view2 = resp.json()
        assert view2["source"] == "sealed"
        assert view2["batch_no"] == sealed["batch_no"]
        assert view2["account"]["market_value"] == 10500.0

    def test_rerun_settlement_is_idempotent(self, client):
        _connect_and_buy(client)
        payload = {"trade_date": "2026-10-07", "quotes": {"000001": 10.5}}

        r1 = client.post("/api/trading/settlement/run", json=payload).json()
        r2 = client.post("/api/trading/settlement/run", json=payload).json()

        assert r1["batch_no"] == r2["batch_no"]
        assert r2["reused"] is True
        assert len(r1["ledger"]) == len(r2["ledger"]) == 3

    def test_settlement_detail_positions_ledger_history(self, client):
        _connect_and_buy(client)
        client.post(
            "/api/trading/settlement/run",
            json={"trade_date": "2026-10-07", "quotes": {"000001": 10.0}},
        )

        resp = client.get("/api/trading/settlement/2026-10-07")
        assert resp.status_code == 200
        assert resp.json()["status"] == "sealed"

        resp = client.get("/api/trading/settlement/2026-10-07/positions")
        assert resp.status_code == 200
        assert resp.json()["positions"][0]["stock_code"] == "000001"

        resp = client.get("/api/trading/settlement/2026-10-07/ledger")
        assert resp.status_code == 200
        types = [e["entry_type"] for e in resp.json()["ledger"]]
        assert "opening" in types and "trade" in types

        resp = client.get(
            "/api/trading/settlements/history",
            params={"start_date": "2026-10-01", "end_date": "2026-10-31"},
        )
        assert resp.status_code == 200
        dates = [s["trade_date"] for s in resp.json()["settlements"]]
        assert "2026-10-07" in dates

    def test_missing_settlement_responses(self, client):
        _connect_and_buy(client)

        # 不存在的批次：摘要端点 200 + sealed=False
        resp = client.get("/api/trading/settlement/2099-01-01")
        assert resp.status_code == 200
        assert resp.json()["sealed"] is False

        # 持仓/流水端点对不存在批次报 404
        resp = client.get("/api/trading/settlement/2099-01-01/positions")
        assert resp.status_code == 404
        resp = client.get("/api/trading/settlement/2099-01-01/ledger")
        assert resp.status_code == 404

    def test_invalid_trade_date_400(self, client):
        _connect_and_buy(client)
        resp = client.post(
            "/api/trading/settlement/run",
            json={"trade_date": "2026/10/07", "quotes": {}},
        )
        assert resp.status_code == 400
        assert resp.json()["error"]["code"] == "SETTLEMENT_ERROR"


class TestLateTradeAndCarryForwardAPI:
    """迟到成交与跨日续办接口。"""

    def test_late_trade_flow(self, client):
        _connect_and_buy(client)
        # Day1 封账
        client.post(
            "/api/trading/settlement/run",
            json={"trade_date": "2026-10-07", "quotes": {"000001": 10.0}},
        )

        # 上报迟到成交
        resp = client.post(
            "/api/trading/late-trades",
            json={
                "order_id": "LATEAPI1",
                "intended_date": "2026-10-07",
                "stock_code": "000001",
                "side": "sell",
                "quantity": 500,
                "price": 10.0,
                "commission": 5.0,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "pending"
        assert body["registered"] is True
        assert body["intended_date_sealed"] is True

        # 续办视图包含该迟到成交
        resp = client.get("/api/trading/carry-forward")
        assert resp.status_code == 200
        carry = resp.json()
        assert carry["latest_sealed_date"] == "2026-10-07"
        assert len(carry["pending_late_trades"]) == 1

        # Day2 结算补入一次
        resp = client.post(
            "/api/trading/settlement/run",
            json={"trade_date": "2026-10-08", "quotes": {"000001": 10.0}},
        )
        assert resp.status_code == 200
        d2 = resp.json()
        late = [e for e in d2["ledger"] if e["entry_type"] == "late_trade"]
        assert len(late) == 1
        assert late[0]["ref_id"] == "LATEAPI1"

        # 已补入的不再挂起
        carry = client.get("/api/trading/carry-forward").json()
        assert carry["pending_late_trades"] == []

    def test_pending_order_frozen_in_api(self, client):
        _connect_and_buy(client)
        # 低于市价的限价买单挂起不成交
        resp = client.post(
            "/api/trading/buy",
            json={"stock_code": "000002", "quantity": 1000, "price": 8.0},
        )
        assert resp.json()["status"] == "submitted"

        resp = client.post(
            "/api/trading/settlement/run",
            json={"trade_date": "2026-10-07", "quotes": {"000001": 10.0, "000002": 9.0}},
        )
        result = resp.json()
        assert result["pending_order_count"] == 1
        assert result["next_day"]["frozen_cash"] == 8000.0
        assert result["next_day"]["available_cash"] == 81995.0
