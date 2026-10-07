"""业务模块说明。"""

from typing import Optional, List, Any, Dict
from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services.trading_service import TradingService

router = APIRouter(prefix="/api/trading", tags=["trading"])
trading_service = TradingService()


class ConnectRequest(BaseModel):
    """业务模块说明。"""
    adapter_type: str = "simulation"  # simulation, vnpy
    config: Optional[dict] = None


class BuyRequest(BaseModel):
    """业务模块说明。"""
    stock_code: str
    quantity: int
    price: Optional[float] = None
    order_type: str = "limit"  # limit, market
    signal_type: Optional[str] = None
    signal_strength: float = 0.0


class SellRequest(BaseModel):
    """业务模块说明。"""
    stock_code: str
    quantity: int
    price: Optional[float] = None
    order_type: str = "limit"
    signal_type: Optional[str] = None
    signal_strength: float = 0.0


class SignalTradeRequest(BaseModel):
    """业务模块说明。"""
    stock_code: str
    signal_type: str
    signal_strength: float
    price: float
    position_ratio: float = 0.1


class SettlementRunRequest(BaseModel):
    """日终结算请求：固定行情、企业行动与费用快照。"""
    trade_date: str
    # 收盘价 {股票代码: 价格}；缺省取适配器当前行情固化
    quotes: Optional[Dict[str, Any]] = None
    # 企业行动：{"type": "dividend", "stock_code": "...", "cash_per_share": 0.1}
    # 或 {"type": "split", "stock_code": "...", "ratio": 2}
    corporate_actions: Optional[List[Dict[str, Any]]] = None
    # 附加费用覆盖项
    fees: Optional[Dict[str, Any]] = None


class LateTradeRequest(BaseModel):
    """封账后迟到成交上报。"""
    order_id: str
    intended_date: str
    stock_code: str
    side: str  # buy / sell
    quantity: int
    price: float
    commission: float = 0.0
    account_id: Optional[str] = None


@router.post("/connect")
async def connect(request: ConnectRequest):
    """业务模块说明。"""
    success = trading_service.connect(request.adapter_type, request.config)
    return {
        "success": success,
        "adapter_type": request.adapter_type,
        "message": "连接成功" if success else "连接失败",
    }


@router.post("/disconnect")
async def disconnect():
    """业务模块说明。"""
    trading_service.disconnect()
    return {"success": True, "message": "已断开连接"}


@router.get("/account")
async def get_account():
    """业务模块说明。"""
    return trading_service.get_account()


@router.get("/account/view")
async def get_account_view(
    trade_date: Optional[str] = Query(default=None, description="交易日 YYYY-MM-DD"),
):
    """权益视图：source=sealed 返回封账权威数字，否则返回 provisional 临时估值。"""
    return trading_service.get_account_view(trade_date=trade_date)


@router.get("/settlement/valuation")
async def provisional_valuation(
    trade_date: Optional[str] = Query(default=None, description="交易日 YYYY-MM-DD"),
):
    """显式临时估值：只审计留痕，不产生任何分类账流水。"""
    return trading_service.provisional_valuation(trade_date=trade_date)


@router.post("/settlement/run")
async def run_settlement(request: SettlementRunRequest):
    """执行或续办日终结算（重复触发不重复入账，已封账直接返回封账结果）。"""
    return trading_service.run_settlement(
        trade_date=request.trade_date,
        quotes=request.quotes,
        corporate_actions=request.corporate_actions,
        fees=request.fees,
    )


@router.get("/settlement/{trade_date}")
async def get_settlement(trade_date: str):
    """查询指定交易日的结算批次与封账数字（含批次号、来源、快照哈希）。"""
    result = trading_service.get_settlement(trade_date)
    if result is None:
        return {"sealed": False, "trade_date": trade_date, "message": "该交易日尚无结算批次"}
    return result


@router.get("/settlement/{trade_date}/positions")
async def get_settlement_positions(trade_date: str):
    """查询指定交易日的封账持仓快照。"""
    return {
        "trade_date": trade_date,
        "positions": trading_service.get_settlement_positions(trade_date),
    }


@router.get("/settlement/{trade_date}/ledger")
async def get_settlement_ledger(trade_date: str):
    """查询指定交易日的分类账流水（含幂等键，供审计）。"""
    return {
        "trade_date": trade_date,
        "ledger": trading_service.get_settlement_ledger(trade_date),
    }


@router.get("/settlements/history")
async def list_settlements(
    start_date: Optional[str] = Query(default=None),
    end_date: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
):
    """历史结算批次列表。"""
    return {
        "settlements": trading_service.list_settlements(start_date, end_date, limit)
    }


@router.post("/late-trades")
async def record_late_trade(request: LateTradeRequest):
    """登记封账后迟到成交：挂起至下一交易日补入，绝不重开已封账批次。"""
    return trading_service.record_late_trade(request.model_dump())


@router.get("/carry-forward")
async def get_carry_forward():
    """下一交易日续办信息：结转未完成订单与待补迟到成交。"""
    return trading_service.get_carry_forward()


@router.post("/settlement/restore")
async def restore_latest_settlement():
    """进程重启后从最近封账批次恢复账户内存状态（现金/跨日持仓）。"""
    result = trading_service.restore_latest_settlement()
    if result is None:
        return {"restored": False, "message": "账户尚无已封账批次"}
    return {"restored": True, "batch_no": result["batch_no"], "trade_date": result["trade_date"]}


@router.get("/positions")
async def get_positions():
    """业务模块说明。"""
    return {"positions": trading_service.get_positions()}


@router.get("/positions/{stock_code}")
async def get_position(stock_code: str):
    """业务模块说明。"""
    position = trading_service.get_position(stock_code)
    if not position:
        return {"error": "未持有该股票"}
    return position


@router.post("/buy")
async def buy(request: BuyRequest):
    """业务模块说明。"""
    return trading_service.buy(
        stock_code=request.stock_code,
        quantity=request.quantity,
        price=request.price,
        order_type=request.order_type,
        signal_type=request.signal_type,
        signal_strength=request.signal_strength,
    )


@router.post("/sell")
async def sell(request: SellRequest):
    """业务模块说明。"""
    return trading_service.sell(
        stock_code=request.stock_code,
        quantity=request.quantity,
        price=request.price,
        order_type=request.order_type,
        signal_type=request.signal_type,
        signal_strength=request.signal_strength,
    )


@router.delete("/orders/{order_id}")
async def cancel_order(order_id: str):
    """业务模块说明。"""
    return trading_service.cancel_order(order_id)


@router.get("/orders/{order_id}")
async def get_order(order_id: str):
    """业务模块说明。"""
    return trading_service.get_order(order_id)


@router.get("/orders")
async def get_orders(
    stock_code: Optional[str] = Query(default=None, description="股票代码"),
    status: Optional[str] = Query(default=None, description="订单状态"),
):
    """业务模块说明。"""
    return {"orders": trading_service.get_orders(stock_code, status)}


@router.get("/quote/{stock_code}")
async def get_quote(stock_code: str):
    """业务模块说明。"""
    return trading_service.get_quote(stock_code)


@router.post("/signal-trade")
async def execute_signal_trade(request: SignalTradeRequest):
    """业务模块说明。"""
    result = trading_service.execute_signal(
        stock_code=request.stock_code,
        signal_type=request.signal_type,
        signal_strength=request.signal_strength,
        price=request.price,
        position_ratio=request.position_ratio,
    )
    
    if result:
        return result
    return {"message": "自动交易未启用或条件不满足"}


@router.post("/auto-trade/enable")
async def enable_auto_trade():
    """业务模块说明。"""
    trading_service.enable_auto_trade(True)
    return {"success": True, "message": "自动交易已启用"}


@router.post("/auto-trade/disable")
async def disable_auto_trade():
    """业务模块说明。"""
    trading_service.enable_auto_trade(False)
    return {"success": True, "message": "自动交易已禁用"}


@router.post("/check-stop-loss")
async def check_stop_loss():
    """业务模块说明。"""
    results = trading_service.check_stop_loss_take_profit()
    return {
        "triggered_count": len(results),
        "orders": results,
    }
