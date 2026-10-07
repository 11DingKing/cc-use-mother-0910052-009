"""业务模块说明。"""

from typing import Optional
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
    """日终结算请求。"""
    trade_date: str
    # 显式固定收盘价（股票代码 -> 价格）；缺省取适配器收盘价
    closing_prices: Optional[dict] = None
    next_trading_date: Optional[str] = None
    # 首个批次的期初资金（缺省取适配器 initial_cash）
    initial_cash: Optional[float] = None


class LateFillRequest(BaseModel):
    """封账后迟到成交登记。"""
    trade_date: str
    order_id: str
    stock_code: str
    side: str  # buy / sell
    quantity: int
    filled_price: float
    stock_name: Optional[str] = None


class RestatementRequest(BaseModel):
    """重新估值登记（不回改已封账数字）。"""
    trade_date: str
    reason: str
    corrected_quotes: Optional[dict] = None


class CorporateActionRequest(BaseModel):
    """企业行动登记。"""
    stock_code: str
    action_type: str  # cash_dividend / stock_dividend / split
    ex_date: str
    ratio: float = 0.0
    cash_per_share: float = 0.0


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


# ---------------------------------------------------------------------------
# 日终结算
# ---------------------------------------------------------------------------

@router.post("/settlements/run")
async def run_settlement(request: SettlementRunRequest):
    """执行或续办日终结算（同一交易日重复调用幂等，返回同一批次）。"""
    return trading_service.run_settlement(
        trade_date=request.trade_date,
        closing_prices=request.closing_prices,
        next_trading_date=request.next_trading_date,
        initial_cash=request.initial_cash,
    )


@router.get("/settlements/{trade_date}")
async def get_settlement(
    trade_date: str,
    include_orders: bool = Query(default=False),
):
    """查询指定交易日的结算批次（已封账数字或进行中状态）。"""
    result = trading_service.get_settlement(
        trade_date, include_orders=include_orders
    )
    if result is None:
        return {
            "trade_date": trade_date,
            "status": "not_found",
            "source": "none",
            "sealed": False,
        }
    return result


@router.get("/settlements")
async def list_settlements(
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
):
    """历史结算批次列表。"""
    return {"settlements": trading_service.list_settlements(status=status, limit=limit)}


@router.get("/account-view/{trade_date}")
async def get_account_view(trade_date: str):
    """权益视图：已封账返回封账数字，否则临时估值，并标注数据来源。"""
    return trading_service.get_account_view(trade_date)


@router.get("/valuation")
async def get_valuation(
    trade_date: Optional[str] = Query(default=None),
):
    """临时估值（不落库、不封账）。"""
    return trading_service.get_valuation(trade_date=trade_date)


@router.post("/settlements/late-fills")
async def record_late_fill(request: LateFillRequest):
    """登记封账后迟到成交：只登记不改账，由后续交易日批次幂等消费。"""
    return trading_service.record_late_fill(
        trade_date=request.trade_date,
        order_id=request.order_id,
        stock_code=request.stock_code,
        side=request.side,
        quantity=request.quantity,
        filled_price=request.filled_price,
        stock_name=request.stock_name,
    )


@router.post("/settlements/restatements")
async def register_restatement(request: RestatementRequest):
    """登记重新估值：已封账数字不可变，请求仅 rejected 留痕。"""
    return trading_service.register_restatement(
        trade_date=request.trade_date,
        reason=request.reason,
        corrected_quotes=request.corrected_quotes,
    )


@router.post("/corporate-actions")
async def register_corporate_action(request: CorporateActionRequest):
    """登记企业行动（分红/送股/拆股）。"""
    return trading_service.register_corporate_action(
        stock_code=request.stock_code,
        action_type=request.action_type,
        ex_date=request.ex_date,
        ratio=request.ratio,
        cash_per_share=request.cash_per_share,
    )


@router.get("/post-seal-adjustments")
async def list_post_seal_adjustments(
    trade_date: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
):
    """查询封账后调整登记（迟到成交/重新估值）。"""
    return {
        "adjustments": trading_service.list_post_seal_adjustments(
            trade_date=trade_date, status=status
        )
    }
