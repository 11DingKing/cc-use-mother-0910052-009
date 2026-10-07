"""可恢复的日终结算服务。

核心思路
========
1. 同一账户同一交易日只有唯一批次（DB 唯一约束），批次状态机：
   pending（补快照）→ sealing（停止接单、定格订单）→ sealed（已封账）；
   快照缺失等错误落 failed，输入补齐后仍可续办。
2. 封账数字不是内存增量，而是从「上一已封账批次 + 订单/费用/企业行动快照」
   确定性重放账本得到。任一阶段崩溃后重跑只会得到同一结果：
   - 订单快照唯一、posted_quantity 记录过账进度；
   - 费用明细按 (批次, 订单, 类型) 唯一；
   - 企业行动应用记录幂等；
   - 持仓/汇总在封账事务内整体替换。
3. 封账后发生的事件一律不回改历史：
   - 迟到成交只登记 post_seal_adjustments(late_fill, pending)，
     由之后的交易日批次消费，applied 后不再重复；
   - 重新估值登记 restatement 并置 rejected，仅留痕、永不自动入账；
   - 对已封账日期重复触发结算返回同一批次。
4. 查询接口区分 source=sealed_batch（封账数字）与 provisional（临时估值）。
"""

import hashlib
import json
import logging
import threading
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.config import get_engine, get_db_session
from app.entities.settlement import PostSealAdjustment
from app.mappers.settlement_mapper import SettlementMapper, create_settlement_tables
from app.middleware.exception_handler import AppException
from app.trading.base import Order, OrderSide, OrderStatus

logger = logging.getLogger(__name__)

D = Decimal
_TWO_PLACES = D("0.01")

# 不参与账本的订单终态
_CLOSED_STATUSES = {
    OrderStatus.CANCELLED.value,
    OrderStatus.REJECTED.value,
    OrderStatus.FAILED.value,
}
# 未完成、需要跨日续办的订单状态
_OPEN_STATUSES = {
    OrderStatus.PENDING.value,
    OrderStatus.SUBMITTED.value,
    OrderStatus.PARTIAL_FILLED.value,
}


class SettlementException(AppException):
    """结算业务异常。"""

    def __init__(
        self,
        message: str,
        status_code: int = 400,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            code="SETTLEMENT_ERROR",
            status_code=status_code,
            details=details or {},
        )


def _money(value: D) -> D:
    return value.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)


def _d(value: Any) -> D:
    if value is None:
        return D("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _float(value: Any) -> float:
    return float(_d(value))


def _payload_hash(payload: Any) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class SettlementService:
    """日终结算编排服务。"""

    def __init__(self, session_factory=None):
        self._session_factory = session_factory
        self._locks: Dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ------------------------------------------------------------------
    # 基础设施
    # ------------------------------------------------------------------

    def init_storage(self, engine=None) -> None:
        """创建结算表（幂等）。"""
        if engine is None and self._session_factory is not None:
            engine = self._session_factory.kwargs.get("bind") or getattr(
                self._session_factory, "bind", None
            )
        create_settlement_tables(engine or get_engine())

    def _session(self):
        if self._session_factory is not None:
            return self._session_factory()
        return get_db_session()

    def _lock_for(self, account_id: str) -> threading.Lock:
        with self._locks_guard:
            if account_id not in self._locks:
                self._locks[account_id] = threading.Lock()
            return self._locks[account_id]

    @staticmethod
    def _is_unique_violation(exc: Exception) -> bool:
        text = str(exc).lower()
        return (
            "unique constraint failed" in text
            or "duplicate key" in text
            or "unique violation" in text
        )

    # ------------------------------------------------------------------
    # 对外主流程：日终结算（可安全续办）
    # ------------------------------------------------------------------

    def run_settlement(
        self,
        adapter: Any,
        trade_date: str,
        closing_prices: Optional[Dict[str, float]] = None,
        next_trading_date: Optional[str] = None,
        initial_cash: Optional[float] = None,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """执行（或续办）某交易日的日终结算。

        重复调用是安全的：已封账则原样返回；未完成则从断点继续。
        """
        self._validate_date(trade_date)
        account_id = account_id or self._account_id(adapter)
        closing_prices = closing_prices or {}
        next_trading_date = next_trading_date or self._default_next_date(trade_date)

        with self._lock_for(account_id):
            return self._run_locked(
                adapter=adapter,
                account_id=account_id,
                trade_date=trade_date,
                closing_prices=closing_prices,
                next_trading_date=next_trading_date,
                initial_cash=initial_cash,
            )

    def _run_locked(
        self,
        adapter: Any,
        account_id: str,
        trade_date: str,
        closing_prices: Dict[str, float],
        next_trading_date: str,
        initial_cash: Optional[float],
    ) -> Dict[str, Any]:
        session = self._session()
        try:
            mapper = SettlementMapper(session)
            batch = mapper.get_batch(account_id, trade_date)

            # 已封账：任何补发/重复触发都直接返回同一结果；
            # 但若调用方试图用不同的固定收盘价重封，显式拒绝
            if batch is not None and batch.status == "sealed":
                self._assert_quotes_match_snapshot(mapper, batch, closing_prices)
                logger.info(
                    "Settlement %s for %s already sealed, returning existing batch",
                    batch.batch_no,
                    trade_date,
                )
                return self._summary(mapper, batch, include_orders=True)

            # 顺序封账：更早日期存在未完成批次时必须先续办，避免把之后才
            # 出现的成交/状态错误并入历史账本
            open_earlier = mapper.open_batches_before(account_id, trade_date)
            if open_earlier:
                pending_dates = [b.trade_date for b in open_earlier]
                raise SettlementException(
                    f"存在更早交易日的未完成结算 {pending_dates}，"
                    f"请先按日期顺序续办完成后再结算 {trade_date}",
                    status_code=409,
                    details={
                        "open_trade_dates": pending_dates,
                    },
                )

            if batch is None:
                batch = mapper.add_batch(
                    self._new_batch(account_id, trade_date, adapter, initial_cash)
                )
                try:
                    session.commit()
                except Exception as exc:
                    # 并发触发时唯一约束 (account_id, trade_date) 可能冲突，
                    # 回滚后重取既有批次续办
                    session.rollback()
                    batch = mapper.get_batch(account_id, trade_date)
                    if batch is None or not self._is_unique_violation(exc):
                        raise
                    if batch.status == "sealed":
                        return self._summary(mapper, batch, include_orders=True)

            # 1) 固定费率快照（仅首次写入，之后不可变）
            self._freeze_rate_snapshot(batch, adapter)

            # 2) 进入 sealing 并定格订单快照（含未完成订单），可重复续办
            batch.status = "sealing"
            session.flush()
            session.commit()
            open_orders, last_updated = self._snapshot_orders(
                mapper, batch, adapter
            )
            session.commit()

            # 3) 固定行情快照：显式收盘价优先，其余取适配器收盘价
            quotes = self._snapshot_quotes(
                mapper, batch, adapter, closing_prices
            )
            session.commit()  # 行情快照作为断点保留

            # 4) 收集本批次应消费的封账后事件（之前日期的迟到成交）
            late_adjustments = [
                adj
                for adj in mapper.pending_adjustments_before(account_id, trade_date)
                if adj.adjustment_type == "late_fill"
            ]

            # 5) 确定性重放账本并写封账结果（整体替换，天然可续办）。
            #    行情完整性在重放后按「仍持仓股票」校验：已清仓股票无需收盘价。
            try:
                summary = self._seal(
                    mapper=mapper,
                    batch=batch,
                    adapter=adapter,
                    trade_date=trade_date,
                    quotes=quotes,
                    late_adjustments=late_adjustments,
                    open_orders=open_orders,
                    last_updated=last_updated,
                    next_trading_date=next_trading_date,
                )
            except SettlementException as exc:
                missing = exc.details.get("missing_quotes") if exc.details else None
                if missing:
                    # 回滚本次未提交的重放改动（过账/费用/迟到消费标记），
                    # 只保留此前已提交的行情/订单快照断点，便于补齐后续办
                    session.rollback()
                    batch = mapper.get_batch(account_id, trade_date)
                    batch.status = "failed"
                    batch.error_message = (
                        f"缺少固定收盘价: {','.join(missing)}"
                    )
                    session.commit()
                raise
            session.commit()
            logger.info(
                "Settlement sealed: batch=%s account=%s date=%s total=%s",
                batch.batch_no,
                account_id,
                trade_date,
                batch.total_assets,
            )
            return summary
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # 封账：账本重放
    # ------------------------------------------------------------------

    def _seal(
        self,
        mapper: SettlementMapper,
        batch: Any,
        adapter: Any,
        trade_date: str,
        quotes: Dict[str, D],
        late_adjustments: List[PostSealAdjustment],
        open_orders: List[Dict[str, Any]],
        last_updated: Optional[datetime],
        next_trading_date: str,
    ) -> Dict[str, Any]:
        # ---- 起点：上一已封账批次 ----
        prev = mapper.latest_sealed_before(batch.account_id, trade_date)
        positions: Dict[str, Dict[str, Any]] = {}
        if prev is not None:
            batch.prev_batch_id = prev.id
            for row in mapper.list_positions(prev.id):
                positions[row.stock_code] = {
                    "stock_name": row.stock_name or row.stock_code,
                    "quantity": row.quantity,
                    "avg_cost": _d(row.avg_cost),
                }
            begin_cash = _d(prev.cash)
        else:
            begin_cash = _d(batch.begin_cash)

        cash = begin_cash
        realized = D("0")
        total_fees = D("0")

        # ---- 迟到成交（适配器之外补记的成交；语义上不属于适配器快照） ----
        # 先补记迟到成交，并在订单过账台账中留下 posted 记录，
        # 使后续批次的增量计算能感知这些数量，永不重复。
        late_qty_by_order: Dict[str, int] = {}
        synthetic_order_ids: List[str] = []
        for adj in late_adjustments:
            payload = json.loads(adj.payload_json)
            rates_batch = (
                mapper.get_batch_by_id(adj.sealed_batch_id)
                if adj.sealed_batch_id
                else None
            )
            quantity = int(payload["quantity"])
            fee, fee_rows = self._compute_fees(
                side=payload["side"],
                amount=_d(payload["filled_price"]) * quantity,
                rate_source=rates_batch,
                fallback_adapter=adapter,
            )
            delta, gain = self._apply_fill(
                positions,
                side=payload["side"],
                stock_code=payload["stock_code"],
                stock_name=payload.get("stock_name", payload["stock_code"]),
                quantity=quantity,
                price=_d(payload["filled_price"]),
            )
            cash += delta - fee
            realized += gain
            total_fees += fee
            adj.status = "applied"
            adj.applied_batch_id = batch.id
            for fee_type, amount in fee_rows:
                mapper.upsert_fee(
                    batch.id, f"LATE:{adj.id}", fee_type, str(_money(amount))
                )

            order_id = payload.get("order_id") or f"LATEADJ:{adj.id}"
            late_qty_by_order[order_id] = (
                late_qty_by_order.get(order_id, 0) + quantity
            )
            existing_order_row = mapper.get_order_row(batch.id, order_id)
            if existing_order_row is None:
                # 合成订单台账行：真实 order_id 缺失时用 LATEADJ 前缀，
                # 保证台账唯一且可审计
                mapper.upsert_order(
                    batch.id,
                    {
                        "order_id": order_id,
                        "stock_code": payload["stock_code"],
                        "side": payload["side"],
                        "order_type": None,
                        "quantity": quantity,
                        "filled_quantity": quantity,
                        "posted_quantity": quantity,
                        "late_quantity": quantity,
                        "filled_price": str(_d(payload["filled_price"])),
                        "commission_amount": None,
                        "status": OrderStatus.FILLED.value,
                        "created_at": None,
                        "updated_at": None,
                    },
                )
                synthetic_order_ids.append(order_id)
            else:
                existing_order_row.late_quantity = (
                    int(existing_order_row.late_quantity or 0) + quantity
                )
            # 若快照中已有同订单行，合并 posted 数量在下方统一循环处理

        # ---- 当日订单过账（只过账增量；迟到成交数量已先补记） ----
        order_rows = mapper.list_orders(batch.id)
        for row in order_rows:
            posted = int(row.posted_quantity or 0)
            if row.order_id in synthetic_order_ids:
                continue  # 迟到合成行已补记
            late_qty = late_qty_by_order.get(row.order_id, 0)
            qty = max(0, posted - late_qty)
            if late_qty:
                # 台账记录本批次过账量 = 迟到补记量 + 快照新增量
                row.posted_quantity = qty + late_qty
                row.late_quantity = late_qty
                if qty == 0:
                    # 快照增量全部来自迟到补记，不再重复入账
                    continue
            if qty <= 0 or row.filled_price is None:
                continue
            price = _d(row.filled_price)
            fee, fee_rows = self._compute_fees(
                side=row.side,
                amount=price * qty,
                rate_source=batch,
                fallback_adapter=adapter,
            )
            delta, gain = self._apply_fill(
                positions,
                side=row.side,
                stock_code=row.stock_code,
                stock_name=row.stock_code,
                quantity=qty,
                price=price,
            )
            cash += delta - fee
            realized += gain
            total_fees += fee
            for fee_type, amount in fee_rows:
                mapper.upsert_fee(
                    batch.id, row.order_id, fee_type, str(_money(amount))
                )

        # ---- 企业行动（幂等应用；续办时按登记效果确定性重放） ----
        ca_cash = D("0")
        for action in mapper.list_corporate_actions(batch.account_id, trade_date):
            link = mapper.get_settlement_action(batch.id, action.id)
            if link is not None and link.applied == 1:
                effect_cash = _d(link.effect_cash)
                effect_quantity = int(link.effect_quantity or 0)
                if action.action_type == "cash_dividend":
                    cash += effect_cash
                elif effect_quantity:
                    self._apply_quantity_effect(
                        positions, action.stock_code, effect_quantity
                    )
            else:
                effect_cash, effect_quantity = self._apply_corporate_action(
                    positions, action
                )
                if action.action_type == "cash_dividend":
                    cash += effect_cash
                mapper.mark_settlement_action(
                    batch.id,
                    action.id,
                    str(_money(effect_cash)),
                    effect_quantity,
                )
            if action.action_type == "cash_dividend":
                ca_cash += effect_cash

        # ---- 固定收盘价估值 ----
        position_payloads: List[Dict[str, Any]] = []
        market_value = D("0")
        unrealized = D("0")
        missing_quotes = sorted(
            code
            for code, pos in positions.items()
            if pos["quantity"] > 0 and code not in quotes
        )
        if missing_quotes:
            raise SettlementException(
                f"交易日 {trade_date} 缺少收盘价: {', '.join(missing_quotes)}，"
                f"请通过 closing_prices 补齐后续办",
                status_code=422,
                details={
                    "missing_quotes": missing_quotes,
                    "batch_no": batch.batch_no,
                },
            )
        for code in sorted(positions.keys()):
            pos = positions[code]
            qty = pos["quantity"]
            if qty <= 0:
                continue
            close = quotes[code]
            mv = close * qty
            pnl = (close - pos["avg_cost"]) * qty
            market_value += mv
            unrealized += pnl
            position_payloads.append(
                {
                    "stock_code": code,
                    "stock_name": pos.get("stock_name", code),
                    "quantity": qty,
                    "available_quantity": qty,
                    "avg_cost": str(_money(pos["avg_cost"])),
                    "close_price": str(_money(close)),
                    "market_value": str(_money(mv)),
                    "unrealized_pnl": str(_money(pnl)),
                }
            )
        mapper.replace_positions(batch.id, position_payloads)

        cash = _money(cash)
        total_assets = _money(cash + market_value)
        frozen = _d(getattr(adapter.get_account() if adapter else None, "frozen_cash", 0) or 0)

        # ---- 定格批次 ----
        batch.cash = str(cash)
        batch.begin_cash = str(_money(begin_cash))
        batch.market_value = str(_money(market_value))
        batch.total_assets = str(total_assets)
        batch.unrealized_pnl = str(_money(unrealized))
        batch.realized_pnl = str(_money(realized))
        batch.total_fees = str(_money(total_fees))
        batch.frozen_cash = str(_money(frozen))
        batch.next_available_cash = str(_money(cash - frozen))
        batch.next_trading_date = next_trading_date
        batch.unfinished_order_count = len(open_orders)
        batch.cancelled_order_ids = json.dumps(
            [
                o["order_id"]
                for o in order_rows
                if o.status in _CLOSED_STATUSES
            ],
            ensure_ascii=False,
        )
        batch.last_order_updated_at = last_updated
        batch.error_message = None
        batch.status = "sealed"
        batch.sealed_at = datetime.now()
        batch.snapshot_hash = self._snapshot_hash(
            mapper=mapper,
            batch=batch,
            quotes=quotes,
            next_trading_date=next_trading_date,
        )
        return self._summary(mapper, batch, include_orders=True)

    # ------------------------------------------------------------------
    # 账本原语
    # ------------------------------------------------------------------

    @staticmethod
    def _apply_fill(
        positions: Dict[str, Dict[str, Any]],
        side: str,
        stock_code: str,
        stock_name: str,
        quantity: int,
        price: D,
    ) -> Tuple[D, D]:
        """把一笔成交写入账本，返回 (含费前资金变动, 已实现盈亏)。"""
        if quantity <= 0:
            return D("0"), D("0")
        amount = price * quantity
        is_buy = side == OrderSide.BUY.value
        pos = positions.get(stock_code)

        if is_buy:
            if pos is None:
                positions[stock_code] = {
                    "stock_name": stock_name,
                    "quantity": quantity,
                    "avg_cost": price,
                }
            else:
                total_cost = pos["avg_cost"] * pos["quantity"] + amount
                new_qty = pos["quantity"] + quantity
                pos["avg_cost"] = total_cost / new_qty
                pos["quantity"] = new_qty
            return -amount, D("0")

        # 卖出
        if pos is None:
            # 账本中没有持仓（理论上不该发生），保守按裸卖空拒绝式记账
            raise SettlementException(
                f"订单成交时账本无持仓: {stock_code} {quantity}股，无法封账"
            )
        gain = (price - pos["avg_cost"]) * min(quantity, pos["quantity"])
        pos["quantity"] -= quantity
        if pos["quantity"] <= 0:
            pos["quantity"] = 0
        return amount, gain

    def _compute_fees(
        self,
        side: str,
        amount: D,
        rate_source: Any,
        fallback_adapter: Any,
    ) -> Tuple[D, List[Tuple[str, D]]]:
        """按批次费率快照计算单笔费用，返回 (总额, [(类型, 金额)])。"""
        commission_rate, min_commission, stamp_rate = self._rates_from(
            rate_source, fallback_adapter
        )
        commission = max(amount * commission_rate, min_commission)
        rows = [("commission", commission)]
        if side == OrderSide.SELL.value:
            stamp = amount * stamp_rate
            rows.append(("stamp_tax", stamp))
        total = sum((v for _, v in rows), D("0"))
        return total, rows

    def _rates_from(self, source: Any, adapter: Any) -> Tuple[D, D, D]:
        if source is not None and getattr(source, "commission_rate", None) is not None:
            return (
                _d(source.commission_rate),
                _d(source.min_commission),
                _d(source.stamp_tax_rate),
            )
        config = getattr(adapter, "config", None) or {}
        return (
            _d(config.get("commission_rate", "0.0003")),
            _d(config.get("min_commission", "5")),
            _d(config.get("stamp_tax_rate", "0.001")),
        )

    def _apply_corporate_action(
        self, positions: Dict[str, Dict[str, Any]], action: Any
    ) -> Tuple[D, int]:
        """首次应用企业行动，返回 (现金效果, 数量增量)。

        现金分红：现金 += 每股派现 * 持仓，持仓不变；
        送股：数量 += floor(数量 * 比例)，成本价等比例摊薄；
        拆股：数量 = floor(数量 * 比例)（ratio 表示新股/旧股），成本摊薄。
        """
        pos = positions.get(action.stock_code)
        qty = pos["quantity"] if pos else 0
        if action.action_type == "cash_dividend":
            return _d(action.cash_per_share) * qty, 0

        if pos is None or qty <= 0:
            return D("0"), 0
        ratio = _d(action.ratio)
        if ratio <= 0:
            return D("0"), 0
        if action.action_type == "stock_dividend":
            new_qty = qty + int(qty * ratio)
        else:  # split
            new_qty = int(qty * ratio)
        if new_qty <= 0:
            return D("0"), 0
        if pos["avg_cost"] > 0:
            pos["avg_cost"] = pos["avg_cost"] * qty / new_qty
        delta_qty = new_qty - qty
        pos["quantity"] = new_qty
        return D("0"), delta_qty

    @staticmethod
    def _apply_quantity_effect(
        positions: Dict[str, Dict[str, Any]], stock_code: str, delta_qty: int
    ) -> None:
        """续办时按登记的数量增量确定性重放送股/拆股。"""
        pos = positions.get(stock_code)
        if pos is None or delta_qty == 0:
            return
        old_qty = pos["quantity"]
        new_qty = old_qty + delta_qty
        if new_qty > 0 and pos["avg_cost"] > 0:
            pos["avg_cost"] = pos["avg_cost"] * old_qty / new_qty
        pos["quantity"] = new_qty

    # ------------------------------------------------------------------
    # 快照步骤
    # ------------------------------------------------------------------

    def _new_batch(
        self,
        account_id: str,
        trade_date: str,
        adapter: Any,
        initial_cash: Optional[float],
    ) -> Any:
        from app.entities.settlement import SettlementBatch

        if initial_cash is not None:
            begin = _d(initial_cash)
        else:
            config = getattr(adapter, "config", None) or {}
            begin = _d(config.get("initial_cash", "1000000"))
        return SettlementBatch(
            batch_no=self._generate_batch_no(trade_date, account_id),
            account_id=account_id,
            trade_date=trade_date,
            status="pending",
            begin_cash=str(_money(begin)),
        )

    def _freeze_rate_snapshot(self, batch: Any, adapter: Any) -> None:
        if batch.commission_rate is not None:
            return  # 续办时费率快照不可变
        config = getattr(adapter, "config", None) or {}
        batch.commission_rate = str(config.get("commission_rate", "0.0003"))
        batch.min_commission = str(config.get("min_commission", "5"))
        batch.stamp_tax_rate = str(config.get("stamp_tax_rate", "0.001"))

    def _snapshot_orders(
        self,
        mapper: SettlementMapper,
        batch: Any,
        adapter: Any,
    ) -> Tuple[List[Dict[str, Any]], Optional[datetime]]:
        """定格订单快照并计算本批次增量成交量（与系统时钟无关）。

        - 首个批次：适配器内全部订单作为建账以来订单；
        - 后续批次：仅纳入新订单、有新增成交的订单、未完成订单；
        - posted_quantity = 当前成交量 - 更早批次累计过账量，
          跨日部分成交只过账增量，重复/续办不会重复入账。
        """
        account_id = batch.account_id
        trade_date = batch.trade_date
        orders: Sequence[Order] = adapter.get_orders()
        open_orders: List[Dict[str, Any]] = []
        last_updated: Optional[datetime] = None

        for order in orders:
            prior_posted, history_row = mapper.order_history_before(
                account_id, order.order_id, trade_date
            )
            current_filled = int(order.filled_quantity or 0)
            delta = max(0, current_filled - prior_posted)
            is_open = order.status.value in _OPEN_STATUSES
            if history_row is None and delta == 0 and not is_open:
                # 新出现的废单/拒单也留档
                pass
            elif history_row is not None and delta == 0 and not is_open:
                # 之前批次已完整过账且已终态：无需再进本批次
                continue

            if order.updated_at and (last_updated is None or order.updated_at > last_updated):
                last_updated = order.updated_at

            data = {
                "order_id": order.order_id,
                "stock_code": order.stock_code,
                "side": order.side.value,
                "order_type": order.order_type.value,
                "quantity": order.quantity,
                "filled_quantity": current_filled,
                "posted_quantity": delta,
                "filled_price": (
                    str(order.filled_price) if order.filled_price is not None else None
                ),
                "commission_amount": (
                    str(order.commission) if order.commission else None
                ),
                "status": order.status.value,
                "created_at": order.created_at,
                "updated_at": order.updated_at,
            }
            mapper.upsert_order(batch.id, data)
            if is_open:
                open_orders.append(
                    {
                        "order_id": order.order_id,
                        "stock_code": order.stock_code,
                        "side": order.side.value,
                        "status": order.status.value,
                        "quantity": order.quantity,
                        "filled_quantity": current_filled,
                    }
                )
        return open_orders, last_updated

    def _snapshot_quotes(
        self,
        mapper: SettlementMapper,
        batch: Any,
        adapter: Any,
        closing_prices: Dict[str, float],
    ) -> Dict[str, D]:
        """收集固定收盘价：显式价格优先，其余取适配器现价；已存快照不可变。"""
        quotes: Dict[str, D] = {}
        for row in mapper.list_quotes(batch.id):
            quotes[row.stock_code] = _d(row.close_price)

        for code, price in closing_prices.items():
            if code in quotes:
                if quotes[code] != _d(price):
                    raise SettlementException(
                        f"行情快照已固定，{code} 收盘价不可更改 "
                        f"(已封快照 {quotes[code]}，传入 {price})",
                        status_code=409,
                        details={"stock_code": code, "batch_no": batch.batch_no},
                    )
                continue
            mapper.upsert_quote(batch.id, code, str(_d(price)), "closing_input")
            quotes[code] = _d(price)

        # 适配器现价补充（仅在尚未有快照时写入）
        for code in self._adapter_position_codes(adapter):
            if code in quotes:
                continue
            quote = adapter.get_quote(code)
            if quote and quote.get("last_price") is not None:
                price = _d(quote["last_price"])
                mapper.upsert_quote(batch.id, code, str(price), "adapter_quote")
                quotes[code] = price
        return quotes

    @staticmethod
    def _adapter_position_codes(adapter: Any) -> List[str]:
        try:
            return [p.stock_code for p in adapter.get_positions()]
        except Exception:
            return []

    def _assert_quotes_match_snapshot(
        self,
        mapper: SettlementMapper,
        batch: Any,
        closing_prices: Dict[str, float],
    ) -> None:
        """已封账批次：拒绝与固定行情快照冲突的收盘价输入。"""
        if not closing_prices:
            return
        snapshotted = {
            row.stock_code: _d(row.close_price)
            for row in mapper.list_quotes(batch.id)
        }
        for code, price in closing_prices.items():
            if code in snapshotted and snapshotted[code] != _d(price):
                raise SettlementException(
                    f"批次 {batch.batch_no} 已封账，{code} 固定收盘价不可更改 "
                    f"(已封 {snapshotted[code]}，传入 {price})",
                    status_code=409,
                    details={
                        "stock_code": code,
                        "batch_no": batch.batch_no,
                    },
                )

    # ------------------------------------------------------------------
    # 封账后事件：迟到成交 / 重新估值 / 企业行动登记
    # ------------------------------------------------------------------

    def register_corporate_action(
        self,
        account_id: str,
        stock_code: str,
        action_type: str,
        ex_date: str,
        ratio: float = 0.0,
        cash_per_share: float = 0.0,
    ) -> Dict[str, Any]:
        """登记企业行动。ex_date 当天（或之后首个）批次消费。"""
        self._validate_date(ex_date)
        if action_type not in ("cash_dividend", "stock_dividend", "split"):
            raise SettlementException(f"不支持的企业行动类型: {action_type}")
        session = self._session()
        try:
            mapper = SettlementMapper(session)
            action = mapper.upsert_corporate_action(
                {
                    "account_id": account_id,
                    "stock_code": stock_code,
                    "action_type": action_type,
                    "ex_date": ex_date,
                    "ratio": str(ratio),
                    "cash_per_share": str(cash_per_share),
                    "status": "confirmed",
                }
            )
            session.commit()
            return {
                "id": action.id,
                "account_id": account_id,
                "stock_code": stock_code,
                "action_type": action_type,
                "ex_date": ex_date,
                "ratio": ratio,
                "cash_per_share": cash_per_share,
            }
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def record_late_fill(
        self,
        account_id: str,
        trade_date: str,
        order_id: str,
        stock_code: str,
        side: str,
        quantity: int,
        filled_price: float,
        stock_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """登记迟到成交。

        目标日期未封账：报错，提示该成交应正常进入当日结算；
        目标日期已封账：只登记不改账，由下一交易日批次幂等消费。
        同一事件重复登记（相同载荷哈希）直接返回原记录。
        """
        self._validate_date(trade_date)
        side = self._normalize_side(side)
        if quantity <= 0:
            raise SettlementException("迟到成交数量必须大于 0")
        payload = {
            "order_id": order_id,
            "stock_code": stock_code,
            "stock_name": stock_name or stock_code,
            "side": side,
            "quantity": int(quantity),
            "filled_price": str(_d(filled_price)),
        }
        payload_hash = _payload_hash(payload)

        session = self._session()
        try:
            mapper = SettlementMapper(session)
            existing = mapper.find_adjustment(account_id, "late_fill", payload_hash)
            if existing is not None:
                return self._adjustment_dict(existing, duplicated=True)

            batch = mapper.get_batch(account_id, trade_date)
            if batch is None:
                raise SettlementException(
                    f"交易日 {trade_date} 尚未结算，该成交应在日终结算中正常入账，"
                    f"无需登记迟到成交",
                    status_code=422,
                )
            if batch.status != "sealed":
                raise SettlementException(
                    f"交易日 {trade_date} 批次 {batch.batch_no} 尚未封账 "
                    f"(状态 {batch.status})，请先完成或续办当日结算",
                    status_code=422,
                    details={"batch_no": batch.batch_no, "status": batch.status},
                )

            adjustment = mapper.add_adjustment(
                PostSealAdjustment(
                    account_id=account_id,
                    sealed_batch_id=batch.id,
                    trade_date=trade_date,
                    order_id=order_id,
                    adjustment_type="late_fill",
                    status="pending",
                    payload_json=json.dumps(payload, ensure_ascii=False, default=str),
                    payload_hash=payload_hash,
                )
            )
            session.commit()
            logger.info(
                "Late fill registered against sealed batch %s: %s %s %s@%s",
                batch.batch_no,
                order_id,
                side,
                quantity,
                filled_price,
            )
            return self._adjustment_dict(adjustment, duplicated=False)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def register_restatement(
        self,
        account_id: str,
        trade_date: str,
        reason: str,
        corrected_quotes: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """登记重新估值请求。

        已封账数字不可变：重新估值永不自动入账，只做 rejected 留痕，
        供审计与人工处理。相同载荷重复登记返回原记录。
        """
        self._validate_date(trade_date)
        payload = {
            "reason": reason,
            "corrected_quotes": {
                code: str(_d(price)) for code, price in (corrected_quotes or {}).items()
            },
        }
        payload_hash = _payload_hash(payload)
        session = self._session()
        try:
            mapper = SettlementMapper(session)
            existing = mapper.find_adjustment(account_id, "restatement", payload_hash)
            if existing is not None:
                return self._adjustment_dict(existing, duplicated=True)

            batch = mapper.get_batch(account_id, trade_date)
            if batch is None or batch.status != "sealed":
                raise SettlementException(
                    f"交易日 {trade_date} 没有已封账批次，无需登记重新估值",
                    status_code=422,
                )

            adjustment = mapper.add_adjustment(
                PostSealAdjustment(
                    account_id=account_id,
                    sealed_batch_id=batch.id,
                    trade_date=trade_date,
                    order_id=None,
                    adjustment_type="restatement",
                    status="rejected",
                    payload_json=json.dumps(payload, ensure_ascii=False, default=str),
                    payload_hash=payload_hash,
                )
            )
            session.commit()
            logger.info(
                "Restatement registered and rejected (sealed books are immutable) "
                "for batch %s: %s",
                batch.batch_no,
                reason,
            )
            result = self._adjustment_dict(adjustment, duplicated=False)
            result["note"] = "已封账数字不可变，重新估值仅留痕，未调整任何批次"
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ------------------------------------------------------------------
    # 查询：已封账 / 临时估值 / 历史
    # ------------------------------------------------------------------

    def get_settlement(
        self,
        account_id: str,
        trade_date: str,
        include_orders: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """获取某交易日批次（任意状态）。已封账返回封账数字。"""
        session = self._session()
        try:
            mapper = SettlementMapper(session)
            batch = mapper.get_batch(account_id, trade_date)
            if batch is None:
                return None
            return self._summary(mapper, batch, include_orders=include_orders)
        finally:
            session.close()

    def get_valuation(
        self,
        adapter: Any,
        trade_date: Optional[str] = None,
        quotes: Optional[Dict[str, float]] = None,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """临时估值：基于适配器实时状态计算，明确标注非封账数字。"""
        account_id = account_id or self._account_id(adapter)
        account = adapter.get_account()
        if account is None:
            raise SettlementException("无法获取账户信息，请检查交易连接")

        positions = adapter.get_positions()
        market_value = D("0")
        unrealized = D("0")
        position_dicts: List[Dict[str, Any]] = []
        for pos in positions:
            price = _d(quotes[pos.stock_code]) if quotes and pos.stock_code in quotes else pos.current_price
            mv = price * pos.quantity
            pnl = (price - pos.avg_cost) * pos.quantity
            market_value += mv
            unrealized += pnl
            position_dicts.append(
                {
                    "stock_code": pos.stock_code,
                    "stock_name": pos.stock_name,
                    "quantity": pos.quantity,
                    "available_quantity": pos.available_quantity,
                    "avg_cost": _float(pos.avg_cost),
                    "current_price": _float(price),
                    "market_value": _float(mv),
                    "unrealized_pnl": _float(pnl),
                    "price_source": (
                        "valuation_input"
                        if quotes and pos.stock_code in quotes
                        else "live_adapter"
                    ),
                }
            )

        cash = _d(account.available_cash)
        return {
            "source": "provisional",
            "values_source": "provisional:live_adapter",
            "account_id": account_id,
            "trade_date": trade_date or datetime.now().date().isoformat(),
            "as_of": datetime.now().isoformat(),
            "sealed": False,
            "cash": _float(cash),
            "frozen_cash": _float(account.frozen_cash),
            "market_value": _float(market_value),
            "total_assets": _float(cash + market_value),
            "unrealized_pnl": _float(unrealized),
            "next_available_cash": _float(cash - _d(account.frozen_cash)),
            "positions": position_dicts,
            "note": "临时估值，重启或行情变化后可能不同；日终封账数字请查询 settlement 接口",
        }

    def get_account_view(
        self,
        adapter: Any,
        trade_date: str,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """权益视图：优先返回已封账数字，否则返回临时估值，并给出来源。"""
        account_id = account_id or self._account_id(adapter)
        sealed = self.get_settlement(account_id, trade_date)
        if sealed is not None and sealed["status"] == "sealed":
            sealed["view"] = "settlement"
            return sealed
        valuation = self.get_valuation(adapter, trade_date=trade_date, account_id=account_id)
        valuation["view"] = "valuation"
        if sealed is not None:
            valuation["in_progress_batch_no"] = sealed["batch_no"]
            valuation["in_progress_status"] = sealed["status"]
        return valuation

    def list_settlements(
        self,
        account_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """历史批次查询。"""
        session = self._session()
        try:
            mapper = SettlementMapper(session)
            return [
                self._summary(mapper, batch, include_positions=False)
                for batch in mapper.list_batches(account_id, status, limit)
            ]
        finally:
            session.close()

    def list_adjustments(
        self,
        account_id: str,
        trade_date: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """查询封账后调整登记（迟到成交/重新估值）。"""
        session = self._session()
        try:
            mapper = SettlementMapper(session)
            rows = mapper.list_adjustments(account_id=account_id, status=status)
            result = []
            for row in rows:
                if trade_date and row.trade_date != trade_date:
                    continue
                result.append(self._adjustment_dict(row, duplicated=False))
            return result
        finally:
            session.close()

    # ------------------------------------------------------------------
    # 汇总与工具
    # ------------------------------------------------------------------

    def _summary(
        self,
        mapper: SettlementMapper,
        batch: Any,
        include_orders: bool = False,
        include_positions: bool = True,
    ) -> Dict[str, Any]:
        prev = (
            mapper.get_batch_by_id(batch.prev_batch_id)
            if batch.prev_batch_id
            else mapper.latest_sealed_before(batch.account_id, batch.trade_date)
        )
        positions = []
        if include_positions and batch.status == "sealed":
            positions = [
                {
                    "stock_code": row.stock_code,
                    "stock_name": row.stock_name,
                    "quantity": row.quantity,
                    "available_quantity": row.available_quantity,
                    "avg_cost": _float(row.avg_cost),
                    "close_price": _float(row.close_price),
                    "market_value": _float(row.market_value),
                    "unrealized_pnl": _float(row.unrealized_pnl),
                    "price_source": "settlement_close",
                }
                for row in mapper.list_positions(batch.id)
            ]

        quotes = [
            {
                "stock_code": row.stock_code,
                "close_price": _float(row.close_price),
                "source": row.source,
            }
            for row in mapper.list_quotes(batch.id)
        ]

        unfinished: List[Dict[str, Any]] = []
        cancelled: List[str] = []
        if batch.status == "sealed":
            cancelled = json.loads(batch.cancelled_order_ids or "[]")
            for row in mapper.list_orders(batch.id):
                if row.status in _OPEN_STATUSES:
                    unfinished.append(
                        {
                            "order_id": row.order_id,
                            "stock_code": row.stock_code,
                            "side": row.side,
                            "status": row.status,
                            "quantity": row.quantity,
                            "filled_quantity": row.filled_quantity,
                        }
                    )

        ca_effects = []
        for link in mapper.list_settlement_actions(batch.id):
            action = mapper.get_corporate_action(link.corporate_action_id)
            ca_effects.append(
                {
                    "corporate_action_id": link.corporate_action_id,
                    "stock_code": action.stock_code if action else None,
                    "action_type": action.action_type if action else None,
                    "effect_cash": _float(link.effect_cash),
                    "effect_quantity": link.effect_quantity,
                }
            )

        adjustments = mapper.list_adjustments(sealed_batch_id=batch.id)

        sealed = batch.status == "sealed"
        summary: Dict[str, Any] = {
            "source": "sealed_batch" if sealed else f"batch:{batch.status}",
            "values_source": "settlement:sealed_batch" if sealed else "settlement:in_progress",
            "sealed": sealed,
            "batch_no": batch.batch_no,
            "batch_id": batch.id,
            "account_id": batch.account_id,
            "trade_date": batch.trade_date,
            "status": batch.status,
            "snapshot_hash": batch.snapshot_hash,
            "prev_batch_no": prev.batch_no if prev is not None else None,
            "next_trading_date": batch.next_trading_date,
            "created_at": batch.created_at.isoformat() if batch.created_at else None,
            "sealed_at": batch.sealed_at.isoformat() if batch.sealed_at else None,
            "error_message": batch.error_message,
            "quotes": quotes,
            "corporate_actions": ca_effects,
            "unfinished_orders": unfinished,
            "unfinished_order_count": batch.unfinished_order_count if sealed else len(unfinished),
            "cancelled_order_ids": cancelled,
            "post_seal_adjustments": [
                self._adjustment_dict(row, duplicated=False) for row in adjustments
            ],
            "positions": positions,
        }

        if sealed:
            cash_flow = self._reconstruct_cash_flow(mapper, batch)
            summary.update(
                {
                    "cash": _float(batch.cash),
                    "begin_cash": _float(batch.begin_cash),
                    "frozen_cash": _float(batch.frozen_cash),
                    "market_value": _float(batch.market_value),
                    "total_assets": _float(batch.total_assets),
                    "unrealized_pnl": _float(batch.unrealized_pnl),
                    "realized_pnl": _float(batch.realized_pnl),
                    "total_fees": _float(batch.total_fees),
                    "next_available_cash": _float(batch.next_available_cash),
                    "cash_flow": cash_flow,
                }
            )

        if include_orders:
            summary["orders"] = [
                {
                    "order_id": row.order_id,
                    "stock_code": row.stock_code,
                    "side": row.side,
                    "order_type": row.order_type,
                    "quantity": row.quantity,
                    "filled_quantity": row.filled_quantity,
                    "posted_quantity": row.posted_quantity,
                    "filled_price": _float(row.filled_price) if row.filled_price else None,
                    "status": row.status,
                }
                for row in mapper.list_orders(batch.id)
            ]
        return summary

    def _reconstruct_cash_flow(self, mapper: SettlementMapper, batch: Any) -> Dict[str, Any]:
        """从费用明细与订单台账重建资金流水（续办后仍可查）。"""
        trade_cash = D("0")
        late_trade_cash = D("0")
        for row in mapper.list_orders(batch.id):
            posted = int(row.posted_quantity or 0)
            late_qty = int(row.late_quantity or 0)
            if row.filled_price is None:
                continue
            today_qty = max(0, posted - late_qty)
            price = _d(row.filled_price)
            spec = 1 if row.side == OrderSide.SELL.value else -1
            trade_cash += price * today_qty * spec
            late_trade_cash += price * late_qty * spec
        today_fees = D("0")
        late_fees = D("0")
        for fee in mapper.list_fees(batch.id):
            if fee.order_id.startswith("LATE:"):
                late_fees += _d(fee.amount)
            else:
                today_fees += _d(fee.amount)
        ca_cash = sum(
            (_d(link.effect_cash) for link in mapper.list_settlement_actions(batch.id)),
            D("0"),
        )
        return {
            "begin_cash": _float(batch.begin_cash),
            "trade_cash_net": _float(trade_cash),
            "today_fees": _float(today_fees),
            "late_fill_cash_net": _float(late_trade_cash),
            "late_fill_fees": _float(late_fees),
            "corporate_action_cash": _float(ca_cash),
            "end_cash": _float(batch.cash),
        }

    def _snapshot_hash(
        self,
        mapper: SettlementMapper,
        batch: Any,
        quotes: Dict[str, D],
        next_trading_date: str,
    ) -> str:
        orders = [
            {
                "order_id": row.order_id,
                "side": row.side,
                "qty": row.posted_quantity,
                "price": row.filled_price,
                "status": row.status,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            }
            for row in mapper.list_orders(batch.id)
        ]
        cas = [
            {
                "action": link.corporate_action_id,
                "cash": link.effect_cash,
                "qty": link.effect_quantity,
            }
            for link in sorted(
                mapper.list_settlement_actions(batch.id),
                key=lambda x: x.corporate_action_id,
            )
        ]
        payload = {
            "account_id": batch.account_id,
            "trade_date": batch.trade_date,
            "rates": {
                "commission_rate": batch.commission_rate,
                "min_commission": batch.min_commission,
                "stamp_tax_rate": batch.stamp_tax_rate,
            },
            "quotes": {k: str(v) for k, v in sorted(quotes.items())},
            "orders": orders,
            "corporate_actions": cas,
            "begin_cash": batch.begin_cash,
            "next_trading_date": next_trading_date,
        }
        return _payload_hash(payload)

    def _adjustment_dict(self, row: PostSealAdjustment, duplicated: bool) -> Dict[str, Any]:
        return {
            "id": row.id,
            "account_id": row.account_id,
            "trade_date": row.trade_date,
            "sealed_batch_id": row.sealed_batch_id,
            "order_id": row.order_id,
            "adjustment_type": row.adjustment_type,
            "status": row.status,
            "applied_batch_id": row.applied_batch_id,
            "payload": json.loads(row.payload_json),
            "payload_hash": row.payload_hash,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "duplicated": duplicated,
        }

    # ------------------------------------------------------------------
    # 小工具
    # ------------------------------------------------------------------

    @staticmethod
    def _account_id(adapter: Any) -> str:
        account = adapter.get_account()
        if account is not None and getattr(account, "account_id", None):
            return account.account_id
        return "SIM_DEFAULT"

    @staticmethod
    def _normalize_side(side: str) -> str:
        if isinstance(side, OrderSide):
            return side.value
        side = str(side).lower()
        if side not in (OrderSide.BUY.value, OrderSide.SELL.value):
            raise SettlementException(f"非法买卖方向: {side}")
        return side

    @staticmethod
    def _validate_date(value: str) -> None:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except (ValueError, TypeError):
            raise SettlementException(f"日期格式必须为 YYYY-MM-DD: {value}")

    @staticmethod
    def _parse_date(value: str):
        return datetime.strptime(value, "%Y-%m-%d").date()

    @staticmethod
    def _default_next_date(trade_date: str) -> str:
        return (datetime.strptime(trade_date, "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

    @staticmethod
    def _generate_batch_no(trade_date: str, account_id: str) -> str:
        import uuid

        compact = trade_date.replace("-", "")
        suffix = hashlib.sha1(account_id.encode("utf-8")).hexdigest()[:6]
        return f"STL{compact}-{suffix}-{uuid.uuid4().hex[:8]}"
