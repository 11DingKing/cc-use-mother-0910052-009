"""可恢复的日终结算服务。

设计要点
========

1. **唯一批次**：同一账户同一交易日只有一条批次记录（数据库唯一约束），
   批次号 ``STL_{账户}_{交易日}`` 可复现。批次内保存行情、订单、费用、
   企业行动四类固定快照与输入哈希，封账后永不改变。

2. **断点续办**：批次带 ``phase`` 阶段标记；进程崩溃/重复触发时，
   已存在的 running/failed 批次会被续办而不是新建。分类账流水按全局
   幂等键写入，持仓与结转订单按批次整批替换，任何阶段重做都安全。

3. **确定性重放**：封账数字不信任查询时的临时计算，而是从上一封账批次
   （首日则由当前持仓/资金与当日成交反推期首日）起，按固定订单快照
   逐笔重放，再叠加企业行动、按固定收盘价估值，结果可复现。

4. **迟到成交**：封账后到达的成交只入 ``late_trades`` 挂起表，绝不重开
   已封账批次，在下一交易日结算时以 ``late_trade`` 流水补入一次。

5. **临时估值与封账分离**：盘中/盘后查询只给 ``provisional`` 临时估值
   （记录估值审计日志，绝不产生流水）；一旦封账，查询一律返回
   ``sealed`` 权威数字并附批次号与来源。
"""

import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional

from app.middleware.exception_handler import AppException
from app.services.settlement_store import SettlementStore, dumps
from app.trading.base import OrderStatus

logger = logging.getLogger(__name__)

# 订单状态分组
FILLED_STATUSES = {OrderStatus.FILLED.value}
PARTIAL_STATUSES = {OrderStatus.PARTIAL_FILLED.value}
OPEN_STATUSES = {
    OrderStatus.PENDING.value,
    OrderStatus.SUBMITTED.value,
    OrderStatus.PARTIAL_FILLED.value,
}

CENT = Decimal("0.01")
ZERO = Decimal("0")


class SettlementException(AppException):
    """日终结算业务异常。"""

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


def _D(value: Any) -> Decimal:
    """宽松地转 Decimal（None 视为 0）。"""
    if value is None:
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


class SettlementService:
    """日终结算编排服务（无状态，可被多个控制器共享）。"""

    def __init__(self, store: Optional[SettlementStore] = None):
        self.store = store or SettlementStore()

    # ==================================================================
    # 对外主流程：执行/续办日终结算
    # ==================================================================

    def run_settlement(
        self,
        adapter: Any,
        trade_date: str,
        quotes: Optional[Dict[str, Any]] = None,
        corporate_actions: Optional[List[Dict[str, Any]]] = None,
        fees: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """执行指定交易日的日终结算；重复触发安全。

        - 已封账：直接返回封账结果（``reused=True``），不重复入账；
        - running/failed：从断点续办；
        - 不存在：按固定快照创建唯一批次并执行。
        """
        account = adapter.get_account()
        if account is None:
            raise SettlementException("无法获取账户信息，请检查交易连接")
        account_id = account.account_id
        self._validate_trade_date(trade_date)
        corporate_actions = corporate_actions or []

        # 1) 固定输入快照
        snapshots = self._capture_snapshots(
            adapter, quotes or {}, corporate_actions, fees
        )
        input_hash = hashlib.sha256(
            dumps(snapshots).encode("utf-8")
        ).hexdigest()

        # 2) 定位唯一批次（数据库唯一约束兜底并发）
        batch = self.store.get_batch(account_id, trade_date)
        if batch is not None and batch.status == "sealed":
            # 修复“已封账含迟到成交但仍挂起”的陈旧状态（封账标记窗口崩溃）
            self.store.reconcile_applied_late_trades(batch.id, batch.batch_no)
            result = self._build_result(batch, reused=True)
            result["source"] = "sealed"
            result["input_conflict"] = batch.input_hash != input_hash
            if batch.input_hash != input_hash:
                logger.warning(
                    "交易日 %s 已封账但输入快照与本次不一致，"
                    "仍以已封账数字为准（sealed=%s current=%s）",
                    trade_date,
                    batch.input_hash[:12],
                    input_hash[:12],
                )
            return result

        if batch is None:
            # 不允许补做早于最近封账日的结算：期首日只能从最近封账批次结转，
            # 乱序补做会产生错误数字（重复触发/续办走已有批次路径，不受影响）。
            latest = self.store.latest_sealed_batch(account_id)
            if latest is not None and latest.trade_date > trade_date:
                raise SettlementException(
                    f"已存在交易日 {latest.trade_date} 的封账批次，"
                    f"不能补做更早的 {trade_date} 结算，请按交易日顺序执行；"
                    "如需修正历史，请由管理员走受控重封账流程",
                    status_code=409,
                    details={
                        "latest_sealed_date": latest.trade_date,
                        "requested_date": trade_date,
                    },
                )
            prev = self.store.previous_sealed_batch(account_id, trade_date)
            beginning = self._compute_beginning(
                prev, account, adapter, snapshots, corporate_actions
            )
            try:
                batch = self.store.create_batch(
                    account_id=account_id,
                    trade_date=trade_date,
                    beginning_cash=beginning["cash"],
                    beginning_equity=beginning["equity"],
                    beginning_positions=beginning["positions"],
                    quotes_snapshot=snapshots["quotes"],
                    orders_snapshot=snapshots["orders"],
                    pending_snapshot=snapshots["pending_orders"],
                    fees_snapshot=snapshots["fees"],
                    corporate_actions_snapshot=snapshots["corporate_actions"],
                    input_hash=input_hash,
                )
            except Exception as e:  # 并发下唯一约束冲突 -> 续办另一条
                logger.info("批次已存在，转入续办: %s", e)
                batch = self.store.get_batch(account_id, trade_date)
                if batch is None:
                    # 极端并发：对方创建后回滚/删除，重试一次创建
                    batch = self.store.create_batch(
                        account_id=account_id,
                        trade_date=trade_date,
                        beginning_cash=beginning["cash"],
                        beginning_equity=beginning["equity"],
                        beginning_positions=beginning["positions"],
                        quotes_snapshot=snapshots["quotes"],
                        orders_snapshot=snapshots["orders"],
                        pending_snapshot=snapshots["pending_orders"],
                        fees_snapshot=snapshots["fees"],
                        corporate_actions_snapshot=snapshots["corporate_actions"],
                        input_hash=input_hash,
                    )
                elif batch.status == "sealed":
                    result = self._build_result(batch, reused=True)
                    result["source"] = "sealed"
                    result["input_conflict"] = batch.input_hash != input_hash
                    return result
        else:
            self.store.touch_attempt(batch.id)
            logger.info(
                "续办结算批次 %s（第 %s 次，阶段 %s）",
                batch.batch_no,
                batch.attempts,
                batch.phase,
            )

        # 3) 分阶段执行；任何异常落 failed 并外抛，下一次触发可续办
        try:
            self._execute_batch(batch, snapshots)
        except SettlementException:
            self.store.mark_failed(batch.id, self._current_error())
            raise
        except Exception as e:
            logger.exception("结算批次 %s 执行异常", batch.batch_no)
            self.store.mark_failed(batch.id, str(e))
            raise SettlementException(
                f"日终结算执行失败，批次已保留可续办: {e}",
                status_code=500,
                details={"batch_no": batch.batch_no},
            )

        sealed = self.store.get_batch_by_no(batch.batch_no)
        result = self._build_result(sealed, reused=False)
        result["source"] = "sealed"
        return result

    # ==================================================================
    # 查询：临时估值 vs 已封账
    # ==================================================================

    def get_account_view(
        self,
        adapter: Any,
        trade_date: Optional[str] = None,
        quotes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """账户视图。

        有已封账批次 -> 返回 ``sealed`` 权威数字（附批次号与来源）；
        否则 -> 按当前行情实时计算 ``provisional`` 临时估值并写审计日志，
        临时估值永远不产生分类账流水。
        """
        account = adapter.get_account()
        if account is None:
            raise SettlementException("无法获取账户信息，请检查交易连接")
        trade_date = trade_date or datetime.now().strftime("%Y-%m-%d")

        sealed = self.store.get_batch(account.account_id, trade_date)
        if sealed is not None and sealed.status == "sealed":
            view = self._build_result(sealed, reused=True)
            view["source"] = "sealed"
            return view

        return self.provisional_valuation(adapter, trade_date, quotes)

    def provisional_valuation(
        self,
        adapter: Any,
        trade_date: Optional[str] = None,
        quotes: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """只做临时估值：显式区分口径，绝不入账、绝不封账。"""
        account = adapter.get_account()
        if account is None:
            raise SettlementException("无法获取账户信息，请检查交易连接")
        trade_date = trade_date or datetime.now().strftime("%Y-%m-%d")

        quote_basis: Dict[str, Decimal] = {}
        market_value = ZERO
        unrealized_total = ZERO
        position_rows = []
        for pos in adapter.get_positions():
            close = self._resolve_provisional_quote(
                adapter, pos.stock_code, quotes or {}
            )
            quote_basis[pos.stock_code] = close
            mv = _money(close * pos.quantity)
            upl = _money((close - pos.avg_cost) * pos.quantity)
            market_value += mv
            unrealized_total += upl
            position_rows.append(
                {
                    "stock_code": pos.stock_code,
                    "stock_name": pos.stock_name,
                    "quantity": pos.quantity,
                    "available_quantity": pos.available_quantity,
                    "avg_cost": float(pos.avg_cost),
                    "price": float(close),
                    "market_value": float(mv),
                    "unrealized_pnl": float(upl),
                }
            )

        cash = _money(account.available_cash)
        total = _money(cash + market_value)
        basis = {code: str(p) for code, p in quote_basis.items()}
        basis_hash = hashlib.sha256(dumps(basis).encode("utf-8")).hexdigest()

        valuation = {
            "account_id": account.account_id,
            "trade_date": trade_date,
            "source": "provisional",
            "valuation_basis": "live_quotes",
            "basis_hash": basis_hash,
            "cash": float(cash),
            "market_value": float(_money(market_value)),
            "total_assets": float(total),
            "unrealized_pnl": float(_money(unrealized_total)),
            "positions": position_rows,
            "sealed": False,
            "note": "临时估值，以日终封账数字为准",
        }

        # 审计留痕（临时口径每次可重复记录，但与分类账完全隔离）
        try:
            self.store.add_valuation_log(
                {
                    "account_id": account.account_id,
                    "trade_date": trade_date,
                    "quote_basis": basis,
                    "basis_hash": basis_hash,
                    "cash": cash,
                    "market_value": _money(market_value),
                    "total_assets": total,
                    "unrealized_pnl": _money(unrealized_total),
                }
            )
        except Exception as e:  # 审计失败不影响查询
            logger.warning("临时估值审计日志写入失败: %s", e)

        return valuation

    # ==================================================================
    # 历史查询
    # ==================================================================

    def get_settlement(self, account_id: str, trade_date: str) -> Optional[Dict[str, Any]]:
        batch = self.store.get_batch(account_id, trade_date)
        if batch is None:
            return None
        result = self._build_result(batch, reused=batch.status == "sealed")
        result["source"] = batch.status
        return result

    def list_settlements(
        self,
        account_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        rows = self.store.list_batches(account_id, start_date, end_date, limit)
        return [self._batch_summary(r) for r in rows]

    def get_settlement_positions(
        self, account_id: str, trade_date: str
    ) -> List[Dict[str, Any]]:
        batch = self.store.get_batch(account_id, trade_date)
        if batch is None:
            raise SettlementException(
                f"交易日 {trade_date} 不存在结算批次", status_code=404
            )
        return [self._position_dict(p) for p in self.store.list_positions(batch.id)]

    def get_ledger(self, account_id: str, trade_date: str) -> List[Dict[str, Any]]:
        batch = self.store.get_batch(account_id, trade_date)
        if batch is None:
            raise SettlementException(
                f"交易日 {trade_date} 不存在结算批次", status_code=404
            )
        return [self._ledger_dict(e) for e in self.store.list_ledger_entries(batch.id)]

    # ==================================================================
    # 迟到成交与跨日续办
    # ==================================================================

    def record_late_trade(self, trade: Dict[str, Any]) -> Dict[str, Any]:
        """登记封账后才到达的成交。

        已封账批次绝不重开：迟到成交挂起，在下一交易日结算时恰好补入一次。
        同一订单号重复上报安全。
        """
        required = (
            "account_id",
            "order_id",
            "intended_date",
            "stock_code",
            "side",
            "quantity",
            "price",
        )
        missing = [k for k in required if trade.get(k) is None]
        if missing:
            raise SettlementException(f"迟到成交缺少字段: {', '.join(missing)}")
        self._validate_trade_date(trade["intended_date"])
        if str(trade["side"]) not in ("buy", "sell"):
            raise SettlementException(
                "迟到成交方向必须为 buy/sell",
                details={"side": str(trade["side"])},
            )

        created = self.store.enqueue_late_trade(
            {
                "account_id": trade["account_id"],
                "order_id": str(trade["order_id"]),
                "intended_date": trade["intended_date"],
                "stock_code": trade["stock_code"],
                "side": str(trade["side"]),
                "quantity": int(trade["quantity"]),
                "price": trade["price"],
                "commission": trade.get("commission", "0"),
            }
        )
        intended_batch = self.store.get_batch(
            trade["account_id"], trade["intended_date"]
        )
        sealed = intended_batch is not None and intended_batch.status == "sealed"
        return {
            "order_id": str(trade["order_id"]),
            "status": "pending",
            "registered": created,
            "intended_date_sealed": sealed,
            "message": (
                "已挂起，将于下一交易日结算时补入，不重开已封账批次"
                if sealed
                else "已挂起，将在对应交易日结算时补入"
            ),
        }

    def get_carry_forward(self, account_id: str) -> Dict[str, Any]:
        """下一交易日开盘前的续办信息：结转未完成订单与待补迟到成交。"""
        latest = self.store.latest_sealed_batch(account_id)
        pending_orders: List[Dict[str, Any]] = []
        latest_date = None
        if latest is not None:
            latest_date = latest.trade_date
            pending_orders = [
                self._pending_dict(o)
                for o in self.store.list_pending_orders(latest.id)
            ]
        late = self.store.pending_late_trades(account_id)
        return {
            "account_id": account_id,
            "latest_sealed_date": latest_date,
            "pending_orders": pending_orders,
            "pending_late_trades": [
                {
                    "order_id": r.order_id,
                    "intended_date": r.intended_date,
                    "stock_code": r.stock_code,
                    "side": r.side,
                    "quantity": r.quantity,
                    "price": float(_D(r.price)),
                    "commission": float(_D(r.commission)),
                }
                for r in late
            ],
        }

    # ==================================================================
    # 内部：快照
    # ==================================================================

    def _capture_snapshots(
        self,
        adapter: Any,
        quotes: Dict[str, Any],
        corporate_actions: List[Dict[str, Any]],
        fees: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """把行情、订单、费用、未完成订单固定为可复现快照。"""
        orders = [o.to_dict() for o in adapter.get_orders()]

        fixed_quotes: Dict[str, str] = {}
        held_codes = {p.stock_code for p in adapter.get_positions()}
        # 显式传入的收盘价优先；持仓中缺价的取适配器当前行情固化
        for code, price in quotes.items():
            fixed_quotes[code] = str(_D(price))
        for code in held_codes:
            if code not in fixed_quotes:
                live = adapter.get_quote(code)
                if live and live.get("last_price") is not None:
                    price = _D(live["last_price"])
                    if price > 0:
                        fixed_quotes[code] = str(price)

        pending_orders = [
            {
                "order_id": o["order_id"],
                "stock_code": o["stock_code"],
                "side": o["side"],
                "quantity": o["quantity"],
                "filled_quantity": o["filled_quantity"],
                "remaining_quantity": max(0, o["quantity"] - o["filled_quantity"]),
                "price": o.get("price"),
                "status": o["status"],
                "created_at": o.get("created_at"),
            }
            for o in orders
            if o["status"] in OPEN_STATUSES
            and o["quantity"] - o["filled_quantity"] > 0
        ]

        fees_snapshot = {
            "commission_rate": str(_D(getattr(adapter, "commission_rate", ZERO))),
            "min_commission": str(_D(getattr(adapter, "min_commission", ZERO))),
            "stamp_tax_rate": str(_D(getattr(adapter, "stamp_tax_rate", ZERO))),
        }
        if fees:
            for k, v in fees.items():
                fees_snapshot[k] = str(_D(v))

        normalized_actions = []
        for i, action in enumerate(corporate_actions):
            self._validate_corporate_action(action, i)
            row = {
                k: (str(_D(v)) if k not in ("type", "stock_code") else v)
                for k, v in action.items()
            }
            normalized_actions.append(row)

        return {
            "quotes": fixed_quotes,
            "orders": orders,
            "pending_orders": pending_orders,
            "fees": fees_snapshot,
            "corporate_actions": normalized_actions,
        }

    @staticmethod
    def _validate_corporate_action(action: Dict[str, Any], index: int) -> None:
        atype = action.get("type")
        code = action.get("stock_code")
        if not code:
            raise SettlementException(f"第 {index + 1} 条企业行动缺少 stock_code")
        if atype == "dividend":
            if _D(action.get("cash_per_share")) < 0:
                raise SettlementException(
                    "分红金额不能为负", details={"stock_code": code}
                )
        elif atype == "split":
            if _D(action.get("ratio")) <= 0:
                raise SettlementException(
                    "拆送股比例必须大于 0", details={"stock_code": code}
                )
        else:
            raise SettlementException(
                f"不支持的企业行动类型: {atype}（支持 dividend/split）",
                details={"stock_code": code},
            )

    def _resolve_provisional_quote(
        self, adapter: Any, code: str, quotes: Dict[str, Any]
    ) -> Decimal:
        if code in quotes:
            return _D(quotes[code])
        live = adapter.get_quote(code)
        if live and live.get("last_price") is not None:
            return _D(live["last_price"])
        pos = adapter.get_position(code)
        if pos is not None:
            return pos.current_price
        raise SettlementException(
            "临时估值缺少行情价格", details={"stock_code": code}
        )

    # ==================================================================
    # 内部：期首日状态
    # ==================================================================

    def _compute_beginning(
        self,
        prev,
        account,
        adapter: Any,
        snapshots: Dict[str, Any],
        corporate_actions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """计算期首日现金/权益/持仓。

        有上一封账批次：直接以封账数字结转。
        首日（无封账历史）：由当前账户状态反推当日成交前的状态——
        现金按当日资金净流量回滚，持仓按当日买/卖量与买入成本回滚，
        期首日持仓按成本口径估值（未实现盈亏归零）。
        """
        if prev is not None:
            return {
                "cash": _D(prev.final_cash),
                "equity": _D(prev.final_total_assets),
                "positions": {
                    p.stock_code: {"qty": p.quantity, "cost": p.avg_cost}
                    for p in self.store.list_positions(prev.id)
                },
            }

        buy_qty: Dict[str, Decimal] = defaultdict(lambda: ZERO)
        buy_principal: Dict[str, Decimal] = defaultdict(lambda: ZERO)
        sell_qty: Dict[str, Decimal] = defaultdict(lambda: ZERO)
        net_cash_flow = ZERO  # 正数表示当日资金净流出
        fees_total = ZERO
        for o in snapshots["orders"]:
            if o["status"] not in FILLED_STATUSES | PARTIAL_STATUSES:
                continue
            qty = Decimal(o["filled_quantity"])
            price = _D(o.get("filled_price"))
            if qty <= 0 or price <= 0:
                continue
            principal = price * qty
            commission = self._order_commission(o, qty)
            fees_total += commission
            code = o["stock_code"]
            if o["side"] == "buy":
                buy_qty[code] += qty
                buy_principal[code] += principal
                net_cash_flow += principal + commission
            elif o["side"] == "sell":
                sell_qty[code] += qty
                net_cash_flow -= principal - commission

        # 注意：分红/拆送只存在于结算重放中，适配器持仓始终是行动前状态，
        # 因此期首日反推不回滚企业行动。
        positions_by_code = {p.stock_code: p for p in adapter.get_positions()}
        current_qty: Dict[str, Decimal] = {
            code: Decimal(p.quantity) for code, p in positions_by_code.items()
        }
        code_universe = set(current_qty) | set(buy_qty) | set(sell_qty)
        for action in corporate_actions:
            code_universe.add(action["stock_code"])

        beginning_positions: Dict[str, Dict[str, Any]] = {}
        for code in code_universe:
            pos = positions_by_code.get(code)
            held_qty = Decimal(pos.quantity) if pos is not None else ZERO
            qty0 = held_qty - buy_qty.get(code, ZERO) + sell_qty.get(code, ZERO)
            if qty0 > 0:
                held_cost_total = (
                    pos.avg_cost * pos.quantity if pos is not None else ZERO
                )
                cost_total0 = held_cost_total - buy_principal.get(code, ZERO)
                if cost_total0 <= 0:
                    # 当日卖出了启用结算前就存在、但已无残留可推断成本的持仓
                    raise SettlementException(
                        "无法反推期首日持仓成本：该股票在启用结算前已有持仓且当日已卖出，"
                        "请先做期初建账后再结算",
                        details={"stock_code": code, "beginning_qty": int(qty0)},
                    )
                beginning_positions[code] = {
                    "qty": int(qty0),
                    "cost": str(_money(cost_total0 / qty0)),
                }
            elif qty0 < 0:
                raise SettlementException(
                    "当日卖出数量超过期初与买入之和，订单快照与持仓不一致",
                    details={"stock_code": code, "beginning_qty": int(qty0)},
                )

        cash_begin = _money(account.available_cash + net_cash_flow)
        beginning_cost_value = sum(
            _D(v["cost"]) * Decimal(v["qty"]) for v in beginning_positions.values()
        )
        return {
            "cash": cash_begin,
            # 期初持仓按成本口径估值（期初未实现盈亏为 0）
            "equity": _money(cash_begin + beginning_cost_value),
            "positions": beginning_positions,
        }

    # ==================================================================
    # 内部：分阶段执行
    # ==================================================================

    def _execute_batch(self, batch, snapshots: Dict[str, Any]) -> None:
        # 阶段一：重放分类账（幂等键去重，崩溃后可重做）
        self.store.update_phase(batch.id, "ledger")
        outcome = self._replay_ledger(batch, snapshots)

        # 阶段二：封账持仓（整批替换，天然幂等）
        self.store.update_phase(batch.id, "positions")
        self.store.replace_positions(batch.id, outcome["position_rows"])

        # 阶段三：未完成订单结转
        self.store.update_phase(batch.id, "pending")
        self.store.replace_pending_orders(batch.id, outcome["pending_rows"])

        # 阶段四：封账写结果；迟到成交在同一事务内标记 applied
        self.store.update_phase(batch.id, "sealing")
        results = {
            **outcome["totals"],
            "pending_order_count": len(outcome["pending_rows"]),
            "sealed_position_count": len(outcome["position_rows"]),
        }
        self.store.seal_batch(
            batch.id,
            results,
            late_trade_ids=outcome["late_trade_ids"],
            late_trade_batch_no=batch.batch_no,
        )

    @staticmethod
    def _order_commission(order: Dict[str, Any], filled_qty: Decimal) -> Decimal:
        """订单快照费用按已成交比例摊算。"""
        recorded = _D(order.get("commission"))
        total_qty = Decimal(order["quantity"])
        if recorded <= 0 or total_qty <= 0:
            return ZERO
        if filled_qty >= total_qty:
            return recorded
        return _money(recorded * filled_qty / total_qty)

    def _replay_ledger(self, batch, snapshots: Dict[str, Any]) -> Dict[str, Any]:
        """从期首日状态 + 固定订单/行动/行情快照确定性重放。

        续办时内存状态每次从期首日重建；分类账幂等键只决定“是否补写流水”，
        现金/持仓等内存效果无论流水是否已存在都会重新施加，二者互不干扰。
        """
        account_id = batch.account_id
        trade_date = batch.trade_date
        existing_keys = self.store.existing_ledger_keys(batch.id)

        def post(entry: Dict[str, Any]) -> bool:
            key = entry["idempotency_key"]
            if key in existing_keys:
                return False
            created = self.store.add_ledger_entry(entry)
            if created:
                existing_keys.add(key)
            return created

        # ---- 期首日流水（锁定账本起点）----
        post(
            {
                "batch_id": batch.id,
                "account_id": account_id,
                "trade_date": trade_date,
                "entry_type": "opening",
                "ref_id": "OPENING_CASH",
                "amount": _D(batch.beginning_cash),
                "idempotency_key": f"{batch.batch_no}:opening:cash",
            }
        )

        # ---- 载入期首日持仓与现金（续办也从同一固定起点出发）----
        prev = self.store.previous_sealed_batch(account_id, trade_date)
        lots: Dict[str, Dict[str, Decimal]] = {}
        if prev is not None:
            cash = _D(prev.final_cash)
            for p in self.store.list_positions(prev.id):
                lots[p.stock_code] = {"qty": Decimal(p.quantity), "cost": _D(p.avg_cost)}
        else:
            cash = _D(batch.beginning_cash)
            beginning_positions = json.loads(batch.beginning_positions_snapshot or "{}")
            for code, v in beginning_positions.items():
                lots[code] = {"qty": Decimal(v["qty"]), "cost": _D(v["cost"])}

        fees_total = ZERO
        realized = ZERO

        def apply_fill(
            order: Dict[str, Any],
            fill_type: str,
            qty: Decimal,
            price: Decimal,
            commission: Decimal,
        ) -> None:
            nonlocal cash, realized, fees_total
            code = order["stock_code"]
            side = order["side"]
            ref = str(order["order_id"])
            principal = _money(price * qty)
            key_prefix = f"{batch.batch_no}:{fill_type}:{ref}"

            if side == "buy":
                post(
                    {
                        "batch_id": batch.id,
                        "account_id": account_id,
                        "trade_date": trade_date,
                        "entry_type": fill_type,
                        "ref_id": ref,
                        "stock_code": code,
                        "side": side,
                        "quantity": int(qty),
                        "price": price,
                        "amount": -principal,
                        "idempotency_key": key_prefix + ":principal",
                    }
                )
                # 内存效果无条件施加（续办重建时同样执行）
                cash -= principal
                lot = lots.setdefault(code, {"qty": ZERO, "cost": ZERO})
                total_cost = lot["cost"] * lot["qty"] + principal
                lot["qty"] += qty
                lot["cost"] = total_cost / lot["qty"] if lot["qty"] > 0 else ZERO

            elif side == "sell":
                lot = lots.get(code)
                if lot is None or lot["qty"] < qty:
                    raise SettlementException(
                        f"订单 {ref} 卖出数量超过重放持仓，快照数据不一致",
                        details={
                            "stock_code": code,
                            "sell_qty": int(qty),
                            "held_qty": int(lot["qty"]) if lot else 0,
                        },
                    )
                pnl = (price - lot["cost"]) * qty
                post(
                    {
                        "batch_id": batch.id,
                        "account_id": account_id,
                        "trade_date": trade_date,
                        "entry_type": fill_type,
                        "ref_id": ref,
                        "stock_code": code,
                        "side": side,
                        "quantity": int(qty),
                        "price": price,
                        "amount": principal,
                        "idempotency_key": key_prefix + ":principal",
                    }
                )
                cash += principal
                realized += pnl
                lot["qty"] -= qty
                if lot["qty"] <= 0:
                    lot["cost"] = ZERO
            else:
                raise SettlementException(
                    f"订单 {ref} 买卖方向非法: {side}",
                    details={"stock_code": code},
                )

            if commission > 0:
                post(
                    {
                        "batch_id": batch.id,
                        "account_id": account_id,
                        "trade_date": trade_date,
                        "entry_type": "fee",
                        "ref_id": ref,
                        "stock_code": code,
                        "side": side,
                        "quantity": int(qty),
                        "price": price,
                        "amount": -commission,
                        "idempotency_key": key_prefix + ":fee",
                    }
                )
                cash -= commission
                fees_total += commission

        # ---- 先补入挂起的迟到成交（按登记顺序）----
        late_rows = self.store.pending_late_trades(account_id)
        late_trade_ids: List[int] = []
        late_order_ids = set()
        for late in late_rows:
            if late.intended_date > trade_date:
                continue  # 属于未来交易日，本次不处理
            late_trade_ids.append(late.id)
            late_order_ids.add(late.order_id)
            apply_fill(
                {
                    "order_id": late.order_id,
                    "stock_code": late.stock_code,
                    "side": late.side,
                },
                fill_type="late_trade",
                qty=Decimal(late.quantity),
                price=_D(late.price),
                commission=_D(late.commission),
            )

        # ---- 再重放当日订单快照（全部成交与部分成交的已成交部分）----
        # 更早封账批次已入账的数量要扣减（适配器保留全部历史订单，
        # 部分成交订单跨日补量时只入新增部分），保证历史成交不重复入账。
        already_booked = self.store.booked_fill_quantities(account_id, trade_date)
        fills = []
        for o in snapshots["orders"]:
            if o["order_id"] in late_order_ids:
                continue  # 已作为迟到成交处理，不重复入账
            if o["status"] in FILLED_STATUSES | PARTIAL_STATUSES:
                booked_qty = Decimal(already_booked.get(str(o["order_id"]), 0))
                qty = Decimal(o["filled_quantity"]) - booked_qty
                price = _D(o.get("filled_price"))
                if qty > 0 and price > 0:
                    # 跨批次的新增成交按同一成交价补入，费用按数量摊算
                    fills.append(
                        (
                            o.get("filled_at")
                            or o.get("updated_at")
                            or o.get("created_at")
                            or "",
                            str(o["order_id"]),
                            o,
                            qty,
                            price,
                        )
                    )
        fills.sort(key=lambda x: (x[0], x[1]))
        for _, _, order, qty, price in fills:
            apply_fill(
                order,
                fill_type="trade",
                qty=qty,
                price=price,
                commission=self._order_commission(order, qty),
            )

        # ---- 企业行动：先分红后拆送，顺序固定可复现 ----
        dividend_total = ZERO
        actions = sorted(
            snapshots["corporate_actions"],
            key=lambda a: (0 if a["type"] == "dividend" else 1, a["stock_code"]),
        )
        for action in actions:
            code = action["stock_code"]
            lot = lots.get(code)
            ref = f"{action['type'].upper()}:{code}"
            key = f"{batch.batch_no}:corporate_action:{ref}"
            if action["type"] == "dividend":
                per_share = _D(action["cash_per_share"])
                qty = lot["qty"] if lot else ZERO
                amount = _money(per_share * qty)
                post(
                    {
                        "batch_id": batch.id,
                        "account_id": account_id,
                        "trade_date": trade_date,
                        "entry_type": "dividend",
                        "ref_id": ref,
                        "stock_code": code,
                        "quantity": int(qty),
                        "price": per_share,
                        "amount": amount,
                        "idempotency_key": key,
                    }
                )
                cash += amount
                dividend_total += amount
            else:  # split
                ratio = _D(action["ratio"])
                qty = lot["qty"] if lot else ZERO
                post(
                    {
                        "batch_id": batch.id,
                        "account_id": account_id,
                        "trade_date": trade_date,
                        "entry_type": "split",
                        "ref_id": ref,
                        "stock_code": code,
                        "quantity": int(qty),
                        "price": ratio,
                        "amount": ZERO,
                        "idempotency_key": key,
                    }
                )
                if lot is not None:
                    lot["qty"] = (lot["qty"] * ratio).to_integral_value()
                    lot["cost"] = lot["cost"] / ratio

        # ---- 固定收盘价估值 ----
        quotes = {c: _D(p) for c, p in snapshots["quotes"].items()}
        missing = [c for c in lots if lots[c]["qty"] > 0 and c not in quotes]
        if missing:
            raise SettlementException(
                "结算快照缺少持仓收盘价，无法封账",
                details={"missing_quotes": sorted(missing)},
            )

        market_value = ZERO
        unrealized = ZERO
        position_rows = []
        for code in sorted(c for c in lots if lots[c]["qty"] > 0):
            lot = lots[code]
            qty = lot["qty"]
            close = quotes[code]
            mv = _money(close * qty)
            upl = _money((close - lot["cost"]) * qty)
            market_value += mv
            unrealized += upl
            # T+1：封账后全部持仓（含当日买入）下一交易日可卖
            applied = [
                a for a in snapshots["corporate_actions"]
                if a["stock_code"] == code
            ]
            position_rows.append(
                {
                    "account_id": account_id,
                    "trade_date": trade_date,
                    "stock_code": code,
                    "stock_name": code,
                    "quantity": int(qty),
                    "available_quantity": int(qty),
                    "avg_cost": _money(lot["cost"]),
                    "close_price": _money(close),
                    "market_value": mv,
                    "unrealized_pnl": upl,
                    "corporate_actions_applied": applied,
                }
            )

        # ---- 未完成订单冻结与跨日结转 ----
        frozen = ZERO
        pending_rows = []
        for p in snapshots["pending_orders"]:
            remaining = Decimal(p["remaining_quantity"])
            if p.get("price") is not None:
                price = _D(p["price"])
            else:
                price = quotes.get(p["stock_code"], ZERO)
            frozen_amount = ZERO
            if p["side"] == "buy":
                frozen_amount = _money(price * remaining)
                frozen += frozen_amount
            pending_rows.append(
                {
                    "account_id": account_id,
                    "trade_date": trade_date,
                    "order_id": p["order_id"],
                    "stock_code": p["stock_code"],
                    "side": p["side"],
                    "quantity": p["quantity"],
                    "remaining_quantity": int(remaining),
                    "price": p.get("price"),
                    "frozen_amount": frozen_amount,
                    "reason": "carried_to_next",
                }
            )

        cash = _money(cash)
        market_value = _money(market_value)
        total_assets = _money(cash + market_value)
        day_pnl = _money(total_assets - _D(batch.beginning_equity))

        return {
            "position_rows": position_rows,
            "pending_rows": pending_rows,
            "late_trade_ids": late_trade_ids,
            "totals": {
                "final_cash": cash,
                "final_market_value": market_value,
                "final_total_assets": total_assets,
                "unrealized_pnl": _money(unrealized),
                "realized_pnl": _money(realized),
                "day_pnl": day_pnl,
                "fees_total": _money(fees_total),
                "dividend_total": _money(dividend_total),
                "next_day_available_cash": _money(cash - frozen),
                "next_day_frozen_cash": _money(frozen),
            },
        }

    # ==================================================================
    # 内部：结果组装
    # ==================================================================

    def _build_result(self, batch, reused: bool) -> Dict[str, Any]:
        result = self._batch_summary(batch)
        result["reused"] = reused
        result["ledger"] = [
            self._ledger_dict(e) for e in self.store.list_ledger_entries(batch.id)
        ]
        result["positions"] = [
            self._position_dict(p) for p in self.store.list_positions(batch.id)
        ]
        result["pending_orders"] = [
            self._pending_dict(o) for o in self.store.list_pending_orders(batch.id)
        ]
        result["snapshots"] = {
            "input_hash": batch.input_hash,
            "quotes": json.loads(batch.quotes_snapshot or "{}"),
            "orders_count": len(json.loads(batch.orders_snapshot or "[]")),
            "pending_count": len(json.loads(batch.pending_snapshot or "[]")),
            "fees": json.loads(batch.fees_snapshot or "{}"),
            "corporate_actions": json.loads(
                batch.corporate_actions_snapshot or "[]"
            ),
        }
        result["source"] = batch.status
        return result

    @staticmethod
    def _batch_summary(batch) -> Dict[str, Any]:
        def f(col):
            return float(_D(col)) if col is not None else None

        return {
            "batch_no": batch.batch_no,
            "account_id": batch.account_id,
            "trade_date": batch.trade_date,
            "status": batch.status,
            "phase": batch.phase,
            "attempts": batch.attempts,
            "sealed": batch.status == "sealed",
            "sealed_at": batch.sealed_at.isoformat() if batch.sealed_at else None,
            "error_message": batch.error_message,
            "beginning": {
                "cash": f(batch.beginning_cash),
                "equity": f(batch.beginning_equity),
            },
            "account": {
                "cash": f(batch.final_cash),
                "market_value": f(batch.final_market_value),
                "total_assets": f(batch.final_total_assets),
                "unrealized_pnl": f(batch.unrealized_pnl),
            },
            "realized_pnl": f(batch.realized_pnl),
            "day_pnl": f(batch.day_pnl),
            "fees_total": f(batch.fees_total),
            "dividend_total": f(batch.dividend_total),
            "next_day": {
                "available_cash": f(batch.next_day_available_cash),
                "frozen_cash": f(batch.next_day_frozen_cash),
            },
            "pending_order_count": batch.pending_order_count,
            "sealed_position_count": batch.sealed_position_count,
        }

    @staticmethod
    def _ledger_dict(row) -> Dict[str, Any]:
        def f(col):
            return float(_D(col)) if col is not None else None

        return {
            "entry_type": row.entry_type,
            "ref_id": row.ref_id,
            "stock_code": row.stock_code,
            "side": row.side,
            "quantity": row.quantity,
            "price": f(row.price),
            "amount": f(row.amount),
            "idempotency_key": row.idempotency_key,
        }

    @staticmethod
    def _position_dict(row) -> Dict[str, Any]:
        return {
            "stock_code": row.stock_code,
            "stock_name": row.stock_name,
            "quantity": row.quantity,
            "available_quantity": row.available_quantity,
            "avg_cost": float(_D(row.avg_cost)),
            "close_price": float(_D(row.close_price)),
            "market_value": float(_D(row.market_value)),
            "unrealized_pnl": float(_D(row.unrealized_pnl)),
            "corporate_actions_applied": json.loads(
                row.corporate_actions_applied or "[]"
            ),
        }

    @staticmethod
    def _pending_dict(row) -> Dict[str, Any]:
        return {
            "order_id": row.order_id,
            "stock_code": row.stock_code,
            "side": row.side,
            "quantity": row.quantity,
            "remaining_quantity": row.remaining_quantity,
            "price": float(_D(row.price)) if row.price is not None else None,
            "frozen_amount": float(_D(row.frozen_amount)),
            "reason": row.reason,
        }

    # ==================================================================
    # 杂项
    # ==================================================================

    @staticmethod
    def _validate_trade_date(trade_date: str) -> None:
        try:
            datetime.strptime(trade_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            raise SettlementException(
                "交易日格式必须为 YYYY-MM-DD", details={"trade_date": trade_date}
            )

    @staticmethod
    def _current_error() -> str:
        import sys

        exc = sys.exc_info()[1]
        return str(exc) if exc else "未知错误"
