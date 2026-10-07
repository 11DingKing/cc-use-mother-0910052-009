"""日终结算持久化访问层。

所有结算状态都通过本类读写，保证：
- 批次按 (账户, 交易日) 唯一；
- 分类账流水按全局幂等键写入，重复写入被静默跳过；
- 进程重启后可凭 ``phase`` 字段从断点续办。
"""

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from app.entities.settlement import (
    Base,
    BATCH_RUNNING,
    BATCH_SEALED,
    PHASE_INIT,
    SettlementBatch,
    SettlementLedgerEntry,
    SettlementPosition,
    SettlementPendingOrder,
    SettlementLateTrade,
    SettlementValuationLog,
)

logger = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    """JSON 序列化对 Decimal/datetime 的兜底处理。"""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"不可序列化的类型: {type(obj)}")


def dumps(obj: Any) -> str:
    """结算快照统一序列化（键排序，便于内容哈希可复现）。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=_json_default)


class SettlementStore:
    """结算表的数据访问对象。"""

    def __init__(self, engine: Optional[Any] = None, session_factory: Optional[Any] = None):
        if session_factory is not None:
            self._session_factory = session_factory
            if engine is not None:
                Base.metadata.create_all(engine)
        elif engine is not None:
            Base.metadata.create_all(engine)
            self._session_factory = sessionmaker(bind=engine, autoflush=False)
        else:
            from app.config import get_engine

            eng = get_engine()
            Base.metadata.create_all(eng)
            self._session_factory = sessionmaker(bind=eng, autoflush=False)

    # ------------------------------------------------------------------
    # 基础会话工具
    # ------------------------------------------------------------------

    def session(self) -> Session:
        return self._session_factory()

    # ------------------------------------------------------------------
    # 批次
    # ------------------------------------------------------------------

    def get_batch(self, account_id: str, trade_date: str) -> Optional[SettlementBatch]:
        """按账户与交易日取批次（不论状态）。"""
        with self.session() as s:
            return self._get_batch(s, account_id, trade_date)

    @staticmethod
    def _get_batch(
        s: Session, account_id: str, trade_date: str
    ) -> Optional[SettlementBatch]:
        return (
            s.query(SettlementBatch)
            .filter(
                SettlementBatch.account_id == account_id,
                SettlementBatch.trade_date == trade_date,
            )
            .first()
        )

    def get_batch_by_no(self, batch_no: str) -> Optional[SettlementBatch]:
        with self.session() as s:
            return s.query(SettlementBatch).filter(
                SettlementBatch.batch_no == batch_no
            ).first()

    def create_batch(
        self,
        account_id: str,
        trade_date: str,
        beginning_cash: Decimal,
        beginning_equity: Decimal,
        quotes_snapshot: Dict[str, Any],
        orders_snapshot: List[Dict[str, Any]],
        pending_snapshot: List[Dict[str, Any]],
        fees_snapshot: Dict[str, Any],
        corporate_actions_snapshot: List[Dict[str, Any]],
        input_hash: str,
        beginning_positions: Optional[Dict[str, Any]] = None,
    ) -> SettlementBatch:
        """创建新批次。(账户, 交易日) 冲突时抛 IntegrityError。"""
        with self.session() as s:
            batch = SettlementBatch(
                account_id=account_id,
                trade_date=trade_date,
                batch_no=f"STL_{account_id}_{trade_date}",
                status=BATCH_RUNNING,
                phase=PHASE_INIT,
                attempts=1,
                quotes_snapshot=dumps(quotes_snapshot),
                orders_snapshot=dumps(orders_snapshot),
                pending_snapshot=dumps(pending_snapshot),
                fees_snapshot=dumps(fees_snapshot),
                corporate_actions_snapshot=dumps(corporate_actions_snapshot),
                input_hash=input_hash,
                beginning_cash=str(beginning_cash),
                beginning_equity=str(beginning_equity),
                beginning_positions_snapshot=dumps(beginning_positions or {}),
            )
            s.add(batch)
            s.commit()
            s.refresh(batch)
            return batch

    def touch_attempt(self, batch_id: int) -> int:
        """续办开始时递增 attempts，返回最新值。"""
        with self.session() as s:
            batch = s.get(SettlementBatch, batch_id)
            batch.attempts = (batch.attempts or 1) + 1
            s.commit()
            return batch.attempts

    def update_phase(self, batch_id: int, phase: str) -> None:
        with self.session() as s:
            batch = s.get(SettlementBatch, batch_id)
            batch.phase = phase
            s.commit()

    def mark_failed(self, batch_id: int, message: str) -> None:
        with self.session() as s:
            batch = s.get(SettlementBatch, batch_id)
            batch.status = "failed"
            batch.error_message = message
            s.commit()

    def seal_batch(
        self,
        batch_id: int,
        results: Dict[str, Any],
        late_trade_ids: Optional[List[int]] = None,
        late_trade_batch_no: Optional[str] = None,
    ) -> None:
        """写入封账结果数字并置为 sealed，单事务提交。

        本批次补入的迟到成交在同一事务内标记 applied，避免
        “已封账但仍挂起”的崩溃窗口导致次日重复补入。
        """
        with self.session() as s:
            batch = s.get(SettlementBatch, batch_id)
            for key in (
                "final_cash",
                "final_market_value",
                "final_total_assets",
                "unrealized_pnl",
                "realized_pnl",
                "day_pnl",
                "fees_total",
                "dividend_total",
                "next_day_available_cash",
                "next_day_frozen_cash",
            ):
                if results.get(key) is not None:
                    setattr(batch, key, str(results[key]))
            batch.pending_order_count = results.get("pending_order_count")
            batch.sealed_position_count = results.get("sealed_position_count")
            batch.phase = "sealed"
            batch.status = BATCH_SEALED
            batch.error_message = None
            batch.sealed_at = datetime.utcnow()
            if late_trade_ids:
                for late_id in late_trade_ids:
                    row = s.get(SettlementLateTrade, late_id)
                    if row is not None and row.status != "applied":
                        row.status = "applied"
                        row.accepted_batch_no = late_trade_batch_no
                        row.applied_at = datetime.utcnow()
            s.commit()

    def latest_sealed_batch(self, account_id: str) -> Optional[SettlementBatch]:
        """账户最近一个已封账批次（用于历史查询与期首日）。"""
        with self.session() as s:
            return (
                s.query(SettlementBatch)
                .filter(
                    SettlementBatch.account_id == account_id,
                    SettlementBatch.status == BATCH_SEALED,
                )
                .order_by(SettlementBatch.trade_date.desc())
                .first()
            )

    def previous_sealed_batch(
        self, account_id: str, trade_date: str
    ) -> Optional[SettlementBatch]:
        """取指定交易日之前最近的已封账批次（期首日来源）。"""
        with self.session() as s:
            return (
                s.query(SettlementBatch)
                .filter(
                    SettlementBatch.account_id == account_id,
                    SettlementBatch.status == BATCH_SEALED,
                    SettlementBatch.trade_date < trade_date,
                )
                .order_by(SettlementBatch.trade_date.desc())
                .first()
            )

    def list_batches(
        self,
        account_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[SettlementBatch]:
        with self.session() as s:
            q = s.query(SettlementBatch).filter(
                SettlementBatch.account_id == account_id
            )
            if start_date:
                q = q.filter(SettlementBatch.trade_date >= start_date)
            if end_date:
                q = q.filter(SettlementBatch.trade_date <= end_date)
            return q.order_by(SettlementBatch.trade_date.desc()).limit(limit).all()

    # ------------------------------------------------------------------
    # 分类账流水（幂等写入）
    # ------------------------------------------------------------------

    def add_ledger_entry(self, entry: Dict[str, Any]) -> bool:
        """按 idempotency_key 写入流水；已存在则跳过。返回是否新写入。"""
        row = SettlementLedgerEntry(
            batch_id=entry["batch_id"],
            account_id=entry["account_id"],
            trade_date=entry["trade_date"],
            entry_type=entry["entry_type"],
            ref_id=entry["ref_id"],
            stock_code=entry.get("stock_code"),
            side=entry.get("side"),
            quantity=entry.get("quantity"),
            price=str(entry["price"]) if entry.get("price") is not None else None,
            amount=str(entry.get("amount", "0")),
            idempotency_key=entry["idempotency_key"],
        )
        with self.session() as s:
            s.add(row)
            try:
                s.commit()
            except IntegrityError:
                s.rollback()
                logger.info(
                    "分类账流水已存在，跳过: %s", entry["idempotency_key"]
                )
                return False
            return True

    def existing_ledger_keys(self, batch_id: int) -> set:
        """批次内已落账的幂等键集合（断点续办时用于跳过已入账项）。"""
        with self.session() as s:
            rows = (
                s.query(SettlementLedgerEntry.idempotency_key)
                .filter(SettlementLedgerEntry.batch_id == batch_id)
                .all()
            )
            return {r[0] for r in rows}

    def list_ledger_entries(self, batch_id: int) -> List[SettlementLedgerEntry]:
        with self.session() as s:
            return (
                s.query(SettlementLedgerEntry)
                .filter(SettlementLedgerEntry.batch_id == batch_id)
                .order_by(SettlementLedgerEntry.id.asc())
                .all()
            )

    def booked_fill_quantities(
        self, account_id: str, trade_date: str
    ) -> Dict[str, int]:
        """更早封账批次中各订单已入账成交数量合计。

        适配器保留全部历史订单，跨日重放时据此扣减，避免历史成交重复入账；
        对部分成交订单只扣减已封账部分，剩余份额在本批次补入。
        """
        with self.session() as s:
            rows = (
                s.query(
                    SettlementLedgerEntry.ref_id,
                    SettlementLedgerEntry.quantity,
                )
                .join(
                    SettlementBatch,
                    SettlementBatch.id == SettlementLedgerEntry.batch_id,
                )
                .filter(
                    SettlementLedgerEntry.account_id == account_id,
                    SettlementLedgerEntry.entry_type.in_(["trade", "late_trade"]),
                    SettlementBatch.status == BATCH_SEALED,
                    SettlementBatch.trade_date < trade_date,
                )
                .all()
            )
            booked: Dict[str, int] = {}
            for ref_id, qty in rows:
                if qty:
                    booked[ref_id] = booked.get(ref_id, 0) + qty
            return booked

    # ------------------------------------------------------------------
    # 封账持仓
    # ------------------------------------------------------------------

    def replace_positions(self, batch_id: int, rows: Iterable[Dict[str, Any]]) -> None:
        """整批替换封账持仓（续办时先清后写，天然幂等）。"""
        with self.session() as s:
            s.query(SettlementPosition).filter(
                SettlementPosition.batch_id == batch_id
            ).delete(synchronize_session=False)
            for r in rows:
                s.add(
                    SettlementPosition(
                        batch_id=batch_id,
                        account_id=r["account_id"],
                        trade_date=r["trade_date"],
                        stock_code=r["stock_code"],
                        stock_name=r.get("stock_name"),
                        quantity=r["quantity"],
                        available_quantity=r["available_quantity"],
                        avg_cost=str(r["avg_cost"]),
                        close_price=str(r["close_price"]),
                        market_value=str(r["market_value"]),
                        unrealized_pnl=str(r["unrealized_pnl"]),
                        corporate_actions_applied=dumps(
                            r.get("corporate_actions_applied", [])
                        ),
                    )
                )
            s.commit()

    def list_positions(self, batch_id: int) -> List[SettlementPosition]:
        with self.session() as s:
            return (
                s.query(SettlementPosition)
                .filter(SettlementPosition.batch_id == batch_id)
                .order_by(SettlementPosition.stock_code.asc())
                .all()
            )

    # ------------------------------------------------------------------
    # 未完成订单结转
    # ------------------------------------------------------------------

    def replace_pending_orders(
        self, batch_id: int, rows: Iterable[Dict[str, Any]]
    ) -> None:
        """整批替换结转未完成订单（订单号全局唯一，先清后写幂等）。"""
        with self.session() as s:
            s.query(SettlementPendingOrder).filter(
                SettlementPendingOrder.batch_id == batch_id
            ).delete(synchronize_session=False)
            for r in rows:
                s.add(
                    SettlementPendingOrder(
                        batch_id=batch_id,
                        account_id=r["account_id"],
                        trade_date=r["trade_date"],
                        order_id=r["order_id"],
                        stock_code=r["stock_code"],
                        side=r["side"],
                        quantity=r["quantity"],
                        remaining_quantity=r["remaining_quantity"],
                        price=str(r["price"]) if r.get("price") is not None else None,
                        frozen_amount=str(r.get("frozen_amount", "0")),
                        reason=r.get("reason", "carried_to_next"),
                    )
                )
            s.commit()

    def list_pending_orders(self, batch_id: int) -> List[SettlementPendingOrder]:
        with self.session() as s:
            return (
                s.query(SettlementPendingOrder)
                .filter(SettlementPendingOrder.batch_id == batch_id)
                .order_by(SettlementPendingOrder.order_id.asc())
                .all()
            )

    # ------------------------------------------------------------------
    # 迟到成交
    # ------------------------------------------------------------------

    def enqueue_late_trade(self, trade: Dict[str, Any]) -> bool:
        """登记迟到成交（订单号唯一，重复上报安全）。返回是否新登记。"""
        with self.session() as s:
            row = SettlementLateTrade(
                account_id=trade["account_id"],
                order_id=trade["order_id"],
                intended_date=trade["intended_date"],
                stock_code=trade["stock_code"],
                side=trade["side"],
                quantity=trade["quantity"],
                price=str(trade["price"]),
                commission=str(trade.get("commission", "0")),
            )
            s.add(row)
            try:
                s.commit()
            except IntegrityError:
                s.rollback()
                logger.info("迟到成交已登记，跳过: %s", trade["order_id"])
                return False
            return True

    def pending_late_trades(
        self, account_id: str, intended_date: Optional[str] = None
    ) -> List[SettlementLateTrade]:
        with self.session() as s:
            q = s.query(SettlementLateTrade).filter(
                SettlementLateTrade.account_id == account_id,
                SettlementLateTrade.status == "pending",
            )
            if intended_date:
                q = q.filter(SettlementLateTrade.intended_date == intended_date)
            return q.order_by(SettlementLateTrade.id.asc()).all()

    def mark_late_trade_applied(
        self, late_trade_id: int, accepted_batch_no: str
    ) -> None:
        with self.session() as s:
            row = s.get(SettlementLateTrade, late_trade_id)
            row.status = "applied"
            row.accepted_batch_no = accepted_batch_no
            row.applied_at = datetime.utcnow()
            s.commit()

    def reconcile_applied_late_trades(
        self, batch_id: int, batch_no: str
    ) -> int:
        """对已封账批次做一致性修复：分类账中已存在 late_trade 流水、
        但迟到成交仍挂 pending（旧版本封账后标记窗口内崩溃）的记录补标 applied。"""
        with self.session() as s:
            refs = {
                r[0]
                for r in s.query(SettlementLedgerEntry.ref_id)
                .filter(
                    SettlementLedgerEntry.batch_id == batch_id,
                    SettlementLedgerEntry.entry_type == "late_trade",
                )
                .all()
            }
            if not refs:
                return 0
            rows = (
                s.query(SettlementLateTrade)
                .filter(
                    SettlementLateTrade.order_id.in_(refs),
                    SettlementLateTrade.status == "pending",
                )
                .all()
            )
            for row in rows:
                row.status = "applied"
                row.accepted_batch_no = batch_no
                row.applied_at = datetime.utcnow()
            s.commit()
            return len(rows)

    # ------------------------------------------------------------------
    # 临时估值审计
    # ------------------------------------------------------------------

    def add_valuation_log(self, valuation: Dict[str, Any]) -> None:
        with self.session() as s:
            s.add(
                SettlementValuationLog(
                    account_id=valuation["account_id"],
                    trade_date=valuation["trade_date"],
                    quote_basis=dumps(valuation.get("quote_basis", {})),
                    basis_hash=valuation["basis_hash"],
                    cash=str(valuation["cash"]),
                    market_value=str(valuation["market_value"]),
                    total_assets=str(valuation["total_assets"]),
                    unrealized_pnl=str(valuation["unrealized_pnl"]),
                )
            )
            s.commit()
