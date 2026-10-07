"""日终结算数据访问层。"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import asc
from sqlalchemy.orm import Session

from app.entities.settlement import (
    Base,
    SettlementBatch,
    SettlementQuote,
    SettlementOrder,
    SettlementFee,
    SettlementPosition,
    CorporateAction,
    SettlementCorporateAction,
    PostSealAdjustment,
)


class SettlementMapper:
    """封装结算相关表的读写。"""

    def __init__(self, session: Session):
        self.session = session

    # ---- 批次 ----

    def get_batch(
        self, account_id: str, trade_date: str
    ) -> Optional[SettlementBatch]:
        return (
            self.session.query(SettlementBatch)
            .filter(
                SettlementBatch.account_id == account_id,
                SettlementBatch.trade_date == trade_date,
            )
            .first()
        )

    def get_batch_by_id(self, batch_id: int) -> Optional[SettlementBatch]:
        return self.session.get(SettlementBatch, batch_id)

    def get_batch_by_no(self, batch_no: str) -> Optional[SettlementBatch]:
        return (
            self.session.query(SettlementBatch)
            .filter(SettlementBatch.batch_no == batch_no)
            .first()
        )

    def latest_sealed_before(
        self, account_id: str, trade_date: str
    ) -> Optional[SettlementBatch]:
        return (
            self.session.query(SettlementBatch)
            .filter(
                SettlementBatch.account_id == account_id,
                SettlementBatch.trade_date < trade_date,
                SettlementBatch.status == "sealed",
            )
            .order_by(SettlementBatch.trade_date.desc())
            .first()
        )

    def list_batches(
        self,
        account_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[SettlementBatch]:
        query = self.session.query(SettlementBatch)
        if account_id:
            query = query.filter(SettlementBatch.account_id == account_id)
        if status:
            query = query.filter(SettlementBatch.status == status)
        return (
            query.order_by(SettlementBatch.trade_date.desc()).limit(limit).all()
        )

    def open_batches_before(
        self, account_id: str, trade_date: str
    ) -> List[SettlementBatch]:
        """更早日期中尚未封账（pending/sealing/failed）的批次。"""
        return (
            self.session.query(SettlementBatch)
            .filter(
                SettlementBatch.account_id == account_id,
                SettlementBatch.trade_date < trade_date,
                SettlementBatch.status != "sealed",
            )
            .order_by(SettlementBatch.trade_date.asc())
            .all()
        )

    def add_batch(self, batch: SettlementBatch) -> SettlementBatch:
        self.session.add(batch)
        self.session.flush()
        return batch

    # ---- 行情快照 ----

    def upsert_quote(
        self,
        batch_id: int,
        stock_code: str,
        close_price: str,
        source: str,
    ) -> SettlementQuote:
        row = self._get_quote(batch_id, stock_code)
        if row is None:
            row = SettlementQuote(
                batch_id=batch_id,
                stock_code=stock_code,
                close_price=str(close_price),
                source=source,
            )
            self.session.add(row)
            self.session.flush()
        # 已存在的快照价格不可变，续办时保留原固定行情
        return row

    def _get_quote(
        self, batch_id: int, stock_code: str
    ) -> Optional[SettlementQuote]:
        return (
            self.session.query(SettlementQuote)
            .filter(
                SettlementQuote.batch_id == batch_id,
                SettlementQuote.stock_code == stock_code,
            )
            .first()
        )

    def list_quotes(self, batch_id: int) -> List[SettlementQuote]:
        return (
            self.session.query(SettlementQuote)
            .filter(SettlementQuote.batch_id == batch_id)
            .order_by(SettlementQuote.stock_code.asc())
            .all()
        )

    # ---- 订单快照 / 过账 ----

    def upsert_order(self, batch_id: int, data: Dict) -> SettlementOrder:
        row = (
            self.session.query(SettlementOrder)
            .filter(
                SettlementOrder.batch_id == batch_id,
                SettlementOrder.order_id == data["order_id"],
            )
            .first()
        )
        if row is None:
            row = SettlementOrder(batch_id=batch_id, **data)
            self.session.add(row)
            self.session.flush()
        else:
            for key, value in data.items():
                setattr(row, key, value)
            self.session.flush()
        return row

    def list_orders(self, batch_id: int) -> List[SettlementOrder]:
        return (
            self.session.query(SettlementOrder)
            .filter(SettlementOrder.batch_id == batch_id)
            .order_by(
                asc(SettlementOrder.updated_at), asc(SettlementOrder.order_id)
            )
            .all()
        )

    def get_order_row(
        self, batch_id: int, order_id: str
    ) -> Optional[SettlementOrder]:
        return (
            self.session.query(SettlementOrder)
            .filter(
                SettlementOrder.batch_id == batch_id,
                SettlementOrder.order_id == order_id,
            )
            .first()
        )

    def order_history_before(
        self, account_id: str, order_id: str, trade_date: str
    ) -> Tuple[int, Optional[SettlementOrder]]:
        """查询订单在更早批次中的累计过账量与最近一次快照。

        返回 (prior_posted_sum, latest_row_or_None)，用于跨日批次增量过账。
        """
        rows = (
            self.session.query(SettlementOrder)
            .join(
                SettlementBatch,
                SettlementOrder.batch_id == SettlementBatch.id,
            )
            .filter(
                SettlementBatch.account_id == account_id,
                SettlementOrder.order_id == order_id,
                SettlementBatch.trade_date < trade_date,
            )
            .all()
        )
        if not rows:
            return 0, None
        prior = sum(int(row.posted_quantity or 0) for row in rows)
        latest = max(rows, key=lambda row: row.id)
        return prior, latest

    # ---- 费用 ----

    def upsert_fee(
        self,
        batch_id: int,
        order_id: str,
        fee_type: str,
        amount: str,
    ) -> SettlementFee:
        row = (
            self.session.query(SettlementFee)
            .filter(
                SettlementFee.batch_id == batch_id,
                SettlementFee.order_id == order_id,
                SettlementFee.fee_type == fee_type,
            )
            .first()
        )
        if row is None:
            row = SettlementFee(
                batch_id=batch_id,
                order_id=order_id,
                fee_type=fee_type,
                amount=str(amount),
            )
            self.session.add(row)
        else:
            row.amount = str(amount)
        self.session.flush()
        return row

    def list_fees(self, batch_id: int) -> List[SettlementFee]:
        return (
            self.session.query(SettlementFee)
            .filter(SettlementFee.batch_id == batch_id)
            .order_by(SettlementFee.order_id.asc(), SettlementFee.fee_type.asc())
            .all()
        )

    # ---- 持仓快照 ----

    def replace_positions(self, batch_id: int, positions: List[Dict]) -> None:
        self.session.query(SettlementPosition).filter(
            SettlementPosition.batch_id == batch_id
        ).delete(synchronize_session=False)
        for data in positions:
            self.session.add(SettlementPosition(batch_id=batch_id, **data))
        self.session.flush()

    def list_positions(self, batch_id: int) -> List[SettlementPosition]:
        return (
            self.session.query(SettlementPosition)
            .filter(SettlementPosition.batch_id == batch_id)
            .order_by(SettlementPosition.stock_code.asc())
            .all()
        )

    # ---- 企业行动 ----

    def list_corporate_actions(
        self, account_id: str, ex_date: str
    ) -> List[CorporateAction]:
        return (
            self.session.query(CorporateAction)
            .filter(
                CorporateAction.account_id == account_id,
                CorporateAction.ex_date == ex_date,
                CorporateAction.status == "confirmed",
            )
            .order_by(CorporateAction.id.asc())
            .all()
        )

    def get_corporate_action(self, action_id: int) -> Optional[CorporateAction]:
        return self.session.get(CorporateAction, action_id)

    def upsert_corporate_action(self, data: Dict) -> CorporateAction:
        row = (
            self.session.query(CorporateAction)
            .filter(
                CorporateAction.account_id == data["account_id"],
                CorporateAction.stock_code == data["stock_code"],
                CorporateAction.ex_date == data["ex_date"],
                CorporateAction.action_type == data["action_type"],
            )
            .first()
        )
        if row is None:
            row = CorporateAction(**data)
            self.session.add(row)
            self.session.flush()
        return row

    def get_settlement_action(
        self, batch_id: int, corporate_action_id: int
    ) -> Optional[SettlementCorporateAction]:
        return (
            self.session.query(SettlementCorporateAction)
            .filter(
                SettlementCorporateAction.batch_id == batch_id,
                SettlementCorporateAction.corporate_action_id
                == corporate_action_id,
            )
            .first()
        )

    def mark_settlement_action(
        self,
        batch_id: int,
        corporate_action_id: int,
        effect_cash: str,
        effect_quantity: int,
    ) -> SettlementCorporateAction:
        row = self.get_settlement_action(batch_id, corporate_action_id)
        if row is None:
            row = SettlementCorporateAction(
                batch_id=batch_id,
                corporate_action_id=corporate_action_id,
            )
            self.session.add(row)
        row.applied = 1
        row.effect_cash = str(effect_cash)
        row.effect_quantity = effect_quantity
        self.session.flush()
        return row

    def list_settlement_actions(
        self, batch_id: int
    ) -> List[SettlementCorporateAction]:
        return (
            self.session.query(SettlementCorporateAction)
            .filter(SettlementCorporateAction.batch_id == batch_id)
            .all()
        )

    # ---- 封账后调整 ----

    def add_adjustment(self, adjustment: PostSealAdjustment) -> PostSealAdjustment:
        self.session.add(adjustment)
        self.session.flush()
        return adjustment

    def find_adjustment(
        self,
        account_id: str,
        adjustment_type: str,
        payload_hash: str,
    ) -> Optional[PostSealAdjustment]:
        return (
            self.session.query(PostSealAdjustment)
            .filter(
                PostSealAdjustment.account_id == account_id,
                PostSealAdjustment.adjustment_type == adjustment_type,
                PostSealAdjustment.payload_hash == payload_hash,
            )
            .first()
        )

    def pending_adjustments_before(
        self, account_id: str, trade_date: str
    ) -> List[PostSealAdjustment]:
        return (
            self.session.query(PostSealAdjustment)
            .filter(
                PostSealAdjustment.account_id == account_id,
                PostSealAdjustment.trade_date < trade_date,
                PostSealAdjustment.status == "pending",
            )
            .order_by(PostSealAdjustment.id.asc())
            .all()
        )

    def list_adjustments(
        self,
        account_id: Optional[str] = None,
        sealed_batch_id: Optional[int] = None,
        status: Optional[str] = None,
    ) -> List[PostSealAdjustment]:
        query = self.session.query(PostSealAdjustment)
        if account_id:
            query = query.filter(PostSealAdjustment.account_id == account_id)
        if sealed_batch_id is not None:
            query = query.filter(
                PostSealAdjustment.sealed_batch_id == sealed_batch_id
            )
        if status:
            query = query.filter(PostSealAdjustment.status == status)
        return query.order_by(PostSealAdjustment.id.asc()).all()


def create_settlement_tables(engine) -> None:
    """在指定引擎上创建结算表。"""
    Base.metadata.create_all(bind=engine)
