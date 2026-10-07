"""日终结算持久化模型。

封账后的结算数据是模拟账户的唯一权威账本，全部落库（SQLite），
重启进程后可据此恢复。核心表：

- ``settlement_batches``      唯一结算批次（账户 + 交易日唯一），保存固定快照
- ``settlement_ledger_entries`` 分类账流水（幂等键保证不重复入账）
- ``settlement_positions``    批次封账持仓快照（含企业行动调整后数量/成本）
- ``settlement_pending_orders`` 未完成订单跨日续办记录与冻结资金
- ``settlement_late_trades``  封账后迟到成交，等待下一交易日补入
- ``settlement_valuation_log`` 临时估值记录（只审计、不入账）
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# 批次状态
BATCH_RUNNING = "running"
BATCH_SEALED = "sealed"
BATCH_FAILED = "failed"

# 结算流水线阶段（用于断点续办）
PHASE_INIT = "init"
PHASE_SNAPSHOTS = "snapshots"
PHASE_LEDGER = "ledger"
PHASE_POSITIONS = "positions"
PHASE_PENDING = "pending"
PHASE_SEALED = "sealed"


class SettlementBatch(Base):
    """唯一结算批次：同一账户同一交易日至多一条。"""

    __tablename__ = "settlement_batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(64), nullable=False, index=True)
    trade_date = Column(String(10), nullable=False, index=True)
    batch_no = Column(String(80), nullable=False, unique=True, index=True)
    status = Column(String(16), nullable=False, default=BATCH_RUNNING, index=True)
    phase = Column(String(32), nullable=False, default=PHASE_INIT)
    attempts = Column(Integer, nullable=False, default=1)

    # 固定输入快照（JSON 文本），封账后不再变化，保证可复现
    quotes_snapshot = Column(Text, nullable=False, default="{}")
    orders_snapshot = Column(Text, nullable=False, default="[]")
    pending_snapshot = Column(Text, nullable=False, default="[]")
    fees_snapshot = Column(Text, nullable=False, default="{}")
    corporate_actions_snapshot = Column(Text, nullable=False, default="[]")
    input_hash = Column(String(64), nullable=False)

    # 期首日（上一封账批次结转，或首日由当日成交反推）
    beginning_cash = Column(String(32), nullable=False)
    beginning_equity = Column(String(32), nullable=False)
    beginning_positions_snapshot = Column(Text, nullable=False, default="{}")

    # 封账结果数字（Decimal 以字符串精确存储）
    final_cash = Column(String(32), nullable=True)
    final_market_value = Column(String(32), nullable=True)
    final_total_assets = Column(String(32), nullable=True)
    unrealized_pnl = Column(String(32), nullable=True)
    realized_pnl = Column(String(32), nullable=True)
    day_pnl = Column(String(32), nullable=True)
    fees_total = Column(String(32), nullable=True)
    dividend_total = Column(String(32), nullable=True)
    next_day_available_cash = Column(String(32), nullable=True)
    next_day_frozen_cash = Column(String(32), nullable=True)

    pending_order_count = Column(Integer, nullable=True)
    sealed_position_count = Column(Integer, nullable=True)

    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
    sealed_at = Column(DateTime, nullable=True)

    ledger_entries = relationship(
        "SettlementLedgerEntry",
        back_populates="batch",
        cascade="all, delete-orphan",
    )
    positions = relationship(
        "SettlementPosition",
        back_populates="batch",
        cascade="all, delete-orphan",
    )
    pending_orders = relationship(
        "SettlementPendingOrder",
        back_populates="batch",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("account_id", "trade_date", name="uix_settlement_account_date"),
        Index("ix_settlement_account_status", "account_id", "status"),
    )


class SettlementLedgerEntry(Base):
    """分类账流水。

    ``idempotency_key`` 全局唯一：批次内任何一笔流水（成交、费用、
    分红、拆股、估值、期首日余额、迟到成交）只入账一次，重跑/续办安全。
    """

    __tablename__ = "settlement_ledger_entries"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(
        Integer, ForeignKey("settlement_batches.id"), nullable=False, index=True
    )
    account_id = Column(String(64), nullable=False, index=True)
    trade_date = Column(String(10), nullable=False, index=True)

    # opening / late_trade / trade / fee / dividend / split / valuation
    entry_type = Column(String(24), nullable=False)
    ref_id = Column(String(80), nullable=False)
    stock_code = Column(String(20), nullable=True)
    side = Column(String(8), nullable=True)
    quantity = Column(Integer, nullable=True)
    price = Column(String(32), nullable=True)
    amount = Column(String(32), nullable=False, default="0")
    idempotency_key = Column(String(160), nullable=False, unique=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    batch = relationship("SettlementBatch", back_populates="ledger_entries")

    __table_args__ = (
        Index("ix_ledger_batch_type", "batch_id", "entry_type"),
    )


class SettlementPosition(Base):
    """封账持仓行：企业行动调整后的跨日持仓。"""

    __tablename__ = "settlement_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(
        Integer, ForeignKey("settlement_batches.id"), nullable=False, index=True
    )
    account_id = Column(String(64), nullable=False, index=True)
    trade_date = Column(String(10), nullable=False, index=True)

    stock_code = Column(String(20), nullable=False)
    stock_name = Column(String(64), nullable=True)
    quantity = Column(Integer, nullable=False)
    available_quantity = Column(Integer, nullable=False)
    avg_cost = Column(String(32), nullable=False)
    close_price = Column(String(32), nullable=False)
    market_value = Column(String(32), nullable=False)
    unrealized_pnl = Column(String(32), nullable=False)
    corporate_actions_applied = Column(Text, nullable=False, default="[]")

    batch = relationship("SettlementBatch", back_populates="positions")

    __table_args__ = (
        UniqueConstraint("batch_id", "stock_code", name="uix_settlement_pos_batch_code"),
    )


class SettlementPendingOrder(Base):
    """未完成订单：随批次封账结转至下一交易日。"""

    __tablename__ = "settlement_pending_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(
        Integer, ForeignKey("settlement_batches.id"), nullable=False, index=True
    )
    account_id = Column(String(64), nullable=False, index=True)
    trade_date = Column(String(10), nullable=False, index=True)

    order_id = Column(String(80), nullable=False, unique=True)
    stock_code = Column(String(20), nullable=False)
    side = Column(String(8), nullable=False)
    quantity = Column(Integer, nullable=False)
    remaining_quantity = Column(Integer, nullable=False)
    price = Column(String(32), nullable=True)
    frozen_amount = Column(String(32), nullable=False, default="0")
    reason = Column(String(64), nullable=False, default="carried_to_next")

    batch = relationship("SettlementBatch", back_populates="pending_orders")


class SettlementLateTrade(Base):
    """封账后才到达的成交：不改动已封账批次，挂起到下一交易日补入。"""

    __tablename__ = "settlement_late_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(64), nullable=False, index=True)
    order_id = Column(String(80), nullable=False, unique=True)
    intended_date = Column(String(10), nullable=False, index=True)
    stock_code = Column(String(20), nullable=False)
    side = Column(String(8), nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(String(32), nullable=False)
    commission = Column(String(32), nullable=False, default="0")

    # pending / applied / rejected
    status = Column(String(16), nullable=False, default="pending", index=True)
    accepted_batch_no = Column(String(80), nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    applied_at = Column(DateTime, nullable=True)


class SettlementValuationLog(Base):
    """临时估值审计记录：只记录来源口径，从不产生分类账流水。"""

    __tablename__ = "settlement_valuation_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(64), nullable=False, index=True)
    trade_date = Column(String(10), nullable=False, index=True)
    quote_basis = Column(Text, nullable=False, default="{}")
    basis_hash = Column(String(64), nullable=False)

    cash = Column(String(32), nullable=False)
    market_value = Column(String(32), nullable=False)
    total_assets = Column(String(32), nullable=False)
    unrealized_pnl = Column(String(32), nullable=False)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
