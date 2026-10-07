"""日终结算持久化模型。

所有金额一律以字符串存储 Decimal，避免浮点误差；时间统一使用 naive UTC/本地
时间（与交易域内 datetime.now() 保持同一时钟基准）。
"""

from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SettlementBatch(Base):
    """日终结算批次：同一账户同一交易日唯一。"""

    __tablename__ = "settlement_batches"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_no = Column(String(40), nullable=False, unique=True)
    account_id = Column(String(64), nullable=False)
    trade_date = Column(String(10), nullable=False)  # YYYY-MM-DD

    # pending：快照/过账中；sealing：已停止接单正在封账；sealed：已封账；
    # failed：快照缺失等不可续办错误（修正输入后续办）
    status = Column(String(16), nullable=False, default="pending")

    # 行情/订单/费用/企业行动快照的规范化哈希，封账时定格
    snapshot_hash = Column(String(64), nullable=True)
    prev_batch_id = Column(Integer, nullable=True)

    # 费用费率快照（封账数字按批次时的费率重算，不受重启后配置变化影响）
    commission_rate = Column(String(20), nullable=True)
    min_commission = Column(String(20), nullable=True)
    stamp_tax_rate = Column(String(20), nullable=True)

    # 封账数字
    begin_cash = Column(String(40), nullable=True)
    cash = Column(String(40), nullable=True)
    market_value = Column(String(40), nullable=True)
    total_assets = Column(String(40), nullable=True)
    unrealized_pnl = Column(String(40), nullable=True)
    realized_pnl = Column(String(40), nullable=True)
    total_fees = Column(String(40), nullable=True)
    frozen_cash = Column(String(40), nullable=True, default="0")
    next_available_cash = Column(String(40), nullable=True)
    next_trading_date = Column(String(20), nullable=True)

    unfinished_order_count = Column(Integer, default=0)
    cancelled_order_ids = Column(Text, nullable=True)  # JSON 列表
    last_order_updated_at = Column(DateTime, nullable=True)

    error_message = Column(String(500), nullable=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    sealed_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("account_id", "trade_date", name="uix_batch_account_date"),
        Index("ix_batch_account_sealed", "account_id", "status"),
    )


class SettlementQuote(Base):
    """固定行情快照：批次使用的收盘价不可变。"""

    __tablename__ = "settlement_quotes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, ForeignKey("settlement_batches.id"), nullable=False)
    stock_code = Column(String(20), nullable=False)
    close_price = Column(String(40), nullable=False)
    # closing_input：调用方指定收盘价；adapter_quote：取自交易适配器现价
    source = Column(String(20), default="adapter_quote")
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint("batch_id", "stock_code", name="uix_quote_batch_stock"),
    )


class SettlementOrder(Base):
    """订单快照及过账进度。posted_quantity 使部分成交/中断续办可以增量补账。"""

    __tablename__ = "settlement_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, ForeignKey("settlement_batches.id"), nullable=False)
    order_id = Column(String(64), nullable=False)
    stock_code = Column(String(20), nullable=False)
    side = Column(String(8), nullable=False)
    order_type = Column(String(16), nullable=True)
    quantity = Column(Integer, default=0)
    filled_quantity = Column(Integer, default=0)
    posted_quantity = Column(Integer, default=0)
    # 其中通过封账后迟到成交补记的数量（用于资金流水区分，不参与重复入账）
    late_quantity = Column(Integer, default=0)
    filled_price = Column(String(40), nullable=True)
    commission_amount = Column(String(40), nullable=True)
    status = Column(String(16), nullable=False)
    created_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("batch_id", "order_id", name="uix_order_batch_order"),
        Index("ix_order_order_id", "order_id"),
    )


class SettlementFee(Base):
    """单笔费用明细，(批次, 订单, 费用类型) 唯一，保证不重复入账。"""

    __tablename__ = "settlement_fees"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, ForeignKey("settlement_batches.id"), nullable=False)
    order_id = Column(String(64), nullable=False)
    fee_type = Column(String(20), nullable=False)  # commission / stamp_tax
    amount = Column(String(40), nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            "batch_id", "order_id", "fee_type", name="uix_fee_batch_order_type"
        ),
    )


class SettlementPosition(Base):
    """封账持仓快照。"""

    __tablename__ = "settlement_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, ForeignKey("settlement_batches.id"), nullable=False)
    stock_code = Column(String(20), nullable=False)
    stock_name = Column(String(40), nullable=True)
    quantity = Column(Integer, nullable=False)
    available_quantity = Column(Integer, nullable=False)
    avg_cost = Column(String(40), nullable=False)
    close_price = Column(String(40), nullable=False)
    market_value = Column(String(40), nullable=False)
    unrealized_pnl = Column(String(40), nullable=False)

    __table_args__ = (
        UniqueConstraint("batch_id", "stock_code", name="uix_position_batch_stock"),
    )


class CorporateAction(Base):
    """企业行动（分红/送股/拆股）登记。"""

    __tablename__ = "corporate_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(64), nullable=False)
    stock_code = Column(String(20), nullable=False)
    action_type = Column(String(20), nullable=False)  # cash_dividend/stock_dividend/split
    ex_date = Column(String(10), nullable=False)
    ratio = Column(String(20), default="0")           # 送股/拆股比例
    cash_per_share = Column(String(20), default="0")  # 每股派现（税前）
    status = Column(String(16), default="confirmed")
    created_at = Column(DateTime, default=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "stock_code",
            "ex_date",
            "action_type",
            name="uix_ca_account_stock_date_type",
        ),
    )


class SettlementCorporateAction(Base):
    """批次对企业行动的应用记录，applied=1 后幂等。"""

    __tablename__ = "settlement_corporate_actions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(Integer, ForeignKey("settlement_batches.id"), nullable=False)
    corporate_action_id = Column(
        Integer, ForeignKey("corporate_actions.id"), nullable=False
    )
    applied = Column(Integer, default=0)
    effect_cash = Column(String(40), default="0")
    effect_quantity = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint(
            "batch_id", "corporate_action_id", name="uix_sca_batch_action"
        ),
    )


class PostSealAdjustment(Base):
    """封账后到达的事件登记。

    迟到成交(late_fill)在已封账批次上只登记、不改账，由下一交易日批次消费；
    重新估值(restatement)以 rejected 状态留存，永不自动入账。
    """

    __tablename__ = "post_seal_adjustments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(String(64), nullable=False)
    sealed_batch_id = Column(Integer, ForeignKey("settlement_batches.id"), nullable=False)
    trade_date = Column(String(10), nullable=False)  # 原封账日期
    order_id = Column(String(64), nullable=True)
    adjustment_type = Column(String(20), nullable=False)  # late_fill / restatement
    # pending：待后续批次消费；applied：已并入后续批次；rejected：仅留痕（重新估值）
    status = Column(String(16), default="pending")
    payload_json = Column(Text, nullable=False)
    payload_hash = Column(String(64), nullable=False)
    applied_batch_id = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            "account_id",
            "adjustment_type",
            "payload_hash",
            name="uix_adj_account_type_hash",
        ),
    )
