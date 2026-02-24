import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database import Base

# ---------------------------------------------------------------------------
# Strategy types
# ---------------------------------------------------------------------------
STRATEGY_FIXED_WEIGHT = "fixed_weight"
STRATEGY_EQUAL_WEIGHT = "equal_weight"
STRATEGY_MOMENTUM = "momentum"
STRATEGY_CONTRARIAN_SURGE = "contrarian_surge"

STRATEGY_TYPES = {STRATEGY_FIXED_WEIGHT, STRATEGY_EQUAL_WEIGHT, STRATEGY_MOMENTUM, STRATEGY_CONTRARIAN_SURGE}

# ---------------------------------------------------------------------------
# RebalanceJob status constants
# ---------------------------------------------------------------------------
STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_NO_ACTION = "no_action"
STATUS_EXECUTING_TRADES = "executing_trades"
STATUS_PARTIALLY_EXECUTED = "partially_executed"
STATUS_COMPLETED = "completed"
STATUS_NOTIFICATION_FAILED = "notification_failed"
STATUS_STUCK = "stuck"
STATUS_CANCELLING = "cancelling"
STATUS_CANCELLED = "cancelled"
STATUS_REFUNDED = "refunded"

TERMINAL_STATUSES = {
    STATUS_COMPLETED,
    STATUS_NO_ACTION,
    STATUS_CANCELLED,
    STATUS_REFUNDED,
}

# ---------------------------------------------------------------------------
# TradeExecution status constants
# ---------------------------------------------------------------------------
TRADE_STATUS_PENDING = "pending"
TRADE_STATUS_SUBMITTED = "submitted"
TRADE_STATUS_FILLED = "filled"
TRADE_STATUS_PARTIALLY_FILLED = "partially_filled"
TRADE_STATUS_FAILED = "failed"
TRADE_STATUS_CANCELLED = "cancelled"


class Strategy(Base):
    __tablename__ = "strategies"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_type: Mapped[str] = mapped_column(
        String, nullable=False, default=STRATEGY_FIXED_WEIGHT
    )
    assets: Mapped[list] = mapped_column(JSON, nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    rebalance_interval_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, default=86400
    )
    drift_threshold: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.05
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    funded_amount: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )
    holdings: Mapped[dict | None] = mapped_column(
        JSON, nullable=True, default=dict
    )
    last_evaluated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    next_evaluation_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    current_allocation: Mapped[dict | None] = mapped_column(
        JSON, nullable=True
    )
    last_evaluation_result: Mapped[str | None] = mapped_column(
        Text, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    rebalance_jobs: Mapped[list["RebalanceJob"]] = relationship(
        "RebalanceJob", back_populates="strategy"
    )


class RebalanceJob(Base):
    __tablename__ = "rebalance_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    strategy_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategies.id"), nullable=True
    )
    account_id: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=STATUS_PENDING
    )
    target_allocation: Mapped[dict] = mapped_column(JSON, nullable=False)
    drift_threshold: Mapped[float] = mapped_column(Float, nullable=False, default=0.05)
    current_task_id: Mapped[str | None] = mapped_column(String, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    positions_fetched_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    trades_calculated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    execution_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    strategy: Mapped["Strategy | None"] = relationship(
        "Strategy", back_populates="rebalance_jobs"
    )
    trade_executions: Mapped[list["TradeExecution"]] = relationship(
        "TradeExecution", back_populates="job"
    )


class TradeExecution(Base):
    __tablename__ = "trade_executions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("rebalance_jobs.id"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    side: Mapped[str] = mapped_column(String, nullable=False)
    intended_qty: Mapped[float] = mapped_column(Float, nullable=False)
    alpaca_order_id: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default=TRADE_STATUS_PENDING
    )
    filled_qty: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_avg_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    filled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    job: Mapped["RebalanceJob"] = relationship(
        "RebalanceJob", back_populates="trade_executions"
    )
