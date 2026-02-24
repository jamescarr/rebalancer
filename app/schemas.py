import uuid
from datetime import datetime

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------


class CreateStrategyRequest(BaseModel):
    name: str
    description: str | None = None
    strategy_type: str = Field(description="One of: fixed_weight, equal_weight, momentum")
    assets: list[str] = Field(description="List of crypto symbols, e.g. ['BTCUSD', 'ETHUSD']")
    config: dict = Field(default_factory=dict)
    rebalance_interval_seconds: int = Field(default=86400, ge=10)
    drift_threshold: float = Field(default=0.05, ge=0.0, le=1.0)


class UpdateStrategyRequest(BaseModel):
    name: str | None = None
    description: str | None = None
    assets: list[str] | None = None
    rebalance_interval_seconds: int | None = Field(default=None, ge=10)
    drift_threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    config: dict | None = None


class FundStrategyRequest(BaseModel):
    amount: float = Field(gt=0, description="Dollar amount to fund this strategy with")


class StrategyResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: str | None
    strategy_type: str
    assets: list[str]
    config: dict
    rebalance_interval_seconds: int
    drift_threshold: float
    is_active: bool
    funded_amount: float | None
    holdings: dict | None
    last_evaluated_at: datetime | None
    next_evaluation_at: datetime | None
    current_allocation: dict | None
    last_evaluation_result: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# RebalanceJob
# ---------------------------------------------------------------------------


class TriggerRebalanceRequest(BaseModel):
    strategy_id: uuid.UUID


class RebalanceJobResponse(BaseModel):
    job_id: uuid.UUID = Field(validation_alias="id")
    status: str

    model_config = {"from_attributes": True}


class TradeExecutionResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    symbol: str
    side: str
    intended_qty: float
    alpaca_order_id: str | None
    status: str
    filled_qty: float | None
    filled_avg_price: float | None
    submitted_at: datetime | None
    filled_at: datetime | None
    error_message: str | None

    model_config = {"from_attributes": True}


class RebalanceJobDetailResponse(BaseModel):
    id: uuid.UUID
    strategy_id: uuid.UUID | None
    account_id: str
    status: str
    target_allocation: dict[str, float]
    drift_threshold: float
    attempt_count: int
    last_error: str | None
    current_task_id: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    positions_fetched_at: datetime | None
    trades_calculated_at: datetime | None
    execution_started_at: datetime | None
    trade_executions: list[TradeExecutionResponse]

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    active_tasks: list[dict] | None
