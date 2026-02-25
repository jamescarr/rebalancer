"""Persistence activities: all database reads/writes as individual activities.

Each activity owns its own session lifecycle via the repository layer.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from temporalio import activity

from app.repositories import JobRepository, StrategyRepository, TradeExecutionRepository

strategies = StrategyRepository()
jobs = JobRepository()
trades = TradeExecutionRepository()


@activity.defn
async def load_strategy_snapshot(strategy_id: str) -> dict | None:
    return strategies.snapshot(uuid.UUID(strategy_id))


@activity.defn
async def update_strategy_evaluation(
    strategy_id: str, allocation: dict, description: str, interval_seconds: int,
) -> None:
    strategies.update_evaluation(uuid.UUID(strategy_id), allocation, description, interval_seconds)


@activity.defn
async def update_strategy_holdings(strategy_id: str, holdings: dict) -> None:
    strategies.update_holdings(uuid.UUID(strategy_id), holdings)


@activity.defn
async def clear_strategy_for_liquidation(strategy_id: str) -> None:
    strategies.clear_for_liquidation(uuid.UUID(strategy_id))


@activity.defn
async def create_job(strategy_id: str, drift_threshold: float) -> str:
    job = jobs.create(uuid.UUID(strategy_id), drift_threshold)
    return str(job.id)


@activity.defn
async def mark_job_running(job_id: str) -> None:
    jobs.mark_running(uuid.UUID(job_id), "temporal-workflow")


@activity.defn
async def mark_job_no_action(job_id: str, reason: str) -> None:
    jobs.mark_no_action(uuid.UUID(job_id), reason)


@activity.defn
async def mark_job_executing(job_id: str) -> None:
    jobs.mark_executing(uuid.UUID(job_id))


@activity.defn
async def mark_job_completed(job_id: str) -> None:
    jobs.mark_completed(uuid.UUID(job_id))


@activity.defn
async def mark_job_stuck(job_id: str, error: str) -> None:
    jobs.mark_stuck(uuid.UUID(job_id), error)


@activity.defn
async def set_job_target_allocation(job_id: str, allocation: dict) -> None:
    jobs.set_target_allocation(uuid.UUID(job_id), allocation)


@activity.defn
async def set_job_trades_calculated(job_id: str) -> None:
    jobs.set_trades_calculated(uuid.UUID(job_id))


@activity.defn
async def record_trade_submission(
    job_id: str, symbol: str, side: str, qty: float, order_id: str,
) -> None:
    trades.record_submission(uuid.UUID(job_id), symbol, side, qty, order_id)


@activity.defn
async def update_trade_fill(
    order_id: str, status: str, filled_qty: float, avg_price: float | None,
) -> None:
    trades.update_fill(order_id, status, filled_qty, avg_price)


@activity.defn
async def get_fills_for_job(job_id: str) -> list[dict]:
    return trades.get_fills_for_job(uuid.UUID(job_id))


@dataclass
class ActiveStrategy:
    id: str
    drift_threshold: float
    rebalance_interval_seconds: int
    next_evaluation_at: str | None


@activity.defn
async def get_due_strategies() -> list[ActiveStrategy]:
    """Return active+funded strategies past their next evaluation time.

    No in-flight check here. Temporal's workflow ID deduplication prevents
    duplicate rebalances (the scheduler uses a stable workflow ID per strategy).
    """
    now = datetime.now(tz=timezone.utc)
    active = strategies.get_active_funded()
    due: list[ActiveStrategy] = []
    for s in active:
        if s.next_evaluation_at and s.next_evaluation_at > now:
            continue
        due.append(ActiveStrategy(
            id=str(s.id),
            drift_threshold=s.drift_threshold,
            rebalance_interval_seconds=s.rebalance_interval_seconds,
            next_evaluation_at=s.next_evaluation_at.isoformat() if s.next_evaluation_at else None,
        ))
    return due


@activity.defn
async def set_strategy_next_evaluation(strategy_id: str, interval_seconds: int) -> None:
    strategies.set_next_evaluation(uuid.UUID(strategy_id), interval_seconds)
