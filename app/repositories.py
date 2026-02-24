"""Repository classes encapsulating all database access patterns.

Each method is a complete unit of work: open session, do work, commit, close.
Callers never touch SQLAlchemy directly.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import (
    TERMINAL_STATUSES,
    STATUS_STUCK,
    TRADE_STATUS_FILLED,
    RebalanceJob,
    Strategy,
    TradeExecution,
)


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class StrategyRepository:

    def get(self, strategy_id: uuid.UUID) -> Strategy | None:
        with SessionLocal() as db:
            return db.execute(
                select(Strategy).where(Strategy.id == strategy_id)
            ).scalar_one_or_none()

    def get_active_funded(self) -> list[Strategy]:
        with SessionLocal() as db:
            return list(
                db.execute(
                    select(Strategy).where(
                        Strategy.is_active.is_(True),
                        Strategy.funded_amount.isnot(None),
                    )
                ).scalars().all()
            )

    def snapshot(self, strategy_id: uuid.UUID) -> dict | None:
        """Return a plain dict snapshot of strategy fields (detached from session)."""
        with SessionLocal() as db:
            s = db.execute(
                select(Strategy).where(Strategy.id == strategy_id)
            ).scalar_one_or_none()
            if s is None:
                return None
            return {
                "id": s.id,
                "strategy_type": s.strategy_type,
                "assets": list(s.assets),
                "config": dict(s.config),
                "drift_threshold": s.drift_threshold,
                "funded_amount": s.funded_amount,
                "holdings": dict(s.holdings or {}),
                "is_active": s.is_active,
                "rebalance_interval_seconds": s.rebalance_interval_seconds,
            }

    def update_evaluation(
        self, strategy_id: uuid.UUID, allocation: dict, description: str, interval_seconds: int,
    ) -> None:
        with SessionLocal() as db:
            s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one()
            s.current_allocation = allocation
            s.last_evaluated_at = _now()
            s.last_evaluation_result = description
            s.next_evaluation_at = _now() + timedelta(seconds=interval_seconds)
            db.commit()

    def set_next_evaluation(self, strategy_id: uuid.UUID, interval_seconds: int) -> None:
        with SessionLocal() as db:
            s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one()
            s.next_evaluation_at = _now() + timedelta(seconds=interval_seconds)
            db.commit()

    def update_holdings(self, strategy_id: uuid.UUID, holdings: dict) -> None:
        with SessionLocal() as db:
            s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one()
            s.holdings = holdings
            db.commit()

    def clear_for_liquidation(self, strategy_id: uuid.UUID) -> None:
        with SessionLocal() as db:
            s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one()
            s.holdings = {}
            s.funded_amount = None
            s.current_allocation = None
            db.commit()


class JobRepository:

    def create(self, strategy_id: uuid.UUID, drift_threshold: float) -> RebalanceJob:
        with SessionLocal() as db:
            job = RebalanceJob(
                strategy_id=strategy_id,
                account_id="default",
                target_allocation={},
                drift_threshold=drift_threshold,
            )
            db.add(job)
            db.commit()
            db.refresh(job)
            return job

    def has_in_flight(self, strategy_id: uuid.UUID) -> bool:
        with SessionLocal() as db:
            return db.execute(
                select(RebalanceJob).where(
                    RebalanceJob.strategy_id == strategy_id,
                    RebalanceJob.status.not_in(list(TERMINAL_STATUSES) + [STATUS_STUCK]),
                )
            ).scalar_one_or_none() is not None

    def mark_running(self, job_id: uuid.UUID, task_id: str) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one()
            job.status = "running"
            job.current_task_id = task_id
            job.started_at = _now()
            job.attempt_count = (job.attempt_count or 0) + 1
            db.commit()

    def mark_no_action(self, job_id: uuid.UUID, reason: str = "") -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one()
            job.status = "no_action"
            job.completed_at = _now()
            job.last_error = reason
            db.commit()

    def mark_executing(self, job_id: uuid.UUID) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one()
            job.status = "executing_trades"
            job.execution_started_at = _now()
            db.commit()

    def mark_completed(self, job_id: uuid.UUID) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one()
            job.status = "completed"
            job.completed_at = _now()
            db.commit()

    def mark_stuck(self, job_id: uuid.UUID, error: str) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one()
            job.status = "stuck"
            job.last_error = error
            job.failed_at = _now()
            db.commit()

    def set_target_allocation(self, job_id: uuid.UUID, allocation: dict) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one()
            job.target_allocation = allocation
            job.positions_fetched_at = _now()
            db.commit()

    def set_trades_calculated(self, job_id: uuid.UUID) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one()
            job.trades_calculated_at = _now()
            db.commit()

    def set_error(self, job_id: uuid.UUID, error: str) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one_or_none()
            if job:
                job.last_error = error
                db.commit()

    def clear_task_id(self, job_id: uuid.UUID) -> None:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one_or_none()
            if job:
                job.current_task_id = None
                db.commit()


class TradeExecutionRepository:

    def symbols_for_job(self, job_id: uuid.UUID) -> set[str]:
        with SessionLocal() as db:
            rows = db.execute(
                select(TradeExecution.symbol).where(TradeExecution.job_id == job_id)
            ).all()
            return {r[0] for r in rows}

    def get_order_id(self, job_id: uuid.UUID, symbol: str) -> str | None:
        with SessionLocal() as db:
            ex = db.execute(
                select(TradeExecution).where(
                    TradeExecution.job_id == job_id,
                    TradeExecution.symbol == symbol,
                )
            ).scalar_one_or_none()
            return ex.alpaca_order_id if ex else None

    def record_submission(
        self, job_id: uuid.UUID, symbol: str, side: str, qty: float, order_id: str,
    ) -> None:
        with SessionLocal() as db:
            db.add(TradeExecution(
                job_id=job_id, symbol=symbol, side=side,
                intended_qty=qty, alpaca_order_id=order_id,
                status="submitted", submitted_at=_now(),
            ))
            db.commit()

    def update_fill(self, order_id: str, status: str, filled_qty: float, avg_price: float | None) -> None:
        with SessionLocal() as db:
            ex = db.execute(
                select(TradeExecution).where(TradeExecution.alpaca_order_id == order_id)
            ).scalar_one_or_none()
            if ex:
                ex.status = status
                ex.filled_qty = filled_qty
                ex.filled_avg_price = avg_price
                if status == "filled":
                    ex.filled_at = _now()
                db.commit()

    def get_fills_for_job(self, job_id: uuid.UUID) -> list[dict]:
        with SessionLocal() as db:
            execs = db.execute(
                select(TradeExecution).where(
                    TradeExecution.job_id == job_id,
                    TradeExecution.status == TRADE_STATUS_FILLED,
                )
            ).scalars().all()
            return [
                {
                    "symbol": ex.symbol,
                    "side": ex.side,
                    "filled_qty": ex.filled_qty or 0,
                    "filled_avg_price": ex.filled_avg_price or 0,
                }
                for ex in execs
            ]
