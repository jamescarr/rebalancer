import logging
import math
import uuid
from datetime import datetime, timedelta, timezone

from celery.exceptions import MaxRetriesExceededError
from sqlalchemy import select

from app.alpaca_client import AlpacaAPIError, get_alpaca_client
from app.celery_app import app
from app.database import SessionLocal
from app.models import (
    TERMINAL_STATUSES,
    TRADE_STATUS_FILLED,
    TRADE_STATUS_PARTIALLY_FILLED,
    TRADE_STATUS_SUBMITTED,
    STATUS_COMPLETED,
    STATUS_EXECUTING_TRADES,
    STATUS_NO_ACTION,
    STATUS_RUNNING,
    STATUS_STUCK,
    RebalanceJob,
    Strategy,
    TradeExecution,
)
from app.strategy_engine import describe_evaluation, evaluate_strategy

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _get_prices(client, symbols: list[str]) -> dict[str, float]:
    """Get current prices for symbols from Alpaca positions or snapshot."""
    positions = client.get_positions()
    prices: dict[str, float] = {}
    for p in positions:
        if p.symbol in symbols:
            prices[p.symbol] = float(p.current_price)
    return prices


def _holdings_value(holdings: dict, prices: dict[str, float]) -> float:
    """Calculate total market value of holdings given current prices."""
    total = 0.0
    for symbol, info in (holdings or {}).items():
        qty = info.get("qty", 0) if isinstance(info, dict) else 0
        price = prices.get(symbol, 0)
        total += qty * price
    return total


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


@app.task
def evaluate_due_strategies() -> None:
    now = _utcnow()
    with SessionLocal() as db:
        strategies = (
            db.execute(
                select(Strategy).where(
                    Strategy.is_active.is_(True),
                    Strategy.funded_amount.isnot(None),
                )
            )
            .scalars()
            .all()
        )
        pending_dispatches: list[tuple[str, str]] = []
        for strategy in strategies:
            if strategy.next_evaluation_at and strategy.next_evaluation_at > now:
                continue

            in_flight = db.execute(
                select(RebalanceJob).where(
                    RebalanceJob.strategy_id == strategy.id,
                    RebalanceJob.status.not_in(list(TERMINAL_STATUSES) + [STATUS_STUCK]),
                )
            ).scalar_one_or_none()
            if in_flight:
                continue

            job = RebalanceJob(
                strategy_id=strategy.id,
                account_id="default",
                target_allocation={},
                drift_threshold=strategy.drift_threshold,
            )
            db.add(job)
            db.flush()
            pending_dispatches.append((str(job.id), str(strategy.id)))
            strategy.next_evaluation_at = now + timedelta(
                seconds=strategy.rebalance_interval_seconds
            )
        db.commit()

    for job_id, strategy_id in pending_dispatches:
        run_strategy_rebalance.delay(job_id, strategy_id)
    logger.info("evaluate_due_strategies: checked %d active strategies", len(strategies))


# ---------------------------------------------------------------------------
# Core rebalance — uses strategy's funded_amount as budget, holdings as positions
# ---------------------------------------------------------------------------


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def run_strategy_rebalance(self, job_id: str, strategy_id: str) -> None:
    job_uuid = uuid.UUID(job_id)
    strategy_uuid = uuid.UUID(strategy_id)

    with SessionLocal() as db:
        job = db.execute(
            select(RebalanceJob).where(RebalanceJob.id == job_uuid)
        ).scalar_one_or_none()
        if job is None:
            logger.error("run_strategy_rebalance: job %s not found", job_id)
            return
        job.status = STATUS_RUNNING
        job.current_task_id = self.request.id
        job.started_at = _utcnow()
        job.attempt_count = (job.attempt_count or 0) + 1
        db.commit()

    try:
        client = get_alpaca_client()

        with SessionLocal() as db:
            strategy = db.execute(
                select(Strategy).where(Strategy.id == strategy_uuid)
            ).scalar_one()
            if not strategy.is_active or not strategy.funded_amount:
                job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
                job.status = STATUS_NO_ACTION
                job.completed_at = _utcnow()
                job.last_error = "Strategy not active or not funded"
                db.commit()
                return
            strategy_type = strategy.strategy_type
            assets = strategy.assets
            config = strategy.config
            drift_threshold = strategy.drift_threshold
            funded_amount = strategy.funded_amount
            holdings = dict(strategy.holdings or {})

        price_changes = None
        if strategy_type in ("momentum", "contrarian_surge"):
            lookback = config.get("lookback_days", 7)
            try:
                price_changes = client.get_crypto_performance(assets, lookback)
            except Exception as exc:
                logger.warning("Price data fetch failed, falling back: %s", exc)

        target_allocation = evaluate_strategy(strategy_type, assets, config, price_changes)
        evaluation_desc = describe_evaluation(strategy_type, target_allocation, price_changes)

        prices = _get_prices(client, assets)

        current_positions: dict[str, float] = {}
        for symbol in assets:
            info = holdings.get(symbol, {})
            qty = info.get("qty", 0) if isinstance(info, dict) else 0
            price = prices.get(symbol, 0)
            current_positions[symbol] = qty * price

        with SessionLocal() as db:
            strategy = db.execute(select(Strategy).where(Strategy.id == strategy_uuid)).scalar_one()
            strategy.current_allocation = target_allocation
            strategy.last_evaluated_at = _utcnow()
            strategy.last_evaluation_result = evaluation_desc
            strategy.next_evaluation_at = _utcnow() + timedelta(seconds=strategy.rebalance_interval_seconds)
            db.commit()

        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
            job.target_allocation = target_allocation
            job.positions_fetched_at = _utcnow()
            db.commit()

        total_value = sum(current_positions.values())
        budget = funded_amount

        if total_value > 0:
            drift: dict[str, float] = {}
            for symbol, target_pct in target_allocation.items():
                current_pct = current_positions.get(symbol, 0.0) / total_value
                drift[symbol] = current_pct - target_pct
            needs_rebalance = any(abs(d) > drift_threshold for d in drift.values())
        else:
            needs_rebalance = True

        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
            job.trades_calculated_at = _utcnow()
            db.commit()

        if not needs_rebalance:
            max_drift = max((abs(d) for d in drift.values()), default=0)
            reason = f"All positions within threshold. Max drift: {max_drift:.1%} (threshold: {drift_threshold:.0%})"
            with SessionLocal() as db:
                job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
                job.status = STATUS_NO_ACTION
                job.completed_at = _utcnow()
                job.last_error = reason
                db.commit()
            logger.info("run_strategy_rebalance: job %s — %s", job_id, reason)
            return

        account = client.get_account()
        available_cash = float(account.cash)

        sells: list[dict] = []
        buys: list[dict] = []
        for symbol in set(target_allocation.keys()) | set(current_positions.keys()):
            cur_val = current_positions.get(symbol, 0.0)
            target_pct = target_allocation.get(symbol, 0.0)
            target_val = target_pct * budget
            diff = target_val - cur_val
            if diff < -10.0:
                qty = min(round(abs(diff), 2), budget)
                sells.append({"symbol": symbol, "side": "sell", "qty": qty})
            elif diff > 10.0:
                qty = min(math.floor(diff * 100) / 100, budget)
                buys.append({"symbol": symbol, "side": "buy", "qty": qty})

        total_buys = sum(t["qty"] for t in buys)
        sell_proceeds = sum(t["qty"] for t in sells)

        max_spend = min(available_cash + sell_proceeds, budget)
        max_spend = math.floor(max_spend * 100) / 100

        if total_buys > max_spend and total_buys > 0:
            scale = max_spend / total_buys
            buys = [
                {**t, "qty": math.floor(t["qty"] * scale * 100) / 100}
                for t in buys if math.floor(t["qty"] * scale * 100) / 100 >= 10.0
            ]

        trades = sells + buys
        if not trades:
            with SessionLocal() as db:
                job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
                job.status = STATUS_NO_ACTION
                job.completed_at = _utcnow()
                job.last_error = f"Trade amounts too small after cash check (${available_cash:.2f} available)"
                db.commit()
            return

        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
            job.status = STATUS_EXECUTING_TRADES
            job.execution_started_at = _utcnow()
            db.commit()

        execute_trades.delay(job_id, str(strategy_id), trades)

    except AlpacaAPIError as exc:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
            if job:
                job.last_error = str(exc)
                db.commit()
        raise self.retry(exc=exc)

    except Exception as exc:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
            if job:
                job.status = STATUS_STUCK
                job.last_error = str(exc)
                job.failed_at = _utcnow()
                db.commit()
        logger.warning("[ALERT] Job %s stuck: %s", job_id, exc)
        raise


# ---------------------------------------------------------------------------
# Trade execution — tracks per-strategy holdings
# ---------------------------------------------------------------------------


@app.task(bind=True, max_retries=2, default_retry_delay=30)
def execute_trades(self, job_id: str, strategy_id: str, trades: list[dict]) -> None:
    job_uuid = uuid.UUID(job_id)
    strategy_uuid = uuid.UUID(strategy_id)
    client = get_alpaca_client()
    order_ids: list[str] = []

    with SessionLocal() as db:
        already_submitted = set(
            row[0] for row in db.execute(
                select(TradeExecution.symbol).where(TradeExecution.job_id == job_uuid)
            ).all()
        )

    try:
        for trade in trades:
            if trade["symbol"] in already_submitted:
                with SessionLocal() as db:
                    existing = db.execute(
                        select(TradeExecution).where(
                            TradeExecution.job_id == job_uuid,
                            TradeExecution.symbol == trade["symbol"],
                        )
                    ).scalar_one_or_none()
                if existing and existing.alpaca_order_id:
                    order_ids.append(existing.alpaca_order_id)
                continue

            order = client.submit_market_order(
                symbol=trade["symbol"], qty=trade["qty"], side=trade["side"],
            )
            execution = TradeExecution(
                job_id=job_uuid, symbol=trade["symbol"], side=trade["side"],
                intended_qty=trade["qty"], alpaca_order_id=str(order.id),
                status=TRADE_STATUS_SUBMITTED, submitted_at=_utcnow(),
            )
            with SessionLocal() as db:
                db.add(execution)
                db.commit()
            order_ids.append(str(order.id))

    except Exception as exc:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
            if job:
                job.status = STATUS_STUCK
                job.last_error = str(exc)
                job.failed_at = _utcnow()
                db.commit()
        logger.warning("[ALERT] Job %s trade execution failed: %s", job_id, exc)
        return

    with SessionLocal() as db:
        job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
        if job:
            job.current_task_id = None
            db.commit()

    poll_order_fills.delay(job_id, str(strategy_uuid), order_ids)


@app.task(bind=True, max_retries=10, default_retry_delay=15)
def poll_order_fills(self, job_id: str, strategy_id: str, order_ids: list[str]) -> None:
    job_uuid = uuid.UUID(job_id)
    strategy_uuid = uuid.UUID(strategy_id)
    client = get_alpaca_client()
    unfilled: list[str] = []

    with SessionLocal() as db:
        for order_id in order_ids:
            order = client.get_order(order_id)
            execution = db.execute(
                select(TradeExecution).where(TradeExecution.alpaca_order_id == order_id)
            ).scalar_one_or_none()

            if execution is not None:
                if order.status in ("filled",):
                    execution.status = TRADE_STATUS_FILLED
                    execution.filled_qty = float(order.filled_qty or 0)
                    execution.filled_avg_price = float(order.filled_avg_price) if order.filled_avg_price else None
                    execution.filled_at = _utcnow()
                elif order.status in ("partially_filled",):
                    execution.status = TRADE_STATUS_PARTIALLY_FILLED
                    execution.filled_qty = float(order.filled_qty or 0)
                    execution.filled_avg_price = float(order.filled_avg_price) if order.filled_avg_price else None
                    unfilled.append(order_id)
                else:
                    unfilled.append(order_id)
        db.commit()

    if unfilled:
        try:
            raise self.retry(countdown=15)
        except MaxRetriesExceededError:
            with SessionLocal() as db:
                job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
                if job:
                    job.status = STATUS_STUCK
                    job.failed_at = _utcnow()
                    db.commit()
            logger.warning("[ALERT] Job %s fill polling exhausted. Unfilled: %s", job_id, unfilled)
            _sync_holdings(strategy_uuid, job_uuid)
            return

    _sync_holdings(strategy_uuid, job_uuid)

    with SessionLocal() as db:
        job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
        if job:
            job.status = STATUS_COMPLETED
            job.completed_at = _utcnow()
            db.commit()

    logger.info("Job %s completed", job_id)


def _sync_holdings(strategy_uuid: uuid.UUID, job_uuid: uuid.UUID) -> None:
    """Update strategy.holdings based on filled trades for this job."""
    with SessionLocal() as db:
        strategy = db.execute(
            select(Strategy).where(Strategy.id == strategy_uuid)
        ).scalar_one_or_none()
        if not strategy:
            return

        holdings = dict(strategy.holdings or {})

        executions = (
            db.execute(
                select(TradeExecution).where(
                    TradeExecution.job_id == job_uuid,
                    TradeExecution.status == TRADE_STATUS_FILLED,
                )
            )
            .scalars()
            .all()
        )

        for ex in executions:
            info = holdings.get(ex.symbol, {"qty": 0, "cost_basis": 0})
            if isinstance(info, (int, float)):
                info = {"qty": info, "cost_basis": 0}

            filled_qty = ex.filled_qty or 0
            filled_price = ex.filled_avg_price or 0

            if ex.side == "buy":
                info["cost_basis"] = info.get("cost_basis", 0) + (filled_qty * filled_price)
                info["qty"] = info.get("qty", 0) + filled_qty
            elif ex.side == "sell":
                old_qty = info.get("qty", 0)
                if old_qty > 0:
                    sell_fraction = min(filled_qty / old_qty, 1.0)
                    info["cost_basis"] = info.get("cost_basis", 0) * (1 - sell_fraction)
                info["qty"] = max(0, old_qty - filled_qty)

            if info["qty"] < 0.000001:
                holdings.pop(ex.symbol, None)
            else:
                holdings[ex.symbol] = info

        strategy.holdings = holdings
        db.commit()


# ---------------------------------------------------------------------------
# Liquidation
# ---------------------------------------------------------------------------


@app.task(bind=True, max_retries=2, default_retry_delay=30)
def liquidate_strategy_holdings(self, job_id: str, strategy_id: str) -> None:
    job_uuid = uuid.UUID(job_id)
    strategy_uuid = uuid.UUID(strategy_id)

    with SessionLocal() as db:
        job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
        if not job:
            return
        job.status = STATUS_RUNNING
        job.started_at = _utcnow()
        db.commit()

    with SessionLocal() as db:
        strategy = db.execute(select(Strategy).where(Strategy.id == strategy_uuid)).scalar_one()
        holdings = dict(strategy.holdings or {})

    if not holdings:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
            job.status = STATUS_COMPLETED
            job.completed_at = _utcnow()
            db.commit()
            strategy = db.execute(select(Strategy).where(Strategy.id == strategy_uuid)).scalar_one()
            strategy.funded_amount = None
            strategy.holdings = {}
            db.commit()
        return

    client = get_alpaca_client()
    order_ids: list[str] = []

    try:
        for symbol, info in holdings.items():
            qty = info.get("qty", 0) if isinstance(info, dict) else 0
            if qty < 0.000001:
                continue

            order = client.submit_qty_market_order(symbol=symbol, qty=qty, side="sell")
            execution = TradeExecution(
                job_id=job_uuid, symbol=symbol, side="sell",
                intended_qty=qty, alpaca_order_id=str(order.id),
                status=TRADE_STATUS_SUBMITTED, submitted_at=_utcnow(),
            )
            with SessionLocal() as db:
                db.add(execution)
                db.commit()
            order_ids.append(str(order.id))

    except Exception as exc:
        with SessionLocal() as db:
            job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one_or_none()
            if job:
                job.status = STATUS_STUCK
                job.last_error = str(exc)
                job.failed_at = _utcnow()
                db.commit()
        logger.warning("[ALERT] Liquidation job %s failed: %s", job_id, exc)
        return

    with SessionLocal() as db:
        job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
        job.status = STATUS_EXECUTING_TRADES
        job.execution_started_at = _utcnow()
        db.commit()

    poll_liquidation_fills.delay(job_id, str(strategy_uuid), order_ids)


@app.task(bind=True, max_retries=10, default_retry_delay=15)
def poll_liquidation_fills(self, job_id: str, strategy_id: str, order_ids: list[str]) -> None:
    job_uuid = uuid.UUID(job_id)
    strategy_uuid = uuid.UUID(strategy_id)
    client = get_alpaca_client()
    unfilled: list[str] = []

    with SessionLocal() as db:
        for order_id in order_ids:
            order = client.get_order(order_id)
            execution = db.execute(
                select(TradeExecution).where(TradeExecution.alpaca_order_id == order_id)
            ).scalar_one_or_none()
            if execution:
                if order.status in ("filled",):
                    execution.status = TRADE_STATUS_FILLED
                    execution.filled_qty = float(order.filled_qty or 0)
                    execution.filled_avg_price = float(order.filled_avg_price) if order.filled_avg_price else None
                    execution.filled_at = _utcnow()
                else:
                    unfilled.append(order_id)
        db.commit()

    if unfilled:
        try:
            raise self.retry(countdown=15)
        except MaxRetriesExceededError:
            pass

    with SessionLocal() as db:
        strategy = db.execute(select(Strategy).where(Strategy.id == strategy_uuid)).scalar_one()
        strategy.holdings = {}
        strategy.funded_amount = None
        strategy.current_allocation = None
        db.commit()

        job = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_uuid)).scalar_one()
        job.status = STATUS_COMPLETED
        job.completed_at = _utcnow()
        db.commit()

    logger.info("Liquidation job %s completed", job_id)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------


@app.task
def find_and_alert_stuck_jobs() -> None:
    cutoff = _utcnow() - timedelta(minutes=30)
    with SessionLocal() as db:
        stuck_jobs = (
            db.execute(
                select(RebalanceJob).where(
                    RebalanceJob.status.not_in(list(TERMINAL_STATUSES)),
                    RebalanceJob.created_at < cutoff,
                )
            ).scalars().all()
        )
        for job in stuck_jobs:
            age = int((_utcnow() - job.created_at).total_seconds() / 60)
            logger.warning("[ALERT] Stuck job — id=%s status=%s age=%dm", job.id, job.status, age)
    logger.info("find_and_alert_stuck_jobs: found %d", len(stuck_jobs))
