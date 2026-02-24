"""Celery tasks — thin orchestration wrappers over domain logic and repositories.

Each task follows the same pattern:
  1. Load state from repositories
  2. Delegate to domain functions for decisions
  3. Call broker for side effects
  4. Persist results via repositories
"""

import logging
import uuid

from celery.exceptions import MaxRetriesExceededError

from app.alpaca_client import AlpacaAPIError, get_alpaca_client
from app.celery_app import app
from app.domain import strategy as strategy_domain
from app.domain import trading
from app.repositories import JobRepository, StrategyRepository, TradeExecutionRepository

logger = logging.getLogger(__name__)

strategies = StrategyRepository()
jobs = JobRepository()
trades = TradeExecutionRepository()


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


@app.task
def evaluate_due_strategies() -> None:
    active = strategies.get_active_funded()
    dispatches: list[tuple[str, str]] = []

    for s in active:
        if s.next_evaluation_at and s.next_evaluation_at > trading._now():
            continue
        if jobs.has_in_flight(s.id):
            continue

        job = jobs.create(s.id, s.drift_threshold)
        dispatches.append((str(job.id), str(s.id)))
        strategies.set_next_evaluation(s.id, s.rebalance_interval_seconds)

    for job_id, strategy_id in dispatches:
        run_strategy_rebalance.delay(job_id, strategy_id)

    logger.info("evaluate_due_strategies: checked %d active strategies", len(active))


# ---------------------------------------------------------------------------
# Rebalance
# ---------------------------------------------------------------------------


@app.task(bind=True, max_retries=3, default_retry_delay=30)
def run_strategy_rebalance(self, job_id: str, strategy_id: str) -> None:
    jid = uuid.UUID(job_id)
    sid = uuid.UUID(strategy_id)

    jobs.mark_running(jid, self.request.id)

    try:
        snap = strategies.snapshot(sid)
        if not snap or not snap["is_active"] or not snap["funded_amount"]:
            jobs.mark_no_action(jid, "Strategy not active or not funded")
            return

        client = get_alpaca_client()

        price_changes = None
        if strategy_domain.needs_price_data(snap["strategy_type"]):
            try:
                lookback = snap["config"].get("lookback_days", 7)
                price_changes = client.get_crypto_performance(snap["assets"], lookback)
            except Exception as exc:
                logger.warning("Price data fetch failed, falling back: %s", exc)

        allocation = strategy_domain.evaluate(
            snap["strategy_type"], snap["assets"], snap["config"], price_changes,
        )
        desc = strategy_domain.describe(snap["strategy_type"], allocation, price_changes)
        strategies.update_evaluation(sid, allocation, desc, snap["rebalance_interval_seconds"])

        prices = _get_prices(client, snap["assets"])
        current = trading.holdings_to_positions(snap["holdings"], prices, snap["assets"])

        jobs.set_target_allocation(jid, allocation)

        total_value = sum(current.values())
        if total_value > 0:
            drift = trading.calculate_drift(current, allocation)
            if not trading.needs_rebalancing(drift, snap["drift_threshold"]):
                md = trading.max_drift(drift)
                jobs.set_trades_calculated(jid)
                jobs.mark_no_action(
                    jid, f"Within threshold. Max drift: {md:.1%} (threshold: {snap['drift_threshold']:.0%})",
                )
                return

        jobs.set_trades_calculated(jid)

        cash = float(client.get_account().cash)
        planned = trading.plan_trades(current, allocation, snap["funded_amount"], cash)

        if not planned:
            jobs.mark_no_action(jid, f"Trades too small after cash check (${cash:.2f} available)")
            return

        jobs.mark_executing(jid)
        execute_trades.delay(job_id, strategy_id, planned)

    except AlpacaAPIError as exc:
        jobs.set_error(jid, str(exc))
        raise self.retry(exc=exc)

    except Exception as exc:
        jobs.mark_stuck(jid, str(exc))
        logger.warning("[ALERT] Job %s stuck: %s", job_id, exc)
        raise


# ---------------------------------------------------------------------------
# Trade execution
# ---------------------------------------------------------------------------


@app.task(bind=True, max_retries=2, default_retry_delay=30)
def execute_trades(self, job_id: str, strategy_id: str, planned: list[dict]) -> None:
    jid = uuid.UUID(job_id)
    sid = uuid.UUID(strategy_id)
    client = get_alpaca_client()
    order_ids: list[str] = []

    already = trades.symbols_for_job(jid)

    try:
        for t in planned:
            if t["symbol"] in already:
                oid = trades.get_order_id(jid, t["symbol"])
                if oid:
                    order_ids.append(oid)
                continue

            order = client.submit_market_order(symbol=t["symbol"], qty=t["qty"], side=t["side"])
            oid = str(order.id)
            trades.record_submission(jid, t["symbol"], t["side"], t["qty"], oid)
            order_ids.append(oid)

    except Exception as exc:
        jobs.mark_stuck(jid, str(exc))
        logger.warning("[ALERT] Job %s trade execution failed: %s", job_id, exc)
        return

    jobs.clear_task_id(jid)
    poll_order_fills.delay(job_id, strategy_id, order_ids)


# ---------------------------------------------------------------------------
# Fill polling (shared by rebalance and liquidation)
# ---------------------------------------------------------------------------


@app.task(bind=True, max_retries=10, default_retry_delay=15)
def poll_order_fills(self, job_id: str, strategy_id: str, order_ids: list[str]) -> None:
    jid = uuid.UUID(job_id)
    sid = uuid.UUID(strategy_id)
    client = get_alpaca_client()
    unfilled: list[str] = []

    for oid in order_ids:
        order = client.get_order(oid)
        status = str(order.status).lower()
        fq = float(order.filled_qty or 0)
        fp = float(order.filled_avg_price) if order.filled_avg_price else None

        if "filled" in status and "partially" not in status:
            trades.update_fill(oid, "filled", fq, fp)
        elif "partially" in status:
            trades.update_fill(oid, "partially_filled", fq, fp)
            unfilled.append(oid)
        else:
            unfilled.append(oid)

    if unfilled:
        try:
            raise self.retry(countdown=15)
        except MaxRetriesExceededError:
            jobs.mark_stuck(jid, f"Fill polling exhausted. Unfilled: {unfilled}")
            _sync_holdings(sid, jid)
            return

    _sync_holdings(sid, jid)
    jobs.mark_completed(jid)
    logger.info("Job %s completed", job_id)


def _sync_holdings(strategy_id: uuid.UUID, job_id: uuid.UUID) -> None:
    snap = strategies.snapshot(strategy_id)
    if not snap:
        return
    fills = trades.get_fills_for_job(job_id)
    updated = trading.apply_fills_to_holdings(snap["holdings"], fills)
    strategies.update_holdings(strategy_id, updated)


# ---------------------------------------------------------------------------
# Liquidation
# ---------------------------------------------------------------------------


@app.task(bind=True, max_retries=2, default_retry_delay=30)
def liquidate_strategy_holdings(self, job_id: str, strategy_id: str) -> None:
    jid = uuid.UUID(job_id)
    sid = uuid.UUID(strategy_id)

    jobs.mark_running(jid, self.request.id or "")

    snap = strategies.snapshot(sid)
    if not snap or not snap["holdings"]:
        strategies.clear_for_liquidation(sid)
        jobs.mark_completed(jid)
        return

    client = get_alpaca_client()
    order_ids: list[str] = []

    try:
        for symbol, info in snap["holdings"].items():
            qty = info.get("qty", 0) if isinstance(info, dict) else 0
            if qty < 1e-6:
                continue
            order = client.submit_qty_market_order(symbol=symbol, qty=qty, side="sell")
            oid = str(order.id)
            trades.record_submission(jid, symbol, "sell", qty, oid)
            order_ids.append(oid)

    except Exception as exc:
        jobs.mark_stuck(jid, str(exc))
        logger.warning("[ALERT] Liquidation %s failed: %s", job_id, exc)
        return

    jobs.mark_executing(jid)
    poll_liquidation_fills.delay(job_id, strategy_id, order_ids)


@app.task(bind=True, max_retries=10, default_retry_delay=15)
def poll_liquidation_fills(self, job_id: str, strategy_id: str, order_ids: list[str]) -> None:
    jid = uuid.UUID(job_id)
    sid = uuid.UUID(strategy_id)
    client = get_alpaca_client()
    unfilled: list[str] = []

    for oid in order_ids:
        order = client.get_order(oid)
        fq = float(order.filled_qty or 0)
        fp = float(order.filled_avg_price) if order.filled_avg_price else None
        if str(order.status).lower() == "filled":
            trades.update_fill(oid, "filled", fq, fp)
        else:
            unfilled.append(oid)

    if unfilled:
        try:
            raise self.retry(countdown=15)
        except MaxRetriesExceededError:
            pass

    strategies.clear_for_liquidation(sid)
    jobs.mark_completed(jid)
    logger.info("Liquidation %s completed", job_id)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------


@app.task
def find_and_alert_stuck_jobs() -> None:
    from datetime import timedelta
    from sqlalchemy import select
    from app.database import SessionLocal
    from app.models import TERMINAL_STATUSES, RebalanceJob

    cutoff = trading._now() - timedelta(minutes=30)
    with SessionLocal() as db:
        stuck = db.execute(
            select(RebalanceJob).where(
                RebalanceJob.status.not_in(list(TERMINAL_STATUSES)),
                RebalanceJob.created_at < cutoff,
            )
        ).scalars().all()
        for j in stuck:
            age = int((trading._now() - j.created_at).total_seconds() / 60)
            logger.warning("[ALERT] Stuck job — id=%s status=%s age=%dm", j.id, j.status, age)
    logger.info("find_and_alert_stuck_jobs: found %d", len(stuck))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_prices(client, symbols: list[str]) -> dict[str, float]:
    positions = client.get_positions()
    return {p.symbol: float(p.current_price) for p in positions if p.symbol in symbols}
