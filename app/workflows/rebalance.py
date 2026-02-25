"""RebalanceWorkflow: single durable workflow replacing the entire Celery task chain.

Evaluates strategy, plans trades, submits orders, polls fills, syncs holdings.
Worker crashes replay from the last checkpoint. Failed orders are skipped, not retried.
"""

from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from app.activities.broker import (
        check_order_status,
        fetch_account_cash,
        fetch_crypto_performance,
        fetch_positions,
        submit_notional_order,
    )
    from app.activities.persistence import (
        get_fills_for_job,
        load_strategy_snapshot,
        mark_job_completed,
        mark_job_executing,
        mark_job_no_action,
        mark_job_running,
        mark_job_stuck,
        record_trade_submission,
        set_job_target_allocation,
        set_job_trades_calculated,
        update_strategy_evaluation,
        update_strategy_holdings,
        update_trade_fill,
    )
    from app.domain import strategy as strategy_domain
    from app.domain import trading


BROKER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
    non_retryable_error_types=["InsufficientFunds", "InvalidSymbol", "InvalidInput"],
)

DB_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3)

BROKER_TIMEOUT = timedelta(seconds=30)
DB_TIMEOUT = timedelta(seconds=10)
FILL_POLL_INTERVAL = 15
FILL_POLL_DEADLINE = timedelta(minutes=5)


@dataclass
class RebalanceInput:
    job_id: str
    strategy_id: str


@dataclass
class RebalanceStatus:
    state: str = "initializing"
    submitted_count: int = 0
    filled_count: int = 0
    skipped_count: int = 0
    error: str | None = None


@workflow.defn(name="RebalanceWorkflow")
class RebalanceWorkflow:

    def __init__(self) -> None:
        self.status = RebalanceStatus()
        self._submitted_orders: list[str] = []
        self._filled_orders: set[str] = set()

    @workflow.query
    def get_status(self) -> dict:
        return {
            "state": self.status.state,
            "submitted": self.status.submitted_count,
            "filled": self.status.filled_count,
            "skipped": self.status.skipped_count,
            "error": self.status.error,
        }

    async def _broker(self, activity_fn, *args):
        return await workflow.execute_activity(
            activity_fn, *args,
            start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
        )

    async def _db(self, activity_fn, *args):
        return await workflow.execute_activity(
            activity_fn, *args,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

    @workflow.run
    async def run(self, input: RebalanceInput) -> str:
        jid, sid = input.job_id, input.strategy_id

        await self._db(mark_job_running, jid)

        self.status.state = "loading_strategy"
        snap = await self._db(load_strategy_snapshot, sid)
        if not snap or not snap["is_active"] or not snap["funded_amount"]:
            await self._db(mark_job_no_action, args=[jid, "Strategy not active or not funded"])
            return "no_action"

        self.status.state = "evaluating"
        allocation, desc = await self._evaluate(snap)

        await self._db(update_strategy_evaluation, args=[sid, allocation, desc, snap["rebalance_interval_seconds"]])

        self.status.state = "fetching_positions"
        prices = await self._broker(fetch_positions)
        current = trading.holdings_to_positions(snap["holdings"], prices, snap["assets"])
        await self._db(set_job_target_allocation, args=[jid, allocation])

        self.status.state = "calculating_drift"
        total_value = sum(current.values())
        if total_value > 0:
            drift = trading.calculate_drift(current, allocation)
            if not trading.needs_rebalancing(drift, snap["drift_threshold"]):
                md = trading.max_drift(drift)
                await self._db(set_job_trades_calculated, jid)
                await self._db(mark_job_no_action, args=[jid, f"Within threshold. Max drift: {md:.1%} (threshold: {snap['drift_threshold']:.0%})"])
                return "no_action"

        await self._db(set_job_trades_calculated, jid)

        self.status.state = "planning_trades"
        cash = await self._broker(fetch_account_cash)
        planned = trading.plan_trades(current, allocation, snap["funded_amount"], cash)
        if not planned:
            await self._db(mark_job_no_action, args=[jid, f"Trades too small (${cash:.2f} cash)"])
            return "no_action"

        self.status.state = "executing_trades"
        await self._db(mark_job_executing, jid)
        await self._submit_orders(jid, planned)

        if not self._submitted_orders:
            self.status.error = "All submissions failed"
            await self._db(mark_job_stuck, args=[jid, self.status.error])
            return "stuck"

        self.status.state = "polling_fills"
        await self._poll_fills(jid)

        self.status.state = "syncing_holdings"
        fills = await self._db(get_fills_for_job, jid)
        updated = trading.apply_fills_to_holdings(snap["holdings"], fills)
        await self._db(update_strategy_holdings, args=[sid, updated])

        await self._db(mark_job_completed, jid)
        self.status.state = "completed"
        return "completed"

    async def _evaluate(self, snap: dict) -> tuple[dict, str]:
        price_changes = None
        if strategy_domain.needs_price_data(snap["strategy_type"]):
            try:
                price_changes = await self._broker(
                    fetch_crypto_performance,
                    args=[snap["assets"], snap["config"].get("lookback_days", 7)],
                )
            except ActivityError:
                workflow.logger.warning("Price data unavailable, using fallback")

        allocation = strategy_domain.evaluate(
            snap["strategy_type"], snap["assets"], snap["config"], price_changes,
        )
        desc = strategy_domain.describe(snap["strategy_type"], allocation, price_changes)
        return allocation, desc

    async def _submit_orders(self, jid: str, planned: list[dict]) -> None:
        for t in planned:
            try:
                result = await self._broker(
                    submit_notional_order,
                    args=[t["symbol"], t["qty"], t["side"]],
                )
                await self._db(
                    record_trade_submission,
                    args=[jid, t["symbol"], t["side"], t["qty"], result.order_id],
                )
                self._submitted_orders.append(result.order_id)
                self.status.submitted_count += 1
            except ActivityError:
                self.status.skipped_count += 1
                workflow.logger.warning("Skipped %s %s $%.2f", t["side"], t["symbol"], t["qty"])

    async def _poll_fills(self, jid: str) -> None:
        deadline = workflow.now() + FILL_POLL_DEADLINE
        while True:
            pending = [oid for oid in self._submitted_orders if oid not in self._filled_orders]
            if not pending:
                break

            for oid in pending:
                status = await self._broker(check_order_status, oid)
                if "filled" in status["status"] and "partially" not in status["status"]:
                    await self._db(
                        update_trade_fill,
                        args=[oid, "filled", status["filled_qty"], status["filled_avg_price"]],
                    )
                    self._filled_orders.add(oid)
                    self.status.filled_count += 1

            if len(self._filled_orders) == len(self._submitted_orders):
                break
            if workflow.now() > deadline:
                unfilled = [o for o in self._submitted_orders if o not in self._filled_orders]
                self.status.error = f"Fill timeout. Unfilled: {unfilled}"
                await self._db(mark_job_stuck, args=[jid, self.status.error])
                break

            await workflow.sleep(FILL_POLL_INTERVAL)
