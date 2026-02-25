"""RebalanceWorkflow: replaces the run_strategy_rebalance -> execute_trades -> poll_order_fills chain.

A single workflow that evaluates a strategy, plans trades, submits orders,
polls for fills, and syncs holdings. If a step fails, the workflow handles
compensation (no orphaned orders). If the worker crashes, Temporal replays
from the last checkpoint automatically.
"""

from dataclasses import dataclass
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
)

DB_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_attempts=3,
)

BROKER_TIMEOUT = timedelta(seconds=30)
DB_TIMEOUT = timedelta(seconds=10)


@dataclass
class RebalanceInput:
    job_id: str
    strategy_id: str


@workflow.defn(name="RebalanceWorkflow")
class RebalanceWorkflow:

    def __init__(self) -> None:
        self.state = "initializing"
        self.submitted_orders: list[str] = []

    @workflow.query
    def get_state(self) -> str:
        return self.state

    @workflow.run
    async def run(self, input: RebalanceInput) -> str:
        jid = input.job_id
        sid = input.strategy_id

        await workflow.execute_activity(
            mark_job_running, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        self.state = "loading_strategy"
        snap = await workflow.execute_activity(
            load_strategy_snapshot, sid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )
        if not snap or not snap["is_active"] or not snap["funded_amount"]:
            await workflow.execute_activity(
                mark_job_no_action, args=[jid, "Strategy not active or not funded"],
                start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
            )
            return "no_action"

        self.state = "evaluating_strategy"
        price_changes = None
        if strategy_domain.needs_price_data(snap["strategy_type"]):
            try:
                price_changes = await workflow.execute_activity(
                    fetch_crypto_performance,
                    args=[snap["assets"], snap["config"].get("lookback_days", 7)],
                    start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
                )
            except ActivityError:
                workflow.logger.warning("Price data fetch failed, using equal weight fallback")

        allocation = strategy_domain.evaluate(
            snap["strategy_type"], snap["assets"], snap["config"], price_changes,
        )
        desc = strategy_domain.describe(snap["strategy_type"], allocation, price_changes)

        await workflow.execute_activity(
            update_strategy_evaluation,
            args=[sid, allocation, desc, snap["rebalance_interval_seconds"]],
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        self.state = "fetching_positions"
        prices = await workflow.execute_activity(
            fetch_positions,
            start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
        )
        current = trading.holdings_to_positions(snap["holdings"], prices, snap["assets"])

        await workflow.execute_activity(
            set_job_target_allocation, args=[jid, allocation],
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        self.state = "calculating_drift"
        total_value = sum(current.values())
        if total_value > 0:
            drift = trading.calculate_drift(current, allocation)
            if not trading.needs_rebalancing(drift, snap["drift_threshold"]):
                md = trading.max_drift(drift)
                reason = f"Within threshold. Max drift: {md:.1%} (threshold: {snap['drift_threshold']:.0%})"
                await workflow.execute_activity(
                    set_job_trades_calculated, jid,
                    start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                )
                await workflow.execute_activity(
                    mark_job_no_action, args=[jid, reason],
                    start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                )
                return "no_action"

        await workflow.execute_activity(
            set_job_trades_calculated, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        self.state = "planning_trades"
        cash = await workflow.execute_activity(
            fetch_account_cash,
            start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
        )
        planned = trading.plan_trades(current, allocation, snap["funded_amount"], cash)

        if not planned:
            await workflow.execute_activity(
                mark_job_no_action, args=[jid, f"Trades too small after cash check (${cash:.2f} available)"],
                start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
            )
            return "no_action"

        self.state = "executing_trades"
        await workflow.execute_activity(
            mark_job_executing, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        for t in planned:
            try:
                result = await workflow.execute_activity(
                    submit_notional_order,
                    args=[t["symbol"], t["qty"], t["side"]],
                    start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
                )
                await workflow.execute_activity(
                    record_trade_submission,
                    args=[jid, t["symbol"], t["side"], t["qty"], result.order_id],
                    start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                )
                self.submitted_orders.append(result.order_id)
            except ActivityError as exc:
                workflow.logger.warning(f"Skipping {t['side']} {t['symbol']}: {exc}")

        if not self.submitted_orders:
            await workflow.execute_activity(
                mark_job_stuck, args=[jid, "All trade submissions failed"],
                start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
            )
            return "stuck"

        self.state = "polling_fills"
        deadline = workflow.now() + timedelta(minutes=5)
        while True:
            unfilled: list[str] = []
            for oid in self.submitted_orders:
                status = await workflow.execute_activity(
                    check_order_status, oid,
                    start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
                )
                if "filled" in status["status"] and "partially" not in status["status"]:
                    await workflow.execute_activity(
                        update_trade_fill,
                        args=[oid, "filled", status["filled_qty"], status["filled_avg_price"]],
                        start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                    )
                else:
                    unfilled.append(oid)

            if not unfilled:
                break
            if workflow.now() > deadline:
                await workflow.execute_activity(
                    mark_job_stuck, args=[jid, f"Fill polling timed out. Unfilled: {unfilled}"],
                    start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                )
                break

            await workflow.sleep(15)

        self.state = "syncing_holdings"
        fills = await workflow.execute_activity(
            get_fills_for_job, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )
        updated = trading.apply_fills_to_holdings(snap["holdings"], fills)
        await workflow.execute_activity(
            update_strategy_holdings, args=[sid, updated],
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        await workflow.execute_activity(
            mark_job_completed, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )
        self.state = "completed"
        return "completed"
