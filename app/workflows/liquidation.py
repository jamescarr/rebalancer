"""LiquidationWorkflow: sells all holdings for a strategy, then clears them.

Key improvement over Celery: holdings are only cleared AFTER all sells confirm filled.
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from app.activities.broker import check_order_status, submit_qty_order
    from app.activities.persistence import (
        clear_strategy_for_liquidation,
        load_strategy_snapshot,
        mark_job_completed,
        mark_job_executing,
        mark_job_running,
        mark_job_stuck,
        record_trade_submission,
        update_trade_fill,
    )


BROKER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_attempts=5,
)
BROKER_TIMEOUT = timedelta(seconds=30)
DB_TIMEOUT = timedelta(seconds=10)
DB_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3)


@dataclass
class LiquidationInput:
    job_id: str
    strategy_id: str


@workflow.defn(name="LiquidationWorkflow")
class LiquidationWorkflow:

    def __init__(self) -> None:
        self.state = "initializing"

    @workflow.query
    def get_state(self) -> str:
        return self.state

    @workflow.run
    async def run(self, input: LiquidationInput) -> str:
        jid = input.job_id
        sid = input.strategy_id

        await workflow.execute_activity(
            mark_job_running, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        snap = await workflow.execute_activity(
            load_strategy_snapshot, sid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )
        if not snap or not snap["holdings"]:
            await workflow.execute_activity(
                clear_strategy_for_liquidation, sid,
                start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
            )
            await workflow.execute_activity(
                mark_job_completed, jid,
                start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
            )
            return "completed"

        self.state = "submitting_sells"
        await workflow.execute_activity(
            mark_job_executing, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        order_ids: list[str] = []
        for symbol, info in snap["holdings"].items():
            qty = info.get("qty", 0) if isinstance(info, dict) else 0
            if qty < 1e-6:
                continue
            try:
                result = await workflow.execute_activity(
                    submit_qty_order, args=[symbol, qty, "sell"],
                    start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
                )
                await workflow.execute_activity(
                    record_trade_submission,
                    args=[jid, symbol, "sell", qty, result.order_id],
                    start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                )
                order_ids.append(result.order_id)
            except ActivityError as exc:
                workflow.logger.warning(f"Failed to sell {symbol}: {exc}")

        if not order_ids:
            await workflow.execute_activity(
                mark_job_stuck, args=[jid, "All sell submissions failed"],
                start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
            )
            return "stuck"

        self.state = "polling_fills"
        deadline = workflow.now() + timedelta(minutes=5)
        while True:
            unfilled: list[str] = []
            for oid in order_ids:
                status = await workflow.execute_activity(
                    check_order_status, oid,
                    start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
                )
                if status["status"] == "filled":
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
                workflow.logger.warning(f"Liquidation fill polling timed out. Unfilled: {unfilled}")
                break
            await workflow.sleep(15)

        self.state = "clearing_holdings"
        await workflow.execute_activity(
            clear_strategy_for_liquidation, sid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

        await workflow.execute_activity(
            mark_job_completed, jid,
            start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )
        self.state = "completed"
        return "completed"
