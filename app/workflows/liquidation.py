"""LiquidationWorkflow: sells all strategy holdings, clears only after all fills confirm."""

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
    non_retryable_error_types=["InsufficientFunds"],
)
BROKER_TIMEOUT = timedelta(seconds=30)
DB_TIMEOUT = timedelta(seconds=10)
DB_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3)
FILL_POLL_DEADLINE = timedelta(minutes=5)


@dataclass
class LiquidationInput:
    job_id: str
    strategy_id: str


@workflow.defn(name="LiquidationWorkflow")
class LiquidationWorkflow:

    def __init__(self) -> None:
        self.state = "initializing"
        self._order_ids: list[str] = []
        self._filled: set[str] = set()

    @workflow.query
    def get_state(self) -> str:
        return self.state

    async def _broker(self, fn, *args, **kwargs):
        return await workflow.execute_activity(
            fn, *args, **kwargs, start_to_close_timeout=BROKER_TIMEOUT, retry_policy=BROKER_RETRY,
        )

    async def _db(self, fn, *args, **kwargs):
        return await workflow.execute_activity(
            fn, *args, **kwargs, start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
        )

    @workflow.run
    async def run(self, input: LiquidationInput) -> str:
        jid, sid = input.job_id, input.strategy_id

        await self._db(mark_job_running, jid)

        snap = await self._db(load_strategy_snapshot, sid)
        if not snap or not snap["holdings"]:
            await self._db(clear_strategy_for_liquidation, sid)
            await self._db(mark_job_completed, jid)
            return "completed"

        self.state = "submitting_sells"
        await self._db(mark_job_executing, jid)

        for symbol, info in snap["holdings"].items():
            qty = info.get("qty", 0) if isinstance(info, dict) else 0
            if qty < 1e-6:
                continue
            try:
                result = await self._broker(submit_qty_order, args=[symbol, qty, "sell"])
                await self._db(record_trade_submission, args=[jid, symbol, "sell", qty, result.order_id])
                self._order_ids.append(result.order_id)
            except ActivityError:
                workflow.logger.warning("Failed to sell %s", symbol)

        if not self._order_ids:
            await self._db(mark_job_stuck, args=[jid, "All sell submissions failed"])
            return "stuck"

        self.state = "polling_fills"
        deadline = workflow.now() + FILL_POLL_DEADLINE
        while True:
            pending = [oid for oid in self._order_ids if oid not in self._filled]
            if not pending:
                break
            for oid in pending:
                status = await self._broker(check_order_status, oid)
                if status["status"] == "filled":
                    await self._db(update_trade_fill, args=[oid, "filled", status["filled_qty"], status["filled_avg_price"]])
                    self._filled.add(oid)
            if len(self._filled) == len(self._order_ids):
                break
            if workflow.now() > deadline:
                workflow.logger.warning("Liquidation fill timeout. Unfilled: %s", pending)
                break
            await workflow.sleep(15)

        self.state = "clearing_holdings"
        await self._db(clear_strategy_for_liquidation, sid)
        await self._db(mark_job_completed, jid)
        self.state = "completed"
        return "completed"
