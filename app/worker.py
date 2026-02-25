"""Temporal worker entrypoint.

Registers all workflows and activities, connects to Temporal server,
and runs the worker. This replaces the Celery worker + beat processes.
"""

import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from app.activities.broker import (
    check_order_status,
    fetch_account_cash,
    fetch_crypto_performance,
    fetch_positions,
    submit_notional_order,
    submit_qty_order,
)
from app.activities.persistence import (
    clear_strategy_for_liquidation,
    create_job,
    get_due_strategies,
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
    set_strategy_next_evaluation,
    update_strategy_evaluation,
    update_strategy_holdings,
    update_trade_fill,
)
from app.config import get_settings
from app.workflows.liquidation import LiquidationWorkflow
from app.workflows.rebalance import RebalanceWorkflow
from app.workflows.scheduler import SchedulerWorkflow

TASK_QUEUE = "rebalancer"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    settings = get_settings()
    temporal_address = getattr(settings, "TEMPORAL_ADDRESS", "temporal:7233")

    logger.info("Connecting to Temporal at %s", temporal_address)
    client = await Client.connect(temporal_address)

    logger.info("Starting worker on queue %s", TASK_QUEUE)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[
            RebalanceWorkflow,
            LiquidationWorkflow,
            SchedulerWorkflow,
        ],
        activities=[
            fetch_positions,
            fetch_account_cash,
            fetch_crypto_performance,
            submit_notional_order,
            submit_qty_order,
            check_order_status,
            load_strategy_snapshot,
            update_strategy_evaluation,
            update_strategy_holdings,
            clear_strategy_for_liquidation,
            create_job,
            mark_job_running,
            mark_job_no_action,
            mark_job_executing,
            mark_job_completed,
            mark_job_stuck,
            set_job_target_allocation,
            set_job_trades_calculated,
            record_trade_submission,
            update_trade_fill,
            get_fills_for_job,
            get_due_strategies,
            set_strategy_next_evaluation,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
