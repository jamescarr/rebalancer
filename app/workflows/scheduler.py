"""SchedulerWorkflow: long-running workflow that replaces Celery Beat.

Uses a STABLE workflow ID per strategy (rebalance-{strategy_id}) so Temporal's
built-in deduplication prevents double rebalances. If a rebalance is already
running for a strategy, start_child_workflow with the same ID is silently
rejected. No DB-level in-flight check needed.

Uses continue-as-new every 500 iterations to avoid event history growth.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

with workflow.unsafe.imports_passed_through():
    from app.activities.persistence import (
        create_job,
        get_due_strategies,
        set_strategy_next_evaluation,
    )
    from app.workflows.rebalance import RebalanceInput, RebalanceWorkflow


DB_TIMEOUT = timedelta(seconds=10)
DB_RETRY = RetryPolicy(initial_interval=timedelta(seconds=1), maximum_attempts=3)

CHECK_INTERVAL_SECONDS = 10
MAX_ITERATIONS = 500


@workflow.defn(name="SchedulerWorkflow")
class SchedulerWorkflow:

    @workflow.run
    async def run(self, iteration: int = 0) -> None:
        while iteration < MAX_ITERATIONS:
            due = await workflow.execute_activity(
                get_due_strategies,
                start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
            )

            for s in due:
                job_id = await workflow.execute_activity(
                    create_job, args=[s.id, s.drift_threshold],
                    start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                )
                await workflow.execute_activity(
                    set_strategy_next_evaluation, args=[s.id, s.rebalance_interval_seconds],
                    start_to_close_timeout=DB_TIMEOUT, retry_policy=DB_RETRY,
                )

                try:
                    await workflow.start_child_workflow(
                        RebalanceWorkflow.run,
                        RebalanceInput(job_id=job_id, strategy_id=s.id),
                        id=f"rebalance-{s.id}",
                    )
                except WorkflowAlreadyStartedError:
                    workflow.logger.info("Rebalance already running for strategy %s, skipping", s.id)

            await workflow.sleep(CHECK_INTERVAL_SECONDS)
            iteration += 1

        workflow.continue_as_new(0)
