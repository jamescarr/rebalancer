# Pain Points

This document catalogues the intentional architectural problems in the `rebalancer` codebase. Each pain point is a real pattern that appears in production systems that grew without a durable workflow engine. They are the narrative foundation for the Temporal migration that follows.

---

## 1. State as database flags

**File:** `app/models.py` — `RebalanceJob`

The lifecycle of a rebalance is encoded in a `status` string column *plus* six nullable timestamp columns: `started_at`, `completed_at`, `failed_at`, `positions_fetched_at`, `trades_calculated_at`, `execution_started_at`. None of the timestamps were part of the original design; each was added after a production incident to answer "where exactly did it stop?"

To reconstruct what a job was doing when it failed, you have to query the job row, inspect all seven fields, cross-reference the trade execution rows, and still only form a hypothesis. The `GET /rebalance/{job_id}` endpoint makes three DB queries and explicitly notes that it *still cannot tell you why a job is stuck*.

With Temporal, the event history is the source of truth. Every activity invocation, retry, and result is durably recorded with timestamps, inputs, and outputs — no schema changes required.

---

## 2. Retry = re-execution

**File:** `app/tasks.py` — `execute_trades`

When `execute_trades` fails mid-loop (e.g., network error after submitting trade 2 of 5), Celery retries the *entire task* from trade 1. There is no idempotency check before calling `submit_market_order` — if trade 1 was already submitted and filled, a duplicate market order is sent.

The `# BUG:` comment in the code describes the exact failure mode: a second filled order for the same symbol creates an unintended overweight position. Detecting duplicates would require querying Alpaca's order history per symbol before every submission, which this code does not do.

With Temporal, activity retries replay *only the failed activity*. Prior activities are not re-executed. Idempotency is built into the execution model.

---

## 3. Partial execution has no recovery path

**File:** `app/tasks.py` — `execute_trades` (status `partially_executed`)

When a failure leaves the portfolio partially rebalanced, the job is marked `partially_executed` and retried. But the retry re-submits *all* trades (see Pain Point 2), potentially double-buying already-filled positions and further distorting the allocation from the intended target.

There is no compensation logic — no rollback, no partial-trade reversal, no re-calculation of remaining trades from the current post-failure state. The portfolio is left in an allocation that was never intended, and the system has no automated path to recover from it.

With Temporal, a saga pattern or explicit compensation activities can undo completed steps before retrying, or continue from the exact point of failure using persistent workflow state.

---

## 4. Polling disguised as a task

**File:** `app/tasks.py` — `poll_order_fills`

`poll_order_fills` is not a real task — it is a `while unfilled: sleep(15)` loop implemented as Celery retries. Each retry fetches all order statuses, updates the DB, and reschedules itself.

Two problems:
1. **Progress is not durable.** If the worker restarts while a retry countdown is pending, the countdown timer is lost. The job stalls in `executing_trades` and only the watchdog cron will notice.
2. **Max retries is a blunt instrument.** The limit of 10 retries × 15 seconds = ~2.5 minutes was chosen arbitrarily. Partial fills, market volatility, and order queue depth are not considered.

With Temporal, `workflow.sleep()` is durable — a worker restart does not lose the timer. Polling loops are idiomatic and survivable.

---

## 5. The watchdog cron

**File:** `app/tasks.py` — `find_and_alert_stuck_jobs`

```python
# This task exists because the main fulfillment flow is not reliable
# enough to guarantee jobs reach a terminal state on their own.
# It was added after the second production incident.
```

`find_and_alert_stuck_jobs` runs every 15 minutes and scans for jobs that have been running for more than 30 minutes. It emits a warning log (simulating a Slack alert) for each one. It takes no corrective action — it only alerts.

The existence of this task is an admission that the main workflow cannot be trusted to reach a terminal state. Every system without a durable workflow engine eventually grows a watchdog like this one.

With Temporal, a workflow that is blocked does not silently disappear — it is visible in the UI with its full history. Timeouts and heartbeats are first-class primitives; a watchdog cron is unnecessary.

---

## 6. Cancel does not cancel

**File:** `app/main.py` — `DELETE /rebalance/{job_id}`

The cancel endpoint sets `job.status = "cancelling"` in the database and returns. It does **not** interrupt the running Celery task.

The `# TODO:` comment explains why: `task.revoke()` requires the current task ID, which is stored in `current_task_id`. But `current_task_id` expires from the Celery result backend after 24 hours. Even if the ID is available, `revoke()` only prevents the task from *starting* on a new worker pickup — it cannot stop a task that is already executing.

The net result: a "cancelled" job may continue executing, submitting orders, and polling fills for several minutes after the user requested cancellation.

With Temporal, `workflow.cancel()` sends a cancellation signal that the workflow cooperatively handles at the next cancellation point. The cancellation is durable and guaranteed to be delivered.

---

## 7. Notification failure poisons job status

**File:** `app/tasks.py` — `send_completion_notification`

```python
# BUG: A notification failure sets job.status = 'notification_failed',
# overwriting 'completed'. This means a transient notification error
# (network blip, email service down) permanently hides a successful rebalance.
```

`send_completion_notification` is the last step in the chain. On failure, it overwrites `job.status` with `notification_failed` — permanently obscuring the fact that the rebalance itself succeeded. An operator inspecting the job sees a failed status and cannot easily tell whether the portfolio was actually rebalanced.

This is a symptom of using a single status field to represent both the rebalance outcome and the notification outcome. With Temporal, activities are independent and their outcomes are recorded separately. A failed notification activity does not affect the workflow's overall completion status.

---

## 8. Task IDs expire

**File:** `app/models.py` — `RebalanceJob.current_task_id`

```python
# The Celery task ID for the currently active task.
# Celery result backend entries expire after 24 hours (configurable, default 24h).
# After expiry this column is stale and cannot be used to inspect or revoke
# the task. There is no durable link between a job and its execution history.
```

`current_task_id` is overwritten with each new task in the chain (`run_rebalance` → `execute_trades` → `poll_order_fills` → `send_completion_notification`). After 24 hours, Celery purges the result backend entry, and the stored ID becomes useless for debugging, inspection, or revocation.

There is no log of which tasks ran in which order, what their inputs were, or why they failed. Reconstructing a timeline requires correlating structured log lines (if they exist) across multiple worker processes.

With Temporal, the workflow execution history is the permanent, queryable audit log. Every activity that ran, its inputs, outputs, retries, and timestamps are accessible indefinitely (subject to retention policy) without any custom instrumentation.
