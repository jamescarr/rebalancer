# Temporal Migration Analysis

An analysis of durable execution gaps in the current Celery-based architecture and a plan for migrating to Temporal.

## Current Architecture

The rebalancer orchestrates a multi-step process: evaluate strategy, fetch prices, plan trades, submit orders, poll for fills, sync holdings. Today this is implemented as a chain of Celery tasks that dispatch each other via `.delay()`, with PostgreSQL holding the source of truth and Redis as the message broker.

The core flow:

```
evaluate_due_strategies (Beat, every 10s)
  -> run_strategy_rebalance (per strategy)
       -> execute_trades (per job)
            -> poll_order_fills (retries until filled)
                 -> _sync_holdings (updates strategy state)
```

Each arrow is a Celery `.delay()` call, which is a task handoff through Redis. State is persisted to PostgreSQL between steps via the repository layer.

## Durable Execution Gaps

### Task Handoff Failures

Every `.delay()` call is a point where the process can break. The current code marks state in the database, then dispatches the next task. If the dispatch fails (Redis down, worker crash, network blip), the job is stranded in a transitional status with no recovery mechanism.

**Gap: rebalance to execution handoff**

In `tasks.py:run_strategy_rebalance`, the job is marked `executing_trades` and then `execute_trades.delay()` is called. If the delay fails, the job sits in `executing_trades` forever.

```python
jobs.mark_executing(jid)
execute_trades.delay(job_id, strategy_id, planned)  # what if this fails?
```

The same pattern repeats at execution to polling (`execute_trades` dispatches `poll_order_fills`) and liquidation to polling (`liquidate_strategy_holdings` dispatches `poll_liquidation_fills`). Each is a potential break point.

**How Temporal fixes this:** In a Temporal workflow, the equivalent of these handoffs is simply calling the next activity. The workflow engine guarantees the activity will execute. There is no "dispatch" that can silently fail. If the worker crashes mid-activity, Temporal retries it automatically from the last checkpoint.

### Order Submission Without Transaction Safety

In `tasks.py:execute_trades`, orders are submitted to Alpaca one at a time in a loop. Each submission is immediately recorded in the database. If the loop fails mid-way, some orders exist at the broker and in the DB, while others do not. There is no rollback.

```python
for t in planned:
    order = client.submit_market_order(...)  # side effect at broker
    trades.record_submission(...)             # side effect in DB
    # crash here = next order never submitted, but previous are live
```

This is the classic "dual write" problem. The broker and the database are two separate systems with no shared transaction.

**How Temporal fixes this:** Each order submission becomes an activity. Temporal records the activity result in its event history. On retry, it replays the already-completed activities (returning their recorded results) and only re-executes the ones that did not complete. This gives you idempotent replay of the entire sequence without duplicate orders.

### Duplicate Execution from At-Least-Once Delivery

Celery uses at-least-once delivery. If a worker crashes after processing a message but before acknowledging it, the broker redelivers the message. The task runs again.

For `execute_trades`, this means orders can be submitted twice. The current code has a partial guard (checking if a symbol was already submitted for this job), but this check is not atomic with the submission itself. Two concurrent executions of the same task can both pass the check.

**How Temporal fixes this:** Temporal provides exactly-once execution semantics for workflow steps. Activity results are recorded in the event history. If a worker crashes and the workflow replays, completed activities are not re-executed. The broker-side deduplication is built into the platform.

### Worker Restart Loses In-Progress State

If a Celery worker restarts while `run_strategy_rebalance` is running (e.g., between fetching prices and planning trades), all in-memory state is lost. The job is stuck in `running` status. The `current_task_id` field becomes stale. The only recovery is the stuck-job watchdog, which runs every 15 minutes and only logs an alert.

**How Temporal fixes this:** Workflow state survives worker restarts. When a worker comes back, Temporal replays the event history to reconstruct the workflow's state, then continues from where it left off. No data is lost, no manual intervention needed.

### Holdings Sync Race Condition

When a rebalance job completes, `_sync_holdings` reads the strategy's current holdings, applies the fills, and writes the result back. If two jobs for the same strategy complete simultaneously, both read the same snapshot, both compute updated holdings, and the last write wins. One job's fills are silently lost.

```python
def _sync_holdings(strategy_id, job_id):
    snap = strategies.snapshot(strategy_id)   # read
    fills = trades.get_fills_for_job(job_id)  # read
    updated = trading.apply_fills_to_holdings(snap["holdings"], fills)  # compute
    strategies.update_holdings(strategy_id, updated)  # write (last writer wins)
```

**How Temporal fixes this:** The scheduler's in-flight check (one job at a time per strategy) is enforced at the workflow level. Temporal can ensure only one workflow instance runs per strategy ID using workflow ID deduplication. Sequential execution eliminates the race entirely.

### Scheduler Race Condition

The scheduler (`evaluate_due_strategies`) checks for in-flight jobs and creates new ones. This check-then-act sequence is not atomic. Two concurrent scheduler runs can both see no in-flight job and create duplicates.

**How Temporal fixes this:** Temporal's `start_workflow` with a deterministic workflow ID is inherently deduplicated. Starting a workflow that already exists (same ID) is a no-op. The race condition is impossible.

### Partial Fill Data Loss

When fill polling retries are exhausted, the job is marked `stuck` and `_sync_holdings` runs with whatever fills were recorded. Partially filled orders may not have their fill data captured, leading to holdings that do not reflect actual positions at the broker.

**How Temporal fixes this:** Fill polling becomes a workflow loop with configurable timeouts and retry policies. The workflow can wait indefinitely (or up to a configured deadline) without consuming resources. When the deadline is reached, the workflow can execute a reconciliation activity that queries the broker for the definitive fill state before syncing holdings.

### Liquidation Clears Holdings Prematurely

In `poll_liquidation_fills`, `strategies.clear_for_liquidation()` runs even when some sell orders never filled (retries exhausted). The strategy's holdings are wiped to empty, but the actual positions may still exist at the broker.

**How Temporal fixes this:** The liquidation workflow can gate the "clear holdings" step on all sells being confirmed filled. If some fail, the workflow can execute a compensation activity (cancel unfilled orders, reconcile with broker) before deciding whether to clear holdings.

### No Saga/Compensation Pattern

The current code has no rollback mechanism. If order 3 of 5 fails, orders 1-2 remain live. There is no attempt to cancel them. The job is marked stuck and a human must intervene.

**How Temporal fixes this:** A saga pattern in Temporal accumulates compensation actions as the workflow progresses. If a step fails, compensations execute in reverse order (cancel order 2, cancel order 1). This is a first-class pattern in Temporal with built-in support for compensation activity retries.

## Migration Plan

### Target Architecture

Replace Celery tasks with Temporal workflows and activities. Keep the existing domain layer (`app/domain/`) and repositories (`app/repositories.py`) unchanged. The Temporal code becomes the new orchestration layer.

```
app/
  domain/              (unchanged - pure business logic)
    strategy.py
    trading.py
  workflows/           (new - Temporal workflows)
    rebalance.py       RebalanceWorkflow
    liquidation.py     LiquidationWorkflow
    scheduler.py       SchedulerWorkflow
  activities/          (new - Temporal activities)
    broker.py          Alpaca API calls
    strategy.py        Strategy evaluation + DB ops
    trading.py         Trade execution + fill polling
  repositories.py      (unchanged - DB access)
  alpaca_client.py     (unchanged - broker adapter)
  models.py            (unchanged - SQLAlchemy models)
  main.py              (updated - start workflows instead of dispatch tasks)
  worker.py            (new - Temporal worker entrypoint)
```

### Phase 1: Activities

Extract every side-effectful operation into a Temporal activity. Each activity is a small, retryable unit:

**Broker activities** (`activities/broker.py`):
- `fetch_positions` - get current positions from Alpaca
- `fetch_account` - get account equity and cash
- `fetch_crypto_performance` - get price changes for momentum strategies
- `submit_notional_order` - submit a dollar-amount market order
- `submit_qty_order` - submit a quantity market order (for liquidation)
- `check_order_status` - poll a single order's fill status

**Strategy activities** (`activities/strategy.py`):
- `load_strategy_snapshot` - read strategy config from DB
- `update_evaluation` - write evaluation results to DB
- `sync_holdings` - apply fills to strategy holdings
- `clear_for_liquidation` - wipe holdings after liquidation

**Trading activities** (`activities/trading.py`):
- `record_trade_submission` - write TradeExecution to DB
- `record_fill` - update TradeExecution with fill data

Each activity gets its own retry policy. Broker calls get aggressive retries with backoff. DB writes get short retries. Non-retryable errors (insufficient funds, invalid symbol) are marked as such.

### Phase 2: Rebalance Workflow

The 591-line `run_strategy_rebalance` + `execute_trades` + `poll_order_fills` chain becomes a single workflow:

```
RebalanceWorkflow:
  1. execute_activity(load_strategy_snapshot)
  2. execute_activity(fetch_crypto_performance)  [if momentum/contrarian]
  3. Call domain.strategy.evaluate() in workflow code (deterministic, no activity needed)
  4. execute_activity(fetch_positions + fetch_account)
  5. Call domain.trading.plan_trades() in workflow code (deterministic)
  6. For each trade:
       execute_activity(submit_notional_order)
       execute_activity(record_trade_submission)
       Add compensation: cancel_order
  7. Poll loop:
       execute_activity(check_order_status) per order
       execute_activity(record_fill) per filled order
       workflow.sleep(15) between polls
       Timeout after 5 minutes
  8. execute_activity(sync_holdings)
  9. execute_activity(update_job_status)
```

If any step fails, compensations run in reverse (cancel submitted orders). The workflow's state (which orders were submitted, which are filled) survives worker restarts. Fill polling uses `workflow.sleep()` instead of Celery retry countdowns, so it cannot be lost.

### Phase 3: Liquidation Workflow

Same structure as RebalanceWorkflow but simpler:

```
LiquidationWorkflow:
  1. execute_activity(load_strategy_snapshot) - get holdings
  2. For each holding:
       execute_activity(submit_qty_order) - sell by quantity
       execute_activity(record_trade_submission)
  3. Poll loop (same as rebalance)
  4. Only after ALL fills confirmed:
       execute_activity(clear_for_liquidation)
```

The critical improvement: holdings are only cleared after all sells are confirmed filled. No premature wipe.

### Phase 4: Scheduler Workflow

Replace Celery Beat with a long-running Temporal workflow:

```
SchedulerWorkflow:
  loop:
    1. execute_activity(get_active_strategies)
    2. For each due strategy:
         start_child_workflow(RebalanceWorkflow, id=f"rebalance-{strategy_id}")
         (duplicate IDs are automatically deduplicated)
    3. workflow.sleep(10)
    4. Continue-as-new every 1000 iterations (avoid event history growth)
```

Workflow ID deduplication (`rebalance-{strategy_id}`) replaces the `has_in_flight` check. Two scheduler iterations cannot create duplicate rebalance workflows for the same strategy because Temporal rejects the second `start_child_workflow` with a "workflow already exists" error.

### Phase 5: API Integration

Update `main.py` to start workflows instead of dispatching Celery tasks:

```python
@app.post("/rebalance")
async def trigger_rebalance(body: TriggerRebalanceRequest):
    handle = await temporal_client.start_workflow(
        RebalanceWorkflow.run,
        RebalanceInput(strategy_id=str(body.strategy_id)),
        id=f"rebalance-{body.strategy_id}-{uuid4()}",
        task_queue="rebalancer",
    )
    return {"job_id": handle.id, "status": "started"}

@app.post("/strategies/{id}/liquidate")
async def liquidate(strategy_id: UUID):
    handle = await temporal_client.start_workflow(
        LiquidationWorkflow.run,
        LiquidationInput(strategy_id=str(strategy_id)),
        id=f"liquidate-{strategy_id}",
        task_queue="rebalancer",
    )
    return {"job_id": handle.id, "status": "started"}
```

Query handlers on the workflows replace the job detail API:

```python
@app.get("/rebalance/{job_id}")
async def get_job(job_id: str):
    handle = temporal_client.get_workflow_handle(job_id)
    return await handle.query(RebalanceWorkflow.get_status)
```

### Phase 6: Infrastructure

Replace Redis + Celery with Temporal Server:

**Docker Compose changes:**
- Remove: Celery worker, Celery beat
- Add: Temporal server, Temporal worker (runs workflow + activity code)
- Keep: Postgres (for both app data and Temporal persistence), Redis (optional, for Temporal visibility)

**Dependencies:**
- Remove: `celery[redis]`, `redis`
- Add: `temporalio`

### What Does Not Change

The domain layer (`app/domain/strategy.py`, `app/domain/trading.py`) remains pure functions with zero framework dependencies. The repository layer (`app/repositories.py`) continues to encapsulate all DB access. The Alpaca client adapter stays the same. The SQLAlchemy models stay the same. The UI stays the same.

The only code that changes is the orchestration layer: `tasks.py` and `celery_app.py` are replaced by `workflows/` and `activities/`.

### Testing Strategy

Temporal provides a `WorkflowEnvironment` with time-skipping for fast tests:

- **Domain tests** (unchanged): Pure function tests, no mocking needed
- **Activity tests**: Use `ActivityEnvironment` to test each activity in isolation
- **Workflow tests**: Use `WorkflowEnvironment.start_time_skipping()` with mock activities to test the full orchestration flow without real broker calls
- **Replay tests**: Record workflow event histories and replay them to verify determinism after code changes

### Migration Sequence

The migration can be done incrementally:

- Start with Phase 1 (activities) alongside the existing Celery tasks
- Run Phase 2 (RebalanceWorkflow) for a single strategy while others still use Celery
- Once validated, migrate all strategies to Temporal workflows
- Remove Celery infrastructure last

This allows a gradual rollout with the ability to fall back at each stage.
