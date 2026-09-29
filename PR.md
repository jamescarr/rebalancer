# Migrate from Celery to Temporal for durable workflow execution

## Summary

Replace the entire Celery orchestration layer with Temporal workflows and activities, eliminating 20 identified durable execution gaps (task handoff failures, duplicate trades, holdings race conditions, missing compensation) while keeping the domain logic, repositories, and UI completely unchanged.

## What changed

### Removed (723 lines deleted)

- `app/celery_app.py` -- Celery init and Beat schedule
- `app/tasks.py` -- 311-line monolithic task module (6 Celery tasks chained via `.delay()`)
- `celery[redis]` and `redis` dependencies
- Redis container, Celery worker container, Celery beat container
- 5 unused repository methods left over from Celery (`has_in_flight`, `set_error`, `clear_task_id`, `symbols_for_job`, `get_order_id`)

### Added (1,277 lines)

**Workflows** (`app/workflows/`)

- `rebalance.py` -- Single durable workflow replacing the 4-task Celery chain. Each step is an activity with its own retry policy and timeout. Worker crashes replay from the last checkpoint. Failed orders are skipped gracefully. Rich query status (state, submitted/filled/skipped counts, error).
- `liquidation.py` -- Sells all strategy holdings by quantity. Holdings are only cleared after ALL sell orders confirm filled (fixes the premature-clear bug from the Celery version).
- `scheduler.py` -- Long-running workflow replacing Celery Beat. Uses stable workflow ID per strategy (`rebalance-{strategy_id}`) so Temporal's built-in deduplication prevents double rebalances. Uses `continue_as_new` every 500 iterations to bound event history growth.
- `shared.py` -- Single source of truth for retry policies, timeouts, and constants shared across all workflows.

**Activities** (`app/activities/`)

- `broker.py` -- 6 activities for Alpaca API calls, each independently retryable: `fetch_positions`, `fetch_account_cash`, `fetch_crypto_performance`, `submit_notional_order`, `submit_qty_order`, `check_order_status`
- `persistence.py` -- 18 activities for all database reads/writes via the repository layer

**Infrastructure**

- `app/worker.py` -- Temporal worker entrypoint registering all workflows and activities
- Docker Compose: Temporal server (auto-setup with dedicated PostgreSQL) + Temporal UI (port 8080) + 3 worker replicas with health checks
- API lifespan auto-starts the scheduler workflow with background retry loop (10 attempts, 3s delay)
- `just bootstrap` and `just reset` restart workers after migrations to avoid "table not found" races

**Tests**

- `tests/test_workflows.py` -- 4 workflow tests using Temporal's time-skipping environment with mock activities

### What did NOT change

- `app/domain/strategy.py` -- pure strategy evaluation (zero changes)
- `app/domain/trading.py` -- drift, trade planning, holdings (zero changes)
- `app/alpaca_client.py` -- broker adapter (zero changes)
- `app/models.py`, `app/schemas.py`, `app/seed.py` -- data model (zero changes)
- `static/index.html` -- UI (zero changes)
- `alembic/` -- migrations (zero changes)

## Durable execution gaps addressed

| Gap (Celery) | Fix (Temporal) |
|---|---|
| Task handoff via `.delay()` can silently fail, stranding jobs | Workflow steps are activities with guaranteed execution |
| Worker crash loses all in-progress state | Temporal replays from event history on restart |
| At-least-once delivery causes duplicate order submissions | Activity results recorded in history; replayed activities return cached results |
| Fill polling via Celery retry countdowns lost on worker restart | `workflow.sleep()` is durable; survives restarts |
| Holdings synced before all fills confirmed (liquidation) | Workflow gates clear-holdings on all fills confirmed |
| No saga/compensation for failed trades | Individual trade failures caught and skipped without killing the workflow |
| Scheduler race condition (duplicate jobs from concurrent Beat runs) | Stable workflow ID per strategy; Temporal rejects duplicates |
| Orphaned "running" DB jobs block future scheduling | Scheduler uses Temporal workflow state, not DB status |
| Inconsistent retry policies across task chains | Single `shared.py` defines all retry policies |
| `APIError` retried 5 times when it should fail immediately | `non_retryable_error_types=["APIError"]` fails on first attempt |

## Iterative fixes on the branch

| Commit | Issue discovered | Fix |
|---|---|---|
| `cd80db8` | Initial migration | Workflows, activities, worker, Docker infra |
| `c0b2aa9` | Broker errors retried needlessly | `non_retryable_error_types`, DRY helpers, richer queries |
| `ef856e1` | Orphaned DB jobs block scheduler | Replace DB check with Temporal workflow ID dedup |
| `c4ada2d` | Scheduler fails to start on boot | Background retry loop with `WorkflowAlreadyStartedError` handling |
| `f387344` | Single worker bottleneck | Scale to 3 replicas, clean up inline imports |
| `b4420bd` | Inconsistent retry policies, dead code | Extract `shared.py`, remove 5 unused repo methods, harden liquidation timeout |

## How to test

```bash
just reset
# App UI:      http://localhost:8000
# Temporal UI: http://localhost:8080
```

Fund a strategy from the UI, watch it rebalance on schedule. Check the Temporal UI at `:8080` to see workflow execution history, activity retries, and the scheduler's child workflows.

```bash
just test     # 36 tests (32 domain/client + 4 workflow)
```

Stop everything with `docker compose down`, restart with `docker compose up -d`, verify the scheduler auto-resumes and strategies continue rebalancing.
