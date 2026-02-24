# Rebalancer

Automated crypto portfolio rebalancing with strategy-based fund management, real-time UI, and Celery-driven scheduling. Built on Alpaca's paper trading API (24/7 crypto).

This is intentionally the "before" version of a Temporal migration demo. It works, but embeds every classic pain point of hand-rolled workflow orchestration. See [TEMPORAL_MIGRATION.md](TEMPORAL_MIGRATION.md) for the full analysis.

![Rebalancer UI](https://cdn.zappy.app/3fe83eb40b035c327814b770a0b2c001.png)

## How it works

You create **strategies** that define rules for trading crypto. Each strategy has a type (fixed weight, equal weight, momentum, or contrarian surge), a set of assets, and a rebalance interval. Strategies start inactive. You **fund** a strategy with a dollar amount to activate it. The system then automatically rebalances the strategy's holdings to match its target allocation on the configured schedule.

Each strategy tracks its own holdings independently. Funding one strategy does not affect another. Liquidating a strategy sells only its tracked positions.

### Strategy types

- **Fixed Weight**: Static allocation weights (e.g. 50% BTC, 30% ETH, 20% SOL)
- **Equal Weight**: Split equally across all assets
- **Momentum**: Overweight recent top performers, underweight laggards (uses Alpaca crypto data API for price history)
- **Contrarian Surge**: Mean-reversion (buy the dip) with a rotating surge bonus that changes every 30 seconds. Guaranteed to produce different weights on every evaluation, so it always trades.

### Strategy lifecycle

1. **Create** a strategy (starts inactive)
2. **Fund** it with a dollar amount (activates it, triggers first rebalance)
3. The scheduler evaluates due strategies every 10 seconds, creates rebalance jobs
4. Each job: evaluate strategy -> fetch prices -> plan trades -> submit orders -> poll fills -> sync holdings
5. **Liquidate** to sell all holdings and deactivate
6. **Re-fund** to start again

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- [just](https://github.com/casey/just#installation) (command runner)
- An [Alpaca](https://app.alpaca.markets/signup) paper trading account (free)

## Quick start

### 1. Create an Alpaca paper trading account

Go to [app.alpaca.markets/signup](https://app.alpaca.markets/signup), confirm your email, set up MFA, then switch to the **Paper Trading** dashboard and generate API keys. Copy both the key ID and secret (the secret is shown only once).

### 2. Configure and run

```bash
cd rebalancer

# Create .env from example and fill in your Alpaca keys
cp .env.example .env
# Edit .env with your ALPACA_API_KEY and ALPACA_SECRET_KEY

# Bootstrap: install deps, start Docker, run migrations, seed strategies
just bootstrap
```

The UI is at **http://localhost:8000**.

### 3. Fund a strategy

Open the UI, pick a strategy, enter a dollar amount, and click **Fund**. The strategy activates and the first rebalance runs immediately. Subsequent rebalances happen on the configured interval.

## Environment variables

```env
DATABASE_URL=postgresql://rebalancer:rebalancer@postgres:5432/rebalancer
REDIS_URL=redis://redis:6379/0
ALPACA_API_BASE_URL=https://paper-api.alpaca.markets
ALPACA_API_KEY=your_paper_key_id
ALPACA_SECRET_KEY=your_paper_secret_key
```

## Just recipes

```
bootstrap    Full bootstrap: .env + install + docker up + migrate + seed
reset        Wipe DB, rebuild, re-seed (fresh start)
up           Start all services
down         Stop all services
down-v       Stop all services and destroy volumes
logs         Tail logs for all services
log <svc>    Tail logs for a single service
migrate      Run Alembic migrations
seed         Seed example crypto strategies
install      Install all dependencies
test         Run tests (uv run pytest)
strategies   List all strategies (curl)
health       Health check (curl)
psql         Open a psql shell
```

## Running tests

```bash
just test           # all tests
just test -v        # verbose
just test -k drift  # filter by name
```

32 tests covering domain strategy evaluation, trade planning (drift, cash capping, holdings management), and the Alpaca client adapter.

## Architecture

```
app/
  domain/                Pure business logic (no framework deps)
    strategy.py          Strategy evaluation: 4 types, dispatch, description
    trading.py           Drift, trade planning, cash capping, holdings sync
  repositories.py        Repository pattern over SQLAlchemy (all DB access)
  tasks.py               Thin Celery orchestration wrappers (~220 lines)
  alpaca_client.py       Alpaca trading + crypto data adapter
  models.py              SQLAlchemy models: Strategy, RebalanceJob, TradeExecution
  schemas.py             Pydantic request/response models
  main.py                FastAPI routes + static file serving
  celery_app.py          Celery init + Beat schedule
  seed.py                Example strategy seeder
  config.py              Settings via pydantic-settings
  database.py            SQLAlchemy engine + session
alembic/                 Database migrations
static/                  Alpine.js + Tailwind CSS UI (single HTML file)
tests/                   Unit tests for domain + client layers
```

### Layered design

The codebase follows DDD-inspired separation:

- **Domain layer** (`app/domain/`): Pure functions with zero framework dependencies. Strategy evaluation and trade planning are fully testable with plain Python types.
- **Repository layer** (`app/repositories.py`): Encapsulates all database access. Every "fetch entity, update field, commit" is a single named method call.
- **Task layer** (`app/tasks.py`): Thin Celery wrappers that read state from repositories, delegate to domain functions, call the broker, and persist results. No raw SQLAlchemy.
- **API layer** (`app/main.py`): Thin FastAPI routes.

## Seeded strategies

The seed creates four strategies (all inactive, fund to activate):

| Strategy | Type | Assets | Interval |
|----------|------|--------|----------|
| Blue Chip HODL | fixed_weight | BTC 55%, ETH 35%, LTC 10% | 1 day |
| Alt Season | equal_weight | SOL, AVAX, DOT, LINK, UNI | 10 min |
| Momentum Top 3 | momentum | BTC, ETH, SOL, DOGE, SHIB, AAVE | 30 sec |
| Contrarian Surge | contrarian_surge | 15 cryptos (BTC, ETH, SOL, AVAX, DOT, LINK, UNI, AAVE, LTC, DOGE, SHIB, BCH, GRT, CRV, SUSHI) | 30 sec |

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/strategies` | List all strategies |
| POST | `/strategies` | Create a strategy |
| GET | `/strategies/:id` | Get strategy detail |
| PATCH | `/strategies/:id` | Update strategy config |
| DELETE | `/strategies/:id` | Delete (must liquidate first) |
| POST | `/strategies/:id/fund` | Fund and activate |
| POST | `/strategies/:id/liquidate` | Sell all holdings and deactivate |
| POST | `/rebalance` | Trigger manual rebalance |
| GET | `/rebalance` | List recent rebalance jobs |
| GET | `/rebalance/:id` | Get job detail with trades |
| DELETE | `/rebalance/:id` | Cancel a job |
| GET | `/account` | Live Alpaca account state |
| GET | `/health` | Worker health check |
| GET | `/` | UI |

## Stack

- Python 3.14
- FastAPI + Uvicorn
- Celery 5.x with Redis broker/backend
- SQLAlchemy 2.x + Alembic
- PostgreSQL 16
- alpaca-py (Alpaca Markets SDK)
- Alpine.js + Tailwind CSS (UI)
- Docker Compose (Postgres, Redis, API, worker, beat)
- uv (package management)
- just (command runner)

## Known limitations and next steps

This is the "before" version of a Temporal migration. See [TEMPORAL_MIGRATION.md](TEMPORAL_MIGRATION.md) for a detailed analysis of 20 durable execution gaps (task handoff failures, duplicate trades, holdings race conditions, missing saga/compensation) and a phased migration plan.
