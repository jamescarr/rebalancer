# Rebalancer

A portfolio rebalancing system built with FastAPI, Celery, and Redis. This is intentionally the "before" version of a Temporal migration demo — it works, but it embeds every classic pain point of hand-rolled workflow orchestration. See [PAIN_POINTS.md](PAIN_POINTS.md) for the full catalogue.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python package manager)
- [just](https://github.com/casey/just#installation) (command runner, optional but recommended)

## Quick start (local dev with mock API)

No Alpaca account needed. The local stack uses [Prism](https://stoplight.io/open-source/prism) to serve a fully compliant mock of the Alpaca trading API from their published OpenAPI spec.

```bash
# Clone and enter the project
cd rebalancer

# Bootstrap: creates .env, installs deps, starts Docker, runs migrations
just bootstrap
```

Or step by step:

```bash
cp .env.example .env          # local dev defaults point at Prism
uv sync                        # install all deps (runtime + dev)
docker compose up --build -d   # start Postgres, Redis, Prism, API, worker, beat
just migrate                   # run Alembic migrations
```

Verify:

```bash
just health
# → {"status": "ok", "active_tasks": []}
```

Try it out:

```bash
just create-portfolio
just rebalance
# copy the job_id from the output, then:
just job-status <job_id>
```

## Paper trading setup (real market data)

Paper trading is free — no funding or brokerage application required. Accounts are available globally to anyone with an email address.

### 1. Create an Alpaca account

Go to [https://app.alpaca.markets/signup](https://app.alpaca.markets/signup) and sign up with your email.

Confirm your email via the verification link Alpaca sends.

### 2. Set up MFA

After email confirmation you'll be prompted to activate multi-factor authentication — this is now required before API access is granted.

Use any authenticator app (Google Authenticator, 1Password, Authy, etc.). Complete MFA activation before proceeding.

### 3. Switch to the Paper Trading dashboard

After login you may land on a live account prompt — skip it.

1. In the upper-left corner, click the **account selector**
2. Choose **Paper Trading** (it exists by default) ![](https://cdn.zappy.app/2683b99b6d9981d289e5bb14e0977988.png)
3. If it's not listed, click **Open New Paper Account**

Paper accounts start with **$100,000 in simulated cash**.

### 4. Generate API keys

1. In the Paper Trading dashboard, find the **API Keys** panel
![](https://cdn.zappy.app/9af77ed0234c5d468f84757f0ff7f9fe.png)
2. Click **Generate New Key**
3. Copy both values immediately — **the secret is shown only once**. If you lose it, regenerate (this invalidates the old key).

### 5. Update your `.env`

Comment out the local dev block and uncomment the paper trading block:

```env
DATABASE_URL=postgresql://rebalancer:rebalancer@postgres:5432/rebalancer
REDIS_URL=redis://redis:6379/0

# --- Local development (Prism mock, no Alpaca account needed) ---
# ALPACA_API_BASE_URL=http://alpaca-mock:4010
# ALPACA_API_KEY=local-dev-key
# ALPACA_SECRET_KEY=local-dev-secret

# --- Paper trading (real market data, free Alpaca account required) ---
ALPACA_API_BASE_URL=https://paper-api.alpaca.markets
ALPACA_API_KEY=<your_paper_key_id>
ALPACA_SECRET_KEY=<your_paper_secret_key>
```

Then restart the stack:

```bash
docker compose up --build -d
```

### Things to know about paper trading

- Orders only fill during market hours (9:30 AM – 4:00 PM ET, weekdays). Orders submitted outside market hours queue for the next open.
- Partial fills are simulated randomly ~10% of the time.
- The free IEX data feed covers ~8–10% of market volume. Fine for this demo.
- Account balance resets require deleting and recreating the paper account.
- Up to 3 paper accounts can exist simultaneously.
- Paper trading does **not** simulate dividends or send order fill emails.

### Useful links

| Resource | URL |
|----------|-----|
| Dashboard | https://app.alpaca.markets |
| Paper trading docs | https://docs.alpaca.markets/docs/paper-trading |
| alpaca-py SDK docs | https://docs.alpaca.markets/docs/about-alpaca-py |
| Trading OpenAPI spec | https://github.com/alpacahq/alpaca-docs/tree/master/oas |

## Running tests

```bash
uv run pytest           # all tests
uv run pytest -v        # verbose
uv run pytest -k drift  # filter by name
```

Or with just:

```bash
just test
just test -v
just test -k drift
```

## Project layout

```
app/
  config.py            Settings via pydantic-settings, reads .env
  database.py          SQLAlchemy engine, session, Base
  models.py            RebalanceJob, TradeExecution, UserPortfolio
  schemas.py           Pydantic request/response models
  alpaca_client.py     Thin wrapper around alpaca-py
  rebalance_logic.py   Pure functions: drift, threshold, trade sizing
  celery_app.py        Celery init and Beat schedule
  tasks.py             All Celery tasks (6 total)
  main.py              FastAPI routes (7 endpoints)
alembic/               Database migrations
tests/                 Unit and integration tests
```

## Just recipes

Run `just` to see all available commands:

```
bootstrap    Full bootstrap: .env + install + docker up + migrate
up           Start all services
down         Stop all services
down-v       Stop all services and destroy volumes
logs         Tail logs for all services
migrate      Run Alembic migrations inside the api container
install      Install all dependencies into a uv-managed venv
test         Run unit and integration tests
health       Health check
...
```

## Stack

- Python 3.14
- FastAPI + Uvicorn
- Celery 5.x with Redis broker/backend
- SQLAlchemy 2.x + Alembic
- PostgreSQL 16
- alpaca-py (Alpaca Markets SDK)
- Docker Compose (Postgres, Redis, Prism mock)
- uv (package management)
- just (command runner)
