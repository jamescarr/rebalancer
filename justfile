# Default: list available recipes
default:
    @just --list

# ---------------------------------------------------------------------------
# Local dev (Docker)
# ---------------------------------------------------------------------------

# Start all services (Postgres, Temporal, API, worker)
up:
    docker compose up --build -d

# Stop all services
down:
    docker compose down

# Stop all services and destroy volumes
down-v:
    docker compose down -v

# Tail logs for all services (Ctrl-C to stop)
logs:
    docker compose logs -f

# Tail logs for a single service
log service:
    docker compose logs -f {{ service }}

# Run Alembic migrations inside the api container
migrate:
    docker compose exec api alembic upgrade head

# Seed the database with example crypto strategies
seed:
    docker compose exec api python -m app.seed

# Start the scheduler workflow (run once after boot)
start-scheduler:
    docker compose exec api python -c "import asyncio; from app.main import start_scheduler; asyncio.run(start_scheduler())"

# Open a psql shell
psql:
    docker compose exec postgres psql -U rebalancer rebalancer

# ---------------------------------------------------------------------------
# Project setup
# ---------------------------------------------------------------------------

# Install all dependencies (runtime + dev) into a uv-managed venv
install:
    uv sync

# Create .env from the example if it doesn't exist
env:
    @[ -f .env ] && echo ".env already exists" || (cp .env.example .env && echo "Created .env from .env.example")

# Full bootstrap: .env + install + docker up + migrate + seed
bootstrap: env install up
    @echo "Waiting for services to be ready..."
    @sleep 15
    just migrate
    just seed
    @echo ""
    @echo "Done!"
    @echo "  App UI:      http://localhost:8000"
    @echo "  Temporal UI: http://localhost:8080"

# Reset everything: wipe DB, rebuild, re-seed
reset: down-v up
    @echo "Waiting for services to be ready..."
    @sleep 15
    just migrate
    just seed
    @echo ""
    @echo "Done! Fresh start."
    @echo "  App UI:      http://localhost:8000"
    @echo "  Temporal UI: http://localhost:8080"

# ---------------------------------------------------------------------------
# Code quality
# ---------------------------------------------------------------------------

# Run unit and integration tests
test *args:
    uv run pytest {{ args }}

# ---------------------------------------------------------------------------
# Useful one-liners
# ---------------------------------------------------------------------------

# List all strategies
strategies:
    curl -s http://localhost:8000/strategies | python -m json.tool

# Health check
health:
    curl -s http://localhost:8000/health | python -m json.tool
