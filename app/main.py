"""FastAPI routes. Starts Temporal workflows instead of dispatching Celery tasks."""

import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.orm import Session
from temporalio.client import Client

from app.config import get_settings
from app.database import get_db
from app.models import (
    STRATEGY_TYPES,
    STATUS_CANCELLING,
    RebalanceJob,
    Strategy,
    TradeExecution,
)
from app.schemas import (
    CreateStrategyRequest,
    FundStrategyRequest,
    HealthResponse,
    RebalanceJobDetailResponse,
    RebalanceJobResponse,
    StrategyResponse,
    TriggerRebalanceRequest,
    UpdateStrategyRequest,
)
from app.workflows.liquidation import LiquidationInput, LiquidationWorkflow
from app.workflows.rebalance import RebalanceInput, RebalanceWorkflow
from app.workflows.scheduler import SchedulerWorkflow

TASK_QUEUE = "rebalancer"
SCHEDULER_WORKFLOW_ID = "rebalancer-scheduler"
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

temporal_client: Client | None = None


async def ensure_scheduler_running() -> None:
    """Start the scheduler workflow if not already running. Retries on failure."""
    import asyncio
    import logging
    logger = logging.getLogger(__name__)

    for attempt in range(10):
        try:
            tc = get_temporal()
            try:
                handle = tc.get_workflow_handle(SCHEDULER_WORKFLOW_ID)
                desc = await handle.describe()
                if desc.status and desc.status.name == "RUNNING":
                    logger.info("Scheduler workflow already running")
                    return
            except Exception:
                pass

            await tc.start_workflow(
                SchedulerWorkflow.run, 0,
                id=SCHEDULER_WORKFLOW_ID,
                task_queue=TASK_QUEUE,
            )
            logger.info("Scheduler workflow started")
            return
        except Exception as e:
            logger.warning("Scheduler start attempt %d failed: %s", attempt + 1, e)
            await asyncio.sleep(3)

    logger.error("Could not start scheduler after 10 attempts")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    global temporal_client
    settings = get_settings()
    temporal_client = await Client.connect(settings.TEMPORAL_ADDRESS)
    scheduler_task = asyncio.create_task(ensure_scheduler_running())
    yield
    scheduler_task.cancel()
    temporal_client = None


app = FastAPI(
    title="Rebalancer",
    description="Crypto strategy rebalancing service (Temporal)",
    lifespan=lifespan,
)


def get_temporal() -> Client:
    if temporal_client is None:
        raise HTTPException(status_code=503, detail="Temporal client not connected")
    return temporal_client


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@app.get("/strategies", response_model=list[StrategyResponse])
def list_strategies(db: Session = Depends(get_db)) -> list[Strategy]:
    return list(
        db.execute(select(Strategy).order_by(Strategy.created_at)).scalars().all()
    )


@app.post("/strategies", response_model=StrategyResponse, status_code=201)
def create_strategy(body: CreateStrategyRequest, db: Session = Depends(get_db)) -> Strategy:
    if body.strategy_type not in STRATEGY_TYPES:
        raise HTTPException(status_code=422, detail=f"strategy_type must be one of {sorted(STRATEGY_TYPES)}")
    strategy = Strategy(
        name=body.name, description=body.description, strategy_type=body.strategy_type,
        assets=body.assets, config=body.config,
        rebalance_interval_seconds=body.rebalance_interval_seconds,
        drift_threshold=body.drift_threshold, is_active=False, funded_amount=None, holdings={},
    )
    db.add(strategy)
    db.commit()
    db.refresh(strategy)
    return strategy


@app.get("/strategies/{strategy_id}", response_model=StrategyResponse)
def get_strategy(strategy_id: uuid.UUID, db: Session = Depends(get_db)) -> Strategy:
    s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one_or_none()
    if s is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return s


@app.patch("/strategies/{strategy_id}", response_model=StrategyResponse)
def update_strategy(strategy_id: uuid.UUID, body: UpdateStrategyRequest, db: Session = Depends(get_db)) -> Strategy:
    s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one_or_none()
    if s is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    for k, v in body.model_dump(exclude_unset=True).items():
        setattr(s, k, v)
    db.commit()
    db.refresh(s)
    return s


@app.delete("/strategies/{strategy_id}", status_code=204)
def delete_strategy(strategy_id: uuid.UUID, db: Session = Depends(get_db)) -> None:
    s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one_or_none()
    if s is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if s.is_active:
        raise HTTPException(status_code=409, detail="Liquidate strategy before deleting")
    db.delete(s)
    db.commit()


@app.post("/strategies/{strategy_id}/fund", response_model=StrategyResponse)
def fund_strategy(strategy_id: uuid.UUID, body: FundStrategyRequest, db: Session = Depends(get_db)) -> Strategy:
    s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one_or_none()
    if s is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if s.is_active:
        raise HTTPException(status_code=409, detail="Strategy is already funded and active")
    s.funded_amount = body.amount
    s.is_active = True
    s.holdings = {}
    db.commit()
    db.refresh(s)
    return s


@app.post("/strategies/{strategy_id}/liquidate", response_model=RebalanceJobResponse)
async def liquidate_strategy(strategy_id: uuid.UUID, db: Session = Depends(get_db)) -> RebalanceJob:
    tc = get_temporal()
    s = db.execute(select(Strategy).where(Strategy.id == strategy_id)).scalar_one_or_none()
    if s is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if not s.holdings:
        s.is_active = False
        s.funded_amount = None
        db.commit()
        raise HTTPException(status_code=409, detail="Strategy has no holdings to liquidate")

    s.is_active = False
    job = RebalanceJob(strategy_id=s.id, account_id="liquidation", target_allocation={}, drift_threshold=0)
    db.add(job)
    db.commit()
    db.refresh(job)

    await tc.start_workflow(
        LiquidationWorkflow.run,
        LiquidationInput(job_id=str(job.id), strategy_id=str(strategy_id)),
        id=f"liquidate-{strategy_id}-{job.id}",
        task_queue=TASK_QUEUE,
    )
    return job


# ---------------------------------------------------------------------------
# Rebalance jobs
# ---------------------------------------------------------------------------


@app.post("/rebalance", response_model=RebalanceJobResponse, status_code=202)
async def trigger_rebalance(body: TriggerRebalanceRequest, db: Session = Depends(get_db)) -> RebalanceJob:
    tc = get_temporal()
    s = db.execute(select(Strategy).where(Strategy.id == body.strategy_id)).scalar_one_or_none()
    if s is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if not s.is_active or not s.funded_amount:
        raise HTTPException(status_code=409, detail="Strategy must be funded and active")

    job = RebalanceJob(strategy_id=s.id, account_id="default", target_allocation={}, drift_threshold=s.drift_threshold)
    db.add(job)
    db.commit()
    db.refresh(job)

    await tc.start_workflow(
        RebalanceWorkflow.run,
        RebalanceInput(job_id=str(job.id), strategy_id=str(body.strategy_id)),
        id=f"rebalance-{body.strategy_id}-{job.id}",
        task_queue=TASK_QUEUE,
    )
    return job


@app.get("/rebalance", response_model=list[RebalanceJobDetailResponse])
def list_rebalance_jobs(limit: int = 30, db: Session = Depends(get_db)) -> list[RebalanceJob]:
    jobs = list(db.execute(select(RebalanceJob).order_by(RebalanceJob.created_at.desc()).limit(limit)).scalars().all())
    for j in jobs:
        _ = j.trade_executions
    return jobs


@app.get("/rebalance/{job_id}", response_model=RebalanceJobDetailResponse)
def get_rebalance_job(job_id: uuid.UUID, db: Session = Depends(get_db)) -> RebalanceJob:
    j = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one_or_none()
    if j is None:
        raise HTTPException(status_code=404, detail="Rebalance job not found")
    _ = j.trade_executions
    return j


@app.delete("/rebalance/{job_id}", status_code=200)
def cancel_rebalance_job(job_id: uuid.UUID, db: Session = Depends(get_db)) -> dict[str, str]:
    j = db.execute(select(RebalanceJob).where(RebalanceJob.id == job_id)).scalar_one_or_none()
    if j is None:
        raise HTTPException(status_code=404, detail="Rebalance job not found")
    j.status = STATUS_CANCELLING
    db.commit()
    return {"status": STATUS_CANCELLING, "job_id": str(job_id)}


# ---------------------------------------------------------------------------
# Health + Account
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    tc = get_temporal()
    try:
        await tc.service_client.check_health()
        return HealthResponse(status="ok", active_tasks=None)
    except Exception:
        return HealthResponse(status="error", active_tasks=None)


@app.get("/account")
def get_account_info():
    from app.alpaca_client import get_alpaca_client
    try:
        client = get_alpaca_client()
        account = client.get_account()
        positions = client.get_positions()
        return {
            "equity": float(account.equity),
            "cash": float(account.cash),
            "buying_power": float(account.buying_power),
            "positions": [
                {
                    "symbol": p.symbol, "qty": float(p.qty),
                    "market_value": float(p.market_value),
                    "avg_entry_price": float(p.avg_entry_price),
                    "current_price": float(p.current_price),
                    "unrealized_pl": float(p.unrealized_pl),
                    "unrealized_plpc": float(p.unrealized_plpc),
                }
                for p in positions
            ],
        }
    except Exception as e:
        return {"error": str(e)}


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


@app.get("/")
def ui_root():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
