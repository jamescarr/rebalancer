import uuid
from pathlib import Path

from celery.app.control import Inspect
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.celery_app import app as celery_app
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
from app.tasks import liquidate_strategy_holdings, run_strategy_rebalance

app = FastAPI(title="Rebalancer", description="Crypto strategy rebalancing service")

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


@app.get("/strategies", response_model=list[StrategyResponse])
def list_strategies(db: Session = Depends(get_db)) -> list[Strategy]:
    return list(
        db.execute(select(Strategy).order_by(Strategy.created_at)).scalars().all()
    )


@app.post("/strategies", response_model=StrategyResponse, status_code=201)
def create_strategy(
    body: CreateStrategyRequest, db: Session = Depends(get_db)
) -> Strategy:
    if body.strategy_type not in STRATEGY_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"strategy_type must be one of {sorted(STRATEGY_TYPES)}",
        )
    strategy = Strategy(
        name=body.name,
        description=body.description,
        strategy_type=body.strategy_type,
        assets=body.assets,
        config=body.config,
        rebalance_interval_seconds=body.rebalance_interval_seconds,
        drift_threshold=body.drift_threshold,
        is_active=False,
        funded_amount=None,
        holdings={},
    )
    db.add(strategy)
    db.commit()
    db.refresh(strategy)
    return strategy


@app.get("/strategies/{strategy_id}", response_model=StrategyResponse)
def get_strategy(strategy_id: uuid.UUID, db: Session = Depends(get_db)) -> Strategy:
    strategy = db.execute(
        select(Strategy).where(Strategy.id == strategy_id)
    ).scalar_one_or_none()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    return strategy


@app.patch("/strategies/{strategy_id}", response_model=StrategyResponse)
def update_strategy(
    strategy_id: uuid.UUID,
    body: UpdateStrategyRequest,
    db: Session = Depends(get_db),
) -> Strategy:
    strategy = db.execute(
        select(Strategy).where(Strategy.id == strategy_id)
    ).scalar_one_or_none()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")

    updates = body.model_dump(exclude_unset=True)
    for key, value in updates.items():
        setattr(strategy, key, value)
    db.commit()
    db.refresh(strategy)
    return strategy


@app.delete("/strategies/{strategy_id}", status_code=204)
def delete_strategy(strategy_id: uuid.UUID, db: Session = Depends(get_db)) -> None:
    strategy = db.execute(
        select(Strategy).where(Strategy.id == strategy_id)
    ).scalar_one_or_none()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if strategy.is_active:
        raise HTTPException(status_code=409, detail="Liquidate strategy before deleting")
    db.delete(strategy)
    db.commit()


@app.post("/strategies/{strategy_id}/fund", response_model=StrategyResponse)
def fund_strategy(
    strategy_id: uuid.UUID,
    body: FundStrategyRequest,
    db: Session = Depends(get_db),
) -> Strategy:
    strategy = db.execute(
        select(Strategy).where(Strategy.id == strategy_id)
    ).scalar_one_or_none()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if strategy.is_active:
        raise HTTPException(status_code=409, detail="Strategy is already funded and active")

    strategy.funded_amount = body.amount
    strategy.is_active = True
    strategy.holdings = {}
    db.commit()
    db.refresh(strategy)
    return strategy


@app.post("/strategies/{strategy_id}/liquidate", response_model=RebalanceJobResponse)
def liquidate_strategy(
    strategy_id: uuid.UUID,
    db: Session = Depends(get_db),
) -> RebalanceJob:
    strategy = db.execute(
        select(Strategy).where(Strategy.id == strategy_id)
    ).scalar_one_or_none()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if not strategy.holdings:
        strategy.is_active = False
        strategy.funded_amount = None
        db.commit()
        raise HTTPException(status_code=409, detail="Strategy has no holdings to liquidate")

    strategy.is_active = False

    job = RebalanceJob(
        strategy_id=strategy.id,
        account_id="liquidation",
        target_allocation={},
        drift_threshold=0,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    liquidate_strategy_holdings.delay(str(job.id), str(strategy.id))
    return job


# ---------------------------------------------------------------------------
# Rebalance jobs
# ---------------------------------------------------------------------------


@app.post("/rebalance", response_model=RebalanceJobResponse, status_code=202)
def trigger_rebalance(
    body: TriggerRebalanceRequest, db: Session = Depends(get_db)
) -> RebalanceJob:
    strategy = db.execute(
        select(Strategy).where(Strategy.id == body.strategy_id)
    ).scalar_one_or_none()
    if strategy is None:
        raise HTTPException(status_code=404, detail="Strategy not found")
    if not strategy.is_active or not strategy.funded_amount:
        raise HTTPException(status_code=409, detail="Strategy must be funded and active")

    job = RebalanceJob(
        strategy_id=strategy.id,
        account_id="default",
        target_allocation={},
        drift_threshold=strategy.drift_threshold,
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    run_strategy_rebalance.delay(str(job.id), str(strategy.id))
    return job


@app.get("/rebalance", response_model=list[RebalanceJobDetailResponse])
def list_rebalance_jobs(
    limit: int = 30, db: Session = Depends(get_db)
) -> list[RebalanceJob]:
    jobs = list(
        db.execute(
            select(RebalanceJob)
            .order_by(RebalanceJob.created_at.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    for job in jobs:
        _ = job.trade_executions
    return jobs


@app.get("/rebalance/{job_id}", response_model=RebalanceJobDetailResponse)
def get_rebalance_job(
    job_id: uuid.UUID, db: Session = Depends(get_db)
) -> RebalanceJob:
    job = db.execute(
        select(RebalanceJob).where(RebalanceJob.id == job_id)
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Rebalance job not found")
    _ = job.trade_executions
    return job


@app.delete("/rebalance/{job_id}", status_code=200)
def cancel_rebalance_job(
    job_id: uuid.UUID, db: Session = Depends(get_db)
) -> dict[str, str]:
    job = db.execute(
        select(RebalanceJob).where(RebalanceJob.id == job_id)
    ).scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=404, detail="Rebalance job not found")
    job.status = STATUS_CANCELLING
    db.commit()
    return {"status": STATUS_CANCELLING, "job_id": str(job_id)}


# ---------------------------------------------------------------------------
# Health + Account
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    try:
        inspector: Inspect = celery_app.control.inspect(timeout=1.0)
        active = inspector.active()
        if active is None:
            return HealthResponse(status="no_workers", active_tasks=None)
        all_tasks: list[dict] = []
        for worker_tasks in active.values():
            all_tasks.extend(worker_tasks)
        return HealthResponse(status="ok", active_tasks=all_tasks)
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
                    "symbol": p.symbol,
                    "qty": float(p.qty),
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
