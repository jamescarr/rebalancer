"""Workflow tests using Temporal's time-skipping test environment.

Activities are mocked so tests run without Alpaca or a database.
"""

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.activities.broker import OrderResult
from app.activities.persistence import ActiveStrategy
from app.workflows.rebalance import RebalanceInput, RebalanceWorkflow
from app.workflows.liquidation import LiquidationInput, LiquidationWorkflow

TASK_QUEUE = "test-queue"

MOCK_SNAP = {
    "id": "strat-1",
    "strategy_type": "equal_weight",
    "assets": ["BTCUSD", "ETHUSD"],
    "config": {},
    "drift_threshold": 0.05,
    "funded_amount": 1000.0,
    "holdings": {},
    "is_active": True,
    "rebalance_interval_seconds": 86400,
}


@activity.defn(name="load_strategy_snapshot")
async def mock_load_snapshot(strategy_id: str) -> dict | None:
    return MOCK_SNAP


@activity.defn(name="fetch_positions")
async def mock_fetch_positions() -> dict[str, float]:
    return {"BTCUSD": 64000.0, "ETHUSD": 1800.0}


@activity.defn(name="fetch_account_cash")
async def mock_fetch_cash() -> float:
    return 5000.0


@activity.defn(name="submit_notional_order")
async def mock_submit_order(symbol: str, qty: float, side: str) -> OrderResult:
    return OrderResult(order_id=f"order-{symbol}", symbol=symbol, status="accepted")


@activity.defn(name="check_order_status")
async def mock_check_status(order_id: str) -> dict:
    return {"order_id": order_id, "status": "filled", "filled_qty": 0.01, "filled_avg_price": 50000.0}


@activity.defn(name="get_fills_for_job")
async def mock_get_fills(job_id: str) -> list[dict]:
    return [
        {"symbol": "BTCUSD", "side": "buy", "filled_qty": 0.01, "filled_avg_price": 64000.0},
        {"symbol": "ETHUSD", "side": "buy", "filled_qty": 0.3, "filled_avg_price": 1800.0},
    ]


@activity.defn(name="submit_qty_order")
async def mock_submit_qty_order(symbol: str, qty: float, side: str) -> OrderResult:
    return OrderResult(order_id=f"liq-{symbol}", symbol=symbol, status="accepted")


# No-op persistence activities
@activity.defn(name="mark_job_running")
async def noop_mark_running(job_id: str) -> None: pass

@activity.defn(name="mark_job_no_action")
async def noop_mark_no_action(job_id: str, reason: str) -> None: pass

@activity.defn(name="mark_job_executing")
async def noop_mark_executing(job_id: str) -> None: pass

@activity.defn(name="mark_job_completed")
async def noop_mark_completed(job_id: str) -> None: pass

@activity.defn(name="mark_job_stuck")
async def noop_mark_stuck(job_id: str, error: str) -> None: pass

@activity.defn(name="set_job_target_allocation")
async def noop_set_alloc(job_id: str, allocation: dict) -> None: pass

@activity.defn(name="set_job_trades_calculated")
async def noop_set_calc(job_id: str) -> None: pass

@activity.defn(name="record_trade_submission")
async def noop_record(job_id: str, symbol: str, side: str, qty: float, order_id: str) -> None: pass

@activity.defn(name="update_trade_fill")
async def noop_fill(order_id: str, status: str, filled_qty: float, avg_price: float | None) -> None: pass

@activity.defn(name="update_strategy_evaluation")
async def noop_eval(strategy_id: str, allocation: dict, description: str, interval_seconds: int) -> None: pass

@activity.defn(name="update_strategy_holdings")
async def noop_holdings(strategy_id: str, holdings: dict) -> None: pass

@activity.defn(name="clear_strategy_for_liquidation")
async def noop_clear(strategy_id: str) -> None: pass


ALL_MOCK_ACTIVITIES = [
    mock_load_snapshot, mock_fetch_positions, mock_fetch_cash,
    mock_submit_order, mock_check_status, mock_get_fills, mock_submit_qty_order,
    noop_mark_running, noop_mark_no_action, noop_mark_executing,
    noop_mark_completed, noop_mark_stuck, noop_set_alloc, noop_set_calc,
    noop_record, noop_fill, noop_eval, noop_holdings, noop_clear,
]


@pytest.mark.asyncio
async def test_rebalance_workflow_completes():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=[RebalanceWorkflow], activities=ALL_MOCK_ACTIVITIES):
            result = await env.client.execute_workflow(
                RebalanceWorkflow.run,
                RebalanceInput(job_id="job-1", strategy_id="strat-1"),
                id="test-rebalance-1",
                task_queue=TASK_QUEUE,
            )
            assert result == "completed"


@pytest.mark.asyncio
async def test_rebalance_workflow_no_action_when_inactive():
    @activity.defn(name="load_strategy_snapshot")
    async def load_inactive(strategy_id: str) -> dict | None:
        snap = dict(MOCK_SNAP)
        snap["is_active"] = False
        return snap

    activities = [a for a in ALL_MOCK_ACTIVITIES if a.__name__ != "mock_load_snapshot"]
    activities.append(load_inactive)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=[RebalanceWorkflow], activities=activities):
            result = await env.client.execute_workflow(
                RebalanceWorkflow.run,
                RebalanceInput(job_id="job-2", strategy_id="strat-1"),
                id="test-rebalance-inactive",
                task_queue=TASK_QUEUE,
            )
            assert result == "no_action"


@pytest.mark.asyncio
async def test_liquidation_workflow_completes():
    @activity.defn(name="load_strategy_snapshot")
    async def load_with_holdings(strategy_id: str) -> dict | None:
        snap = dict(MOCK_SNAP)
        snap["holdings"] = {"BTCUSD": {"qty": 0.01, "cost_basis": 640}}
        return snap

    activities = [a for a in ALL_MOCK_ACTIVITIES if a.__name__ != "mock_load_snapshot"]
    activities.append(load_with_holdings)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=[LiquidationWorkflow], activities=activities):
            result = await env.client.execute_workflow(
                LiquidationWorkflow.run,
                LiquidationInput(job_id="job-3", strategy_id="strat-1"),
                id="test-liquidation-1",
                task_queue=TASK_QUEUE,
            )
            assert result == "completed"


@pytest.mark.asyncio
async def test_liquidation_workflow_empty_holdings():
    @activity.defn(name="load_strategy_snapshot")
    async def load_empty(strategy_id: str) -> dict | None:
        snap = dict(MOCK_SNAP)
        snap["holdings"] = {}
        return snap

    activities = [a for a in ALL_MOCK_ACTIVITIES if a.__name__ != "mock_load_snapshot"]
    activities.append(load_empty)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue=TASK_QUEUE,
                          workflows=[LiquidationWorkflow], activities=activities):
            result = await env.client.execute_workflow(
                LiquidationWorkflow.run,
                LiquidationInput(job_id="job-4", strategy_id="strat-1"),
                id="test-liquidation-empty",
                task_queue=TASK_QUEUE,
            )
            assert result == "completed"
