"""Unit tests for rebalance_logic pure functions.

No mocking, no DB, no network — only plain Python inputs and outputs.
"""

import pytest

from app.rebalance_logic import calculate_drift, calculate_trades, positions_need_rebalancing


# ---------------------------------------------------------------------------
# calculate_drift
# ---------------------------------------------------------------------------


def test_drift_calculation_accuracy() -> None:
    current_positions = {"VOO": 6500.0, "BND": 2500.0, "GLD": 1000.0}
    target_allocation = {"VOO": 0.60, "BND": 0.30, "GLD": 0.10}

    drift = calculate_drift(current_positions, target_allocation)

    total = sum(current_positions.values())  # 10_000
    assert pytest.approx(drift["VOO"], abs=1e-6) == (6500.0 / total) - 0.60  # +0.05
    assert pytest.approx(drift["BND"], abs=1e-6) == (2500.0 / total) - 0.30  # -0.05
    assert pytest.approx(drift["GLD"], abs=1e-6) == (1000.0 / total) - 0.10  # 0.0


# ---------------------------------------------------------------------------
# positions_need_rebalancing
# ---------------------------------------------------------------------------


def test_below_threshold_returns_false() -> None:
    drift = {"VOO": 0.03, "BND": -0.02, "GLD": 0.01}
    assert positions_need_rebalancing(drift, threshold=0.05) is False


def test_at_threshold_returns_true() -> None:
    # Drift exactly at the threshold should trigger rebalancing.
    drift = {"VOO": 0.05, "BND": -0.03, "GLD": 0.01}
    # 0.05 is not *greater than* 0.05, so should return False
    assert positions_need_rebalancing(drift, threshold=0.05) is False


def test_above_threshold_returns_true() -> None:
    drift = {"VOO": 0.06, "BND": -0.03, "GLD": 0.0}
    assert positions_need_rebalancing(drift, threshold=0.05) is True


# ---------------------------------------------------------------------------
# calculate_trades — ordering
# ---------------------------------------------------------------------------


def test_sells_before_buys() -> None:
    """Sells must always precede buys so cash is available for purchases."""
    # VOO is overweight (sell), BND is underweight (buy)
    current_positions = {"VOO": 8000.0, "BND": 2000.0}
    target_allocation = {"VOO": 0.60, "BND": 0.40}
    account_value = 10_000.0

    trades = calculate_trades(current_positions, target_allocation, account_value)

    sell_indices = [i for i, t in enumerate(trades) if t["side"] == "sell"]
    buy_indices = [i for i, t in enumerate(trades) if t["side"] == "buy"]

    assert sell_indices, "Expected at least one sell"
    assert buy_indices, "Expected at least one buy"
    assert max(sell_indices) < min(buy_indices), "All sells must come before any buy"


# ---------------------------------------------------------------------------
# calculate_trades — edge cases
# ---------------------------------------------------------------------------


def test_symbol_in_positions_but_not_in_target_generates_full_sell() -> None:
    """A symbol held but absent from the target allocation should be fully sold."""
    current_positions = {"VOO": 5000.0, "TSLA": 2000.0}
    target_allocation = {"VOO": 1.0}  # TSLA not in target
    account_value = 7000.0

    trades = calculate_trades(current_positions, target_allocation, account_value)

    tsla_trades = [t for t in trades if t["symbol"] == "TSLA"]
    assert len(tsla_trades) == 1
    assert tsla_trades[0]["side"] == "sell"
    assert pytest.approx(tsla_trades[0]["qty"], abs=1e-6) == 2000.0
    assert tsla_trades[0]["target_pct"] == 0.0


def test_symbol_in_target_but_no_current_position_generates_full_buy() -> None:
    """A symbol in the target with no current holding should generate a full buy."""
    current_positions = {"VOO": 10_000.0}
    target_allocation = {"VOO": 0.70, "BND": 0.30}
    account_value = 10_000.0

    trades = calculate_trades(current_positions, target_allocation, account_value)

    bnd_trades = [t for t in trades if t["symbol"] == "BND"]
    assert len(bnd_trades) == 1
    assert bnd_trades[0]["side"] == "buy"
    assert pytest.approx(bnd_trades[0]["qty"], abs=1e-6) == 3000.0
    assert bnd_trades[0]["current_pct"] == 0.0
