"""Tests for the strategy evaluation engine."""

import pytest

from app.strategy_engine import evaluate_strategy


def test_fixed_weight_returns_configured_weights():
    alloc = evaluate_strategy(
        "fixed_weight",
        ["BTC/USD", "ETH/USD"],
        {"weights": {"BTC/USD": 0.6, "ETH/USD": 0.4}},
    )
    assert alloc == {"BTC/USD": 0.6, "ETH/USD": 0.4}


def test_fixed_weight_fills_missing_assets_with_zero():
    alloc = evaluate_strategy(
        "fixed_weight",
        ["BTC/USD", "ETH/USD", "SOL/USD"],
        {"weights": {"BTC/USD": 0.7}},
    )
    assert alloc["SOL/USD"] == 0.0
    assert alloc["ETH/USD"] == 0.0


def test_equal_weight_splits_evenly():
    alloc = evaluate_strategy("equal_weight", ["BTC/USD", "ETH/USD", "SOL/USD"], {})
    for w in alloc.values():
        assert abs(w - 1 / 3) < 0.001


def test_momentum_overweights_top_performers():
    alloc = evaluate_strategy(
        "momentum",
        ["BTC/USD", "ETH/USD", "SOL/USD"],
        {"top_n": 1, "top_weight": 0.70},
        price_changes={"BTC/USD": 0.10, "ETH/USD": -0.05, "SOL/USD": 0.02},
    )
    assert alloc["BTC/USD"] == 0.70
    assert alloc["ETH/USD"] == pytest.approx(0.15, abs=0.01)
    assert alloc["SOL/USD"] == pytest.approx(0.15, abs=0.01)


def test_momentum_falls_back_to_equal_without_prices():
    alloc = evaluate_strategy(
        "momentum",
        ["BTC/USD", "ETH/USD"],
        {"top_n": 1, "top_weight": 0.70},
        price_changes=None,
    )
    assert alloc["BTC/USD"] == alloc["ETH/USD"]


def test_unknown_strategy_type_raises():
    with pytest.raises(ValueError, match="Unknown strategy type"):
        evaluate_strategy("magic", ["BTC/USD"], {})
