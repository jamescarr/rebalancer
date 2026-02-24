"""Tests for app.domain.trading — trade planning and holdings management."""

import pytest
from app.domain.trading import (
    apply_fills_to_holdings,
    calculate_drift,
    holdings_to_positions,
    max_drift,
    needs_rebalancing,
    plan_trades,
)


class TestDrift:
    def test_calculation_accuracy(self):
        drift = calculate_drift({"A": 600, "B": 400}, {"A": 0.50, "B": 0.50})
        assert drift["A"] == pytest.approx(0.10)
        assert drift["B"] == pytest.approx(-0.10)

    def test_missing_position_is_negative_drift(self):
        drift = calculate_drift({}, {"A": 0.50})
        assert drift["A"] == pytest.approx(-0.50)

    def test_needs_rebalancing_above_threshold(self):
        drift = {"A": 0.06, "B": -0.06}
        assert needs_rebalancing(drift, 0.05) is True

    def test_no_rebalancing_below_threshold(self):
        drift = {"A": 0.02, "B": -0.02}
        assert needs_rebalancing(drift, 0.05) is False

    def test_max_drift(self):
        assert max_drift({"A": -0.08, "B": 0.03}) == pytest.approx(0.08)


class TestPlanTrades:
    def test_generates_buys_for_empty_portfolio(self):
        planned = plan_trades({}, {"A": 0.5, "B": 0.5}, budget=1000, available_cash=1000)
        symbols = {t["symbol"] for t in planned}
        assert symbols == {"A", "B"}
        assert all(t["side"] == "buy" for t in planned)

    def test_sells_before_buys(self):
        planned = plan_trades(
            {"A": 800, "B": 200}, {"A": 0.50, "B": 0.50},
            budget=1000, available_cash=500,
        )
        sell_idx = next(i for i, t in enumerate(planned) if t["side"] == "sell")
        buy_idx = next(i for i, t in enumerate(planned) if t["side"] == "buy")
        assert sell_idx < buy_idx

    def test_caps_to_available_cash(self):
        planned = plan_trades({}, {"A": 0.5, "B": 0.5}, budget=10000, available_cash=100)
        total = sum(t["qty"] for t in planned if t["side"] == "buy")
        assert total <= 100

    def test_caps_individual_trade_to_budget(self):
        planned = plan_trades(
            {}, {"A": 1.0}, budget=500, available_cash=10000,
        )
        assert planned[0]["qty"] <= 500

    def test_drops_trades_below_minimum(self):
        planned = plan_trades(
            {"A": 495}, {"A": 0.5}, budget=1000, available_cash=500,
        )
        assert len(planned) == 0 or all(t["qty"] >= 10 for t in planned)


class TestHoldingsToPositions:
    def test_multiplies_qty_by_price(self):
        holdings = {"BTCUSD": {"qty": 0.5, "cost_basis": 30000}}
        prices = {"BTCUSD": 64000}
        pos = holdings_to_positions(holdings, prices, ["BTCUSD"])
        assert pos["BTCUSD"] == 32000.0

    def test_missing_price_is_zero(self):
        holdings = {"BTCUSD": {"qty": 1.0}}
        pos = holdings_to_positions(holdings, {}, ["BTCUSD"])
        assert pos["BTCUSD"] == 0.0


class TestApplyFills:
    def test_buy_increases_qty(self):
        h = apply_fills_to_holdings(
            {}, [{"symbol": "A", "side": "buy", "filled_qty": 10, "filled_avg_price": 5.0}],
        )
        assert h["A"]["qty"] == 10
        assert h["A"]["cost_basis"] == 50.0

    def test_sell_decreases_qty(self):
        h = apply_fills_to_holdings(
            {"A": {"qty": 10, "cost_basis": 50}},
            [{"symbol": "A", "side": "sell", "filled_qty": 4, "filled_avg_price": 6.0}],
        )
        assert h["A"]["qty"] == 6
        assert h["A"]["cost_basis"] == pytest.approx(30.0)

    def test_full_sell_removes_symbol(self):
        h = apply_fills_to_holdings(
            {"A": {"qty": 10, "cost_basis": 50}},
            [{"symbol": "A", "side": "sell", "filled_qty": 10, "filled_avg_price": 5.0}],
        )
        assert "A" not in h

    def test_multiple_fills(self):
        h = apply_fills_to_holdings({}, [
            {"symbol": "A", "side": "buy", "filled_qty": 5, "filled_avg_price": 10},
            {"symbol": "B", "side": "buy", "filled_qty": 3, "filled_avg_price": 20},
            {"symbol": "A", "side": "buy", "filled_qty": 2, "filled_avg_price": 12},
        ])
        assert h["A"]["qty"] == 7
        assert h["B"]["qty"] == 3
