"""Tests for app.domain.strategy — pure strategy evaluation logic."""

import pytest
from app.domain.strategy import evaluate, needs_price_data, describe


class TestFixedWeight:
    def test_returns_configured_weights(self):
        alloc = evaluate("fixed_weight", ["A", "B"], {"weights": {"A": 0.6, "B": 0.4}})
        assert alloc == {"A": 0.6, "B": 0.4}

    def test_fills_missing_assets_with_zero(self):
        alloc = evaluate("fixed_weight", ["A", "B", "C"], {"weights": {"A": 0.7}})
        assert alloc["B"] == 0.0
        assert alloc["C"] == 0.0


class TestEqualWeight:
    def test_splits_evenly(self):
        alloc = evaluate("equal_weight", ["A", "B", "C"], {})
        for w in alloc.values():
            assert abs(w - 1 / 3) < 0.001

    def test_empty_assets(self):
        assert evaluate("equal_weight", [], {}) == {}


class TestMomentum:
    def test_overweights_top_performers(self):
        alloc = evaluate(
            "momentum", ["A", "B", "C"],
            {"top_n": 1, "top_weight": 0.70},
            price_changes={"A": 0.10, "B": -0.05, "C": 0.02},
        )
        assert alloc["A"] == 0.70
        assert alloc["B"] == pytest.approx(0.15, abs=0.01)

    def test_falls_back_without_prices(self):
        alloc = evaluate("momentum", ["A", "B"], {"top_n": 1}, price_changes=None)
        assert alloc["A"] == alloc["B"]


class TestContrarianSurge:
    def test_produces_valid_weights(self):
        alloc = evaluate(
            "contrarian_surge", ["A", "B", "C", "D"],
            {"surge_pct": 0.15, "contrarian_weight": 0.60},
            price_changes={"A": 0.1, "B": -0.05, "C": 0.02, "D": -0.1},
        )
        assert abs(sum(alloc.values()) - 1.0) < 0.01
        assert all(v > 0 for v in alloc.values())

    def test_works_without_prices(self):
        alloc = evaluate("contrarian_surge", ["A", "B"], {}, price_changes=None)
        assert abs(sum(alloc.values()) - 1.0) < 0.01


class TestMeta:
    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown"):
            evaluate("magic", ["A"], {})

    def test_needs_price_data(self):
        assert needs_price_data("momentum") is True
        assert needs_price_data("contrarian_surge") is True
        assert needs_price_data("fixed_weight") is False
        assert needs_price_data("equal_weight") is False

    def test_describe_includes_allocation(self):
        desc = describe("fixed_weight", {"A": 0.5, "B": 0.5})
        assert "A: 50%" in desc
