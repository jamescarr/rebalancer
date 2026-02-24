"""Strategy evaluation engine — pure functions that compute target allocations.

Each strategy type produces a {symbol: weight} allocation dict.
The rebalance logic then compares this to current positions and computes trades.
"""

from __future__ import annotations


def evaluate_strategy(
    strategy_type: str,
    assets: list[str],
    config: dict,
    price_changes: dict[str, float] | None = None,
) -> dict[str, float]:
    """Dispatch to the correct strategy evaluator and return target weights."""
    evaluators = {
        "fixed_weight": _eval_fixed_weight,
        "equal_weight": _eval_equal_weight,
        "momentum": _eval_momentum,
        "contrarian_surge": _eval_contrarian_surge,
    }
    fn = evaluators.get(strategy_type)
    if fn is None:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
    return fn(assets, config, price_changes)


def _eval_fixed_weight(
    assets: list[str], config: dict, _price_changes: dict | None
) -> dict[str, float]:
    """Return the statically configured weights."""
    weights: dict[str, float] = config.get("weights", {})
    for a in assets:
        weights.setdefault(a, 0.0)
    return weights


def _eval_equal_weight(
    assets: list[str], _config: dict, _price_changes: dict | None
) -> dict[str, float]:
    """Split allocation equally across all assets."""
    n = len(assets)
    if n == 0:
        return {}
    w = round(1.0 / n, 6)
    return {a: w for a in assets}


def _eval_momentum(
    assets: list[str], config: dict, price_changes: dict[str, float] | None
) -> dict[str, float]:
    """Overweight top performers, underweight laggards.

    Config keys:
      top_n (int): Number of top assets to overweight. Defaults to half.
      top_weight (float): Total weight given to top bucket. Default 0.70.
    """
    if not assets:
        return {}

    top_n = config.get("top_n", max(1, len(assets) // 2))
    top_weight = config.get("top_weight", 0.70)
    bottom_weight = 1.0 - top_weight

    if price_changes is None:
        return {a: round(1.0 / len(assets), 6) for a in assets}

    ranked = sorted(assets, key=lambda s: price_changes.get(s, 0.0), reverse=True)
    top = ranked[:top_n]
    bottom = ranked[top_n:]

    allocation: dict[str, float] = {}
    if top:
        w = round(top_weight / len(top), 6)
        for a in top:
            allocation[a] = w
    if bottom:
        w = round(bottom_weight / len(bottom), 6)
        for a in bottom:
            allocation[a] = w

    return allocation


def _eval_contrarian_surge(
    assets: list[str], config: dict, price_changes: dict[str, float] | None
) -> dict[str, float]:
    """Mean-reversion with a rotating surge — guarantees different weights every cycle.

    Two forces ensure constant rebalancing:
    1. Contrarian tilt: rank by recent performance, overweight the WORST performers
       (buy the dip). Since prices always move, rankings always shift.
    2. Surge rotation: one asset per cycle gets a bonus allocation bump. The surge
       index is based on the current timestamp so it advances every evaluation.

    Config keys:
      surge_pct (float): Extra weight for the surge asset. Default 0.15 (15%).
      contrarian_weight (float): Total weight for the bottom half. Default 0.60.
    """
    import time

    if not assets:
        return {}

    n = len(assets)
    surge_pct = config.get("surge_pct", 0.15)
    contrarian_weight = config.get("contrarian_weight", 0.60)
    top_weight = 1.0 - contrarian_weight - surge_pct

    surge_idx = int(time.time() // 30) % n
    surge_asset = assets[surge_idx]

    if price_changes is None:
        base = round((1.0 - surge_pct) / n, 6)
        alloc = {a: base for a in assets}
        alloc[surge_asset] = round(base + surge_pct, 6)
        return alloc

    ranked = sorted(assets, key=lambda s: price_changes.get(s, 0.0))
    bottom_half = ranked[: n // 2]
    top_half = ranked[n // 2:]

    allocation: dict[str, float] = {}

    remaining = 1.0 - surge_pct
    if bottom_half:
        w = round((contrarian_weight / len(bottom_half)), 6)
        for a in bottom_half:
            allocation[a] = w
        remaining -= contrarian_weight
    if top_half:
        w = round((remaining / len(top_half)), 6)
        for a in top_half:
            allocation[a] = w

    allocation[surge_asset] = round(allocation.get(surge_asset, 0) + surge_pct, 6)

    total = sum(allocation.values())
    if total > 0:
        allocation = {k: round(v / total, 6) for k, v in allocation.items()}

    return allocation


def describe_evaluation(
    strategy_type: str,
    allocation: dict[str, float],
    price_changes: dict[str, float] | None = None,
) -> str:
    """Human-readable summary of what the engine decided."""
    parts = [f"Strategy type: {strategy_type}"]
    if price_changes:
        ranked = sorted(price_changes.items(), key=lambda x: x[1], reverse=True)
        perf = ", ".join(f"{s}: {c:+.1%}" for s, c in ranked)
        parts.append(f"Performance: {perf}")
    alloc = ", ".join(f"{s}: {w:.0%}" for s, w in allocation.items())
    parts.append(f"Target: {alloc}")
    return " | ".join(parts)
