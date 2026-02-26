"""Strategy evaluation and scheduling — pure functions.

No framework dependencies. All inputs/outputs are plain Python types.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone


def is_due_for_evaluation(next_evaluation_at: datetime | None) -> bool:
    """Return True if a strategy is past its scheduled evaluation time."""
    if next_evaluation_at is None:
        return True
    return datetime.now(tz=timezone.utc) >= next_evaluation_at


def evaluate(
    strategy_type: str,
    assets: list[str],
    config: dict,
    price_changes: dict[str, float] | None = None,
) -> dict[str, float]:
    evaluators = {
        "fixed_weight": _fixed_weight,
        "equal_weight": _equal_weight,
        "momentum": _momentum,
        "contrarian_surge": _contrarian_surge,
    }
    fn = evaluators.get(strategy_type)
    if fn is None:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
    return fn(assets, config, price_changes)


def describe(
    strategy_type: str,
    allocation: dict[str, float],
    price_changes: dict[str, float] | None = None,
) -> str:
    parts = [f"Strategy: {strategy_type}"]
    if price_changes:
        ranked = sorted(price_changes.items(), key=lambda x: x[1], reverse=True)
        parts.append("Perf: " + ", ".join(f"{s}: {c:+.1%}" for s, c in ranked))
    parts.append("Target: " + ", ".join(f"{s}: {w:.0%}" for s, w in allocation.items()))
    return " | ".join(parts)


def needs_price_data(strategy_type: str) -> bool:
    return strategy_type in ("momentum", "contrarian_surge")


# ---------------------------------------------------------------------------
# Evaluators
# ---------------------------------------------------------------------------


def _fixed_weight(assets, config, _pc):
    weights = dict(config.get("weights", {}))
    for a in assets:
        weights.setdefault(a, 0.0)
    return weights


def _equal_weight(assets, _config, _pc):
    n = len(assets)
    if n == 0:
        return {}
    w = round(1.0 / n, 6)
    return {a: w for a in assets}


def _momentum(assets, config, price_changes):
    if not assets:
        return {}
    top_n = config.get("top_n", max(1, len(assets) // 2))
    top_weight = config.get("top_weight", 0.70)
    bottom_weight = 1.0 - top_weight

    if price_changes is None:
        return {a: round(1.0 / len(assets), 6) for a in assets}

    ranked = sorted(assets, key=lambda s: price_changes.get(s, 0.0), reverse=True)
    top, bottom = ranked[:top_n], ranked[top_n:]
    alloc: dict[str, float] = {}
    if top:
        w = round(top_weight / len(top), 6)
        for a in top:
            alloc[a] = w
    if bottom:
        w = round(bottom_weight / len(bottom), 6)
        for a in bottom:
            alloc[a] = w
    return alloc


def _contrarian_surge(assets, config, price_changes):
    if not assets:
        return {}
    n = len(assets)
    surge_pct = config.get("surge_pct", 0.15)
    contrarian_weight = config.get("contrarian_weight", 0.60)
    surge_asset = assets[int(time.time() // 30) % n]

    if price_changes is None:
        base = round((1.0 - surge_pct) / n, 6)
        alloc = {a: base for a in assets}
        alloc[surge_asset] = round(base + surge_pct, 6)
        return alloc

    ranked = sorted(assets, key=lambda s: price_changes.get(s, 0.0))
    bottom_half, top_half = ranked[:n // 2], ranked[n // 2:]

    alloc: dict[str, float] = {}
    remaining = 1.0 - surge_pct
    if bottom_half:
        w = round(contrarian_weight / len(bottom_half), 6)
        for a in bottom_half:
            alloc[a] = w
        remaining -= contrarian_weight
    if top_half:
        w = round(remaining / len(top_half), 6)
        for a in top_half:
            alloc[a] = w

    alloc[surge_asset] = round(alloc.get(surge_asset, 0) + surge_pct, 6)
    total = sum(alloc.values())
    if total > 0:
        alloc = {k: round(v / total, 6) for k, v in alloc.items()}
    return alloc
