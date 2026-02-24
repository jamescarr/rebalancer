"""Trade planning — pure functions for drift detection and trade sizing.

No framework dependencies. All inputs/outputs are plain Python types.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone

MIN_TRADE_NOTIONAL = 10.0


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def calculate_drift(
    current_positions: dict[str, float],
    target_allocation: dict[str, float],
) -> dict[str, float]:
    total_value = sum(current_positions.values())
    drift: dict[str, float] = {}
    for symbol, target_pct in target_allocation.items():
        current_pct = current_positions.get(symbol, 0.0) / total_value if total_value > 0 else 0.0
        drift[symbol] = current_pct - target_pct
    return drift


def needs_rebalancing(drift: dict[str, float], threshold: float) -> bool:
    return any(abs(d) > threshold for d in drift.values())


def max_drift(drift: dict[str, float]) -> float:
    return max((abs(d) for d in drift.values()), default=0.0)


def plan_trades(
    current_positions: dict[str, float],
    target_allocation: dict[str, float],
    budget: float,
    available_cash: float,
) -> list[dict]:
    """Compute the trades needed to bring positions in line with target allocation.

    Returns sells before buys. Individual trades are capped to the budget.
    Total buys are scaled down if they exceed available cash.
    Trades below $10 (Alpaca crypto minimum) are dropped.
    """
    sells: list[dict] = []
    buys: list[dict] = []

    for symbol in set(target_allocation.keys()) | set(current_positions.keys()):
        cur_val = current_positions.get(symbol, 0.0)
        target_pct = target_allocation.get(symbol, 0.0)
        target_val = target_pct * budget
        diff = target_val - cur_val

        if diff < -MIN_TRADE_NOTIONAL:
            qty = min(round(abs(diff), 2), budget)
            sells.append({"symbol": symbol, "side": "sell", "qty": qty})
        elif diff > MIN_TRADE_NOTIONAL:
            qty = min(_floor2(diff), budget)
            buys.append({"symbol": symbol, "side": "buy", "qty": qty})

    buys = _cap_buys_to_cash(buys, sells, available_cash, budget)
    return sells + buys


def holdings_to_positions(
    holdings: dict, prices: dict[str, float], assets: list[str],
) -> dict[str, float]:
    """Convert a strategy's holdings {symbol: {qty, cost_basis}} + prices into {symbol: market_value}."""
    positions: dict[str, float] = {}
    for symbol in assets:
        info = holdings.get(symbol, {})
        qty = info.get("qty", 0) if isinstance(info, dict) else 0
        positions[symbol] = qty * prices.get(symbol, 0)
    return positions


def apply_fills_to_holdings(
    holdings: dict,
    fills: list[dict],
) -> dict:
    """Apply a list of fill dicts to holdings and return the updated holdings.

    Each fill: {symbol, side, filled_qty, filled_avg_price}
    """
    h = dict(holdings)
    for fill in fills:
        sym = fill["symbol"]
        info = h.get(sym, {"qty": 0, "cost_basis": 0})
        if isinstance(info, (int, float)):
            info = {"qty": info, "cost_basis": 0}
        info = dict(info)

        fq = fill.get("filled_qty", 0)
        fp = fill.get("filled_avg_price", 0)

        if fill["side"] == "buy":
            info["cost_basis"] = info.get("cost_basis", 0) + fq * fp
            info["qty"] = info.get("qty", 0) + fq
        elif fill["side"] == "sell":
            old_qty = info.get("qty", 0)
            if old_qty > 0:
                info["cost_basis"] = info.get("cost_basis", 0) * (1 - min(fq / old_qty, 1.0))
            info["qty"] = max(0, old_qty - fq)

        if info["qty"] < 1e-6:
            h.pop(sym, None)
        else:
            h[sym] = info
    return h


def _floor2(value: float) -> float:
    return math.floor(value * 100) / 100


def _cap_buys_to_cash(
    buys: list[dict], sells: list[dict], available_cash: float, budget: float,
) -> list[dict]:
    total_buys = sum(t["qty"] for t in buys)
    sell_proceeds = sum(t["qty"] for t in sells)
    max_spend = _floor2(min(available_cash + sell_proceeds, budget))

    if total_buys <= max_spend or total_buys == 0:
        return buys

    scale = max_spend / total_buys
    return [
        {**t, "qty": _floor2(t["qty"] * scale)}
        for t in buys
        if _floor2(t["qty"] * scale) >= MIN_TRADE_NOTIONAL
    ]
