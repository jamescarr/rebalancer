# Pure functions — zero imports from the rest of the app.
# All inputs and outputs are plain Python dicts/lists/floats.
# This module must remain side-effect-free so it can be unit tested in
# isolation and eventually transplanted into a Temporal workflow activity.

import math


def calculate_drift(
    current_positions: dict[str, float],
    target_allocation: dict[str, float],
) -> dict[str, float]:
    """Return the drift of each target symbol from its target weight.

    current_positions: {symbol: market_value_in_dollars}
    target_allocation: {symbol: target_weight, e.g. 0.60}

    Returns {symbol: actual_weight - target_weight} for each symbol in
    target_allocation.  Positive means overweight; negative means underweight.
    Symbols present in current_positions but absent from target_allocation are
    not included in the drift result — calculate_trades handles those as full
    sells.
    """
    total_value = sum(current_positions.values())

    drift: dict[str, float] = {}
    for symbol, target_pct in target_allocation.items():
        if total_value > 0:
            current_pct = current_positions.get(symbol, 0.0) / total_value
        else:
            current_pct = 0.0
        drift[symbol] = current_pct - target_pct

    return drift


def positions_need_rebalancing(drift: dict[str, float], threshold: float) -> bool:
    """Return True if any symbol's absolute drift exceeds the threshold."""
    return any(abs(d) > threshold for d in drift.values())


def calculate_trades(
    current_positions: dict[str, float],
    target_allocation: dict[str, float],
    account_value: float,
    max_allocation_pct: float = 1.0,
    min_trade_notional: float = 10.0,
    available_cash: float | None = None,
) -> list[dict]:
    """Calculate the trades required to bring the portfolio to target allocation.

    current_positions: {symbol: market_value_in_dollars}
    target_allocation: {symbol: target_weight}
    account_value: total account equity in dollars (cash + positions)
    max_allocation_pct: fraction of account to allocate (e.g. 0.10 = 10%)
    min_trade_notional: skip trades smaller than this dollar amount
    available_cash: if provided, total buys will be capped to this amount

    Returns a list of trade dicts: {symbol, side, qty, target_pct, current_pct}
    where qty is a notional dollar amount truncated to 2 decimals.

    Sells are returned before buys so that cash is freed before purchases are
    made — this prevents insufficient-funds rejections during execution.
    Buy amounts are floored (not rounded) to avoid overdrawing cash.
    """
    budget = account_value * max_allocation_pct
    all_symbols = set(target_allocation.keys()) | set(current_positions.keys())

    sells: list[dict] = []
    buys: list[dict] = []

    for symbol in all_symbols:
        current_value = current_positions.get(symbol, 0.0)
        target_pct = target_allocation.get(symbol, 0.0)
        target_value = target_pct * budget
        diff = target_value - current_value
        current_pct = current_value / budget if budget > 0 else 0.0

        if diff < 0:
            qty = round(abs(diff), 2)
            if qty < min_trade_notional:
                continue
            sells.append({
                "symbol": symbol, "side": "sell", "qty": qty,
                "target_pct": target_pct, "current_pct": current_pct,
            })
        elif diff > 0:
            qty = math.floor(diff * 100) / 100
            if qty < min_trade_notional:
                continue
            buys.append({
                "symbol": symbol, "side": "buy", "qty": qty,
                "target_pct": target_pct, "current_pct": current_pct,
            })

    if available_cash is not None and buys:
        sell_proceeds = sum(t["qty"] for t in sells)
        cash_for_buys = math.floor((available_cash + sell_proceeds) * 100) / 100
        total_buys = sum(t["qty"] for t in buys)

        if total_buys > cash_for_buys and total_buys > 0:
            scale = cash_for_buys / total_buys
            scaled_buys = []
            for t in buys:
                t["qty"] = math.floor(t["qty"] * scale * 100) / 100
                if t["qty"] >= min_trade_notional:
                    scaled_buys.append(t)
            buys = scaled_buys

    return sells + buys
