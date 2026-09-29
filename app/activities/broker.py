"""Broker activities: every side-effectful Alpaca API call is an activity.

Each activity is independently retryable with its own timeout and retry policy.
"""

from dataclasses import dataclass

from temporalio import activity

from app.alpaca_client import get_alpaca_client


@dataclass
class OrderResult:
    order_id: str
    symbol: str
    status: str


@activity.defn
async def fetch_positions() -> dict[str, float]:
    """Return {symbol: current_price} for all positions."""
    client = get_alpaca_client()
    positions = client.get_positions()
    return {p.symbol: float(p.current_price) for p in positions}


@activity.defn
async def fetch_account_cash() -> float:
    client = get_alpaca_client()
    account = client.get_account()
    return float(account.cash)


@activity.defn
async def fetch_crypto_performance(symbols: list[str], lookback_days: int) -> dict[str, float]:
    client = get_alpaca_client()
    return client.get_crypto_performance(symbols, lookback_days)


@activity.defn
async def submit_notional_order(symbol: str, qty: float, side: str) -> OrderResult:
    client = get_alpaca_client()
    order = client.submit_market_order(symbol=symbol, qty=qty, side=side)
    return OrderResult(order_id=str(order.id), symbol=symbol, status=str(order.status))


@activity.defn
async def submit_qty_order(symbol: str, qty: float, side: str) -> OrderResult:
    client = get_alpaca_client()
    order = client.submit_qty_market_order(symbol=symbol, qty=qty, side=side)
    return OrderResult(order_id=str(order.id), symbol=symbol, status=str(order.status))


@activity.defn
async def check_order_status(order_id: str) -> dict:
    client = get_alpaca_client()
    order = client.get_order(order_id)
    return {
        "order_id": order_id,
        "status": str(order.status).lower(),
        "filled_qty": float(order.filled_qty or 0),
        "filled_avg_price": float(order.filled_avg_price) if order.filled_avg_price else None,
    }
