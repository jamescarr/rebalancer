from alpaca.common.exceptions import APIError as AlpacaAPIError  # re-exported
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.models import Order, Position
from alpaca.trading.requests import MarketOrderRequest

from app.config import Settings, get_settings

__all__ = ["AlpacaClient", "get_alpaca_client", "AlpacaAPIError"]


class AlpacaClient:
    """Thin wrapper around alpaca-py's TradingClient + CryptoHistoricalDataClient."""

    def __init__(self, api_key: str, secret_key: str, base_url: str) -> None:
        if not api_key:
            raise ValueError("ALPACA_API_KEY is required")
        if not secret_key:
            raise ValueError("ALPACA_SECRET_KEY is required")
        if not base_url:
            raise ValueError("ALPACA_API_BASE_URL is required")

        self._client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            url_override=base_url,
        )
        self._data_client = CryptoHistoricalDataClient(
            api_key=api_key,
            secret_key=secret_key,
        )

    def get_account(self) -> object:
        return self._client.get_account()

    def get_positions(self) -> list[Position]:
        return self._client.get_all_positions()  # type: ignore[return-value]

    def submit_market_order(self, symbol: str, qty: float, side: str) -> Order:
        """Submit a notional (dollar amount) market order."""
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        request = MarketOrderRequest(
            symbol=symbol,
            notional=qty,
            side=order_side,
            time_in_force=TimeInForce.GTC,
        )
        return self._client.submit_order(request)  # type: ignore[return-value]

    def submit_qty_market_order(self, symbol: str, qty: float, side: str) -> Order:
        """Submit a quantity (number of units) market order."""
        order_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        request = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=order_side,
            time_in_force=TimeInForce.GTC,
        )
        return self._client.submit_order(request)  # type: ignore[return-value]

    def get_order(self, order_id: str) -> Order:
        return self._client.get_order_by_id(order_id)  # type: ignore[return-value]

    def cancel_order(self, order_id: str) -> None:
        self._client.cancel_order_by_id(order_id)

    def get_crypto_performance(
        self, symbols: list[str], lookback_days: int = 7,
    ) -> dict[str, float]:
        """Return the % price change over the lookback period for each symbol.

        Returns {symbol: pct_change} where pct_change is e.g. 0.05 for +5%.
        Symbols with insufficient data get 0.0.
        """
        from datetime import datetime, timedelta, timezone

        now = datetime.now(tz=timezone.utc)
        start = now - timedelta(days=lookback_days)

        def _to_data_symbol(s: str) -> str:
            if "/" not in s and s.endswith("USD") and len(s) > 3:
                return s[:-3] + "/USD"
            return s

        data_symbols = [_to_data_symbol(s) for s in symbols]
        data_to_orig = dict(zip(data_symbols, symbols))

        request = CryptoBarsRequest(
            symbol_or_symbols=data_symbols,
            timeframe=TimeFrame.Day,
            start=start,
            end=now,
        )
        bars = self._data_client.get_crypto_bars(request)
        bars_dict = bars.data if hasattr(bars, "data") else dict(bars)

        changes: dict[str, float] = {}
        for data_sym, orig_sym in data_to_orig.items():
            symbol_bars = bars_dict.get(data_sym, [])
            if symbol_bars and len(symbol_bars) >= 2:
                first_close = float(symbol_bars[0].close)
                last_close = float(symbol_bars[-1].close)
                if first_close > 0:
                    changes[orig_sym] = (last_close - first_close) / first_close
                else:
                    changes[orig_sym] = 0.0
            else:
                changes[orig_sym] = 0.0

        return changes


def get_alpaca_client(settings: Settings | None = None) -> AlpacaClient:
    s = settings or get_settings()
    return AlpacaClient(
        api_key=s.ALPACA_API_KEY,
        secret_key=s.ALPACA_SECRET_KEY,
        base_url=s.ALPACA_API_BASE_URL,
    )
