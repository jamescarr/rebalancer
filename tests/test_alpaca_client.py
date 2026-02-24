"""Tests for AlpacaClient wrapper."""

from unittest.mock import MagicMock, patch

import pytest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.models import Order, Position

from app.alpaca_client import AlpacaAPIError, AlpacaClient

BASE_URL = "http://test-alpaca.example.com"


def _make_position(**overrides) -> MagicMock:
    pos = MagicMock(spec=Position)
    pos.symbol = overrides.get("symbol", "AAPL")
    pos.market_value = overrides.get("market_value", "1500.00")
    return pos


def _make_order(**overrides) -> MagicMock:
    order = MagicMock(spec=Order)
    order.symbol = overrides.get("symbol", "AAPL")
    order.status = overrides.get("status", "accepted")
    order.filled_qty = overrides.get("filled_qty", "0")
    order.filled_avg_price = overrides.get("filled_avg_price", None)
    return order


@patch("app.alpaca_client.CryptoHistoricalDataClient")
@patch("app.alpaca_client.TradingClient")
def test_get_positions_maps_response_fields(mock_tc_cls, mock_data_cls) -> None:
    mock_tc = mock_tc_cls.return_value
    mock_tc.get_all_positions.return_value = [_make_position(symbol="BTC/USD", market_value="50000.00")]

    client = AlpacaClient("test-key", "test-secret", BASE_URL)
    positions = client.get_positions()

    assert len(positions) == 1
    assert positions[0].symbol == "BTC/USD"
    assert float(positions[0].market_value) == 50000.0


@patch("app.alpaca_client.CryptoHistoricalDataClient")
@patch("app.alpaca_client.TradingClient")
def test_submit_market_order_sends_correct_payload(mock_tc_cls, mock_data_cls) -> None:
    mock_tc = mock_tc_cls.return_value
    mock_tc.submit_order.return_value = _make_order(symbol="BTC/USD")

    client = AlpacaClient("test-key", "test-secret", BASE_URL)
    order = client.submit_market_order("BTC/USD", qty=1500.0, side="buy")

    assert order.symbol == "BTC/USD"
    call_args = mock_tc.submit_order.call_args
    req = call_args[0][0]
    assert req.symbol == "BTC/USD"
    assert req.notional == 1500.0
    assert req.side == OrderSide.BUY
    assert req.time_in_force == TimeInForce.GTC


@patch("app.alpaca_client.CryptoHistoricalDataClient")
@patch("app.alpaca_client.TradingClient")
def test_get_order_maps_fill_status(mock_tc_cls, mock_data_cls) -> None:
    mock_tc = mock_tc_cls.return_value
    mock_tc.get_order_by_id.return_value = _make_order(
        status="filled", filled_qty="10", filled_avg_price="150.50",
    )

    client = AlpacaClient("test-key", "test-secret", BASE_URL)
    order = client.get_order("order-uuid-1234")

    assert order.status == "filled"
    assert float(order.filled_qty) == 10.0
    assert float(order.filled_avg_price) == 150.50


def test_client_raises_value_error_when_credentials_missing() -> None:
    with pytest.raises(ValueError, match="ALPACA_API_KEY"):
        AlpacaClient("", "secret", BASE_URL)
    with pytest.raises(ValueError, match="ALPACA_SECRET_KEY"):
        AlpacaClient("key", "", BASE_URL)
    with pytest.raises(ValueError, match="ALPACA_API_BASE_URL"):
        AlpacaClient("key", "secret", "")


@patch("app.alpaca_client.CryptoHistoricalDataClient")
@patch("app.alpaca_client.TradingClient")
def test_client_raises_on_api_error(mock_tc_cls, mock_data_cls) -> None:
    mock_tc = mock_tc_cls.return_value
    mock_tc.get_all_positions.side_effect = AlpacaAPIError("forbidden")

    client = AlpacaClient("test-key", "test-secret", BASE_URL)
    with pytest.raises(AlpacaAPIError):
        client.get_positions()
