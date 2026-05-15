from decimal import Decimal

from telegram_oi_screener.models import ExchangeName, Instrument
from telegram_oi_screener.normalizer import (
    estimate_oi_value_usdt,
    normalize_okx_symbol,
    normalize_symbol,
    okx_exchange_symbol,
)


def test_symbol_normalization() -> None:
    assert normalize_symbol(ExchangeName.BINANCE, "btcusdt") == "BTCUSDT"
    assert normalize_symbol(ExchangeName.BYBIT, "BTCUSDT") == "BTCUSDT"
    assert normalize_symbol(ExchangeName.OKX, "BTC-USDT-SWAP") == "BTCUSDT"
    assert normalize_okx_symbol("ETH-USDT-SWAP") == "ETHUSDT"
    assert okx_exchange_symbol("SOLUSDT") == "SOL-USDT-SWAP"


def test_estimate_oi_value_uses_multiplier() -> None:
    instrument = Instrument(
        exchange=ExchangeName.OKX,
        symbol="BTCUSDT",
        exchange_symbol="BTC-USDT-SWAP",
        base_asset="BTC",
        quote_asset="USDT",
        contract_multiplier=Decimal("0.01"),
    )
    assert estimate_oi_value_usdt(Decimal("100"), Decimal("50000"), instrument) == Decimal("50000")
