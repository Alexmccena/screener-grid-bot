from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from telegram_oi_screener.exchanges.base import ExchangeApiError
from telegram_oi_screener.config import load_config
from telegram_oi_screener.market_data import MarketDataService
from telegram_oi_screener.models import ExchangeName, MarketSnapshot
from telegram_oi_screener.rolling_buffer import RollingBuffer


def test_oi_only_selection_uses_broader_low_volume_universe() -> None:
    config = load_config("config.yaml")
    settings = replace(
        config.signal,
        enabled_filters=("oi_change_pct",),
        oi_scan_max_symbols=2,
        oi_scan_min_24h_volume_usdt=Decimal("1000000"),
    )
    service = MarketDataService(config=config, buffer=RollingBuffer(), clients={})
    snapshots = {
        ExchangeName.BINANCE: {
            "LOWUSDT": _snapshot("LOWUSDT", "1500000"),
            "MIDUSDT": _snapshot("MIDUSDT", "5000000"),
            "HIGHUSDT": _snapshot("HIGHUSDT", "30000000"),
            "TINYUSDT": _snapshot("TINYUSDT", "900000"),
        }
    }

    selected = service._select_symbols(snapshots, None, (settings,))

    assert selected == {"HIGHUSDT", "MIDUSDT"}


def test_full_filter_selection_keeps_24h_volume_prefilter() -> None:
    config = load_config("config.yaml")
    service = MarketDataService(config=config, buffer=RollingBuffer(), clients={})
    snapshots = {
        ExchangeName.BINANCE: {
            "MIDUSDT": _snapshot("MIDUSDT", "5000000"),
            "HIGHUSDT": _snapshot("HIGHUSDT", "30000000"),
        }
    }

    selected = service._select_symbols(snapshots, None, (config.signal,))

    assert selected == {"HIGHUSDT"}


def test_refresh_uses_coinalyze_fallback_after_exchange_error() -> None:
    import asyncio

    config = load_config("config.yaml")
    fallback = DummyFallback()
    service = MarketDataService(
        config=config,
        buffer=RollingBuffer(),
        clients={ExchangeName.BINANCE: FailingClient()},
        coinalyze_fallback=fallback,  # type: ignore[arg-type]
    )

    settings = replace(
        config.signal,
        enabled_exchanges=(ExchangeName.BINANCE,),
        primary_exchange=ExchangeName.BINANCE,
    )
    refresh = asyncio.run(service.refresh({"BTCUSDT"}, selection_settings=(settings,)))

    snapshot = refresh.snapshots["BTCUSDT"][ExchangeName.BINANCE]
    assert snapshot.open_interest == Decimal("100")
    assert snapshot.open_interest_value_usdt == Decimal("1000000")
    assert snapshot.funding_rate_pct == Decimal("0.01")
    assert "coinalyze" in snapshot.source


class FailingClient:
    async def get_instruments(self):
        return {}

    async def get_ticker_snapshots(self):
        return {
            "BTCUSDT": MarketSnapshot(
                exchange=ExchangeName.BINANCE,
                symbol="BTCUSDT",
                exchange_symbol="BTCUSDT",
                timestamp=datetime(2026, 5, 11, tzinfo=UTC),
                price=Decimal("100000"),
                volume_24h_usdt=Decimal("100000000"),
            )
        }

    async def enrich_snapshot(self, snapshot: MarketSnapshot):
        raise ExchangeApiError(ExchangeName.BINANCE, 403, "403 Request blocked")

    async def get_klines(self, symbol: str, limit: int = 180):
        return []


class DummyFallback:
    async def enrich_snapshot(self, snapshot: MarketSnapshot):
        return snapshot.with_updates(
            open_interest=Decimal("100"),
            open_interest_value_usdt=Decimal("1000000"),
            funding_rate_pct=Decimal("0.01"),
            source=f"{snapshot.source}+coinalyze:fallback",
        )


def _snapshot(symbol: str, volume: str) -> MarketSnapshot:
    return MarketSnapshot(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        exchange_symbol=symbol,
        timestamp=datetime(2026, 5, 6, tzinfo=UTC),
        volume_24h_usdt=Decimal(volume),
    )
