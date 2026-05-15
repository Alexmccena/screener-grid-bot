from decimal import Decimal

from telegram_oi_screener.coinalyze import (
    CoinalyzeMarket,
    ComparisonRow,
    find_market,
    format_comparison_table,
    merge_oi_value,
    merge_snapshots,
    parse_market,
    parse_snapshot,
)
from telegram_oi_screener.models import ExchangeName


def test_parse_market_accepts_common_field_variants() -> None:
    market = parse_market(
        {
            "symbol": "BTCUSDT_PERP.A",
            "exchange": "Binance",
            "base_asset": "BTC",
            "quote_asset": "USDT",
        }
    )

    assert market == CoinalyzeMarket(
        symbol="BTCUSDT_PERP.A",
        exchange="Binance",
        base_asset="BTC",
        quote_asset="USDT",
    )


def test_parse_market_maps_exchange_code_from_suffix() -> None:
    market = parse_market(
        {
            "symbol": "BTCUSDT_PERP.A",
            "base_asset": "BTC",
            "quote_asset": "USDT",
        },
        exchange_map={"A": "Binance"},
    )

    assert market is not None
    assert market.exchange == "Binance"


def test_find_market_matches_exchange_and_pair() -> None:
    markets = [
        CoinalyzeMarket("BTCUSD_PERP.X", "Other", "BTC", "USD"),
        CoinalyzeMarket("BTCUSDT_PERP.A", "Binance Futures", "BTC", "USDT"),
    ]

    market = find_market(markets, ExchangeName.BINANCE, "BTCUSDT")

    assert market is not None
    assert market.symbol == "BTCUSDT_PERP.A"


def test_parse_and_merge_snapshots() -> None:
    oi = parse_snapshot(
        {
            "symbol": "BTCUSDT_PERP.A",
            "open_interest": "123.45",
            "open_interest_usd": "1000000",
            "timestamp": 1_762_000_000,
        },
        value_kind="oi",
    )
    funding = parse_snapshot(
        {
            "symbol": "BTCUSDT_PERP.A",
            "funding_rate": "0.0001",
            "timestamp": 1_762_000_060,
        },
        value_kind="funding",
    )

    merged = merge_snapshots(oi, funding)

    assert merged is not None
    assert merged.open_interest == Decimal("123.45")
    assert merged.open_interest_value_usdt == Decimal("1000000")
    assert merged.funding_rate_pct == Decimal("0.0001")


def test_merge_oi_value_uses_usd_snapshot() -> None:
    native = parse_snapshot(
        {"symbol": "BTCUSDT_PERP.A", "open_interest": "123"},
        value_kind="oi",
    )
    usd = parse_snapshot(
        {"symbol": "BTCUSDT_PERP.A", "open_interest": "1000000"},
        value_kind="oi_usd",
    )

    merged = merge_oi_value(native, usd)

    assert merged is not None
    assert merged.open_interest == Decimal("123")
    assert merged.open_interest_value_usdt == Decimal("1000000")


def test_format_comparison_table_shows_diffs() -> None:
    table = format_comparison_table(
        [
            ComparisonRow(
                exchange=ExchangeName.BINANCE,
                symbol="BTCUSDT",
                coinalyze_symbol="BTCUSDT_PERP.A",
                exchange_oi=Decimal("100"),
                coinalyze_oi=Decimal("105"),
                exchange_oi_value=Decimal("1000000"),
                coinalyze_oi_value=Decimal("1100000"),
                exchange_funding_pct=Decimal("0.01"),
                coinalyze_funding_pct=Decimal("0.011"),
            )
        ]
    )

    assert "BTCUSDT_PERP.A" in table
    assert "5.00%" in table
    assert "10.00%" in table
