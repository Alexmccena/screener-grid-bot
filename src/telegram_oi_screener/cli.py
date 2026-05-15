from __future__ import annotations

import argparse
import asyncio
import logging
import os
from pathlib import Path

from .backfill import BackfillService
from .coinalyze import (
    CoinalyzeApiError,
    CoinalyzeClient,
    ComparisonRow,
    find_market,
    format_comparison_table,
    merge_oi_value,
    merge_snapshots,
)
from .config import load_config
from .exchanges import BinanceClient, BybitClient, OKXClient
from .exchanges.base import ExchangeApiError
from .grid import calculate_grid_dry_run
from .models import ExchangeName, ExecutionExchange
from .realtime.scheduler import ScreenerRuntime
from .storage import SQLiteStorage
from .telegram.bot import TelegramScreenerBot, TelegramUnavailable
from .telegram.formatting import format_grid_dry_run, format_signal


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    load_dotenv(Path(".env"))
    config = load_config(args.config)
    logging.basicConfig(
        level=getattr(logging, config.app.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if args.command == "validate-config":
        validate_config(config)
        return
    if args.command == "init-db":
        storage = SQLiteStorage(config.app.sqlite_path)
        storage.init_schema()
        print(f"SQLite schema ready: {storage.path}")
        return
    if args.command == "scan-once":
        asyncio.run(scan_once(config, set(args.symbols or ())))
        return
    if args.command == "test-signal":
        asyncio.run(test_signal(config, args.symbol.upper()))
        return
    if args.command == "test-grid":
        asyncio.run(test_grid(config, args.symbol.upper(), ExecutionExchange(args.exchange)))
        return
    if args.command == "run":
        asyncio.run(run(config))
        return
    if args.command == "backfill":
        asyncio.run(backfill(config, set(args.symbols or ()), args.days))
        return
    if args.command == "compare-coinalyze":
        asyncio.run(compare_coinalyze(config, args.symbols, args.exchanges))
        return
    parser.print_help()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="oi-screener")
    parser.add_argument("--config", default=os.getenv("CONFIG_FILE", "config.yaml"))
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-config")
    _add_config_arg(validate)
    init_db = subparsers.add_parser("init-db")
    _add_config_arg(init_db)

    scan = subparsers.add_parser("scan-once")
    _add_config_arg(scan)
    scan.add_argument("symbols", nargs="*")

    test_signal_parser = subparsers.add_parser("test-signal")
    _add_config_arg(test_signal_parser)
    test_signal_parser.add_argument("symbol")

    test_grid_parser = subparsers.add_parser("test-grid")
    _add_config_arg(test_grid_parser)
    test_grid_parser.add_argument("symbol")
    test_grid_parser.add_argument("--exchange", choices=["bybit", "okx"], required=True)

    run_parser = subparsers.add_parser("run")
    _add_config_arg(run_parser)

    backfill_parser = subparsers.add_parser("backfill")
    _add_config_arg(backfill_parser)
    backfill_parser.add_argument("symbols", nargs="*")
    backfill_parser.add_argument("--days", type=int, default=None)

    coinalyze_parser = subparsers.add_parser("compare-coinalyze")
    _add_config_arg(coinalyze_parser)
    coinalyze_parser.add_argument(
        "symbols",
        nargs="*",
        default=["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"],
    )
    coinalyze_parser.add_argument(
        "--exchanges",
        nargs="*",
        choices=[exchange.value for exchange in ExchangeName],
        default=[exchange.value for exchange in ExchangeName],
    )
    return parser


def _add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default=argparse.SUPPRESS)


def validate_config(config) -> None:
    print(f"Config OK: {config.app.name}")
    print(f"SQLite: {config.app.sqlite_path}")
    print(f"Profile: {config.signal.profile}")
    print(f"Aggregation: {config.signal.aggregation_mode.value}")
    print(f"Primary exchange: {config.signal.primary_exchange.value}")
    print("Enabled exchanges: " + ", ".join(exchange.value for exchange in config.signal.enabled_exchanges))
    print(f"Execution: {config.execution.mode.value}/{config.execution.exchange.value}")


async def scan_once(config, symbols: set[str]) -> None:
    runtime = _runtime(config)
    signals = await runtime.scan_once(symbols or None, send=False)
    if not signals:
        print("No symbols evaluated.")
        return
    for signal in signals[:10]:
        print(
            format_signal(
                signal,
                enabled_filters=config.signal.enabled_filters,
                oi_period_minutes=config.signal.oi_period_minutes,
                price_change_period_minutes=config.signal.price_change_period_minutes,
            )
        )
        print("")


async def test_signal(config, symbol: str) -> None:
    runtime = _runtime(config)
    signals = await runtime.scan_once({symbol}, send=False)
    signal = next((item for item in signals if item.symbol == symbol), None)
    if signal is None:
        print(f"No data for {symbol}")
        return
    print(
        format_signal(
            signal,
            enabled_filters=config.signal.enabled_filters,
            oi_period_minutes=config.signal.oi_period_minutes,
            price_change_period_minutes=config.signal.price_change_period_minutes,
        )
    )


async def test_grid(config, symbol: str, exchange: ExecutionExchange) -> None:
    runtime = _runtime(config)
    signals = await runtime.scan_once({symbol}, send=False)
    price = None
    for signal in signals:
        for evaluation in signal.evaluations:
            if evaluation.snapshot and evaluation.snapshot.price is not None:
                price = evaluation.snapshot.price
                break
        if price is not None:
            break
    if price is None:
        raise RuntimeError(f"No market price found for {symbol}")
    dry_run = calculate_grid_dry_run(symbol, exchange, price, config.grid)
    runtime.storage.save_grid_action(
        dry_run,
        request={"symbol": symbol, "exchange": exchange.value, "source": "cli"},
    )
    print(format_grid_dry_run(dry_run))


async def run(config) -> None:
    storage = SQLiteStorage(config.app.sqlite_path)
    runtime = ScreenerRuntime(config=config, storage=storage)
    telegram_bot: TelegramScreenerBot | None = None
    if config.telegram.enabled:
        telegram_bot = TelegramScreenerBot(config=config, runtime=runtime, storage=storage)
        runtime.notifier = telegram_bot.notify
        try:
            await telegram_bot.start()
        except TelegramUnavailable as exc:
            logging.getLogger(__name__).warning("Telegram disabled: %s", exc)

    stop_event = asyncio.Event()
    try:
        await runtime.run(stop_event)
    finally:
        if telegram_bot is not None:
            await telegram_bot.stop()


async def backfill(config, symbols: set[str], days: int | None) -> None:
    storage = SQLiteStorage(config.app.sqlite_path)
    service = BackfillService(config=config, storage=storage)
    results = await service.backfill(symbols or None, days=days)
    if not results:
        print("No backfill work performed.")
        return
    for result in results:
        status = "OK" if not result.errors else "WARN"
        print(
            f"{status} {result.exchange.value} {result.symbol}: "
            f"OI={result.oi_rows}, candles_1m={result.candle_1m_rows}, "
            f"candles_5m={result.candle_5m_rows}"
        )
        for error in result.errors:
            print(f"  {error}")


async def compare_coinalyze(config, symbols: list[str], exchanges: list[str]) -> None:
    api_key = os.getenv("COINALYZE_API_KEY")
    if not api_key:
        print("COINALYZE_API_KEY is empty. Add it to .env and rerun this command.")
        return
    selected_symbols = [symbol.upper() for symbol in (symbols or ["BTCUSDT", "ETHUSDT", "SOLUSDT"])]
    selected_exchanges = [ExchangeName(exchange) for exchange in exchanges]
    coinalyze = CoinalyzeClient(api_key)
    clients = {
        ExchangeName.BINANCE: BinanceClient(),
        ExchangeName.BYBIT: BybitClient(),
        ExchangeName.OKX: OKXClient(),
    }

    try:
        markets = await coinalyze.get_future_markets()
    except CoinalyzeApiError as exc:
        print(f"Coinalyze markets failed: {exc}")
        return

    market_by_pair = {
        (exchange, symbol): find_market(markets, exchange, symbol)
        for exchange in selected_exchanges
        for symbol in selected_symbols
    }
    coinalyze_symbols = sorted(
        {
            market.symbol
            for market in market_by_pair.values()
            if market is not None
        }
    )
    coinalyze_data = {}
    coinalyze_error: str | None = None
    if coinalyze_symbols:
        try:
            oi_data = await coinalyze.get_open_interest(coinalyze_symbols)
            oi_usd_data = await coinalyze.get_open_interest_usd(coinalyze_symbols)
            funding_data = await coinalyze.get_funding_rates(coinalyze_symbols)
            oi_data = {
                symbol: merge_oi_value(oi_data.get(symbol), oi_usd_data.get(symbol))
                for symbol in coinalyze_symbols
            }
            coinalyze_data = {
                symbol: merge_snapshots(oi_data.get(symbol), funding_data.get(symbol))
                for symbol in coinalyze_symbols
            }
        except CoinalyzeApiError as exc:
            coinalyze_error = str(exc)

    rows: list[ComparisonRow] = []
    ticker_cache = {}
    for exchange in selected_exchanges:
        client = clients[exchange]
        try:
            ticker_cache[exchange] = await client.get_ticker_snapshots()
        except ExchangeApiError as exc:
            ticker_cache[exchange] = exc
        except Exception as exc:
            ticker_cache[exchange] = exc

        if exchange is ExchangeName.OKX and not isinstance(ticker_cache[exchange], BaseException):
            try:
                await client.get_instruments()
            except Exception:
                pass

    for exchange in selected_exchanges:
        client = clients[exchange]
        exchange_result = ticker_cache[exchange]
        for symbol in selected_symbols:
            market = market_by_pair[(exchange, symbol)]
            cg_snapshot = coinalyze_data.get(market.symbol) if market is not None else None
            row_kwargs = {
                "exchange": exchange,
                "symbol": symbol,
                "coinalyze_symbol": market.symbol if market else None,
                "coinalyze_oi": cg_snapshot.open_interest if cg_snapshot else None,
                "coinalyze_oi_value": cg_snapshot.open_interest_value_usdt if cg_snapshot else None,
                "coinalyze_funding_pct": cg_snapshot.funding_rate_pct if cg_snapshot else None,
                "coinalyze_error": coinalyze_error or (None if market is not None else "market not found"),
            }
            if isinstance(exchange_result, BaseException):
                rows.append(ComparisonRow(**row_kwargs, exchange_error=str(exchange_result)))
                continue
            snapshot = exchange_result.get(symbol)
            if snapshot is None:
                rows.append(ComparisonRow(**row_kwargs, exchange_error="exchange symbol not found"))
                continue
            try:
                enriched = await client.enrich_snapshot(snapshot)
            except Exception as exc:
                rows.append(ComparisonRow(**row_kwargs, exchange_error=str(exc)))
                continue
            rows.append(
                ComparisonRow(
                    **row_kwargs,
                    exchange_oi=enriched.open_interest,
                    exchange_oi_value=enriched.open_interest_value_usdt,
                    exchange_funding_pct=enriched.funding_rate_pct,
                )
            )

    print(format_comparison_table(rows))


def _runtime(config) -> ScreenerRuntime:
    storage = SQLiteStorage(config.app.sqlite_path)
    storage.init_schema()
    return ScreenerRuntime(config=config, storage=storage)


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


if __name__ == "__main__":
    main()
