from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from .coinalyze import CoinalyzeApiError, CoinalyzeClient, CoinalyzeFallbackProvider
from .config import ScreenerConfig
from .exchanges import BinanceClient, BybitClient, OKXClient
from .exchanges.base import BaseExchangeClient, ExchangeApiError
from .models import Candle, ExchangeName, MarketSnapshot, SignalSettings
from .resilience import ExchangeCircuitBreaker
from .rolling_buffer import RollingBuffer
from .storage import SQLiteStorage

LOGGER = logging.getLogger(__name__)

RefreshCallback = Callable[[MarketSnapshot, tuple[Candle, ...]], Awaitable[None]]


@dataclass(frozen=True)
class MarketDataRefresh:
    snapshots: dict[str, dict[ExchangeName, MarketSnapshot]] = field(default_factory=dict)
    candles: dict[str, dict[ExchangeName, tuple[Candle, ...]]] = field(default_factory=dict)
    candles_5m: dict[str, dict[ExchangeName, tuple[Candle, ...]]] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


class MarketDataService:
    def __init__(
        self,
        config: ScreenerConfig,
        buffer: RollingBuffer,
        storage: SQLiteStorage | None = None,
        clients: dict[ExchangeName, BaseExchangeClient] | None = None,
        circuit_breaker: ExchangeCircuitBreaker | None = None,
        coinalyze_fallback: CoinalyzeFallbackProvider | None = None,
    ) -> None:
        self.config = config
        self.buffer = buffer
        self.storage = storage
        self.clients = clients or build_clients(config)
        self.circuit_breaker = circuit_breaker
        self.coinalyze_fallback = coinalyze_fallback or build_coinalyze_fallback(config)
        self.grid_eligible_symbols: dict[ExchangeName, frozenset[str]] = {}
        self._grid_symbols_loaded_at: datetime | None = None

    async def refresh(
        self,
        symbols: set[str] | None = None,
        selection_settings: Iterable[SignalSettings] | None = None,
        on_snapshot: RefreshCallback | None = None,
    ) -> MarketDataRefresh:
        settings_list = tuple(selection_settings or (self.config.signal,))
        active_exchanges = _selection_exchanges(settings_list)
        active_exchanges = frozenset(
            exchange
            for exchange in active_exchanges
            if self._exchange_available(exchange, "full REST refresh", log_skip=False)
        )
        if not active_exchanges:
            LOGGER.warning("full REST refresh skipped: all selected exchanges are circuit-paused")
            return MarketDataRefresh()
        await self.refresh_grid_eligible_symbols()
        ticker_results = await self._fetch_tickers(active_exchanges)
        selected_symbols = self._select_symbols(ticker_results, symbols, settings_list)
        snapshots: dict[str, dict[ExchangeName, MarketSnapshot]] = defaultdict(dict)
        candles: dict[str, dict[ExchangeName, tuple[Candle, ...]]] = defaultdict(dict)
        errors: dict[str, str] = {}
        error_counts: dict[str, int] = defaultdict(int)

        needs_candles = _needs_candles(settings_list)

        def store_enriched(
            enriched: MarketSnapshot,
            candle_list: tuple[Candle, ...],
            oi_history: tuple[MarketSnapshot, ...] = (),
        ) -> None:
            self._warm_buffer(enriched, candle_list, oi_history)
            snapshots[enriched.symbol][enriched.exchange] = enriched
            if candle_list:
                candles[enriched.symbol][enriched.exchange] = candle_list
            self.buffer.add_snapshot(enriched)
            if self.storage:
                if candle_list:
                    self.storage.save_candles(candle_list, interval="1m")
                self.storage.save_open_interest_history(oi_history)
                self.storage.save_snapshot(enriched)

        async def enrich_one(exchange: ExchangeName, snapshot: MarketSnapshot) -> bool:
            if not self._exchange_available(exchange, "full REST enrich", log_skip=False):
                return False
            try:
                client = self.clients[exchange]
                self._warm_buffer_from_storage(exchange, snapshot.symbol)
                enriched = await client.enrich_snapshot(snapshot)
                candle_list = (
                    await client.get_klines(snapshot.symbol)
                    if needs_candles
                    else []
                )
                oi_history = []
                if _needs_oi_history(self.buffer, exchange, snapshot.symbol, snapshot.timestamp, settings_list):
                    oi_history = await client.get_open_interest_history(
                        snapshot.symbol,
                        limit=max(_max_oi_period(settings_list) // 5 + 8, 12),
                    )
                candle_tuple = tuple(candle_list)
                store_enriched(enriched, candle_tuple, tuple(oi_history))
                if on_snapshot is not None:
                    await on_snapshot(enriched, candle_tuple)
                self._record_success(exchange)
                return True
            except ExchangeApiError as exc:
                self._record_failure(exchange, exc)
                fallback = await self._fallback_enrich_snapshot(snapshot, exc)
                if fallback is None:
                    errors[f"{exchange.value}:{snapshot.symbol}"] = str(exc)
                    error_counts[_error_key(exc)] += 1
                    return False
                store_enriched(fallback, ())
                if on_snapshot is not None:
                    await on_snapshot(fallback, ())
                return True
            except Exception as exc:
                self._record_failure(exchange, exc)
                errors[f"{exchange.value}:{snapshot.symbol}"] = repr(exc)
                LOGGER.exception("unexpected refresh failure for %s %s", exchange, snapshot.symbol)
                return False

        async def enrich_exchange(exchange: ExchangeName, exchange_snapshots: dict[str, MarketSnapshot]) -> None:
            if exchange not in active_exchanges:
                return
            pending = [
                exchange_snapshots[symbol]
                for symbol in sorted(selected_symbols)
                if symbol in exchange_snapshots
            ]
            skipped = 0
            while pending:
                if not self._exchange_available(exchange, "full REST enrich", log_skip=False):
                    skipped += len(pending)
                    break
                limit = self._concurrency(frozenset({exchange}))
                batch, pending = pending[:limit], pending[limit:]

                async def run_item(index: int, snapshot: MarketSnapshot) -> bool:
                    delay = self._request_delay_seconds(exchange) * index
                    if delay > 0:
                        await asyncio.sleep(delay)
                    return await enrich_one(exchange, snapshot)

                await asyncio.gather(
                    *(run_item(index, snapshot) for index, snapshot in enumerate(batch))
                )
            if skipped:
                LOGGER.info(
                    "full REST enrich stopped for %s: circuit breaker cooldown %ss, skipped %s symbols",
                    exchange.value,
                    self.circuit_breaker.blocked_for(exchange) if self.circuit_breaker else 0,
                    skipped,
                )

        tasks = [
            enrich_exchange(exchange, exchange_snapshots)
            for exchange, exchange_snapshots in ticker_results.items()
        ]
        if tasks:
            await asyncio.gather(*tasks)
        _log_error_summary("exchange refresh failed", error_counts)
        return MarketDataRefresh(snapshots=dict(snapshots), candles=dict(candles), errors=errors)

    async def refresh_grid_eligible_symbols(self, force: bool = False) -> None:
        now = datetime.now(UTC)
        if (
            not force
            and self._grid_symbols_loaded_at is not None
            and now - self._grid_symbols_loaded_at < timedelta(hours=6)
        ):
            return

        async def load(exchange: ExchangeName, client: BaseExchangeClient) -> tuple[ExchangeName, frozenset[str]]:
            if not self._exchange_available(exchange, "grid-eligible instruments refresh"):
                return exchange, self.grid_eligible_symbols.get(exchange, frozenset())
            try:
                instruments = await client.get_instruments()
                self._record_success(exchange)
                return exchange, frozenset(instruments)
            except ExchangeApiError as exc:
                self._record_failure(exchange, exc)
                LOGGER.warning("grid-eligible instruments refresh failed: %s", exc)
                return exchange, self.grid_eligible_symbols.get(exchange, frozenset())

        results = await asyncio.gather(
            *(load(exchange, client) for exchange, client in self.clients.items())
        )
        self.grid_eligible_symbols = {exchange: symbols for exchange, symbols in results}
        self._grid_symbols_loaded_at = now

    def _exchange_available(self, exchange: ExchangeName, operation: str, *, log_skip: bool = True) -> bool:
        if self.circuit_breaker is None or self.circuit_breaker.allow_request(exchange):
            return True
        if not log_skip:
            return False
        LOGGER.info(
            "%s skipped for %s: circuit breaker cooldown %ss",
            operation,
            exchange.value,
            self.circuit_breaker.blocked_for(exchange),
        )
        return False

    def _record_success(self, exchange: ExchangeName) -> None:
        if self.circuit_breaker is not None:
            self.circuit_breaker.record_success(exchange)

    def _record_failure(self, exchange: ExchangeName, error: BaseException | str) -> None:
        if self.circuit_breaker is not None:
            self.circuit_breaker.record_failure(exchange, error)

    def _concurrency(self, exchanges: frozenset[ExchangeName]) -> int:
        if self.circuit_breaker is None:
            return 4
        if any(self.circuit_breaker.failure_count(exchange) for exchange in exchanges):
            return max(1, self.config.realtime.degraded_max_concurrency)
        return 4

    def _request_delay_seconds(self, exchange: ExchangeName) -> float:
        delay_ms = self.config.realtime.normal_request_delay_ms
        if self.circuit_breaker is not None and self.circuit_breaker.failure_count(exchange):
            delay_ms = self.config.realtime.degraded_request_delay_ms
        return max(0, delay_ms) / 1000

    async def _fallback_enrich_snapshot(
        self,
        snapshot: MarketSnapshot,
        original_error: BaseException,
    ) -> MarketSnapshot | None:
        if self.coinalyze_fallback is None:
            return None
        try:
            enriched = await self.coinalyze_fallback.enrich_snapshot(snapshot)
        except CoinalyzeApiError as exc:
            LOGGER.info(
                "coinalyze fallback failed for %s %s after %s: %s",
                snapshot.exchange.value,
                snapshot.symbol,
                original_error,
                exc,
            )
            return None
        except Exception:
            LOGGER.exception(
                "unexpected coinalyze fallback failure for %s %s",
                snapshot.exchange.value,
                snapshot.symbol,
            )
            return None
        if enriched is not None:
            LOGGER.info(
                "coinalyze fallback used for %s %s after %s",
                snapshot.exchange.value,
                snapshot.symbol,
                original_error,
            )
        return enriched

    def _warm_buffer_from_storage(self, exchange: ExchangeName, symbol: str) -> None:
        if self.storage is None:
            return
        since = datetime.now(UTC) - timedelta(hours=self.config.history.hot_buffer_hours)
        candles = self.storage.load_candles(exchange, symbol, interval="1m", since=since)
        oi_history = self.storage.load_open_interest_history(exchange, symbol, since=since)
        if not candles and not oi_history:
            return
        exchange_symbol = symbol
        if oi_history:
            exchange_symbol = oi_history[-1].exchange_symbol
        self._warm_buffer(
            MarketSnapshot(
                exchange=exchange,
                symbol=symbol,
                exchange_symbol=exchange_symbol,
                timestamp=datetime.now(UTC),
            ),
            tuple(candles),
            tuple(oi_history),
        )

    def _warm_buffer(
        self,
        current: MarketSnapshot,
        candles: tuple[Candle, ...],
        oi_history: tuple[MarketSnapshot, ...],
    ) -> None:
        for candle in candles:
            self.buffer.add_snapshot(
                MarketSnapshot(
                    exchange=candle.exchange,
                    symbol=candle.symbol,
                    exchange_symbol=current.exchange_symbol,
                    timestamp=candle.open_time,
                    price=candle.close,
                    recent_volume_usdt=candle.volume_usdt,
                    source="rest:kline_warmup",
                )
            )
        for point in oi_history:
            price = point.price or _nearest_candle_close(candles, point.timestamp)
            oi_value = point.open_interest_value_usdt
            estimated = point.open_interest_value_estimated
            if oi_value is None and point.open_interest is not None and price is not None:
                oi_value = point.open_interest * price
                estimated = True
            self.buffer.add_snapshot(
                point.with_updates(
                    price=price,
                    open_interest_value_usdt=oi_value,
                    open_interest_value_estimated=estimated,
                )
            )

    async def _fetch_tickers(
        self,
        exchanges: frozenset[ExchangeName] | None = None,
    ) -> dict[ExchangeName, dict[str, MarketSnapshot]]:
        results: dict[ExchangeName, dict[str, MarketSnapshot]] = {}
        clients = {
            exchange: client
            for exchange, client in self.clients.items()
            if exchanges is None or exchange in exchanges
        }

        async def fetch(exchange: ExchangeName, client: BaseExchangeClient) -> None:
            if not self._exchange_available(exchange, "ticker refresh"):
                results[exchange] = {}
                return
            try:
                results[exchange] = await client.get_ticker_snapshots()
                self._record_success(exchange)
            except ExchangeApiError as exc:
                self._record_failure(exchange, exc)
                LOGGER.warning("ticker refresh failed: %s", exc)
                results[exchange] = {}

        await asyncio.gather(*(fetch(exchange, client) for exchange, client in clients.items()))
        return results

    def _select_symbols(
        self,
        ticker_results: dict[ExchangeName, dict[str, MarketSnapshot]],
        requested_symbols: set[str] | None,
        selection_settings: Iterable[SignalSettings] | None = None,
    ) -> set[str]:
        settings_list = tuple(selection_settings or (self.config.signal,))
        settings = self.config.signal
        if requested_symbols:
            return {symbol.upper() for symbol in requested_symbols}
        if settings.whitelist_symbols:
            return set(settings.whitelist_symbols)

        needs_oi_scan_universe = any(
            "oi_change_pct" in set(item.enabled_filters)
            and "volume_24h_usdt" not in set(item.enabled_filters)
            for item in settings_list
        )
        if needs_oi_scan_universe:
            min_volume = min(item.oi_scan_min_24h_volume_usdt for item in settings_list)
            max_symbols = max(item.oi_scan_max_symbols for item in settings_list)
        else:
            min_volume = min(item.thresholds.min_24h_volume_usdt for item in settings_list)
            max_symbols = max(item.max_symbols_per_refresh for item in settings_list)

        volumes: dict[str, int] = {}
        for exchange_snapshots in ticker_results.values():
            for symbol, snapshot in exchange_snapshots.items():
                if symbol in settings.blacklist_symbols:
                    continue
                if snapshot.volume_24h_usdt is None:
                    continue
                if snapshot.volume_24h_usdt < min_volume:
                    continue
                current = volumes.get(symbol, 0)
                volumes[symbol] = max(current, int(snapshot.volume_24h_usdt))

        selected = sorted(volumes, key=volumes.get, reverse=True)[:max_symbols]
        return set(selected)


def build_clients(config: ScreenerConfig) -> dict[ExchangeName, BaseExchangeClient]:
    clients: dict[ExchangeName, BaseExchangeClient] = {}
    if ExchangeName.BINANCE in config.signal.enabled_exchanges:
        clients[ExchangeName.BINANCE] = BinanceClient()
    if ExchangeName.BYBIT in config.signal.enabled_exchanges:
        clients[ExchangeName.BYBIT] = BybitClient()
    if ExchangeName.OKX in config.signal.enabled_exchanges:
        clients[ExchangeName.OKX] = OKXClient()
    return clients


def build_coinalyze_fallback(config: ScreenerConfig) -> CoinalyzeFallbackProvider | None:
    if not config.coinalyze.enabled or not config.coinalyze.api_key:
        return None
    client = CoinalyzeClient(
        config.coinalyze.api_key,
        max_symbol_calls_per_minute=config.coinalyze.max_symbol_calls_per_minute,
    )
    return CoinalyzeFallbackProvider(client)


def _exchange_values(exchanges: frozenset[ExchangeName]) -> str:
    return ", ".join(exchange.value for exchange in sorted(exchanges, key=lambda item: item.value))


def _selection_exchanges(settings_list: Iterable[SignalSettings]) -> frozenset[ExchangeName]:
    exchanges: set[ExchangeName] = set()
    for settings in settings_list:
        exchanges.update(settings.enabled_exchanges)
    return frozenset(exchanges)


def _needs_candles(settings_list: Iterable[SignalSettings]) -> bool:
    candle_filters = {"volume_spike_ratio", "volatility_pct", "min_score"}
    return any(candle_filters.intersection(settings.enabled_filters) for settings in settings_list)


def _max_oi_period(settings_list: Iterable[SignalSettings]) -> int:
    return max((settings.oi_period_minutes for settings in settings_list), default=20)


def _needs_oi_history(
    buffer: RollingBuffer,
    exchange: ExchangeName,
    symbol: str,
    timestamp: datetime,
    settings_list: Iterable[SignalSettings],
) -> bool:
    oi_settings = [
        settings
        for settings in settings_list
        if {"oi_change_pct", "oi_value_change_usdt", "min_score"}.intersection(settings.enabled_filters)
    ]
    if not oi_settings:
        return False
    return any(
        buffer.get_point_ago(exchange, symbol, settings.oi_period_minutes, timestamp) is None
        for settings in oi_settings
    )


def _nearest_candle_close(candles: tuple[Candle, ...], timestamp) -> object:
    if not candles:
        return None
    nearest = min(
        candles,
        key=lambda candle: abs((candle.open_time - timestamp).total_seconds()),
    )
    if abs((nearest.open_time - timestamp).total_seconds()) > timedelta(minutes=10).total_seconds():
        return None
    return nearest.close


def _error_key(exc: ExchangeApiError) -> str:
    return str(exc)


def _log_error_summary(prefix: str, error_counts: dict[str, int]) -> None:
    for message, count in sorted(error_counts.items(), key=lambda item: item[0]):
        if count == 1:
            LOGGER.warning("%s: %s", prefix, message)
        else:
            LOGGER.warning("%s: %sx %s", prefix, count, message)
