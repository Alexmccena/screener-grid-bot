from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from ..config import ScreenerConfig
from ..exchanges.base import ExchangeApiError
from ..market_data import MarketDataRefresh, MarketDataService
from ..models import (
    ZERO,
    AggregatedSignal,
    Candle,
    ExchangeName,
    ExchangeSignalEvaluation,
    MarketSnapshot,
    SignalSettings,
)
from ..resilience import ExchangeCircuitBreaker
from ..rolling_buffer import RollingBuffer
from ..signal_engine import SignalEngine
from ..storage import SQLiteStorage
from ..telegram.formatting import format_signal
from ..user_settings import effective_settings
from .websocket import BinanceTickerWebSocket, BybitTickerWebSocket, OKXPublicWebSocket

LOGGER = logging.getLogger(__name__)

Notifier = Callable[[int | None, AggregatedSignal, str], Awaitable[bool]]


@dataclass
class RuntimeState:
    paused: bool = False
    last_refresh: datetime | None = None
    last_evaluation: datetime | None = None
    refresh_in_progress: bool = False
    refresh_started_at: datetime | None = None
    latest_refresh: MarketDataRefresh = field(default_factory=MarketDataRefresh)
    latest_signals: list[AggregatedSignal] = field(default_factory=list)
    data_revision: int = 0
    last_sent_evaluation_revision: int = -1
    hot_candidates: dict[ExchangeName, dict[str, datetime]] = field(default_factory=dict)
    oi_refresh_offsets: dict[ExchangeName, int] = field(default_factory=dict)
    avg24h_candle_offsets: dict[ExchangeName, int] = field(default_factory=dict)


class ScreenerRuntime:
    def __init__(
        self,
        config: ScreenerConfig,
        storage: SQLiteStorage,
        buffer: RollingBuffer | None = None,
        notifier: Notifier | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.buffer = buffer or RollingBuffer(
            max_age_minutes=max(
                config.signal.volume_baseline_period_minutes + config.signal.volume_spike_period_minutes,
                config.signal.oi_period_minutes,
                config.signal.volatility_period_minutes,
            )
            + 30
        )
        self.circuit_breaker = ExchangeCircuitBreaker(
            failure_threshold=config.realtime.circuit_breaker_failure_threshold,
            cooldown_seconds=config.realtime.circuit_breaker_cooldown_seconds,
        )
        self.market_data = MarketDataService(
            config=config,
            buffer=self.buffer,
            storage=storage,
            circuit_breaker=self.circuit_breaker,
        )
        self.engine = SignalEngine(self.buffer)
        self.notifier = notifier
        self.state = RuntimeState()
        self._ws_stop_event = asyncio.Event()
        self._ws_tasks: list[asyncio.Task[None]] = []
        self._full_refresh_task: asyncio.Task[None] | None = None
        self._background_tasks: dict[str, asyncio.Task[None]] = {}

    async def scan_once(
        self,
        symbols: set[str] | None = None,
        send: bool = False,
        settings: SignalSettings | None = None,
        telegram_user_id: int | None = None,
        incremental: bool = False,
    ) -> list[AggregatedSignal]:
        selection_settings = self._selection_settings(settings)
        self.state.latest_refresh = await self.market_data.refresh(
            symbols,
            selection_settings=selection_settings,
            on_snapshot=self._on_refresh_snapshot if incremental else None,
        )
        self._mark_data_changed()
        self.state.last_refresh = datetime.now(UTC)
        if settings is not None:
            settings = self._with_grid_symbols(settings)
        signals = await self.evaluate_current(
            send=send,
            settings=settings,
            telegram_user_id=telegram_user_id,
        )
        return signals

    async def evaluate_current(
        self,
        send: bool = True,
        settings: SignalSettings | None = None,
        telegram_user_id: int | None = None,
    ) -> list[AggregatedSignal]:
        if self.state.paused:
            return []
        refresh = self.state.latest_refresh
        signals: list[AggregatedSignal] = []
        if settings is not None:
            settings_by_user: list[tuple[int | None, SignalSettings]] = [
                (telegram_user_id, self._with_grid_symbols(settings))
            ]
        elif send:
            settings_by_user = self._runtime_user_settings()
        else:
            settings_by_user = [(None, self.config.signal)]
        for symbol, snapshots in tuple(refresh.snapshots.items()):
            snapshot_items = dict(snapshots)
            for user_id, user_settings in settings_by_user:
                signal = self.engine.evaluate_symbol(
                    symbol=symbol,
                    snapshots=snapshot_items,
                    settings=user_settings,
                    candles=refresh.candles.get(symbol, {}),
                    avg24h_candles=refresh.candles_5m.get(symbol, {}),
                )
                if signal.passed:
                    await self._handle_signal(
                        signal,
                        send=send,
                        telegram_user_id=user_id,
                        settings=user_settings,
                    )
                self._register_hot_candidates(signal, user_settings)
                if user_id == telegram_user_id or settings is None:
                    signals.append(signal)
        signals.sort(key=lambda item: item.score, reverse=True)
        self.state.latest_signals = signals[:20]
        self.state.last_evaluation = datetime.now(UTC)
        if send:
            self.state.last_sent_evaluation_revision = self.state.data_revision
        return signals

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        self.storage.init_schema()
        stop = stop_event or asyncio.Event()
        next_refresh = datetime.min.replace(tzinfo=UTC)
        next_eval = datetime.min.replace(tzinfo=UTC)
        next_oi_refresh = {
            exchange: datetime.min.replace(tzinfo=UTC) for exchange in self.config.signal.enabled_exchanges
        }
        next_hot_oi_refresh = {
            exchange: datetime.min.replace(tzinfo=UTC) for exchange in self.config.signal.enabled_exchanges
        }
        next_hot_candle_refresh = {
            exchange: datetime.min.replace(tzinfo=UTC) for exchange in self.config.signal.enabled_exchanges
        }
        next_avg24h_candle_refresh = {
            exchange: datetime.min.replace(tzinfo=UTC) for exchange in self.config.signal.enabled_exchanges
        }
        next_cleanup = datetime.min.replace(tzinfo=UTC)
        try:
            while not stop.is_set():
                now = datetime.now(UTC)
                self._consume_full_refresh_task()
                self._consume_background_tasks()
                if now >= next_refresh and not self.state.refresh_in_progress:
                    self._start_full_refresh(now)
                    next_refresh = now + timedelta(
                        seconds=self.config.realtime.full_rest_refresh_interval_seconds
                    )
                    for exchange in self.config.signal.enabled_exchanges:
                        next_oi_refresh[exchange] = now + timedelta(
                            seconds=self._oi_refresh_interval(exchange)
                        )
                for exchange in self.config.signal.enabled_exchanges:
                    if now >= next_oi_refresh[exchange]:
                        self._start_background_task(
                            f"oi:{exchange.value}",
                            self.refresh_open_interest_for_current(exchange),
                        )
                        next_oi_refresh[exchange] = now + timedelta(
                            seconds=self._oi_refresh_interval(exchange)
                        )
                    if now >= next_hot_oi_refresh[exchange]:
                        hot_symbols = self._hot_symbols(exchange, now)
                        if hot_symbols:
                            self._start_background_task(
                                f"hot-oi:{exchange.value}",
                                self.refresh_open_interest_for_current(exchange, symbols=hot_symbols),
                            )
                        next_hot_oi_refresh[exchange] = now + timedelta(
                            seconds=self._hot_oi_refresh_interval(exchange)
                        )
                    if now >= next_hot_candle_refresh[exchange]:
                        hot_symbols = self._hot_symbols(exchange, now)
                        if hot_symbols:
                            self._start_background_task(
                                f"hot-candles:{exchange.value}",
                                self.refresh_candles_for_symbols(exchange, hot_symbols),
                            )
                        next_hot_candle_refresh[exchange] = now + timedelta(
                            seconds=self.config.realtime.hot_candle_refresh_interval_seconds
                        )
                    if now >= next_avg24h_candle_refresh[exchange]:
                        self._start_background_task(
                            f"avg24h-candles:{exchange.value}",
                            self.refresh_avg24h_candles_for_current(exchange),
                        )
                        next_avg24h_candle_refresh[exchange] = now + timedelta(
                            seconds=self.config.realtime.avg24h_candle_refresh_interval_seconds
                        )
                if now >= next_eval:
                    if self._has_unevaluated_data():
                        await self.evaluate_current(send=True)
                    next_eval = now + timedelta(
                        seconds=self.config.realtime.signal_eval_interval_seconds
                    )
                if now >= next_cleanup:
                    self._prune_history(now)
                    next_cleanup = now + timedelta(hours=max(1, self.config.history.cleanup_interval_hours))
                await asyncio.sleep(1)
        finally:
            if self._full_refresh_task is not None:
                self._full_refresh_task.cancel()
                await asyncio.gather(self._full_refresh_task, return_exceptions=True)
            for task in self._background_tasks.values():
                task.cancel()
            if self._background_tasks:
                await asyncio.gather(*self._background_tasks.values(), return_exceptions=True)
            await self.stop_websockets()

    def pause(self) -> None:
        self.state.paused = True

    def resume(self) -> None:
        self.state.paused = False

    def _prune_history(self, now: datetime) -> None:
        result = self.storage.prune_history(
            now=now,
            market_snapshot_days=self.config.history.market_snapshot_days,
            oi_history_days=self.config.history.oi_history_days,
            candle_1m_days=self.config.history.candle_1m_days,
            candle_5m_days=self.config.history.candle_5m_days,
            funding_history_days=self.config.history.funding_history_days,
            signal_history_days=self.config.history.signal_history_days,
        )
        if result.total:
            LOGGER.info(
                "sqlite retention cleanup removed %s rows "
                "(snapshots=%s, oi=%s, candles_1m=%s, candles_5m=%s, funding=%s, signals=%s)",
                result.total,
                result.market_snapshots,
                result.open_interest_history,
                result.market_candles_1m,
                result.market_candles_5m,
                result.funding_history,
                result.signals,
            )

    async def on_websocket_snapshot(self, snapshot: MarketSnapshot) -> None:
        self.buffer.add_snapshot(snapshot)
        by_symbol = self.state.latest_refresh.snapshots.setdefault(snapshot.symbol, {})
        old = by_symbol.get(snapshot.exchange)
        by_symbol[snapshot.exchange] = _merge_snapshot(old, snapshot)
        self._mark_data_changed()

    async def _on_refresh_snapshot(
        self,
        snapshot: MarketSnapshot,
        candles: tuple,
    ) -> None:
        by_symbol = self.state.latest_refresh.snapshots.setdefault(snapshot.symbol, {})
        by_symbol[snapshot.exchange] = _merge_snapshot(by_symbol.get(snapshot.exchange), snapshot)
        if candles:
            self.state.latest_refresh.candles.setdefault(snapshot.symbol, {})[snapshot.exchange] = candles
        self._mark_data_changed()

    def _start_full_refresh(self, started_at: datetime) -> None:
        LOGGER.info("starting full REST refresh")
        self.state.refresh_in_progress = True
        self.state.refresh_started_at = started_at
        self._full_refresh_task = asyncio.create_task(self._run_full_refresh())

    async def _run_full_refresh(self) -> None:
        try:
            await self.scan_once(send=False, incremental=True)
            if self.config.realtime.websocket.enabled and not self._ws_tasks:
                self._start_websockets()
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("full REST refresh failed")
        finally:
            self.state.refresh_in_progress = False

    def _consume_full_refresh_task(self) -> None:
        if self._full_refresh_task is None or not self._full_refresh_task.done():
            return
        try:
            self._full_refresh_task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            LOGGER.exception("full REST refresh task failed")
        finally:
            self._full_refresh_task = None

    def _start_background_task(self, key: str, coroutine) -> None:
        existing = self._background_tasks.get(key)
        if existing is not None and not existing.done():
            coroutine.close()
            return
        self._background_tasks[key] = asyncio.create_task(coroutine)

    def _consume_background_tasks(self) -> None:
        done_keys = [key for key, task in self._background_tasks.items() if task.done()]
        for key in done_keys:
            task = self._background_tasks.pop(key)
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                LOGGER.exception("background task failed: %s", key)

    async def refresh_open_interest_for_current(
        self,
        exchange: ExchangeName,
        symbols: set[str] | None = None,
    ) -> None:
        client = self.market_data.clients.get(exchange)
        if client is None:
            return
        if not self._exchange_available(exchange, "OI refresh"):
            return
        snapshots = [
            by_exchange[exchange]
            for by_exchange in tuple(self.state.latest_refresh.snapshots.values())
            if exchange in by_exchange
            and (symbols is None or by_exchange[exchange].symbol in symbols)
        ]
        if not snapshots:
            return
        snapshots = self._oi_refresh_batch(exchange, snapshots, symbols is not None)
        error_counts: dict[str, int] = {}

        async def enrich(snapshot: MarketSnapshot) -> bool:
            if not self._exchange_available(exchange, "OI enrich", log_skip=False):
                return False
            try:
                enriched = await client.enrich_snapshot(snapshot)
            except ExchangeApiError as exc:
                self.circuit_breaker.record_failure(exchange, exc)
                fallback = await self.market_data._fallback_enrich_snapshot(snapshot, exc)
                if fallback is None:
                    _count_error(error_counts, exc)
                    return False
                self.buffer.add_snapshot(fallback)
                by_exchange = self.state.latest_refresh.snapshots.setdefault(fallback.symbol, {})
                by_exchange[exchange] = _merge_snapshot(by_exchange.get(exchange), fallback)
                self.storage.save_snapshot(fallback)
                self._mark_data_changed()
                return True
            except Exception as exc:
                self.circuit_breaker.record_failure(exchange, exc)
                LOGGER.exception("unexpected OI refresh failure for %s %s", exchange, snapshot.symbol)
                return False
            self.circuit_breaker.record_success(exchange)
            self.buffer.add_snapshot(enriched)
            by_exchange = self.state.latest_refresh.snapshots.setdefault(enriched.symbol, {})
            by_exchange[exchange] = _merge_snapshot(by_exchange.get(exchange), enriched)
            self.storage.save_snapshot(enriched)
            self._mark_data_changed()
            return True

        skipped = await self._run_exchange_batches(
            exchange=exchange,
            items=snapshots,
            operation="OI enrich",
            worker=enrich,
        )
        if skipped:
            LOGGER.info(
                "OI refresh stopped for %s: circuit breaker cooldown %ss, skipped %s symbols",
                exchange.value,
                self.circuit_breaker.blocked_for(exchange),
                skipped,
            )
        _log_error_summary("OI refresh failed", error_counts)

    async def refresh_candles_for_symbols(self, exchange: ExchangeName, symbols: set[str]) -> None:
        client = self.market_data.clients.get(exchange)
        if client is None or not symbols:
            return
        if not self._exchange_available(exchange, "hot candle refresh"):
            return
        current = [
            by_exchange[exchange]
            for by_exchange in tuple(self.state.latest_refresh.snapshots.values())
            if exchange in by_exchange and by_exchange[exchange].symbol in symbols
        ]
        if not current:
            return
        error_counts: dict[str, int] = {}

        async def refresh(snapshot: MarketSnapshot) -> bool:
            if not self._exchange_available(exchange, "hot candle request", log_skip=False):
                return False
            try:
                candles = tuple(await client.get_klines(snapshot.symbol))
            except ExchangeApiError as exc:
                self.circuit_breaker.record_failure(exchange, exc)
                _count_error(error_counts, exc)
                return False
            except Exception as exc:
                self.circuit_breaker.record_failure(exchange, exc)
                LOGGER.exception(
                    "unexpected hot candle refresh failure for %s %s",
                    exchange,
                    snapshot.symbol,
                )
                return False
            self.circuit_breaker.record_success(exchange)
            if not candles:
                return False
            self.state.latest_refresh.candles.setdefault(snapshot.symbol, {})[exchange] = candles
            if self.storage:
                self.storage.save_candles(candles, interval="1m")
            self._mark_data_changed()
            return True

        skipped = await self._run_exchange_batches(
            exchange=exchange,
            items=current,
            operation="hot candle request",
            worker=refresh,
        )
        if skipped:
            LOGGER.info(
                "hot candle refresh stopped for %s: circuit breaker cooldown %ss, skipped %s symbols",
                exchange.value,
                self.circuit_breaker.blocked_for(exchange),
                skipped,
            )
        _log_error_summary("hot candle refresh failed", error_counts)

    async def refresh_avg24h_candles_for_current(self, exchange: ExchangeName) -> None:
        client = self.market_data.clients.get(exchange)
        if client is None:
            return
        if not self._exchange_available(exchange, "avg24h candle refresh"):
            return
        current = [
            by_exchange[exchange]
            for by_exchange in tuple(self.state.latest_refresh.snapshots.values())
            if exchange in by_exchange
        ]
        if not current:
            return
        now = datetime.now(UTC)
        start = now - timedelta(hours=24, minutes=10)
        current = self._avg24h_candle_batch(exchange, current, now=now, start=start)
        error_counts: dict[str, int] = {}

        async def refresh(snapshot: MarketSnapshot) -> bool:
            if not self._exchange_available(exchange, "avg24h candle request", log_skip=False):
                return False
            existing = self.storage.load_candles(exchange, snapshot.symbol, interval="5m", since=start)
            if _has_fresh_5m_candles(existing, now):
                self.state.latest_refresh.candles_5m.setdefault(snapshot.symbol, {})[exchange] = tuple(existing)
                return False
            try:
                candles = tuple(
                    await client.get_klines_history(
                        snapshot.symbol,
                        interval="5m",
                        start=start,
                        end=now,
                    )
                )
            except ExchangeApiError as exc:
                self.circuit_breaker.record_failure(exchange, exc)
                _count_error(error_counts, exc)
                return False
            except Exception as exc:
                self.circuit_breaker.record_failure(exchange, exc)
                LOGGER.exception(
                    "unexpected avg24h candle refresh failure for %s %s",
                    exchange,
                    snapshot.symbol,
                )
                return False
            self.circuit_breaker.record_success(exchange)
            if not candles:
                return False
            self.storage.save_candles(candles, interval="5m")
            self.state.latest_refresh.candles_5m.setdefault(snapshot.symbol, {})[exchange] = candles
            self._mark_data_changed()
            return True

        skipped = await self._run_exchange_batches(
            exchange=exchange,
            items=current,
            operation="avg24h candle request",
            worker=refresh,
        )
        if skipped:
            LOGGER.info(
                "avg24h candle refresh stopped for %s: circuit breaker cooldown %ss, skipped %s symbols",
                exchange.value,
                self.circuit_breaker.blocked_for(exchange),
                skipped,
            )
        _log_error_summary("avg24h candle refresh failed", error_counts)

    def _oi_refresh_interval(self, exchange: ExchangeName) -> int:
        if exchange is ExchangeName.BINANCE:
            return self.config.realtime.binance.int("oi_rest_poll_interval_seconds", 30)
        if exchange is ExchangeName.BYBIT:
            return self.config.realtime.bybit.int("oi_rest_fallback_interval_seconds", 60)
        return self.config.realtime.okx.int("oi_rest_fallback_interval_seconds", 60)

    def _hot_oi_refresh_interval(self, exchange: ExchangeName) -> int:
        if exchange is ExchangeName.BINANCE:
            return self.config.realtime.binance.int("candidate_oi_rest_poll_interval_seconds", 10)
        if exchange is ExchangeName.BYBIT:
            return self.config.realtime.bybit.int("candidate_oi_rest_poll_interval_seconds", 15)
        return self.config.realtime.okx.int("candidate_oi_rest_poll_interval_seconds", 15)

    def _exchange_available(self, exchange: ExchangeName, operation: str, *, log_skip: bool = True) -> bool:
        if self.circuit_breaker.allow_request(exchange):
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

    def _exchange_concurrency(self, exchange: ExchangeName) -> int:
        if self.circuit_breaker.failure_count(exchange):
            return max(1, self.config.realtime.degraded_max_concurrency)
        return 4

    def _oi_refresh_batch_size(self, exchange: ExchangeName) -> int:
        if exchange is ExchangeName.BINANCE:
            return self.config.realtime.binance.int("oi_refresh_batch_size", 20)
        if exchange is ExchangeName.BYBIT:
            return self.config.realtime.bybit.int("oi_refresh_batch_size", 20)
        return self.config.realtime.okx.int("oi_refresh_batch_size", 20)

    def _effective_oi_refresh_batch_size(self, exchange: ExchangeName) -> int:
        batch_size = self._oi_refresh_batch_size(exchange)
        if self.circuit_breaker.failure_count(exchange):
            batch_size = min(batch_size, self.config.realtime.degraded_oi_batch_size)
        return max(1, batch_size)

    def _avg24h_candle_batch(
        self,
        exchange: ExchangeName,
        snapshots: list[MarketSnapshot],
        *,
        now: datetime,
        start: datetime,
    ) -> list[MarketSnapshot]:
        batch_size = max(1, self.config.realtime.avg24h_candle_batch_size)
        if self.circuit_breaker.failure_count(exchange):
            batch_size = min(batch_size, self.config.realtime.degraded_oi_batch_size)
        latest_by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
        for snapshot in self.storage.load_market_snapshots_since(start, (exchange,)):
            current = latest_by_symbol.get(snapshot.symbol)
            if current is None or snapshot.timestamp > current.timestamp:
                latest_by_symbol[snapshot.symbol] = snapshot
        snapshots = list(latest_by_symbol.values())
        if len(snapshots) <= batch_size:
            self.state.avg24h_candle_offsets[exchange] = 0
            return snapshots

        candle_stats: dict[str, tuple[int, datetime]] = {}
        for candle in self.storage.load_candles_since(start, "5m", (exchange,)):
            count, latest = candle_stats.get(candle.symbol, (0, datetime.min.replace(tzinfo=UTC)))
            candle_stats[candle.symbol] = (count + 1, max(latest, candle.open_time))
        fresh_symbols = {
            symbol
            for symbol, (count, latest) in candle_stats.items()
            if count >= 200 and latest >= now - timedelta(minutes=15)
        }
        batch = sorted(
            (snapshot for snapshot in snapshots if snapshot.symbol not in fresh_symbols),
            key=lambda item: item.volume_24h_usdt or ZERO,
            reverse=True,
        )[:batch_size]
        if len(batch) < batch_size:
            used = {snapshot.symbol for snapshot in batch}
            refreshable = sorted(
                (snapshot for snapshot in snapshots if snapshot.symbol not in used),
                key=lambda item: item.symbol,
            )
            offset = self.state.avg24h_candle_offsets.get(exchange, 0) % len(refreshable)
            extra = refreshable[offset : offset + batch_size - len(batch)]
            if len(extra) < batch_size - len(batch):
                extra.extend(refreshable[: batch_size - len(batch) - len(extra)])
            batch.extend(extra)
            self.state.avg24h_candle_offsets[exchange] = (offset + len(extra)) % len(refreshable)
        else:
            self.state.avg24h_candle_offsets[exchange] = 0
        return batch

    def _request_delay_seconds(self, exchange: ExchangeName) -> float:
        delay_ms = self.config.realtime.normal_request_delay_ms
        if self.circuit_breaker.failure_count(exchange):
            delay_ms = self.config.realtime.degraded_request_delay_ms
        return max(0, delay_ms) / 1000

    async def _run_exchange_batches(
        self,
        *,
        exchange: ExchangeName,
        items: list[MarketSnapshot],
        operation: str,
        worker: Callable[[MarketSnapshot], Awaitable[bool]],
    ) -> int:
        pending = list(items)
        skipped = 0
        while pending:
            if not self._exchange_available(exchange, operation, log_skip=False):
                skipped += len(pending)
                break
            limit = max(1, self._exchange_concurrency(exchange))
            batch, pending = pending[:limit], pending[limit:]

            async def run_item(index: int, item: MarketSnapshot) -> bool:
                delay = self._request_delay_seconds(exchange) * index
                if delay > 0:
                    await asyncio.sleep(delay)
                return await worker(item)

            await asyncio.gather(*(run_item(index, item) for index, item in enumerate(batch)))
        return skipped

    def _oi_refresh_batch(
        self,
        exchange: ExchangeName,
        snapshots: list[MarketSnapshot],
        hot: bool,
    ) -> list[MarketSnapshot]:
        snapshots = sorted(snapshots, key=lambda item: item.symbol)
        batch_size = self._effective_oi_refresh_batch_size(exchange)
        if hot:
            return snapshots[:batch_size] if self.circuit_breaker.failure_count(exchange) else snapshots
        if len(snapshots) <= batch_size:
            self.state.oi_refresh_offsets[exchange] = 0
            return snapshots
        offset = self.state.oi_refresh_offsets.get(exchange, 0) % len(snapshots)
        batch = snapshots[offset : offset + batch_size]
        if len(batch) < batch_size:
            batch.extend(snapshots[: batch_size - len(batch)])
        self.state.oi_refresh_offsets[exchange] = (offset + batch_size) % len(snapshots)
        return batch

    def _start_websockets(self) -> None:
        symbols = set(self.config.signal.whitelist_symbols)
        if not symbols:
            symbols = set(self.buffer.latest_symbols())
        backoff = self.config.realtime.websocket.reconnect_backoff_seconds
        if ExchangeName.BINANCE in self.config.signal.enabled_exchanges:
            self._ws_tasks.append(
                asyncio.create_task(
                    BinanceTickerWebSocket(backoff, self.on_websocket_snapshot).run_forever(
                        self._ws_stop_event
                    )
                )
            )
        if symbols and ExchangeName.BYBIT in self.config.signal.enabled_exchanges:
            self._ws_tasks.append(
                asyncio.create_task(
                    BybitTickerWebSocket(symbols, backoff, self.on_websocket_snapshot).run_forever(
                        self._ws_stop_event
                    )
                )
            )
        if symbols and ExchangeName.OKX in self.config.signal.enabled_exchanges:
            self._ws_tasks.append(
                asyncio.create_task(
                    OKXPublicWebSocket(symbols, backoff, self.on_websocket_snapshot).run_forever(
                        self._ws_stop_event
                    )
                )
            )

    async def stop_websockets(self) -> None:
        self._ws_stop_event.set()
        for task in self._ws_tasks:
            task.cancel()
        if self._ws_tasks:
            await asyncio.gather(*self._ws_tasks, return_exceptions=True)

    def _runtime_user_settings(self) -> list[tuple[int | None, SignalSettings]]:
        raw_by_user = self.storage.list_user_settings()
        if not raw_by_user:
            return [(None, self.config.signal)]
        return [
            (user_id, self._with_grid_symbols(effective_settings(self.config, raw_settings)))
            for user_id, raw_settings in raw_by_user.items()
        ]

    def _with_grid_symbols(self, settings: SignalSettings) -> SignalSettings:
        return replace(settings, grid_eligible_symbols=self.market_data.grid_eligible_symbols)

    def _selection_settings(self, settings: SignalSettings | None) -> tuple[SignalSettings, ...]:
        if settings is not None:
            return (settings,)
        return tuple(user_settings for _, user_settings in self._runtime_user_settings())

    def _mark_data_changed(self) -> None:
        self.state.data_revision += 1

    def _has_unevaluated_data(self) -> bool:
        return self.state.last_sent_evaluation_revision != self.state.data_revision

    def _register_hot_candidates(
        self,
        signal: AggregatedSignal,
        settings: SignalSettings,
    ) -> None:
        if "oi_change_pct" not in set(settings.enabled_filters):
            return
        threshold = settings.thresholds.min_oi_change_pct * self.config.realtime.hot_candidate_oi_ratio
        if threshold <= Decimal("0"):
            return
        expires_at = datetime.now(UTC) + timedelta(
            minutes=self.config.realtime.hot_candidate_ttl_minutes
        )
        for evaluation in signal.evaluations:
            oi_change = evaluation.metrics.oi_change_pct
            if evaluation.snapshot is None or oi_change is None or oi_change < threshold:
                continue
            candidates = self.state.hot_candidates.setdefault(evaluation.exchange, {})
            current = candidates.get(signal.symbol)
            if current is None or current < expires_at:
                candidates[signal.symbol] = expires_at

    def _hot_symbols(self, exchange: ExchangeName, now: datetime) -> set[str]:
        candidates = self.state.hot_candidates.get(exchange)
        if not candidates:
            return set()
        expired = [symbol for symbol, expires_at in candidates.items() if expires_at <= now]
        for symbol in expired:
            candidates.pop(symbol, None)
        if not candidates:
            return set()
        limited = sorted(candidates.items(), key=lambda item: item[1], reverse=True)[
            : self.config.realtime.hot_candidate_max_symbols_per_exchange
        ]
        return {symbol for symbol, _ in limited}

    async def _handle_signal(
        self,
        signal: AggregatedSignal,
        send: bool,
        telegram_user_id: int | None,
        settings: SignalSettings,
    ) -> None:
        for exchange_signal in _split_passing_exchanges(signal):
            exchange_signal = await self._enrich_signal_before_send(exchange_signal, settings)
            if not exchange_signal.passed:
                continue
            if self.storage.signal_recently_sent(
                symbol=exchange_signal.symbol,
                cooldown_minutes=settings.thresholds.cooldown_minutes,
                now=exchange_signal.created_at,
                telegram_user_id=telegram_user_id,
                exchange=exchange_signal.primary_exchange,
            ):
                continue
            message = format_signal(
                exchange_signal,
                enabled_filters=settings.enabled_filters,
                oi_period_minutes=settings.oi_period_minutes,
                price_change_period_minutes=settings.price_change_period_minutes,
            )
            sent = False
            if send and self.notifier is not None:
                sent = await self.notifier(telegram_user_id, exchange_signal, message)
            self.storage.save_signal(
                exchange_signal,
                message_text=message,
                sent=sent,
                telegram_user_id=telegram_user_id,
            )

    async def _enrich_signal_before_send(
        self,
        signal: AggregatedSignal,
        settings: SignalSettings,
    ) -> AggregatedSignal:
        evaluation = signal.evaluations[0] if signal.evaluations else None
        if evaluation is None or evaluation.snapshot is None:
            return signal
        if not _needs_pre_send_candle_enrich(evaluation):
            return signal
        exchange = evaluation.exchange
        client = self.market_data.clients.get(exchange)
        if client is None or not self._exchange_available(exchange, "pre-send candle enrich", log_skip=False):
            return signal
        snapshot = evaluation.snapshot
        try:
            async with asyncio.timeout(4):
                await self._ensure_pre_send_candles(client, snapshot, settings)
        except TimeoutError:
            LOGGER.warning("pre-send candle enrich timed out for %s %s", exchange.value, snapshot.symbol)
            return signal
        except ExchangeApiError as exc:
            self.circuit_breaker.record_failure(exchange, exc)
            LOGGER.warning("pre-send candle enrich failed for %s %s: %s", exchange.value, snapshot.symbol, exc)
            return signal
        except Exception:
            LOGGER.exception("unexpected pre-send candle enrich failure for %s %s", exchange.value, snapshot.symbol)
            return signal

        enriched = self.engine.evaluate_symbol(
            symbol=signal.symbol,
            snapshots={exchange: snapshot},
            settings=settings,
            candles=self.state.latest_refresh.candles.get(signal.symbol, {}),
            avg24h_candles=self.state.latest_refresh.candles_5m.get(signal.symbol, {}),
        )
        split = _split_passing_exchanges(replace(enriched, created_at=signal.created_at))
        return split[0] if split else replace(signal, passed=False, evaluations=())

    async def _ensure_pre_send_candles(
        self,
        client,
        snapshot: MarketSnapshot,
        settings: SignalSettings,
    ) -> None:
        now = datetime.now(UTC)
        one_minute_start = snapshot.timestamp - timedelta(
            minutes=max(
                settings.volume_baseline_period_minutes + settings.volume_spike_period_minutes,
                settings.volatility_period_minutes,
            )
            + 10
        )
        existing_1m = self.storage.load_candles(
            snapshot.exchange,
            snapshot.symbol,
            interval="1m",
            since=one_minute_start,
        )
        if not _has_fresh_1m_candles(existing_1m, now, settings):
            candles_1m = tuple(await client.get_klines(snapshot.symbol))
            if candles_1m:
                self.storage.save_candles(candles_1m, interval="1m")
                self.state.latest_refresh.candles.setdefault(snapshot.symbol, {})[snapshot.exchange] = candles_1m
                self._mark_data_changed()
        elif existing_1m:
            self.state.latest_refresh.candles.setdefault(snapshot.symbol, {})[snapshot.exchange] = tuple(existing_1m)

        five_minute_start = now - timedelta(hours=24, minutes=10)
        existing_5m = self.storage.load_candles(
            snapshot.exchange,
            snapshot.symbol,
            interval="5m",
            since=five_minute_start,
        )
        if not _has_fresh_5m_candles(existing_5m, now):
            candles_5m = tuple(
                await client.get_klines_history(
                    snapshot.symbol,
                    interval="5m",
                    start=five_minute_start,
                    end=now,
                )
            )
            if candles_5m:
                self.storage.save_candles(candles_5m, interval="5m")
                self.state.latest_refresh.candles_5m.setdefault(snapshot.symbol, {})[snapshot.exchange] = candles_5m
                self._mark_data_changed()
        elif existing_5m:
            self.state.latest_refresh.candles_5m.setdefault(snapshot.symbol, {})[snapshot.exchange] = tuple(existing_5m)


def _merge_snapshot(old: MarketSnapshot | None, new: MarketSnapshot) -> MarketSnapshot:
    if old is None:
        return new
    return old.with_updates(
        timestamp=new.timestamp,
        price=new.price if new.price is not None else old.price,
        open_interest=new.open_interest if new.open_interest is not None else old.open_interest,
        open_interest_value_usdt=(
            new.open_interest_value_usdt
            if new.open_interest_value_usdt is not None
            else old.open_interest_value_usdt
        ),
        open_interest_value_estimated=(
            new.open_interest_value_estimated or old.open_interest_value_estimated
        ),
        volume_24h_usdt=new.volume_24h_usdt if new.volume_24h_usdt is not None else old.volume_24h_usdt,
        price_change_24h_pct=(
            new.price_change_24h_pct
            if new.price_change_24h_pct is not None
            else old.price_change_24h_pct
        ),
        funding_rate_pct=new.funding_rate_pct if new.funding_rate_pct is not None else old.funding_rate_pct,
        source=new.source,
        raw={**old.raw, **new.raw},
    )


def _split_passing_exchanges(signal: AggregatedSignal) -> tuple[AggregatedSignal, ...]:
    passing = tuple(evaluation for evaluation in signal.evaluations if evaluation.passed)
    if not passing:
        return ()
    return tuple(
        replace(
            signal,
            primary_exchange=evaluation.exchange,
            evaluations=(evaluation,),
            score=evaluation.score,
            passed=True,
            reason=f"{evaluation.exchange.value}_passed",
        )
        for evaluation in passing
    )


def _count_error(error_counts: dict[str, int], exc: ExchangeApiError) -> None:
    message = str(exc)
    error_counts[message] = error_counts.get(message, 0) + 1


def _has_fresh_5m_candles(candles: list[Candle], now: datetime) -> bool:
    if len(candles) < 200:
        return False
    latest = max(candle.open_time for candle in candles)
    return latest >= now - timedelta(minutes=15)


def _has_fresh_1m_candles(candles: list[Candle], now: datetime, settings: SignalSettings) -> bool:
    required = max(
        settings.volume_baseline_period_minutes + settings.volume_spike_period_minutes,
        settings.volatility_period_minutes,
    )
    if len(candles) < max(5, required - 5):
        return False
    latest = max(candle.open_time for candle in candles)
    return latest >= now - timedelta(minutes=10)


def _needs_pre_send_candle_enrich(evaluation: ExchangeSignalEvaluation) -> bool:
    for item in evaluation.filters:
        if item.name == "volume_spike_ratio" and item.value in (None, ZERO):
            return True
        if item.name == "volatility_pct":
            if item.value in (None, ZERO):
                return True
            if item.metadata.get("avg24h_pct") is None:
                return True
    return False


def _log_error_summary(prefix: str, error_counts: dict[str, int]) -> None:
    for message, count in sorted(error_counts.items(), key=lambda item: item[0]):
        if count == 1:
            LOGGER.warning("%s: %s", prefix, message)
        else:
            LOGGER.warning("%s: %sx %s", prefix, count, message)
