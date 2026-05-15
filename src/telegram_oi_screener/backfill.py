from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .config import ScreenerConfig
from .exchanges.base import BaseExchangeClient, ExchangeApiError
from .market_data import build_clients
from .models import ExchangeName
from .storage import SQLiteStorage

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BackfillResult:
    exchange: ExchangeName
    symbol: str
    oi_rows: int
    candle_1m_rows: int
    candle_5m_rows: int
    errors: tuple[str, ...] = ()


class BackfillService:
    def __init__(
        self,
        config: ScreenerConfig,
        storage: SQLiteStorage,
        clients: dict[ExchangeName, BaseExchangeClient] | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.clients = clients or build_clients(config)

    async def backfill(
        self,
        symbols: set[str] | None = None,
        days: int | None = None,
    ) -> list[BackfillResult]:
        self.storage.init_schema()
        selected_symbols = symbols or await self._top_symbols()
        semaphore = asyncio.Semaphore(self.config.history.backfill_batch_size)
        tasks = [
            self._backfill_one(exchange, client, symbol.upper(), days, semaphore)
            for exchange, client in self.clients.items()
            for symbol in selected_symbols
        ]
        return await asyncio.gather(*tasks) if tasks else []

    async def _top_symbols(self) -> set[str]:
        by_volume: dict[str, int] = {}
        for client in self.clients.values():
            try:
                snapshots = await client.get_ticker_snapshots()
            except ExchangeApiError as exc:
                LOGGER.warning("ticker fetch failed during backfill symbol selection: %s", exc)
                continue
            for symbol, snapshot in snapshots.items():
                if snapshot.volume_24h_usdt is None:
                    continue
                if snapshot.volume_24h_usdt < self.config.signal.thresholds.min_24h_volume_usdt:
                    continue
                by_volume[symbol] = max(by_volume.get(symbol, 0), int(snapshot.volume_24h_usdt))
        top = sorted(by_volume, key=by_volume.get, reverse=True)
        return set(top[: self.config.history.backfill_top_symbols])

    async def _backfill_one(
        self,
        exchange: ExchangeName,
        client: BaseExchangeClient,
        symbol: str,
        days: int | None,
        semaphore: asyncio.Semaphore,
    ) -> BackfillResult:
        async with semaphore:
            errors: list[str] = []
            oi_rows = 0
            candle_1m_rows = 0
            candle_5m_rows = 0
            end = datetime.now(UTC)
            oi_start = end - timedelta(days=days or self.config.history.oi_history_days)
            candle_1m_start = end - timedelta(days=min(days or self.config.history.candle_1m_days, self.config.history.candle_1m_days))
            candle_5m_start = end - timedelta(days=days or self.config.history.candle_5m_days)

            oi_job = self.storage.record_backfill_job(exchange, symbol, "open_interest", "5m", "running")
            try:
                oi_history = await client.get_open_interest_history(symbol, start=oi_start, end=end, limit=0)
                oi_rows = self.storage.save_open_interest_history(tuple(oi_history))
                self.storage.record_backfill_job(
                    exchange,
                    symbol,
                    "open_interest",
                    "5m",
                    "finished",
                    rows_written=oi_rows,
                    job_id=oi_job,
                )
            except Exception as exc:
                message = repr(exc)
                errors.append(f"oi:{message}")
                self.storage.record_backfill_job(
                    exchange,
                    symbol,
                    "open_interest",
                    "5m",
                    "failed",
                    error=message,
                    job_id=oi_job,
                )

            candle_1m_rows = await self._backfill_candles(
                exchange,
                client,
                symbol,
                "1m",
                candle_1m_start,
                end,
                errors,
            )
            candle_5m_rows = await self._backfill_candles(
                exchange,
                client,
                symbol,
                "5m",
                candle_5m_start,
                end,
                errors,
            )
            return BackfillResult(
                exchange=exchange,
                symbol=symbol,
                oi_rows=oi_rows,
                candle_1m_rows=candle_1m_rows,
                candle_5m_rows=candle_5m_rows,
                errors=tuple(errors),
            )

    async def _backfill_candles(
        self,
        exchange: ExchangeName,
        client: BaseExchangeClient,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        errors: list[str],
    ) -> int:
        job = self.storage.record_backfill_job(exchange, symbol, "candles", interval, "running")
        try:
            candles = await client.get_klines_history(symbol, interval=interval, start=start, end=end)
            rows = self.storage.save_candles(tuple(candles), interval=interval)
            self.storage.record_backfill_job(
                exchange,
                symbol,
                "candles",
                interval,
                "finished",
                rows_written=rows,
                job_id=job,
            )
            return rows
        except Exception as exc:
            message = repr(exc)
            errors.append(f"candles:{interval}:{message}")
            self.storage.record_backfill_job(
                exchange,
                symbol,
                "candles",
                interval,
                "failed",
                error=message,
                job_id=job,
            )
            return 0
