from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterator

from .models import AggregatedSignal, Candle, ExchangeName, GridDryRun, MarketSnapshot, decimal_to_json, decimal_or_none


SCHEMA = """
CREATE TABLE IF NOT EXISTS user_settings (
    telegram_user_id INTEGER PRIMARY KEY,
    settings_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS watched_symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_user_id INTEGER,
    symbol TEXT NOT NULL,
    enabled BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS exchange_symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_symbol TEXT NOT NULL,
    active BOOLEAN DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(exchange, symbol)
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_symbol TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    price TEXT,
    open_interest TEXT,
    open_interest_value_usdt TEXT,
    open_interest_value_estimated BOOLEAN DEFAULT 0,
    volume_24h_usdt TEXT,
    recent_volume_usdt TEXT,
    price_change_24h_pct TEXT,
    funding_rate_pct TEXT,
    source TEXT,
    raw_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_lookup
ON market_snapshots(exchange, symbol, timestamp);

CREATE TABLE IF NOT EXISTS market_candles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    open_time TIMESTAMP NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume_usdt TEXT NOT NULL,
    source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(exchange, symbol, interval, open_time)
);

CREATE INDEX IF NOT EXISTS idx_market_candles_lookup
ON market_candles(exchange, symbol, interval, open_time);

CREATE TABLE IF NOT EXISTS open_interest_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_symbol TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_interest TEXT,
    open_interest_value_usdt TEXT,
    open_interest_value_estimated BOOLEAN DEFAULT 0,
    source TEXT,
    raw_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(exchange, symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_open_interest_history_lookup
ON open_interest_history(exchange, symbol, timestamp);

CREATE TABLE IF NOT EXISTS funding_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange_symbol TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    funding_rate_pct TEXT,
    source TEXT,
    raw_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(exchange, symbol, timestamp)
);

CREATE TABLE IF NOT EXISTS backfill_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    data_type TEXT NOT NULL,
    interval TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    rows_written INTEGER DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_user_id INTEGER,
    symbol TEXT NOT NULL,
    aggregation_mode TEXT NOT NULL,
    primary_exchange TEXT NOT NULL,
    direction TEXT NOT NULL,
    score INTEGER NOT NULL,
    passed_filters_json TEXT NOT NULL,
    failed_filters_json TEXT NOT NULL,
    snapshots_json TEXT NOT NULL,
    message_text TEXT NOT NULL,
    sent BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol_time
ON signals(symbol, created_at);

CREATE TABLE IF NOT EXISTS ignored_symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_user_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    ignore_until TIMESTAMP,
    permanent BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS grid_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_user_id INTEGER,
    signal_id INTEGER,
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    mode TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@dataclass(frozen=True)
class RetentionResult:
    market_snapshots: int = 0
    open_interest_history: int = 0
    market_candles_1m: int = 0
    market_candles_5m: int = 0
    funding_history: int = 0
    signals: int = 0

    @property
    def total(self) -> int:
        return (
            self.market_snapshots
            + self.open_interest_history
            + self.market_candles_1m
            + self.market_candles_5m
            + self.funding_history
            + self.signals
        )


class SQLiteStorage:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def init_schema(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.commit()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def save_snapshot(self, snapshot: MarketSnapshot) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO market_snapshots (
                    exchange, symbol, exchange_symbol, timestamp, price, open_interest,
                    open_interest_value_usdt, open_interest_value_estimated, volume_24h_usdt,
                    recent_volume_usdt, price_change_24h_pct, funding_rate_pct, source, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.exchange.value,
                    snapshot.symbol,
                    snapshot.exchange_symbol,
                    snapshot.timestamp.isoformat(),
                    _str_or_none(snapshot.price),
                    _str_or_none(snapshot.open_interest),
                    _str_or_none(snapshot.open_interest_value_usdt),
                    int(snapshot.open_interest_value_estimated),
                    _str_or_none(snapshot.volume_24h_usdt),
                    _str_or_none(snapshot.recent_volume_usdt),
                    _str_or_none(snapshot.price_change_24h_pct),
                    _str_or_none(snapshot.funding_rate_pct),
                    snapshot.source,
                    json.dumps(decimal_to_json(snapshot.raw), ensure_ascii=False),
                ),
            )
            conn.commit()

    def save_candles(self, candles: list[Candle] | tuple[Candle, ...], interval: str) -> int:
        if not candles:
            return 0
        with self.connect() as conn:
            before = conn.total_changes
            conn.executemany(
                """
                INSERT OR IGNORE INTO market_candles (
                    exchange, symbol, interval, open_time, open, high, low, close, volume_usdt, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        candle.exchange.value,
                        candle.symbol,
                        interval,
                        candle.open_time.isoformat(),
                        str(candle.open),
                        str(candle.high),
                        str(candle.low),
                        str(candle.close),
                        str(candle.volume_usdt),
                        "api",
                    )
                    for candle in candles
                ],
            )
            conn.commit()
            return conn.total_changes - before

    def load_candles(
        self,
        exchange: ExchangeName,
        symbol: str,
        interval: str,
        since: datetime,
    ) -> list[Candle]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, open_time, open, high, low, close, volume_usdt
                FROM market_candles
                WHERE exchange = ? AND symbol = ? AND interval = ? AND open_time >= ?
                ORDER BY open_time ASC
                """,
                (exchange.value, symbol, interval, since.isoformat()),
            ).fetchall()
        return [
            Candle(
                exchange=ExchangeName(str(row["exchange"])),
                symbol=str(row["symbol"]),
                open_time=_dt(row["open_time"]),
                open=decimal_or_none(row["open"]) or decimal_or_none("0"),
                high=decimal_or_none(row["high"]) or decimal_or_none("0"),
                low=decimal_or_none(row["low"]) or decimal_or_none("0"),
                close=decimal_or_none(row["close"]) or decimal_or_none("0"),
                volume_usdt=decimal_or_none(row["volume_usdt"]) or decimal_or_none("0"),
            )
            for row in rows
        ]

    def save_open_interest_history(
        self,
        snapshots: list[MarketSnapshot] | tuple[MarketSnapshot, ...],
    ) -> int:
        if not snapshots:
            return 0
        with self.connect() as conn:
            before = conn.total_changes
            conn.executemany(
                """
                INSERT OR IGNORE INTO open_interest_history (
                    exchange, symbol, exchange_symbol, timestamp, open_interest,
                    open_interest_value_usdt, open_interest_value_estimated, source, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snapshot.exchange.value,
                        snapshot.symbol,
                        snapshot.exchange_symbol,
                        snapshot.timestamp.isoformat(),
                        _str_or_none(snapshot.open_interest),
                        _str_or_none(snapshot.open_interest_value_usdt),
                        int(snapshot.open_interest_value_estimated),
                        snapshot.source,
                        json.dumps(decimal_to_json(snapshot.raw), ensure_ascii=False),
                    )
                    for snapshot in snapshots
                ],
            )
            conn.commit()
            return conn.total_changes - before

    def load_open_interest_history(
        self,
        exchange: ExchangeName,
        symbol: str,
        since: datetime,
    ) -> list[MarketSnapshot]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, exchange_symbol, timestamp, open_interest,
                       open_interest_value_usdt, open_interest_value_estimated, source, raw_json
                FROM open_interest_history
                WHERE exchange = ? AND symbol = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                """,
                (exchange.value, symbol, since.isoformat()),
            ).fetchall()
        snapshots: list[MarketSnapshot] = []
        for row in rows:
            snapshots.append(
                MarketSnapshot(
                    exchange=ExchangeName(str(row["exchange"])),
                    symbol=str(row["symbol"]),
                    exchange_symbol=str(row["exchange_symbol"]),
                    timestamp=_dt(row["timestamp"]),
                    open_interest=decimal_or_none(row["open_interest"]),
                    open_interest_value_usdt=decimal_or_none(row["open_interest_value_usdt"]),
                    open_interest_value_estimated=bool(row["open_interest_value_estimated"]),
                    source=str(row["source"] or "sqlite:oi_history"),
                    raw=json.loads(str(row["raw_json"] or "{}")),
                )
            )
        return snapshots

    def load_market_snapshots_since(
        self,
        since: datetime,
        exchanges: tuple[ExchangeName, ...] = (),
    ) -> list[MarketSnapshot]:
        query = """
            SELECT exchange, symbol, exchange_symbol, timestamp, price, open_interest,
                   open_interest_value_usdt, open_interest_value_estimated,
                   volume_24h_usdt, recent_volume_usdt, price_change_24h_pct,
                   funding_rate_pct, source, raw_json
            FROM market_snapshots
            WHERE timestamp >= ?
        """
        params: list[object] = [since.isoformat()]
        if exchanges:
            placeholders = ", ".join("?" for _ in exchanges)
            query += f" AND exchange IN ({placeholders})"
            params.extend(exchange.value for exchange in exchanges)
        query += " ORDER BY exchange, symbol, timestamp ASC"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            MarketSnapshot(
                exchange=ExchangeName(str(row["exchange"])),
                symbol=str(row["symbol"]),
                exchange_symbol=str(row["exchange_symbol"]),
                timestamp=_dt(row["timestamp"]),
                price=decimal_or_none(row["price"]),
                open_interest=decimal_or_none(row["open_interest"]),
                open_interest_value_usdt=decimal_or_none(row["open_interest_value_usdt"]),
                open_interest_value_estimated=bool(row["open_interest_value_estimated"]),
                volume_24h_usdt=decimal_or_none(row["volume_24h_usdt"]),
                recent_volume_usdt=decimal_or_none(row["recent_volume_usdt"]),
                price_change_24h_pct=decimal_or_none(row["price_change_24h_pct"]),
                funding_rate_pct=decimal_or_none(row["funding_rate_pct"]),
                source=str(row["source"] or "sqlite:snapshot"),
                raw=json.loads(str(row["raw_json"] or "{}")),
            )
            for row in rows
        ]

    def load_candles_since(
        self,
        since: datetime,
        interval: str,
        exchanges: tuple[ExchangeName, ...] = (),
    ) -> list[Candle]:
        query = """
            SELECT exchange, symbol, open_time, open, high, low, close, volume_usdt
            FROM market_candles
            WHERE interval = ? AND open_time >= ?
        """
        params: list[object] = [interval, since.isoformat()]
        if exchanges:
            placeholders = ", ".join("?" for _ in exchanges)
            query += f" AND exchange IN ({placeholders})"
            params.extend(exchange.value for exchange in exchanges)
        query += " ORDER BY exchange, symbol, open_time ASC"
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            Candle(
                exchange=ExchangeName(str(row["exchange"])),
                symbol=str(row["symbol"]),
                open_time=_dt(row["open_time"]),
                open=decimal_or_none(row["open"]) or decimal_or_none("0"),
                high=decimal_or_none(row["high"]) or decimal_or_none("0"),
                low=decimal_or_none(row["low"]) or decimal_or_none("0"),
                close=decimal_or_none(row["close"]) or decimal_or_none("0"),
                volume_usdt=decimal_or_none(row["volume_usdt"]) or decimal_or_none("0"),
            )
            for row in rows
        ]

    def record_backfill_job(
        self,
        exchange: ExchangeName,
        symbol: str,
        data_type: str,
        interval: str | None,
        status: str,
        rows_written: int = 0,
        error: str | None = None,
        job_id: int | None = None,
    ) -> int:
        with self.connect() as conn:
            if job_id is None:
                cursor = conn.execute(
                    """
                    INSERT INTO backfill_jobs (exchange, symbol, data_type, interval, status)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (exchange.value, symbol, data_type, interval, status),
                )
                conn.commit()
                return int(cursor.lastrowid)
            conn.execute(
                """
                UPDATE backfill_jobs
                SET finished_at = CURRENT_TIMESTAMP, status = ?, rows_written = ?, error = ?
                WHERE id = ?
                """,
                (status, rows_written, error, job_id),
            )
            conn.commit()
            return job_id

    def save_signal(
        self,
        signal: AggregatedSignal,
        message_text: str,
        sent: bool,
        telegram_user_id: int | None = None,
    ) -> int:
        passed_filters = [
            item
            for evaluation in signal.evaluations
            for item in evaluation.passed_filters
        ]
        failed_filters = [
            item
            for evaluation in signal.evaluations
            for item in evaluation.failed_filters
        ]
        snapshots = [
            evaluation.snapshot
            for evaluation in signal.evaluations
            if evaluation.snapshot is not None
        ]
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO signals (
                    telegram_user_id, symbol, aggregation_mode, primary_exchange, direction,
                    score, passed_filters_json, failed_filters_json, snapshots_json,
                    message_text, sent, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_user_id,
                    signal.symbol,
                    signal.aggregation_mode.value,
                    signal.primary_exchange.value,
                    signal.direction.value,
                    signal.score,
                    json.dumps(decimal_to_json(passed_filters), ensure_ascii=False),
                    json.dumps(decimal_to_json(failed_filters), ensure_ascii=False),
                    json.dumps(decimal_to_json(snapshots), ensure_ascii=False),
                    message_text,
                    int(sent),
                    signal.created_at.isoformat(),
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def signal_recently_sent(
        self,
        symbol: str,
        cooldown_minutes: int,
        now: datetime,
        telegram_user_id: int | None = None,
        exchange: ExchangeName | None = None,
    ) -> bool:
        cutoff = now - timedelta(minutes=cooldown_minutes)
        query = """
            SELECT 1
            FROM signals
            WHERE symbol = ?
              AND sent = 1
              AND created_at >= ?
        """
        params: list[object] = [symbol, cutoff.isoformat()]
        if exchange is not None:
            query += " AND primary_exchange = ?"
            params.append(exchange.value)
        if telegram_user_id is not None:
            query += " AND telegram_user_id = ?"
            params.append(telegram_user_id)
        query += " LIMIT 1"
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
        return row is not None

    def save_user_settings(self, telegram_user_id: int, settings: dict[str, object]) -> None:
        payload = json.dumps(decimal_to_json(settings), ensure_ascii=False)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO user_settings (telegram_user_id, settings_json, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(telegram_user_id) DO UPDATE SET
                    settings_json = excluded.settings_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (telegram_user_id, payload),
            )
            conn.commit()

    def load_user_settings(self, telegram_user_id: int) -> dict[str, object] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT settings_json FROM user_settings WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(str(row["settings_json"]))

    def list_user_settings(self) -> dict[int, dict[str, object]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT telegram_user_id, settings_json FROM user_settings"
            ).fetchall()
        return {
            int(row["telegram_user_id"]): json.loads(str(row["settings_json"]))
            for row in rows
        }

    def save_grid_action(
        self,
        dry_run: GridDryRun,
        request: dict[str, object],
        telegram_user_id: int | None = None,
        signal_id: int | None = None,
    ) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO grid_actions (
                    telegram_user_id, signal_id, exchange, symbol, mode,
                    request_json, response_json, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_user_id,
                    signal_id,
                    dry_run.exchange.value,
                    dry_run.symbol,
                    "dry_run",
                    json.dumps(decimal_to_json(request), ensure_ascii=False),
                    json.dumps(decimal_to_json(dry_run), ensure_ascii=False),
                    dry_run.status,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def latest_signals(self, limit: int = 10) -> list[sqlite3.Row]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, score, aggregation_mode, primary_exchange, sent, created_at
                FROM signals
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return list(rows)

    def latest_market_snapshot_time(self) -> datetime | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(timestamp) AS last_timestamp
                FROM market_snapshots
                """
            ).fetchone()
        if row is None or row["last_timestamp"] is None:
            return None
        return _dt(row["last_timestamp"])

    def prune_history(
        self,
        *,
        now: datetime,
        market_snapshot_days: int,
        oi_history_days: int,
        candle_1m_days: int,
        candle_5m_days: int,
        funding_history_days: int,
        signal_history_days: int,
    ) -> RetentionResult:
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        cutoffs = {
            "market_snapshots": now - timedelta(days=market_snapshot_days),
            "open_interest_history": now - timedelta(days=oi_history_days),
            "market_candles_1m": now - timedelta(days=candle_1m_days),
            "market_candles_5m": now - timedelta(days=candle_5m_days),
            "funding_history": now - timedelta(days=funding_history_days),
            "signals": now - timedelta(days=signal_history_days),
        }
        with self.connect() as conn:
            market_snapshots = _delete_count(
                conn,
                "DELETE FROM market_snapshots WHERE timestamp < ?",
                (cutoffs["market_snapshots"].isoformat(),),
            )
            open_interest_history = _delete_count(
                conn,
                "DELETE FROM open_interest_history WHERE timestamp < ?",
                (cutoffs["open_interest_history"].isoformat(),),
            )
            market_candles_1m = _delete_count(
                conn,
                "DELETE FROM market_candles WHERE interval = ? AND open_time < ?",
                ("1m", cutoffs["market_candles_1m"].isoformat()),
            )
            market_candles_5m = _delete_count(
                conn,
                "DELETE FROM market_candles WHERE interval = ? AND open_time < ?",
                ("5m", cutoffs["market_candles_5m"].isoformat()),
            )
            funding_history = _delete_count(
                conn,
                "DELETE FROM funding_history WHERE timestamp < ?",
                (cutoffs["funding_history"].isoformat(),),
            )
            signals = _delete_count(
                conn,
                "DELETE FROM signals WHERE created_at < ?",
                (cutoffs["signals"].isoformat(),),
            )
            conn.commit()
        return RetentionResult(
            market_snapshots=market_snapshots,
            open_interest_history=open_interest_history,
            market_candles_1m=market_candles_1m,
            market_candles_5m=market_candles_5m,
            funding_history=funding_history,
            signals=signals,
        )


def _str_or_none(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _delete_count(conn: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> int:
    before = conn.total_changes
    conn.execute(sql, params)
    return conn.total_changes - before


def _dt(value: object) -> datetime:
    text = str(value)
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
