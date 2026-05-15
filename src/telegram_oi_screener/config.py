from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

from .models import (
    AggregationMode,
    Direction,
    ExchangeName,
    ExecutionExchange,
    ExecutionMode,
    SIGNAL_FILTER_NAMES,
    SignalSettings,
    SignalThresholds,
)


DEFAULT_CONFIG_PATH = Path("config.yaml")


@dataclass(frozen=True)
class AppConfig:
    name: str
    log_level: str
    sqlite_path: Path


@dataclass(frozen=True)
class TelegramConfig:
    enabled: bool
    token_env: str
    allowed_user_ids_env: str

    @property
    def token(self) -> str | None:
        return os.getenv(self.token_env) or None

    @property
    def allowed_user_ids(self) -> tuple[int, ...]:
        raw = os.getenv(self.allowed_user_ids_env, "")
        ids: list[int] = []
        for item in raw.split(","):
            item = item.strip()
            if item:
                ids.append(int(item))
        return tuple(ids)


@dataclass(frozen=True)
class CoinalyzeConfig:
    enabled: bool
    api_key_env: str
    max_symbol_calls_per_minute: int

    @property
    def api_key(self) -> str | None:
        return os.getenv(self.api_key_env) or None


@dataclass(frozen=True)
class WebSocketConfig:
    enabled: bool
    reconnect_backoff_seconds: tuple[int, ...]
    heartbeat_timeout_seconds: int


@dataclass(frozen=True)
class RealtimeExchangeConfig:
    values: dict[str, Any]

    def bool(self, key: str, default: bool = False) -> bool:
        return bool(self.values.get(key, default))

    def int(self, key: str, default: int) -> int:
        return int(self.values.get(key, default))


@dataclass(frozen=True)
class RealtimeConfig:
    enabled: bool
    signal_eval_interval_seconds: int
    full_rest_refresh_interval_seconds: int
    hot_candidate_oi_ratio: Decimal
    hot_candidate_ttl_minutes: int
    hot_candidate_max_symbols_per_exchange: int
    hot_candle_refresh_interval_seconds: int
    avg24h_candle_refresh_interval_seconds: int
    avg24h_candle_batch_size: int
    circuit_breaker_failure_threshold: int
    circuit_breaker_cooldown_seconds: int
    degraded_max_concurrency: int
    degraded_oi_batch_size: int
    normal_request_delay_ms: int
    degraded_request_delay_ms: int
    websocket: WebSocketConfig
    binance: RealtimeExchangeConfig
    bybit: RealtimeExchangeConfig
    okx: RealtimeExchangeConfig


@dataclass(frozen=True)
class ExecutionConfig:
    mode: ExecutionMode
    exchange: ExecutionExchange


@dataclass(frozen=True)
class GridConfig:
    default_investment_usdt: Decimal
    default_leverage: int
    default_grid_count: int
    default_range_pct: Decimal
    long_lower_weight: Decimal
    long_upper_weight: Decimal


@dataclass(frozen=True)
class HistoryConfig:
    hot_buffer_hours: int
    market_snapshot_days: int
    oi_history_days: int
    candle_1m_days: int
    candle_5m_days: int
    funding_history_days: int
    signal_history_days: int
    cleanup_interval_hours: int
    backfill_top_symbols: int
    backfill_batch_size: int


@dataclass(frozen=True)
class ScreenerConfig:
    app: AppConfig
    telegram: TelegramConfig
    coinalyze: CoinalyzeConfig
    signal: SignalSettings
    realtime: RealtimeConfig
    execution: ExecutionConfig
    grid: GridConfig
    history: HistoryConfig
    raw: dict[str, Any]


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> ScreenerConfig:
    config_path = Path(path)
    raw = _load_mapping(config_path)

    app_raw = raw.get("app", {})
    app = AppConfig(
        name=str(app_raw.get("name", "telegram-oi-volume-screener")),
        log_level=str(os.getenv("LOG_LEVEL", app_raw.get("log_level", "INFO"))),
        sqlite_path=Path(os.getenv("SQLITE_PATH", app_raw.get("sqlite_path", "data/screener.sqlite3"))),
    )

    telegram_raw = raw.get("telegram", {})
    telegram = TelegramConfig(
        enabled=bool(telegram_raw.get("enabled", True)),
        token_env=str(telegram_raw.get("token_env", "TELEGRAM_BOT_TOKEN")),
        allowed_user_ids_env=str(
            telegram_raw.get("allowed_user_ids_env", "TELEGRAM_ALLOWED_USER_IDS")
        ),
    )

    coinalyze_raw = raw.get("coinalyze", {})
    coinalyze = CoinalyzeConfig(
        enabled=bool(coinalyze_raw.get("enabled", True)),
        api_key_env=str(coinalyze_raw.get("api_key_env", "COINALYZE_API_KEY")),
        max_symbol_calls_per_minute=int(coinalyze_raw.get("max_symbol_calls_per_minute", 30)),
    )

    exchanges_raw = raw.get("exchanges", {})
    enabled_exchanges = tuple(
        exchange
        for exchange in ExchangeName
        if bool(exchanges_raw.get(exchange.value, {}).get("enabled", False))
    )
    if not enabled_exchanges:
        raise ValueError("At least one exchange must be enabled")

    signal_raw = raw.get("signal", {})
    profiles_raw = raw.get("profiles", {})
    profile = str(signal_raw.get("profile", "normal")).lower()
    if profile not in profiles_raw:
        raise ValueError(f"Unknown signal profile: {profile}")

    thresholds_raw = profiles_raw[profile]
    thresholds = SignalThresholds(
        min_oi_change_pct=_decimal(thresholds_raw["min_oi_change_pct"]),
        min_oi_value_change_usdt=_decimal(thresholds_raw["min_oi_value_change_usdt"]),
        min_24h_volume_usdt=_decimal(thresholds_raw["min_24h_volume_usdt"]),
        min_volume_spike_ratio=_decimal(thresholds_raw["min_volume_spike_ratio"]),
        min_price_change_pct=_decimal(
            thresholds_raw.get("min_price_change_pct", thresholds_raw.get("max_price_change_pct", "0"))
        ),
        min_volatility_pct=_decimal(thresholds_raw["min_volatility_pct"]),
        max_volatility_pct=_decimal(thresholds_raw["max_volatility_pct"]),
        max_funding_rate_pct=_decimal(thresholds_raw["max_funding_rate_pct"]),
        min_score_to_alert=int(thresholds_raw["min_score_to_alert"]),
        cooldown_minutes=int(thresholds_raw["cooldown_minutes"]),
    )
    signal = SignalSettings(
        profile=profile,
        direction=Direction(str(signal_raw.get("direction", "long"))),
        aggregation_mode=AggregationMode(str(signal_raw.get("aggregation_mode", "primary_confirmed"))),
        primary_exchange=ExchangeName(str(signal_raw.get("primary_exchange", "binance"))),
        enabled_exchanges=enabled_exchanges,
        thresholds=thresholds,
        oi_period_minutes=int(signal_raw.get("oi_period_minutes", 20)),
        volume_spike_period_minutes=int(signal_raw.get("volume_spike_period_minutes", 15)),
        volume_baseline_period_minutes=int(signal_raw.get("volume_baseline_period_minutes", 120)),
        price_change_period_minutes=int(signal_raw.get("price_change_period_minutes", 15)),
        volatility_period_minutes=int(signal_raw.get("volatility_period_minutes", 30)),
        volatility_display_mode=str(signal_raw.get("volatility_display_mode", "market")),
        whitelist_symbols=_symbols(signal_raw.get("whitelist_symbols", [])),
        blacklist_symbols=_symbols(signal_raw.get("blacklist_symbols", [])),
        max_symbols_per_refresh=int(signal_raw.get("max_symbols_per_refresh", 40)),
        oi_scan_max_symbols=int(signal_raw.get("oi_scan_max_symbols", 250)),
        oi_scan_min_24h_volume_usdt=_decimal(
            signal_raw.get("oi_scan_min_24h_volume_usdt", "1000000")
        ),
        min_secondary_oi_change_pct=_decimal(signal_raw.get("min_secondary_oi_change_pct", 3)),
        min_secondary_volume_spike_ratio=_decimal(
            signal_raw.get("min_secondary_volume_spike_ratio", "1.2")
        ),
        max_price_divergence_pct=_decimal(signal_raw.get("max_price_divergence_pct", "1.5")),
        min_confirming_exchanges=int(signal_raw.get("min_confirming_exchanges", 1)),
        enabled_filters=tuple(signal_raw.get("enabled_filters", SIGNAL_FILTER_NAMES)),
    )
    if signal.primary_exchange not in signal.enabled_exchanges:
        raise ValueError("primary_exchange must be enabled")

    realtime_raw = raw.get("realtime", {})
    ws_raw = realtime_raw.get("websocket", {})
    realtime = RealtimeConfig(
        enabled=bool(realtime_raw.get("enabled", True)),
        signal_eval_interval_seconds=int(realtime_raw.get("signal_eval_interval_seconds", 10)),
        full_rest_refresh_interval_seconds=int(
            realtime_raw.get("full_rest_refresh_interval_seconds", 300)
        ),
        hot_candidate_oi_ratio=_decimal(realtime_raw.get("hot_candidate_oi_ratio", "0.7")),
        hot_candidate_ttl_minutes=int(realtime_raw.get("hot_candidate_ttl_minutes", 15)),
        hot_candidate_max_symbols_per_exchange=int(
            realtime_raw.get("hot_candidate_max_symbols_per_exchange", 30)
        ),
        hot_candle_refresh_interval_seconds=int(
            realtime_raw.get("hot_candle_refresh_interval_seconds", 60)
        ),
        avg24h_candle_refresh_interval_seconds=int(
            realtime_raw.get("avg24h_candle_refresh_interval_seconds", 60)
        ),
        avg24h_candle_batch_size=int(realtime_raw.get("avg24h_candle_batch_size", 5)),
        circuit_breaker_failure_threshold=int(realtime_raw.get("circuit_breaker_failure_threshold", 5)),
        circuit_breaker_cooldown_seconds=int(realtime_raw.get("circuit_breaker_cooldown_seconds", 300)),
        degraded_max_concurrency=int(realtime_raw.get("degraded_max_concurrency", 2)),
        degraded_oi_batch_size=int(realtime_raw.get("degraded_oi_batch_size", 5)),
        normal_request_delay_ms=int(realtime_raw.get("normal_request_delay_ms", 100)),
        degraded_request_delay_ms=int(realtime_raw.get("degraded_request_delay_ms", 500)),
        websocket=WebSocketConfig(
            enabled=bool(ws_raw.get("enabled", True)),
            reconnect_backoff_seconds=tuple(int(v) for v in ws_raw.get("reconnect_backoff_seconds", [1, 2, 5, 10, 30])),
            heartbeat_timeout_seconds=int(ws_raw.get("heartbeat_timeout_seconds", 30)),
        ),
        binance=RealtimeExchangeConfig(dict(realtime_raw.get("binance", {}))),
        bybit=RealtimeExchangeConfig(dict(realtime_raw.get("bybit", {}))),
        okx=RealtimeExchangeConfig(dict(realtime_raw.get("okx", {}))),
    )

    execution_raw = raw.get("execution", {})
    execution = ExecutionConfig(
        mode=ExecutionMode(str(execution_raw.get("mode", "manual"))),
        exchange=ExecutionExchange(str(execution_raw.get("exchange", "none"))),
    )

    grid_raw = raw.get("grid", {})
    grid = GridConfig(
        default_investment_usdt=_decimal(grid_raw.get("default_investment_usdt", "100")),
        default_leverage=int(grid_raw.get("default_leverage", 3)),
        default_grid_count=int(grid_raw.get("default_grid_count", 30)),
        default_range_pct=_decimal(grid_raw.get("default_range_pct", "6")),
        long_lower_weight=_decimal(grid_raw.get("long_lower_weight", "0.7")),
        long_upper_weight=_decimal(grid_raw.get("long_upper_weight", "0.3")),
    )

    history_raw = raw.get("history", {})
    history = HistoryConfig(
        hot_buffer_hours=int(history_raw.get("hot_buffer_hours", 4)),
        market_snapshot_days=int(history_raw.get("market_snapshot_days", 7)),
        oi_history_days=int(history_raw.get("oi_history_days", 14)),
        candle_1m_days=int(history_raw.get("candle_1m_days", 3)),
        candle_5m_days=int(history_raw.get("candle_5m_days", 30)),
        funding_history_days=int(history_raw.get("funding_history_days", 30)),
        signal_history_days=int(history_raw.get("signal_history_days", 90)),
        cleanup_interval_hours=int(history_raw.get("cleanup_interval_hours", 6)),
        backfill_top_symbols=int(history_raw.get("backfill_top_symbols", 40)),
        backfill_batch_size=int(history_raw.get("backfill_batch_size", 8)),
    )

    return ScreenerConfig(
        app=app,
        telegram=telegram,
        coinalyze=coinalyze,
        signal=signal,
        realtime=realtime,
        execution=execution,
        grid=grid,
        history=history,
        raw=raw,
    )


def _load_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".json" or text.lstrip().startswith("{"):
        return json.loads(text)
    try:
        import yaml
    except ModuleNotFoundError as exc:
        loaded = _parse_simple_yaml(text)
        if not isinstance(loaded, dict):
            raise ValueError("Config root must be a mapping") from exc
        return loaded
    loaded = yaml.safe_load(text)
    if not isinstance(loaded, dict):
        raise ValueError("Config root must be a mapping")
    return loaded


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _symbols(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    if isinstance(value, str):
        return (value.upper(),)
    return tuple(str(symbol).upper() for symbol in value)


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    """Parse the small YAML subset used by the bundled config.

    This fallback keeps `validate-config` usable before optional dependencies are
    installed. It supports nested mappings and inline lists/scalars.
    """

    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()
        if stripped.startswith("- "):
            raise RuntimeError("Fallback YAML parser only supports inline lists")
        key, separator, raw_value = stripped.partition(":")
        if not separator:
            raise RuntimeError(f"Invalid config line: {raw_line}")
        key = key.strip()
        raw_value = raw_value.strip()
        while indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if raw_value == "":
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_simple_scalar(raw_value)
    return root


def _parse_simple_scalar(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "~"}:
        return None
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_simple_scalar(item.strip()) for item in inner.split(",")]
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    return value
