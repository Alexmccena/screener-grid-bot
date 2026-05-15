from telegram_oi_screener.config import load_config
from telegram_oi_screener.models import AggregationMode, ExchangeName


def test_load_default_config() -> None:
    config = load_config("config.yaml")
    assert config.signal.profile == "normal"
    assert config.signal.aggregation_mode is AggregationMode.PRIMARY_CONFIRMED
    assert config.signal.enabled_exchanges == (ExchangeName.BYBIT,)
    assert config.signal.primary_exchange is ExchangeName.BYBIT
    assert config.coinalyze.enabled is True
    assert config.coinalyze.api_key_env == "COINALYZE_API_KEY"
    assert config.coinalyze.max_symbol_calls_per_minute == 30
    assert config.execution.exchange.value == "none"
    assert config.realtime.hot_candidate_oi_ratio > 0
    assert config.realtime.hot_candidate_max_symbols_per_exchange == 30
    assert config.realtime.hot_candle_refresh_interval_seconds == 90
    assert config.realtime.avg24h_candle_refresh_interval_seconds == 60
    assert config.realtime.avg24h_candle_batch_size == 5
    assert config.realtime.circuit_breaker_failure_threshold == 5
    assert config.realtime.circuit_breaker_cooldown_seconds == 300
    assert config.realtime.degraded_max_concurrency == 2
    assert config.realtime.degraded_oi_batch_size == 5
    assert config.realtime.normal_request_delay_ms == 200
    assert config.realtime.degraded_request_delay_ms == 500
    assert config.history.market_snapshot_days == 7
    assert config.history.signal_history_days == 90
    assert config.history.cleanup_interval_hours == 6
