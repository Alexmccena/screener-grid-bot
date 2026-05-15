from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any

from .config import ScreenerConfig
from .models import AggregationMode, ExchangeName, SIGNAL_FILTER_NAMES, SignalSettings, SignalThresholds


FILTER_LABELS = {
    "oi_change_pct": "OI",
    "oi_value_change_usdt": "OI",
    "oi_bullish_value_usdt": "Bullish OI",
    "oi_bearish_value_usdt": "Bearish OI",
    "price_change_pct": "Price change",
    "volatility_pct": "Volatility",
    "volume_spike_ratio": "Volume spike",
    "volume_24h_usdt": "24h volume",
    "funding_rate_pct": "Funding",
    "min_score": "Min score",
}


NUMERIC_FIELDS = {
    "min_oi_value_change_usdt": ("Min OI value, USDT", Decimal("0"), None),
    "min_24h_volume_usdt": ("Min 24h volume, USDT", Decimal("0"), None),
    "min_volume_spike_ratio": ("Volume spike, x", Decimal("0"), None),
    "max_funding_rate_pct": ("Max funding, %", Decimal("-10"), Decimal("10")),
    "min_score_to_alert": ("Min score, 0-100", 0, 100),
    "cooldown_minutes": ("Cooldown, minutes", 0, 1440),
}


def default_user_settings(config: ScreenerConfig) -> dict[str, Any]:
    thresholds = config.signal.thresholds
    return {
        "profile": config.signal.profile,
        "oi_period_minutes": config.signal.oi_period_minutes,
        "min_oi_change_pct": str(thresholds.min_oi_change_pct),
        "min_oi_value_change_usdt": str(thresholds.min_oi_value_change_usdt),
        "oi_bias_enabled": config.signal.oi_bias_enabled,
        "min_24h_volume_usdt": str(thresholds.min_24h_volume_usdt),
        "min_volume_spike_ratio": str(thresholds.min_volume_spike_ratio),
        "price_change_period_minutes": config.signal.price_change_period_minutes,
        "min_price_change_pct": str(thresholds.min_price_change_pct),
        "volatility_period_minutes": config.signal.volatility_period_minutes,
        "volatility_display_mode": config.signal.volatility_display_mode,
        "min_volatility_pct": str(thresholds.min_volatility_pct),
        "max_volatility_pct": str(thresholds.max_volatility_pct),
        "max_funding_rate_pct": str(thresholds.max_funding_rate_pct),
        "min_score_to_alert": thresholds.min_score_to_alert,
        "cooldown_minutes": thresholds.cooldown_minutes,
        "enabled_filters": list(config.signal.enabled_filters),
        "enabled_exchanges": [exchange.value for exchange in config.signal.enabled_exchanges],
        "native_grid_only_exchanges": [],
    }


def effective_settings(config: ScreenerConfig, raw: dict[str, Any] | None) -> SignalSettings:
    data = default_user_settings(config)
    if raw:
        data.update(raw)
    thresholds = SignalThresholds(
        min_oi_change_pct=_decimal(data["min_oi_change_pct"]),
        min_oi_value_change_usdt=_decimal(data["min_oi_value_change_usdt"]),
        min_24h_volume_usdt=_decimal(data["min_24h_volume_usdt"]),
        min_volume_spike_ratio=_decimal(data["min_volume_spike_ratio"]),
        min_price_change_pct=_decimal(data.get("min_price_change_pct", data.get("max_price_change_pct", "0"))),
        min_volatility_pct=_decimal(data["min_volatility_pct"]),
        max_volatility_pct=_decimal(data["max_volatility_pct"]),
        max_funding_rate_pct=_decimal(data["max_funding_rate_pct"]),
        min_score_to_alert=int(data["min_score_to_alert"]),
        cooldown_minutes=int(data["cooldown_minutes"]),
    )
    enabled = _normalize_enabled_filters(
        item for item in data.get("enabled_filters", SIGNAL_FILTER_NAMES) if item in SIGNAL_FILTER_NAMES
    )
    allowed_exchange_values = {exchange.value for exchange in config.signal.enabled_exchanges}
    exchanges = tuple(
        ExchangeName(str(item))
        for item in data.get("enabled_exchanges", [exchange.value for exchange in config.signal.enabled_exchanges])
        if str(item) in allowed_exchange_values
    )
    native_grid_only = tuple(
        ExchangeName(str(item))
        for item in data.get("native_grid_only_exchanges", [])
        if str(item) in allowed_exchange_values
    )
    return replace(
        config.signal,
        profile=str(data["profile"]),
        aggregation_mode=AggregationMode.ANY_SELECTED,
        thresholds=thresholds,
        oi_period_minutes=int(data["oi_period_minutes"]),
        oi_bias_enabled=_bool(data.get("oi_bias_enabled", False)),
        price_change_period_minutes=int(data["price_change_period_minutes"]),
        volatility_period_minutes=int(data["volatility_period_minutes"]),
        volatility_display_mode=str(data.get("volatility_display_mode", "market")),
        enabled_filters=enabled or ("oi_change_pct",),
        enabled_exchanges=exchanges or tuple(config.signal.enabled_exchanges) or (ExchangeName.BINANCE,),
        primary_exchange=exchanges[0] if exchanges else config.signal.primary_exchange,
        native_grid_only_exchanges=native_grid_only,
    )


def merge_user_settings(
    config: ScreenerConfig,
    current: dict[str, Any] | None,
    updates: dict[str, Any],
) -> dict[str, Any]:
    data = default_user_settings(config)
    if current:
        data.update(current)
    data.update(updates)
    return data


def apply_profile(config: ScreenerConfig, profile: str, current: dict[str, Any] | None) -> dict[str, Any]:
    if profile not in config.raw.get("profiles", {}):
        raise ValueError(f"Unknown profile: {profile}")
    data = default_user_settings(config)
    if current:
        enabled = current.get("enabled_filters", data["enabled_filters"])
        data["enabled_filters"] = enabled
        exchanges = current.get("enabled_exchanges", data["enabled_exchanges"])
        data["enabled_exchanges"] = exchanges
        grid_only = current.get("native_grid_only_exchanges", data["native_grid_only_exchanges"])
        data["native_grid_only_exchanges"] = grid_only
    data["profile"] = profile
    profile_raw = config.raw["profiles"][profile]
    data.update(
        {
            "min_oi_change_pct": str(profile_raw["min_oi_change_pct"]),
            "min_oi_value_change_usdt": str(profile_raw["min_oi_value_change_usdt"]),
            "min_24h_volume_usdt": str(profile_raw["min_24h_volume_usdt"]),
            "min_volume_spike_ratio": str(profile_raw["min_volume_spike_ratio"]),
            "price_change_period_minutes": current.get("price_change_period_minutes", data["price_change_period_minutes"])
            if current
            else data["price_change_period_minutes"],
            "min_price_change_pct": str(
                profile_raw.get("min_price_change_pct", profile_raw.get("max_price_change_pct", "0"))
            ),
            "volatility_period_minutes": current.get("volatility_period_minutes", data["volatility_period_minutes"])
            if current
            else data["volatility_period_minutes"],
            "volatility_display_mode": current.get(
                "volatility_display_mode", data["volatility_display_mode"]
            )
            if current
            else data["volatility_display_mode"],
            "min_volatility_pct": str(profile_raw["min_volatility_pct"]),
            "max_volatility_pct": str(profile_raw["max_volatility_pct"]),
            "max_funding_rate_pct": str(profile_raw["max_funding_rate_pct"]),
            "min_score_to_alert": int(profile_raw["min_score_to_alert"]),
            "cooldown_minutes": int(profile_raw["cooldown_minutes"]),
        }
    )
    return data


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "да"}


def _normalize_enabled_filters(items: Any) -> tuple[str, ...]:
    enabled = set(items)
    if "oi_change_pct" in enabled or "oi_value_change_usdt" in enabled:
        enabled.update({"oi_change_pct", "oi_value_change_usdt"})
        enabled.discard("oi_bullish_value_usdt")
        enabled.discard("oi_bearish_value_usdt")
    ordered = tuple(item for item in SIGNAL_FILTER_NAMES if item in enabled)
    return ordered
