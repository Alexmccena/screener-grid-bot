from __future__ import annotations

from decimal import Decimal
from html import escape
from typing import Iterable
from urllib.parse import quote

from ..models import AggregatedSignal, ExchangeName, ExchangeSignalEvaluation, FilterResult, GridDryRun, SIGNAL_FILTER_NAMES

PCT_4_FILTERS = {"funding_rate_pct"}
PCT_2_FILTERS = {"volatility_pct", "price_change_pct", "oi_change_pct"}
RATIO_2_FILTERS = {"volume_spike_ratio"}
MONEY_M_FILTERS = {
    "volume_24h_usdt",
    "oi_value_change_usdt",
    "oi_bullish_value_usdt",
    "oi_bearish_value_usdt",
}
FILTER_LABELS = {
    "oi_change_pct": "OI",
    "oi_value_change_usdt": "OI USD",
    "oi_bullish_value_usdt": "Bullish OI",
    "oi_bearish_value_usdt": "Bearish OI",
    "price_change_pct": "Price change",
    "volatility_pct": "Volatility",
    "volume_spike_ratio": "Volume spike",
    "volume_24h_usdt": "24h volume",
    "funding_rate_pct": "Funding",
    "min_score": "Min score",
}
EXCHANGE_LABELS = {
    ExchangeName.BINANCE: "BINANCE",
    ExchangeName.BYBIT: "BYBIT",
    ExchangeName.OKX: "OKX",
}
EXCHANGE_DOTS = {
    ExchangeName.BINANCE: "🟡",
    ExchangeName.BYBIT: "⚫️",
    ExchangeName.OKX: "🔵",
}
TRADINGVIEW_PREFIXES = {
    ExchangeName.BINANCE: "BINANCE",
    ExchangeName.BYBIT: "BYBIT",
    ExchangeName.OKX: "OKX",
}
COINGLASS_EXCHANGES = {
    ExchangeName.BINANCE: "Binance",
    ExchangeName.BYBIT: "Bybit",
    ExchangeName.OKX: "Okx",
}


def format_signal(
    signal: AggregatedSignal,
    enabled_filters: tuple[str, ...] | None = None,
    oi_period_minutes: int | None = None,
    price_change_period_minutes: int | None = None,
) -> str:
    enabled = set(enabled_filters or SIGNAL_FILTER_NAMES)
    lines = [
        f"<b>Кандидат LONG grid: {escape(signal.symbol)}</b>",
        f"Балл: {signal.score}",
        f"Режим: {escape(signal.aggregation_mode.value)}",
        f"Причина: {escape(signal.reason)}",
        "",
    ]
    for evaluation in signal.evaluations:
        lines.extend(_format_evaluation(evaluation, enabled, oi_period_minutes, price_change_period_minutes))
        lines.append("")
    return "\n".join(lines).strip()


def format_evaluation_debug(evaluation: ExchangeSignalEvaluation) -> str:
    return "\n".join(_format_evaluation(evaluation, set(SIGNAL_FILTER_NAMES), None, None))


def format_grid_dry_run(dry_run: GridDryRun) -> str:
    return "\n".join(
        [
            f"Проверка grid: {dry_run.exchange.value} {dry_run.symbol}",
            f"Направление: {dry_run.direction.value}",
            f"Текущая цена: {_fmt(dry_run.current_price)}",
            f"Нижняя граница: {_fmt(dry_run.lower_price)}",
            f"Верхняя граница: {_fmt(dry_run.upper_price)}",
            f"Инвестиция: {_fmt(dry_run.investment_usdt)} USDT",
            f"Плечо: {dry_run.leverage}x",
            f"Количество grid: {dry_run.grid_count}",
            "Статус: только проверка, торговое действие не создано",
        ]
    )


def _format_evaluation(
    evaluation: ExchangeSignalEvaluation,
    enabled_filters: set[str],
    oi_period_minutes: int | None,
    price_change_period_minutes: int | None,
) -> list[str]:
    status = "ПРОШЕЛ" if evaluation.passed else "НЕ ПРОШЕЛ"
    header = (
        f"{_market_links_line(evaluation, oi_period_minutes)}\n"
        f"<b>{escape(evaluation.exchange.value.upper())}: {status}</b> балл={evaluation.score}"
    )
    lines = [header]
    required = [item for item in evaluation.filters if item.name in enabled_filters]
    info = [item for item in evaluation.filters if item.name not in enabled_filters]
    if required:
        lines.append("Обязательные фильтры:")
        lines.append(_format_filter_table(required, oi_period_minutes, price_change_period_minutes))
    if info:
        lines.append("Инфо (OFF):")
        lines.append(_format_filter_table(info, oi_period_minutes, price_change_period_minutes))
    return lines


def _market_links_line(evaluation: ExchangeSignalEvaluation, oi_period_minutes: int | None) -> str:
    snapshot = evaluation.snapshot
    exchange = evaluation.exchange
    symbol = evaluation.symbol.upper()
    exchange_symbol = snapshot.exchange_symbol if snapshot is not None else symbol
    base = _base_symbol(symbol)
    period = f"{oi_period_minutes}мин" if oi_period_minutes is not None else "период"
    exchange_label = EXCHANGE_LABELS.get(exchange, exchange.value.upper())
    return (
        f"{EXCHANGE_DOTS.get(exchange, '')} "
        f"{_link(exchange_label, _exchange_trade_url(exchange, symbol, exchange_symbol))} - "
        f"{escape(period)} - "
        f"{_link(base, _coinglass_url(exchange, exchange_symbol))} - "
        f"{_link('TradingView', _tradingview_url(exchange, symbol))}"
    )


def _format_filter_table(
    items: Iterable[FilterResult],
    oi_period_minutes: int | None,
    price_change_period_minutes: int | None = None,
) -> str:
    rows = [_format_filter_row(item, oi_period_minutes, price_change_period_minutes) for item in items]
    widths = (
        max(len("Фильтр"), *(len(row[1]) for row in rows)),
        max(len("Факт"), *(len(row[2]) for row in rows)),
        max(len("Настройка"), *(len(row[3]) for row in rows)),
    )
    table_lines = [
        f"  {'Фильтр'.ljust(widths[0])}  {'Факт'.rjust(widths[1])}  {'Настройка'.rjust(widths[2])}"
    ]
    for marker, label, value, threshold in rows:
        table_lines.append(
            f"{marker} {label.ljust(widths[0])}  {value.rjust(widths[1])}  {threshold.rjust(widths[2])}"
        )
    return "<pre>" + escape("\n".join(table_lines)) + "</pre>"


def _format_filter_row(
    item: FilterResult,
    oi_period_minutes: int | None,
    price_change_period_minutes: int | None,
) -> tuple[str, str, str, str]:
    marker = "✅" if item.passed else "❌"
    label = FILTER_LABELS.get(item.name, item.name)
    value = _fmt_filter_fact(item)
    threshold = _fmt_filter_threshold(item.name, item.threshold)
    if item.name == "oi_change_pct" and oi_period_minutes is not None:
        threshold = f"{threshold} / {oi_period_minutes}m"
    if item.name == "price_change_pct" and price_change_period_minutes is not None:
        threshold = f"{threshold} / {price_change_period_minutes}m"
    return marker, label, value, threshold


def _fmt_filter_fact(item: FilterResult) -> str:
    if item.name == "oi_change_pct" and item.previous_value is not None:
        return _fmt_oi_range(item.value, item.previous_value, _add(item.previous_value, item.value, pct=True))
    if item.name == "oi_value_change_usdt" and item.previous_value is not None:
        current = _add(item.previous_value, item.value)
        pct = _pct_change(current, item.previous_value)
        return _fmt_oi_usd_range(pct, item.previous_value, current)
    if item.name == "volatility_pct":
        return _fmt_volatility_fact(item)
    return _fmt_filter_value(item.name, item.value, signed=item.name == "oi_change_pct")


def _fmt_volatility_fact(item: FilterResult) -> str:
    metadata = item.metadata or {}
    avg24h = metadata.get("avg24h_pct")
    avg_period = metadata.get("avg24h_period_minutes") or metadata.get("period_minutes")
    suffix = (
        f" | avg24h {_fmt_filter_value('volatility_pct', avg24h)}/{avg_period}m"
        if avg24h is not None and avg_period is not None
        else f" | avg24h {_fmt_filter_value('volatility_pct', avg24h)}"
        if avg24h is not None
        else ""
    )
    if metadata.get("display_mode") == "diagnostic":
        passed = metadata.get("passed_candles")
        total = metadata.get("total_candles")
        per_candle = metadata.get("max_per_candle_pct")
        if passed is not None and total is not None and per_candle is not None:
            return f"{passed}/{total} >= {_fmt_filter_value('volatility_pct', per_candle)}{suffix}"
    return f"{_fmt_filter_value(item.name, item.value)}{suffix}"


def _exchange_trade_url(exchange: ExchangeName, symbol: str, exchange_symbol: str) -> str:
    if exchange is ExchangeName.BINANCE:
        return f"https://www.binance.com/en/futures/{quote(symbol)}"
    if exchange is ExchangeName.BYBIT:
        return f"https://www.bybit.com/trade/usdt/{quote(symbol)}"
    if exchange is ExchangeName.OKX:
        return f"https://www.okx.com/trade-swap/{quote(exchange_symbol.lower())}"
    return ""


def _coinglass_url(exchange: ExchangeName, exchange_symbol: str) -> str:
    exchange_label = COINGLASS_EXCHANGES.get(exchange, exchange.value.title())
    return f"https://www.coinglass.com/tv/ru/{quote(f'{exchange_label}_{exchange_symbol}')}"


def _tradingview_url(exchange: ExchangeName, symbol: str) -> str:
    prefix = TRADINGVIEW_PREFIXES.get(exchange, exchange.value.upper())
    return f"https://www.tradingview.com/chart/?symbol={quote(f'{prefix}:{symbol}.P', safe='')}"


def _base_symbol(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4]
    return symbol


def _link(label: str, url: str) -> str:
    return f'<a href="{escape(url, quote=True)}">{escape(label)}</a>'


def _fmt_filter_value(name: str, value: object, signed: bool = False) -> str:
    if value is None:
        return "n/a"
    if name in MONEY_M_FILTERS:
        return _fmt_millions(value)
    if name in PCT_4_FILTERS:
        return f"{_q(value, '0.0001')}%"
    if name in PCT_2_FILTERS:
        formatted = _q(value, "0.01")
        if signed and Decimal(str(value)) > 0:
            formatted = f"+{formatted}"
        return f"{formatted}%"
    if name in RATIO_2_FILTERS:
        return f"{_q(value, '0.01')}x"
    return _fmt(value)


def _fmt_filter_threshold(name: str, value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, str) and value == "info":
        return value
    if name == "volatility_pct":
        return _fmt_range_pct(value)
    if name in MONEY_M_FILTERS:
        return _fmt_millions(value)
    if name in PCT_4_FILTERS:
        return f"{_q(value, '0.0001')}%"
    if name in PCT_2_FILTERS:
        return f"{_q(value, '0.01')}%"
    if name in RATIO_2_FILTERS:
        return f"{_q(value, '0.01')}x"
    return _fmt(value)


def _fmt_range_pct(value: object) -> str:
    text = str(value)
    if ".." not in text:
        return f"{_q(value, '0.01')}%"
    left, right = text.split("..", 1)
    return f"{_q(left, '0.01')}..{_q(right, '0.01')}%"


def _fmt_millions(value: object) -> str:
    if value is None:
        return "n/a"
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return f"{_q(value / Decimal('1000000'), '0.01')}M $"


def _fmt_oi_range(change_pct: object, previous: object, current: object) -> str:
    pct = _fmt_filter_value("oi_change_pct", change_pct, signed=True)
    return f"{pct} ({_fmt_compact_number(previous)}->{_fmt_compact_number(current)})"


def _fmt_oi_usd_range(change_pct: object | None, previous: object, current: object) -> str:
    pct = _fmt_filter_value("oi_change_pct", change_pct, signed=True) if change_pct is not None else "n/a"
    return f"{pct} ({_fmt_millions(previous)}->{_fmt_millions(current)})"


def _fmt_compact_number(value: object) -> str:
    if value is None:
        return "n/a"
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    abs_value = abs(value)
    if abs_value >= Decimal("1000000000"):
        return f"{_q(value / Decimal('1000000000'), '0.01')}B"
    if abs_value >= Decimal("1000000"):
        return f"{_q(value / Decimal('1000000'), '0.01')}M"
    if abs_value >= Decimal("1000"):
        return f"{_q(value / Decimal('1000'), '0.01')}K"
    return _q(value, "0.01")


def _add(previous: object, change: object, pct: bool = False) -> Decimal | None:
    if previous is None or change is None:
        return None
    previous_decimal = Decimal(str(previous))
    change_decimal = Decimal(str(change))
    if pct:
        return previous_decimal * (Decimal("1") + change_decimal / Decimal("100"))
    return previous_decimal + change_decimal


def _pct_change(current: object | None, previous: object) -> Decimal | None:
    if current is None or previous is None:
        return None
    current_decimal = Decimal(str(current))
    previous_decimal = Decimal(str(previous))
    if previous_decimal == 0:
        return None
    return (current_decimal - previous_decimal) / previous_decimal * Decimal("100")


def _q(value: object, quantum: str) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    return f"{value.quantize(Decimal(quantum)):f}"


def _fmt(value: object) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, Decimal):
        return f"{value.normalize():f}"
    return str(value)
