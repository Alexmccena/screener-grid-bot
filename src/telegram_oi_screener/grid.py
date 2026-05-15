from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from .config import GridConfig
from .models import Direction, ExecutionExchange, GridDryRun
from .normalizer import okx_exchange_symbol


def calculate_grid_dry_run(
    symbol: str,
    exchange: ExecutionExchange,
    current_price: Decimal,
    config: GridConfig,
) -> GridDryRun:
    if exchange not in {ExecutionExchange.BYBIT, ExecutionExchange.OKX}:
        raise ValueError("grid dry-run supports bybit or okx")
    range_pct = config.default_range_pct
    lower_delta = range_pct * config.long_lower_weight / Decimal("100")
    upper_delta = range_pct * config.long_upper_weight / Decimal("100")
    lower = _q_price(current_price * (Decimal("1") - lower_delta))
    upper = _q_price(current_price * (Decimal("1") + upper_delta))
    dry_symbol = okx_exchange_symbol(symbol) if exchange is ExecutionExchange.OKX else symbol.upper()
    return GridDryRun(
        exchange=exchange,
        symbol=dry_symbol,
        direction=Direction.LONG,
        current_price=_q_price(current_price),
        lower_price=lower,
        upper_price=upper,
        investment_usdt=config.default_investment_usdt,
        leverage=config.default_leverage,
        grid_count=config.default_grid_count,
    )


def _q_price(value: Decimal) -> Decimal:
    if value >= Decimal("100"):
        quantum = Decimal("0.01")
    elif value >= Decimal("1"):
        quantum = Decimal("0.0001")
    else:
        quantum = Decimal("0.000001")
    return value.quantize(quantum, rounding=ROUND_HALF_UP)
