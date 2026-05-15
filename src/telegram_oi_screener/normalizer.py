from __future__ import annotations

from decimal import Decimal

from .models import ExchangeName, Instrument


def normalize_symbol(exchange: ExchangeName | str, symbol: str) -> str:
    exchange_name = ExchangeName(str(exchange))
    if exchange_name is ExchangeName.OKX:
        return normalize_okx_symbol(symbol)
    return normalize_linear_symbol(symbol)


def normalize_linear_symbol(symbol: str) -> str:
    return symbol.replace("-", "").replace("_", "").upper()


def normalize_okx_symbol(symbol: str) -> str:
    cleaned = symbol.upper()
    if cleaned.endswith("-SWAP"):
        cleaned = cleaned.removesuffix("-SWAP")
    return cleaned.replace("-", "")


def okx_exchange_symbol(symbol: str) -> str:
    normalized = normalize_linear_symbol(symbol)
    if normalized.endswith("USDT"):
        base = normalized.removesuffix("USDT")
        return f"{base}-USDT-SWAP"
    return normalized


def estimate_oi_value_usdt(
    open_interest: Decimal | None,
    price: Decimal | None,
    instrument: Instrument | None = None,
) -> Decimal | None:
    if open_interest is None or price is None:
        return None
    multiplier = instrument.contract_multiplier if instrument else Decimal("1")
    return open_interest * price * multiplier
