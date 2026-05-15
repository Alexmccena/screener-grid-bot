from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from decimal import Decimal
from typing import Any

from ..models import ExchangeName, MarketSnapshot, utc_now
from ..normalizer import normalize_okx_symbol, normalize_symbol

LOGGER = logging.getLogger(__name__)

SnapshotCallback = Callable[[MarketSnapshot], Awaitable[None]]


class WebSocketUnavailable(RuntimeError):
    pass


class BaseWebSocketManager:
    def __init__(
        self,
        reconnect_backoff_seconds: Iterable[int],
        callback: SnapshotCallback,
    ) -> None:
        self.reconnect_backoff_seconds = tuple(reconnect_backoff_seconds)
        self.callback = callback

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        backoffs = self.reconnect_backoff_seconds or (1, 2, 5, 10, 30)
        index = 0
        while not stop_event.is_set():
            try:
                await self._run_once(stop_event)
                index = 0
            except WebSocketUnavailable:
                LOGGER.info("websockets package is not installed; websocket manager disabled")
                return
            except Exception:
                LOGGER.exception("websocket manager failed; reconnecting")
                await asyncio.sleep(backoffs[min(index, len(backoffs) - 1)])
                index += 1

    async def _run_once(self, stop_event: asyncio.Event) -> None:
        raise NotImplementedError


class BinanceTickerWebSocket(BaseWebSocketManager):
    url = "wss://fstream.binance.com/ws/!ticker@arr"

    async def _run_once(self, stop_event: asyncio.Event) -> None:
        websockets = _import_websockets()
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as websocket:
            while not stop_event.is_set():
                payload = json.loads(await websocket.recv())
                for item in payload:
                    exchange_symbol = str(item.get("s", ""))
                    if not exchange_symbol.endswith("USDT"):
                        continue
                    await self.callback(
                        MarketSnapshot(
                            exchange=ExchangeName.BINANCE,
                            symbol=normalize_symbol(ExchangeName.BINANCE, exchange_symbol),
                            exchange_symbol=exchange_symbol,
                            timestamp=utc_now(),
                            price=_decimal(item.get("c")),
                            volume_24h_usdt=_decimal(item.get("q")),
                            price_change_24h_pct=_decimal(item.get("P")),
                            source="ws:ticker",
                            raw=item,
                        )
                    )


class BybitTickerWebSocket(BaseWebSocketManager):
    url = "wss://stream.bybit.com/v5/public/linear"

    def __init__(
        self,
        symbols: Iterable[str],
        reconnect_backoff_seconds: Iterable[int],
        callback: SnapshotCallback,
    ) -> None:
        super().__init__(reconnect_backoff_seconds, callback)
        self.symbols = tuple(symbol.upper() for symbol in symbols)

    async def _run_once(self, stop_event: asyncio.Event) -> None:
        websockets = _import_websockets()
        args = [f"tickers.{symbol}" for symbol in self.symbols]
        if not args:
            return
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as websocket:
            await websocket.send(json.dumps({"op": "subscribe", "args": args}))
            while not stop_event.is_set():
                payload = json.loads(await websocket.recv())
                data = payload.get("data")
                if not isinstance(data, dict) or "symbol" not in data:
                    continue
                exchange_symbol = str(data["symbol"])
                price = _decimal(data.get("lastPrice"))
                oi = _decimal(data.get("openInterest"))
                await self.callback(
                    MarketSnapshot(
                        exchange=ExchangeName.BYBIT,
                        symbol=normalize_symbol(ExchangeName.BYBIT, exchange_symbol),
                        exchange_symbol=exchange_symbol,
                        timestamp=utc_now(),
                        price=price,
                        open_interest=oi,
                        open_interest_value_usdt=_decimal(data.get("openInterestValue")),
                        volume_24h_usdt=_decimal(data.get("turnover24h")),
                        funding_rate_pct=_funding_pct(data.get("fundingRate")),
                        source="ws:ticker",
                        raw=data,
                    )
                )


class OKXPublicWebSocket(BaseWebSocketManager):
    url = "wss://ws.okx.com:8443/ws/v5/public"

    def __init__(
        self,
        symbols: Iterable[str],
        reconnect_backoff_seconds: Iterable[int],
        callback: SnapshotCallback,
    ) -> None:
        super().__init__(reconnect_backoff_seconds, callback)
        self.symbols = tuple(symbol.upper() for symbol in symbols)

    async def _run_once(self, stop_event: asyncio.Event) -> None:
        websockets = _import_websockets()
        args = []
        for symbol in self.symbols:
            if symbol.endswith("USDT"):
                inst_id = f"{symbol.removesuffix('USDT')}-USDT-SWAP"
            else:
                inst_id = symbol
            args.append({"channel": "tickers", "instId": inst_id})
            args.append({"channel": "open-interest", "instId": inst_id})
        if not args:
            return
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as websocket:
            await websocket.send(json.dumps({"op": "subscribe", "args": args}))
            while not stop_event.is_set():
                payload = json.loads(await websocket.recv())
                arg = payload.get("arg", {})
                channel = arg.get("channel")
                for data in payload.get("data", []):
                    exchange_symbol = str(data.get("instId", arg.get("instId", "")))
                    if not exchange_symbol:
                        continue
                    snapshot = _okx_snapshot(channel, exchange_symbol, data)
                    await self.callback(snapshot)


def _okx_snapshot(channel: str, exchange_symbol: str, data: dict[str, Any]) -> MarketSnapshot:
    price = _decimal(data.get("last"))
    oi = _decimal(data.get("oi"))
    volume_usdt = _decimal(data.get("volCcyQuote24h"))
    return MarketSnapshot(
        exchange=ExchangeName.OKX,
        symbol=normalize_okx_symbol(exchange_symbol),
        exchange_symbol=exchange_symbol,
        timestamp=utc_now(),
        price=price,
        open_interest=oi,
        volume_24h_usdt=volume_usdt,
        source=f"ws:{channel}",
        raw=data,
    )


def _import_websockets():
    try:
        import websockets
    except ModuleNotFoundError as exc:
        raise WebSocketUnavailable from exc
    return websockets


def _decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return Decimal(text)


def _funding_pct(value: object) -> Decimal | None:
    raw = _decimal(value)
    if raw is None:
        return None
    return raw * Decimal("100")
