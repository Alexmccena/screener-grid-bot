from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from .models import ExchangeName, MarketSnapshot, ZERO


@dataclass(frozen=True)
class BufferPoint:
    timestamp: datetime
    price: Decimal | None
    open_interest: Decimal | None
    open_interest_value_usdt: Decimal | None
    volume_usdt: Decimal | None


class RollingBuffer:
    def __init__(self, max_age_minutes: int = 240) -> None:
        self.max_age = timedelta(minutes=max_age_minutes)
        self._points: dict[tuple[ExchangeName, str], deque[BufferPoint]] = defaultdict(deque)

    def add_snapshot(self, snapshot: MarketSnapshot) -> None:
        point = BufferPoint(
            timestamp=snapshot.timestamp,
            price=snapshot.price,
            open_interest=snapshot.open_interest,
            open_interest_value_usdt=snapshot.open_interest_value_usdt,
            volume_usdt=snapshot.recent_volume_usdt,
        )
        key = (snapshot.exchange, snapshot.symbol)
        points = self._points[key]
        points.append(point)
        if len(points) > 1 and points[-2].timestamp > point.timestamp:
            self._points[key] = deque(sorted(points, key=lambda item: item.timestamp))
        self._trim(key, snapshot.timestamp)

    def get_point_ago(
        self,
        exchange: ExchangeName,
        symbol: str,
        minutes: int,
        now: datetime,
    ) -> BufferPoint | None:
        points = self._points.get((exchange, symbol))
        if not points:
            return None
        target = now - timedelta(minutes=minutes)
        timestamp: datetime | None = None
        price: Decimal | None = None
        open_interest: Decimal | None = None
        open_interest_value_usdt: Decimal | None = None
        volume_usdt: Decimal | None = None
        for point in points:
            if point.timestamp <= target:
                timestamp = point.timestamp
                if point.price is not None:
                    price = point.price
                if point.open_interest is not None:
                    open_interest = point.open_interest
                if point.open_interest_value_usdt is not None:
                    open_interest_value_usdt = point.open_interest_value_usdt
                if point.volume_usdt is not None:
                    volume_usdt = point.volume_usdt
            else:
                break
        if timestamp is None:
            return None
        return BufferPoint(
            timestamp=timestamp,
            price=price,
            open_interest=open_interest,
            open_interest_value_usdt=open_interest_value_usdt,
            volume_usdt=volume_usdt,
        )

    def points_between(
        self,
        exchange: ExchangeName,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[BufferPoint, ...]:
        points = self._points.get((exchange, symbol))
        if not points:
            return ()
        return tuple(point for point in points if start <= point.timestamp <= end)

    def sum_volume(
        self,
        exchange: ExchangeName,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> Decimal:
        points = self._points.get((exchange, symbol))
        if not points:
            return ZERO
        return sum(
            (
                point.volume_usdt
                for point in points
                if point.volume_usdt is not None and start <= point.timestamp < end
            ),
            ZERO,
        )

    def min_max_price(
        self,
        exchange: ExchangeName,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> tuple[Decimal | None, Decimal | None]:
        points = self._points.get((exchange, symbol))
        if not points:
            return None, None
        prices = [
            point.price
            for point in points
            if point.price is not None and start <= point.timestamp <= end
        ]
        if not prices:
            return None, None
        return min(prices), max(prices)

    def latest_symbols(self) -> set[str]:
        return {symbol for _, symbol in self._points}

    def _trim(self, key: tuple[ExchangeName, str], now: datetime) -> None:
        cutoff = now - self.max_age
        points = self._points[key]
        while points and points[0].timestamp < cutoff:
            points.popleft()
