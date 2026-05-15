from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass
class RateLimitState:
    max_calls: int
    period_seconds: int
    penalty_until: datetime | None = None


class RateLimiter:
    def __init__(self, max_calls: int, period_seconds: int) -> None:
        self.state = RateLimitState(max_calls=max_calls, period_seconds=period_seconds)
        self._calls: deque[datetime] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = datetime.now(UTC)
            if self.state.penalty_until and now < self.state.penalty_until:
                await asyncio.sleep((self.state.penalty_until - now).total_seconds())
                now = datetime.now(UTC)

            window_start = now - timedelta(seconds=self.state.period_seconds)
            while self._calls and self._calls[0] < window_start:
                self._calls.popleft()

            if len(self._calls) >= self.state.max_calls:
                sleep_for = (self._calls[0] + timedelta(seconds=self.state.period_seconds) - now)
                await asyncio.sleep(max(sleep_for.total_seconds(), 0))
                now = datetime.now(UTC)
            self._calls.append(now)

    def penalize(self, seconds: int) -> None:
        self.state.penalty_until = datetime.now(UTC) + timedelta(seconds=seconds)
