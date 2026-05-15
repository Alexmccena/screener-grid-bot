from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .exchanges.base import ExchangeApiError
from .models import ExchangeName

LOGGER = logging.getLogger(__name__)


@dataclass
class CircuitState:
    failures: int = 0
    blocked_until: datetime | None = None
    last_error: str | None = None


class ExchangeCircuitBreaker:
    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        cooldown_seconds: int = 300,
    ) -> None:
        self.failure_threshold = max(1, failure_threshold)
        self.cooldown_seconds = max(1, cooldown_seconds)
        self._states: dict[ExchangeName, CircuitState] = {}

    def allow_request(self, exchange: ExchangeName, now: datetime | None = None) -> bool:
        now = now or datetime.now(UTC)
        state = self._states.get(exchange)
        if state is None or state.blocked_until is None:
            return True
        if state.blocked_until <= now:
            state.blocked_until = None
            state.failures = max(0, self.failure_threshold - 1)
            LOGGER.info("circuit breaker half-open for %s", exchange.value)
            return True
        return False

    def blocked_for(self, exchange: ExchangeName, now: datetime | None = None) -> int:
        now = now or datetime.now(UTC)
        state = self._states.get(exchange)
        if state is None or state.blocked_until is None:
            return 0
        return max(0, int((state.blocked_until - now).total_seconds()))

    def record_success(self, exchange: ExchangeName) -> None:
        state = self._states.get(exchange)
        if state is None:
            return
        if state.failures or state.blocked_until is not None:
            LOGGER.info("circuit breaker recovered for %s", exchange.value)
        state.failures = 0
        state.blocked_until = None
        state.last_error = None

    def record_failure(self, exchange: ExchangeName, error: BaseException | str) -> None:
        state = self._states.setdefault(exchange, CircuitState())
        state.failures += _failure_weight(error)
        state.last_error = str(error)
        if state.failures < self.failure_threshold:
            return
        now = datetime.now(UTC)
        if state.blocked_until is not None and state.blocked_until > now:
            return
        blocked_until = datetime.now(UTC) + timedelta(seconds=self.cooldown_seconds)
        state.blocked_until = blocked_until
        LOGGER.warning(
            "circuit breaker paused %s for %ss after %s failures; last_error=%s",
            exchange.value,
            self.cooldown_seconds,
            state.failures,
            state.last_error,
        )

    def failure_count(self, exchange: ExchangeName) -> int:
        return self._states.get(exchange, CircuitState()).failures


def _failure_weight(error: BaseException | str) -> int:
    if isinstance(error, ExchangeApiError) and error.status in {403, 418, 429}:
        return 3
    return 1
