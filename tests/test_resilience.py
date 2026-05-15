from datetime import UTC, datetime, timedelta

from telegram_oi_screener.exchanges.base import ExchangeApiError, _compact_error_body, _compact_network_error
from telegram_oi_screener.models import ExchangeName
from telegram_oi_screener.resilience import ExchangeCircuitBreaker


def test_circuit_breaker_pauses_after_weighted_403() -> None:
    breaker = ExchangeCircuitBreaker(failure_threshold=5, cooldown_seconds=60)
    now = datetime(2026, 5, 10, 12, tzinfo=UTC)

    breaker.record_failure(ExchangeName.BINANCE, ExchangeApiError(ExchangeName.BINANCE, 403, "403 Request blocked"))
    assert breaker.allow_request(ExchangeName.BINANCE, now)

    breaker.record_failure(ExchangeName.BINANCE, ExchangeApiError(ExchangeName.BINANCE, 403, "403 Request blocked"))
    assert not breaker.allow_request(ExchangeName.BINANCE, now)
    assert breaker.blocked_for(ExchangeName.BINANCE, now) > 0

    assert breaker.allow_request(ExchangeName.BINANCE, datetime.now(UTC) + timedelta(seconds=61))
    breaker.record_success(ExchangeName.BINANCE)
    assert breaker.failure_count(ExchangeName.BINANCE) == 0


def test_circuit_breaker_does_not_extend_active_cooldown() -> None:
    breaker = ExchangeCircuitBreaker(failure_threshold=2, cooldown_seconds=60)
    breaker.record_failure(ExchangeName.BINANCE, "timeout")
    breaker.record_failure(ExchangeName.BINANCE, "timeout")
    first_blocked_for = breaker.blocked_for(ExchangeName.BINANCE)

    breaker.record_failure(ExchangeName.BINANCE, "late timeout")

    assert breaker.blocked_for(ExchangeName.BINANCE) <= first_blocked_for


def test_error_messages_are_compact() -> None:
    html = """
    <HTML><HEAD><TITLE>ERROR: The request could not be satisfied</TITLE></HEAD>
    <BODY><H1>403 ERROR</H1><H2>The request could not be satisfied.</H2>
    Request blocked.</BODY></HTML>
    """

    assert _compact_error_body(403, html) == "403 Request blocked"
    assert _compact_network_error(TimeoutError("The read operation timed out")) == "read timed out"
