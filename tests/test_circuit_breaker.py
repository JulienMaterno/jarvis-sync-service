"""
Comprehensive tests for lib/circuit_breaker.py

Tests cover:
- CLOSED -> OPEN transition on failure threshold
- OPEN -> HALF_OPEN after recovery timeout
- HALF_OPEN -> CLOSED on success
- HALF_OPEN -> OPEN on failure
- Failure counting and reset
- Decorator usage (sync and async)
- Context manager usage
- Registry and status reporting
"""

import time
import asyncio
import pytest
from unittest.mock import MagicMock


class TestCircuitBreakerStateTransitions:
    """Test the core state machine of the circuit breaker."""

    def _make_breaker(self, **kwargs):
        from lib.circuit_breaker import CircuitBreaker
        # Use unique name to avoid registry collisions between tests
        import uuid
        name = f"test-{uuid.uuid4().hex[:8]}"
        defaults = {
            'name': name,
            'failure_threshold': 3,
            'recovery_timeout': 0.1,  # Short timeout for fast tests
            'success_threshold': 2,
        }
        defaults.update(kwargs)
        return CircuitBreaker(**defaults)

    def test_starts_in_closed_state(self):
        """Circuit breaker should start in CLOSED state."""
        from lib.circuit_breaker import CircuitState
        breaker = self._make_breaker()
        assert breaker.state == CircuitState.CLOSED
        assert breaker.is_closed is True
        assert breaker.is_open is False

    def test_closed_to_open_on_failure_threshold(self):
        """Circuit should open after reaching failure threshold."""
        from lib.circuit_breaker import CircuitState
        breaker = self._make_breaker(failure_threshold=3)

        # Record 3 failures
        for _ in range(3):
            breaker._record_failure(Exception("test failure"))

        assert breaker.state == CircuitState.OPEN
        assert breaker.is_open is True

    def test_failures_below_threshold_stay_closed(self):
        """Circuit should stay closed with failures below threshold."""
        from lib.circuit_breaker import CircuitState
        breaker = self._make_breaker(failure_threshold=3)

        breaker._record_failure(Exception("fail 1"))
        breaker._record_failure(Exception("fail 2"))

        assert breaker.state == CircuitState.CLOSED

    def test_open_to_half_open_after_timeout(self):
        """Circuit should transition to HALF_OPEN after recovery timeout."""
        from lib.circuit_breaker import CircuitState
        breaker = self._make_breaker(failure_threshold=1, recovery_timeout=0.05)

        # Trip the breaker
        breaker._record_failure(Exception("fail"))
        assert breaker.state == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(0.1)

        # _should_allow_request triggers the transition
        assert breaker._should_allow_request() is True
        assert breaker.state == CircuitState.HALF_OPEN

    def test_half_open_to_closed_on_success(self):
        """Circuit should close after enough successes in HALF_OPEN state."""
        from lib.circuit_breaker import CircuitState
        breaker = self._make_breaker(
            failure_threshold=1,
            recovery_timeout=0.01,
            success_threshold=2
        )

        # Trip the breaker
        breaker._record_failure(Exception("fail"))
        assert breaker.state == CircuitState.OPEN

        # Wait for recovery
        time.sleep(0.05)
        breaker._should_allow_request()  # Transition to half-open
        assert breaker.state == CircuitState.HALF_OPEN

        # Record successes
        breaker._record_success()
        assert breaker.state == CircuitState.HALF_OPEN  # Need 2 successes
        breaker._record_success()
        assert breaker.state == CircuitState.CLOSED

    def test_half_open_to_open_on_failure(self):
        """Circuit should reopen on failure in HALF_OPEN state."""
        from lib.circuit_breaker import CircuitState
        breaker = self._make_breaker(
            failure_threshold=1,
            recovery_timeout=0.01,
            success_threshold=2
        )

        # Trip the breaker
        breaker._record_failure(Exception("fail"))
        time.sleep(0.05)
        breaker._should_allow_request()
        assert breaker.state == CircuitState.HALF_OPEN

        # Fail in half-open
        breaker._record_failure(Exception("fail again"))
        assert breaker.state == CircuitState.OPEN

    def test_success_resets_failure_count_in_closed(self):
        """Success in CLOSED state should reset the failure counter."""
        breaker = self._make_breaker(failure_threshold=3)

        breaker._record_failure(Exception("fail 1"))
        breaker._record_failure(Exception("fail 2"))
        breaker._record_success()  # Resets failure count

        # Need 3 more failures to trip
        breaker._record_failure(Exception("fail 3"))
        breaker._record_failure(Exception("fail 4"))
        from lib.circuit_breaker import CircuitState
        assert breaker.state == CircuitState.CLOSED  # Still closed

    def test_manual_reset(self):
        """Manual reset should return circuit to CLOSED state."""
        from lib.circuit_breaker import CircuitState
        breaker = self._make_breaker(failure_threshold=1)

        breaker._record_failure(Exception("fail"))
        assert breaker.state == CircuitState.OPEN

        breaker.reset()
        assert breaker.state == CircuitState.CLOSED
        assert breaker._state.failure_count == 0


class TestCircuitBreakerOpenRejection:
    """Test that OPEN circuit rejects requests."""

    def _make_breaker(self, **kwargs):
        from lib.circuit_breaker import CircuitBreaker
        import uuid
        name = f"test-{uuid.uuid4().hex[:8]}"
        defaults = {
            'name': name,
            'failure_threshold': 1,
            'recovery_timeout': 60.0,  # Long timeout so it stays open
        }
        defaults.update(kwargs)
        return CircuitBreaker(**defaults)

    def test_open_circuit_rejects_requests(self):
        """Open circuit should reject all requests."""
        breaker = self._make_breaker()
        breaker._record_failure(Exception("fail"))

        assert breaker._should_allow_request() is False

    def test_open_circuit_raises_on_context_manager(self):
        """Open circuit should raise CircuitBreakerOpen in context manager."""
        from lib.circuit_breaker import CircuitBreakerOpen
        breaker = self._make_breaker()
        breaker._record_failure(Exception("fail"))

        with pytest.raises(CircuitBreakerOpen) as exc_info:
            with breaker:
                pass

        assert breaker.name in str(exc_info.value)


class TestCircuitBreakerDecorator:
    """Test circuit breaker as a decorator."""

    def _make_breaker(self, **kwargs):
        from lib.circuit_breaker import CircuitBreaker
        import uuid
        name = f"test-{uuid.uuid4().hex[:8]}"
        defaults = {
            'name': name,
            'failure_threshold': 2,
            'recovery_timeout': 60.0,
        }
        defaults.update(kwargs)
        return CircuitBreaker(**defaults)

    def test_sync_decorator_passes_through(self):
        """Sync decorated function should pass through in CLOSED state."""
        breaker = self._make_breaker()

        @breaker
        def my_func():
            return "result"

        assert my_func() == "result"

    def test_sync_decorator_records_failure(self):
        """Sync decorated function failure should be recorded."""
        breaker = self._make_breaker(failure_threshold=3)

        @breaker
        def my_func():
            raise ValueError("fail")

        with pytest.raises(ValueError):
            my_func()

        assert breaker._state.failure_count == 1

    def test_sync_decorator_rejects_when_open(self):
        """Sync decorated function should be rejected when circuit is open."""
        from lib.circuit_breaker import CircuitBreakerOpen
        breaker = self._make_breaker(failure_threshold=1)

        @breaker
        def my_func():
            raise ValueError("fail")

        # Trip the breaker
        with pytest.raises(ValueError):
            my_func()

        # Should now be rejected
        with pytest.raises(CircuitBreakerOpen):
            my_func()

    def test_async_decorator_passes_through(self):
        """Async decorated function should pass through in CLOSED state."""
        breaker = self._make_breaker()

        @breaker
        async def my_func():
            return "result"

        result = asyncio.get_event_loop().run_until_complete(my_func())
        assert result == "result"

    def test_async_decorator_records_failure(self):
        """Async decorated function failure should be recorded."""
        breaker = self._make_breaker(failure_threshold=3)

        @breaker
        async def my_func():
            raise ValueError("fail")

        with pytest.raises(ValueError):
            asyncio.get_event_loop().run_until_complete(my_func())

        assert breaker._state.failure_count == 1


class TestCircuitBreakerMonitoredExceptions:
    """Test that only monitored exceptions trigger the circuit breaker."""

    def _make_breaker(self, **kwargs):
        from lib.circuit_breaker import CircuitBreaker
        import uuid
        name = f"test-{uuid.uuid4().hex[:8]}"
        defaults = {
            'name': name,
            'failure_threshold': 2,
            'recovery_timeout': 60.0,
        }
        defaults.update(kwargs)
        return CircuitBreaker(**defaults)

    def test_unmonitored_exception_not_counted(self):
        """Exceptions not in monitored_exceptions should not count as failures."""
        breaker = self._make_breaker(
            monitored_exceptions=(ConnectionError,)
        )

        # ValueError is NOT monitored
        breaker._record_failure(ValueError("not monitored"))
        assert breaker._state.failure_count == 0

    def test_monitored_exception_counted(self):
        """Monitored exceptions should be counted."""
        breaker = self._make_breaker(
            monitored_exceptions=(ConnectionError,)
        )

        breaker._record_failure(ConnectionError("monitored"))
        assert breaker._state.failure_count == 1


class TestCircuitBreakerStatus:
    """Test status reporting and registry."""

    def _make_breaker(self, **kwargs):
        from lib.circuit_breaker import CircuitBreaker
        import uuid
        name = kwargs.pop('name', f"test-{uuid.uuid4().hex[:8]}")
        defaults = {
            'name': name,
            'failure_threshold': 3,
            'recovery_timeout': 60.0,
        }
        defaults.update(kwargs)
        return CircuitBreaker(**defaults)

    def test_get_status(self):
        """Status should return meaningful data."""
        breaker = self._make_breaker()
        status = breaker.get_status()

        assert status['name'] == breaker.name
        assert status['state'] == 'closed'
        assert status['failure_count'] == 0
        assert status['failure_threshold'] == 3
        assert 'time_in_state' in status

    def test_get_status_when_open(self):
        """Status when open should include time_remaining."""
        breaker = self._make_breaker(failure_threshold=1, recovery_timeout=60.0)
        breaker._record_failure(Exception("fail"))

        status = breaker.get_status()
        assert status['state'] == 'open'
        assert status['time_remaining'] > 0

    def test_registry(self):
        """Circuit breakers should be registered globally."""
        from lib.circuit_breaker import CircuitBreaker
        import uuid
        name = f"test-registry-{uuid.uuid4().hex[:8]}"
        breaker = self._make_breaker(name=name)

        found = CircuitBreaker.get_breaker(name)
        assert found is breaker

    def test_get_all_status(self):
        """get_all_status should include all registered breakers."""
        from lib.circuit_breaker import CircuitBreaker
        all_status = CircuitBreaker.get_all_status()
        # At least our test breakers should be present
        assert isinstance(all_status, dict)


class TestGetOrCreateBreaker:
    """Test the get_or_create_breaker factory function."""

    def test_creates_new_breaker(self):
        from lib.circuit_breaker import get_or_create_breaker
        import uuid
        name = f"test-{uuid.uuid4().hex[:8]}"
        breaker = get_or_create_breaker(name)
        assert breaker.name == name

    def test_returns_existing_breaker(self):
        from lib.circuit_breaker import get_or_create_breaker
        import uuid
        name = f"test-{uuid.uuid4().hex[:8]}"
        b1 = get_or_create_breaker(name)
        b2 = get_or_create_breaker(name)
        assert b1 is b2
