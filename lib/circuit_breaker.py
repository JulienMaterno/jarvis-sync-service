"""
Circuit Breaker Pattern Implementation

Provides fault tolerance for external API calls by preventing cascading failures.
When a service is failing, the circuit breaker "opens" to fail fast instead of
repeatedly trying calls that are likely to fail.

States:
- CLOSED: Normal operation, requests pass through
- OPEN: Circuit is tripped, requests fail immediately
- HALF_OPEN: Testing if service has recovered

Usage:
    from lib.circuit_breaker import CircuitBreaker, CircuitBreakerOpen

    notion_breaker = CircuitBreaker("notion", failure_threshold=5, recovery_timeout=60)

    @notion_breaker
    async def call_notion_api():
        ...

    # Or manually:
    with notion_breaker:
        result = await some_api_call()
"""

import time
import logging
import asyncio
from enum import Enum
from typing import Callable, Any, Optional, Dict, Type, Tuple
from functools import wraps
from dataclasses import dataclass, field
from threading import Lock

logger = logging.getLogger("CircuitBreaker")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject all calls
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreakerOpen(Exception):
    """Raised when circuit breaker is open and rejecting calls."""

    def __init__(self, name: str, time_remaining: float):
        self.name = name
        self.time_remaining = time_remaining
        super().__init__(
            f"Circuit breaker '{name}' is OPEN. "
            f"Rejecting calls for {time_remaining:.1f}s more."
        )


@dataclass
class CircuitBreakerConfig:
    """Configuration for a circuit breaker instance."""
    failure_threshold: int = 5       # Failures before opening
    recovery_timeout: float = 60.0   # Seconds to wait before half-open
    success_threshold: int = 2       # Successes in half-open to close
    # Exceptions that count as failures (None = all exceptions)
    monitored_exceptions: Optional[Tuple[Type[Exception], ...]] = None


@dataclass
class CircuitBreakerState:
    """Runtime state for a circuit breaker."""
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0           # Successes in half-open state
    last_failure_time: Optional[float] = None
    last_state_change: float = field(default_factory=time.time)

    def reset(self):
        """Reset to initial closed state."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_state_change = time.time()


class CircuitBreaker:
    """
    Circuit Breaker implementation for fault tolerance.

    The circuit breaker monitors calls to external services and trips (opens)
    when too many failures occur. While open, calls fail immediately without
    attempting the actual operation. After a recovery timeout, the circuit
    enters half-open state to test if the service has recovered.

    Args:
        name: Identifier for this circuit breaker (for logging)
        failure_threshold: Number of failures before opening (default: 5)
        recovery_timeout: Seconds before attempting recovery (default: 60)
        success_threshold: Successes needed in half-open to close (default: 2)
        monitored_exceptions: Exception types that count as failures
                             (default: all exceptions)

    Example:
        breaker = CircuitBreaker("notion_api", failure_threshold=5)

        @breaker
        async def call_notion():
            return await notion_client.query_database(...)
    """

    # Global registry of circuit breakers for monitoring
    _registry: Dict[str, "CircuitBreaker"] = {}
    _registry_lock = Lock()

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 2,
        monitored_exceptions: Optional[Tuple[Type[Exception], ...]] = None
    ):
        self.name = name
        self.config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            success_threshold=success_threshold,
            monitored_exceptions=monitored_exceptions
        )
        self._state = CircuitBreakerState()
        self._lock = Lock()

        # Register this circuit breaker
        with CircuitBreaker._registry_lock:
            CircuitBreaker._registry[name] = self

    @classmethod
    def get_all_status(cls) -> Dict[str, Dict[str, Any]]:
        """Get status of all registered circuit breakers."""
        with cls._registry_lock:
            return {
                name: breaker.get_status()
                for name, breaker in cls._registry.items()
            }

    @classmethod
    def get_breaker(cls, name: str) -> Optional["CircuitBreaker"]:
        """Get a circuit breaker by name."""
        with cls._registry_lock:
            return cls._registry.get(name)

    def get_status(self) -> Dict[str, Any]:
        """Get current status of this circuit breaker."""
        with self._lock:
            time_in_state = time.time() - self._state.last_state_change
            time_remaining = 0.0

            if self._state.state == CircuitState.OPEN:
                time_remaining = max(
                    0.0,
                    self.config.recovery_timeout - time_in_state
                )

            return {
                "name": self.name,
                "state": self._state.state.value,
                "failure_count": self._state.failure_count,
                "success_count": self._state.success_count,
                "failure_threshold": self.config.failure_threshold,
                "recovery_timeout": self.config.recovery_timeout,
                "time_in_state": round(time_in_state, 2),
                "time_remaining": round(time_remaining, 2),
            }

    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        with self._lock:
            return self._state.state

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed (normal operation)."""
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        """Check if circuit is open (rejecting calls)."""
        return self.state == CircuitState.OPEN

    def _should_allow_request(self) -> bool:
        """
        Check if a request should be allowed through.
        Also handles automatic state transitions.
        """
        with self._lock:
            if self._state.state == CircuitState.CLOSED:
                return True

            if self._state.state == CircuitState.OPEN:
                # Check if recovery timeout has elapsed
                time_since_change = time.time() - self._state.last_state_change
                if time_since_change >= self.config.recovery_timeout:
                    # Transition to half-open
                    self._transition_to(CircuitState.HALF_OPEN)
                    return True
                return False

            if self._state.state == CircuitState.HALF_OPEN:
                # Allow requests in half-open to test recovery
                return True

            return False

    def _transition_to(self, new_state: CircuitState) -> None:
        """
        Transition to a new state (must be called with lock held).
        """
        old_state = self._state.state
        if old_state == new_state:
            return

        self._state.state = new_state
        self._state.last_state_change = time.time()

        if new_state == CircuitState.CLOSED:
            self._state.failure_count = 0
            self._state.success_count = 0
        elif new_state == CircuitState.HALF_OPEN:
            self._state.success_count = 0

        logger.info(
            f"Circuit breaker '{self.name}' transitioned: "
            f"{old_state.value} -> {new_state.value}"
        )

    def _record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state.state == CircuitState.HALF_OPEN:
                self._state.success_count += 1
                if self._state.success_count >= self.config.success_threshold:
                    # Service recovered, close the circuit
                    self._transition_to(CircuitState.CLOSED)
                    logger.info(
                        f"Circuit breaker '{self.name}' recovered after "
                        f"{self._state.success_count} successful calls"
                    )
            elif self._state.state == CircuitState.CLOSED:
                # Reset failure count on success in closed state
                self._state.failure_count = 0

    def _record_failure(self, exception: Exception) -> None:
        """Record a failed call."""
        # Check if this exception type should be monitored
        if self.config.monitored_exceptions is not None:
            if not isinstance(exception, self.config.monitored_exceptions):
                return

        with self._lock:
            self._state.failure_count += 1
            self._state.last_failure_time = time.time()

            if self._state.state == CircuitState.HALF_OPEN:
                # Failure in half-open, go back to open
                self._transition_to(CircuitState.OPEN)
                logger.warning(
                    f"Circuit breaker '{self.name}' failed recovery test, "
                    f"reopening circuit"
                )
            elif self._state.state == CircuitState.CLOSED:
                if self._state.failure_count >= self.config.failure_threshold:
                    # Too many failures, open the circuit
                    self._transition_to(CircuitState.OPEN)
                    logger.warning(
                        f"Circuit breaker '{self.name}' opened after "
                        f"{self._state.failure_count} failures"
                    )

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            old_state = self._state.state
            self._state.reset()
            logger.info(
                f"Circuit breaker '{self.name}' manually reset from "
                f"{old_state.value} to closed"
            )

    def __call__(self, func: Callable) -> Callable:
        """
        Decorator to wrap a function with circuit breaker protection.

        Works with both sync and async functions.
        """
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs) -> Any:
                if not self._should_allow_request():
                    time_remaining = max(
                        0.0,
                        self.config.recovery_timeout -
                        (time.time() - self._state.last_state_change)
                    )
                    raise CircuitBreakerOpen(self.name, time_remaining)

                try:
                    result = await func(*args, **kwargs)
                    self._record_success()
                    return result
                except Exception as e:
                    self._record_failure(e)
                    raise

            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs) -> Any:
                if not self._should_allow_request():
                    time_remaining = max(
                        0.0,
                        self.config.recovery_timeout -
                        (time.time() - self._state.last_state_change)
                    )
                    raise CircuitBreakerOpen(self.name, time_remaining)

                try:
                    result = func(*args, **kwargs)
                    self._record_success()
                    return result
                except Exception as e:
                    self._record_failure(e)
                    raise

            return sync_wrapper

    def __enter__(self):
        """Context manager entry for sync code."""
        if not self._should_allow_request():
            time_remaining = max(
                0.0,
                self.config.recovery_timeout -
                (time.time() - self._state.last_state_change)
            )
            raise CircuitBreakerOpen(self.name, time_remaining)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit for sync code."""
        if exc_val is None:
            self._record_success()
        else:
            self._record_failure(exc_val)
        return False  # Don't suppress exceptions

    async def __aenter__(self):
        """Async context manager entry."""
        if not self._should_allow_request():
            time_remaining = max(
                0.0,
                self.config.recovery_timeout -
                (time.time() - self._state.last_state_change)
            )
            raise CircuitBreakerOpen(self.name, time_remaining)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if exc_val is None:
            self._record_success()
        else:
            self._record_failure(exc_val)
        return False  # Don't suppress exceptions


# Pre-configured circuit breakers for external services
# These are created lazily when first accessed via get_or_create

def get_or_create_breaker(
    name: str,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    success_threshold: int = 2
) -> CircuitBreaker:
    """
    Get an existing circuit breaker or create a new one.

    This is the recommended way to get circuit breakers to ensure
    only one instance exists per name.
    """
    with CircuitBreaker._registry_lock:
        if name in CircuitBreaker._registry:
            return CircuitBreaker._registry[name]

    # Create new breaker (constructor will register it)
    return CircuitBreaker(
        name=name,
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
        success_threshold=success_threshold
    )


# Default circuit breakers for common services
def get_notion_breaker() -> CircuitBreaker:
    """Get circuit breaker for Notion API."""
    return get_or_create_breaker(
        name="notion",
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=2
    )


def get_google_calendar_breaker() -> CircuitBreaker:
    """Get circuit breaker for Google Calendar API."""
    return get_or_create_breaker(
        name="google_calendar",
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=2
    )


def get_google_contacts_breaker() -> CircuitBreaker:
    """Get circuit breaker for Google Contacts API."""
    return get_or_create_breaker(
        name="google_contacts",
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=2
    )


def get_google_gmail_breaker() -> CircuitBreaker:
    """Get circuit breaker for Gmail API."""
    return get_or_create_breaker(
        name="google_gmail",
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=2
    )
