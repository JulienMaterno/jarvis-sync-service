"""
Retry and Circuit Breaker Utilities

Provides robust error handling for external API calls with:
- Exponential backoff retry logic
- Circuit breaker pattern integration
- Configurable retry parameters
"""

import time
import logging
import asyncio
import httpx
import ssl
from typing import Type, Tuple, Callable, Any, Optional, TYPE_CHECKING
from functools import wraps

if TYPE_CHECKING:
    from lib.circuit_breaker import CircuitBreaker

logger = logging.getLogger("Utils")

# Default retry configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = 1.0      # 1 second base delay
DEFAULT_MAX_DELAY = 30.0      # 30 second max delay
DEFAULT_EXPONENTIAL_FACTOR = 2.0

# Default exceptions to retry on - includes network errors, timeouts, and transient failures
RETRYABLE_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.TimeoutException,
    httpx.HTTPStatusError,
    httpx.RemoteProtocolError,  # Server disconnected, malformed responses
    httpx.ReadError,            # Failed to receive data
    httpx.WriteError,           # Failed to send data  
    httpx.NetworkError,         # Base class for network-related errors
    ConnectionResetError,       # [Errno 104] Connection reset by peer
    BrokenPipeError,            # [Errno 32] Broken pipe
    ConnectionAbortedError,     # Connection aborted
    ConnectionRefusedError,     # Connection refused
    ssl.SSLError,               # SSL/TLS errors
    OSError,                    # Covers various socket errors
)

def retry_on_error(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = RETRYABLE_EXCEPTIONS
):
    """
    Decorator to retry async functions on specific exceptions.
    Handles rate limiting with exponential backoff.
    """
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    # Don't retry 4xx client errors (except 429 Too Many Requests)
                    if isinstance(e, httpx.HTTPStatusError):
                        if e.response.status_code < 500 and e.response.status_code != 429:
                            raise e
                        # For rate limiting (429), use longer backoff
                        if e.response.status_code == 429:
                            wait_time = (backoff_factor ** attempt) * 5  # 5x longer for rate limits
                            logger.warning(f"Rate limit hit in {func.__name__}. Retrying in {wait_time}s...")
                            await asyncio.sleep(wait_time)
                            continue
                    
                    if attempt == max_retries - 1:
                        break
                    
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Transient error in {func.__name__}: {e}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
            
            raise last_exception
        return wrapper
    return decorator

def retry_on_error_sync(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = RETRYABLE_EXCEPTIONS
):
    """
    Decorator to retry synchronous functions on specific exceptions.
    """
    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if isinstance(e, httpx.HTTPStatusError):
                        if e.response.status_code < 500 and e.response.status_code != 429:
                            raise e
                    
                    if attempt == max_retries - 1:
                        break
                    
                    wait_time = backoff_factor ** attempt
                    logger.warning(f"Transient error in {func.__name__}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)

            raise last_exception
        return wrapper
    return decorator


def _is_retryable_http_error(e: httpx.HTTPStatusError) -> bool:
    """
    Determine if an HTTP error should be retried.

    Retryable:
    - 429 Too Many Requests (rate limiting)
    - 5xx Server errors

    Not retryable:
    - 4xx Client errors (except 429)
    """
    status = e.response.status_code
    return status == 429 or status >= 500


def _calculate_delay(
    attempt: int,
    base_delay: float,
    max_delay: float,
    exponential_factor: float,
    is_rate_limit: bool = False
) -> float:
    """
    Calculate delay for retry attempt using exponential backoff.

    For rate limits (429), use a longer delay multiplier.
    """
    delay = base_delay * (exponential_factor ** attempt)

    # Rate limits get 5x longer delay
    if is_rate_limit:
        delay *= 5.0

    return min(delay, max_delay)


def retry_with_backoff(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    exponential_factor: float = DEFAULT_EXPONENTIAL_FACTOR,
    exceptions: Tuple[Type[Exception], ...] = RETRYABLE_EXCEPTIONS,
    circuit_breaker: Optional["CircuitBreaker"] = None
) -> Callable:
    """
    Decorator to retry async functions with exponential backoff.

    Optionally wraps with a circuit breaker for additional fault tolerance.
    The retry logic runs first, then the circuit breaker wraps the retried call.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds (default: 1.0)
        max_delay: Maximum delay cap in seconds (default: 30.0)
        exponential_factor: Multiplier for each retry (default: 2.0)
        exceptions: Tuple of exception types to retry on
        circuit_breaker: Optional CircuitBreaker instance to use

    Example:
        from lib.circuit_breaker import get_notion_breaker

        @retry_with_backoff(max_retries=3, circuit_breaker=get_notion_breaker())
        async def call_notion_api():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries):
                try:
                    # If circuit breaker is provided, use it
                    if circuit_breaker is not None:
                        async with circuit_breaker:
                            return await func(*args, **kwargs)
                    else:
                        return await func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    # Check if this is a non-retryable HTTP error
                    if isinstance(e, httpx.HTTPStatusError):
                        if not _is_retryable_http_error(e):
                            raise e

                    # Don't retry if this was the last attempt
                    if attempt >= max_retries - 1:
                        break

                    # Calculate delay
                    is_rate_limit = (
                        isinstance(e, httpx.HTTPStatusError) and
                        e.response.status_code == 429
                    )
                    wait_time = _calculate_delay(
                        attempt, base_delay, max_delay,
                        exponential_factor, is_rate_limit
                    )

                    # Log the retry
                    error_type = "Rate limit" if is_rate_limit else "Transient error"
                    logger.warning(
                        f"{error_type} in {func.__name__} (attempt {attempt + 1}/{max_retries}): "
                        f"{type(e).__name__}: {e}. Retrying in {wait_time:.1f}s..."
                    )

                    await asyncio.sleep(wait_time)

            # All retries exhausted
            logger.error(
                f"All {max_retries} retry attempts failed for {func.__name__}: "
                f"{type(last_exception).__name__}: {last_exception}"
            )
            raise last_exception

        return wrapper
    return decorator


def retry_with_backoff_sync(
    max_retries: int = DEFAULT_MAX_RETRIES,
    base_delay: float = DEFAULT_BASE_DELAY,
    max_delay: float = DEFAULT_MAX_DELAY,
    exponential_factor: float = DEFAULT_EXPONENTIAL_FACTOR,
    exceptions: Tuple[Type[Exception], ...] = RETRYABLE_EXCEPTIONS,
    circuit_breaker: Optional["CircuitBreaker"] = None
) -> Callable:
    """
    Decorator to retry synchronous functions with exponential backoff.

    Optionally wraps with a circuit breaker for additional fault tolerance.

    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        base_delay: Initial delay in seconds (default: 1.0)
        max_delay: Maximum delay cap in seconds (default: 30.0)
        exponential_factor: Multiplier for each retry (default: 2.0)
        exceptions: Tuple of exception types to retry on
        circuit_breaker: Optional CircuitBreaker instance to use

    Example:
        from lib.circuit_breaker import get_notion_breaker

        @retry_with_backoff_sync(max_retries=3, circuit_breaker=get_notion_breaker())
        def call_notion_api():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries):
                try:
                    # If circuit breaker is provided, use it
                    if circuit_breaker is not None:
                        with circuit_breaker:
                            return func(*args, **kwargs)
                    else:
                        return func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    # Check if this is a non-retryable HTTP error
                    if isinstance(e, httpx.HTTPStatusError):
                        if not _is_retryable_http_error(e):
                            raise e

                    # Don't retry if this was the last attempt
                    if attempt >= max_retries - 1:
                        break

                    # Calculate delay
                    is_rate_limit = (
                        isinstance(e, httpx.HTTPStatusError) and
                        e.response.status_code == 429
                    )
                    wait_time = _calculate_delay(
                        attempt, base_delay, max_delay,
                        exponential_factor, is_rate_limit
                    )

                    # Log the retry
                    error_type = "Rate limit" if is_rate_limit else "Transient error"
                    logger.warning(
                        f"{error_type} in {func.__name__} (attempt {attempt + 1}/{max_retries}): "
                        f"{type(e).__name__}: {e}. Retrying in {wait_time:.1f}s..."
                    )

                    time.sleep(wait_time)

            # All retries exhausted
            logger.error(
                f"All {max_retries} retry attempts failed for {func.__name__}: "
                f"{type(last_exception).__name__}: {last_exception}"
            )
            raise last_exception

        return wrapper
    return decorator
