import time
import logging
import asyncio
import httpx
import ssl
from typing import Type, Tuple, Callable, Any

logger = logging.getLogger("Utils")

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
