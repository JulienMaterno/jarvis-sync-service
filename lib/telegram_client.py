import os
import httpx
import logging
import asyncio
import traceback
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_BOT_SERVICE_URL = os.environ.get("TELEGRAM_BOT_SERVICE_URL")

# Notification control - can disable via environment variable
# Set to "false", "0", "no", or "disabled" to disable
NOTIFICATIONS_ENABLED = os.environ.get("TELEGRAM_NOTIFICATIONS_ENABLED", "true").lower() not in ("false", "0", "no", "disabled")

# Error notifications are DISABLED by default - errors go to sync_logs database instead
# Only enable for debugging or critical situations
ERROR_NOTIFICATIONS_ENABLED = os.environ.get("TELEGRAM_ERROR_NOTIFICATIONS_ENABLED", "false").lower() not in ("false", "0", "no", "disabled")

# Track consecutive failures per service to avoid spamming on transient errors
_failure_counts = {}
_last_notification = {}
FAILURE_THRESHOLD = 5  # Increased from 3 - only notify after 5 consecutive failures
NOTIFICATION_COOLDOWN = 600  # Increased from 300 - don't notify same error more than once per 10 minutes

# Transient error patterns that should be suppressed unless persistent
TRANSIENT_ERROR_PATTERNS = [
    "Server disconnected",
    "Connection reset by peer",
    "Broken pipe",
    "Connection refused",
    "Connection aborted",
    "timed out",
    "ETIMEDOUT",
    "ECONNRESET",
    "ECONNREFUSED",
    "SSL",
    "Bad Request",  # Often transient (sync token issues)
    "400",          # HTTP 400 errors
    "502",          # Bad Gateway
    "503",          # Service Unavailable  
    "504",          # Gateway Timeout
    "list index out of range",  # Common sync comparison error - now fixed
    "index out of range",
]

def is_transient_error(error: str) -> bool:
    """Check if an error is likely transient (network issues)."""
    error_lower = error.lower()
    return any(pattern.lower() in error_lower for pattern in TRANSIENT_ERROR_PATTERNS)

async def send_telegram_message(text: str, force: bool = False):
    """
    Sends a message to the configured Telegram chat.
    
    Args:
        text: Message to send
        force: If True, bypass notification settings (for critical alerts only)
    """
    if not NOTIFICATIONS_ENABLED and not force:
        logger.info("Telegram notifications disabled. Skipping message.")
        return
        
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID not configured. Skipping message.")
        return

    if TELEGRAM_BOT_SERVICE_URL:
        # Use internal bot service
        url = f"{TELEGRAM_BOT_SERVICE_URL}/send_message"
        payload = {
            "chat_id": int(TELEGRAM_CHAT_ID),
            "text": text,
            "parse_mode": "Markdown"
        }
    elif TELEGRAM_BOT_TOKEN:
        # Use direct Telegram API (Legacy)
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
    else:
        logger.warning("Telegram credentials not configured. Skipping message.")
        return

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=10.0)
            response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

async def notify_error(context: str, error: str):
    """
    Sends an error alert with traceback if available.
    Uses smart filtering to avoid spamming on transient/network errors.
    Only notifies after multiple consecutive failures for transient errors.
    """
    global _failure_counts, _last_notification
    
    # Check if error notifications are disabled
    if not ERROR_NOTIFICATIONS_ENABLED:
        logger.warning(f"Error notification suppressed (disabled): {context} - {str(error)[:100]}")
        return
    
    error_str = str(error)
    is_transient = is_transient_error(error_str)
    
    # Track consecutive failures
    if context not in _failure_counts:
        _failure_counts[context] = 0
    _failure_counts[context] += 1
    
    # For transient errors, only notify after threshold is reached
    if is_transient and _failure_counts[context] < FAILURE_THRESHOLD:
        logger.warning(f"Transient error in {context} ({_failure_counts[context]}/{FAILURE_THRESHOLD}): {error_str[:100]}")
        return
    
    # Check cooldown - don't spam the same error repeatedly
    now = datetime.now(timezone.utc)
    error_key = f"{context}:{error_str[:50]}"
    if error_key in _last_notification:
        elapsed = (now - _last_notification[error_key]).total_seconds()
        if elapsed < NOTIFICATION_COOLDOWN:
            logger.info(f"Skipping notification for {context} (cooldown: {int(NOTIFICATION_COOLDOWN - elapsed)}s remaining)")
            return
    
    _last_notification[error_key] = now
    
    # Get full traceback
    tb = traceback.format_exc()
    
    # If the error string is just the message, the traceback gives more context.
    # If traceback is "NoneType: None", it means no active exception, so just use the error msg.
    if "NoneType: None" in tb:
        details = error_str
    else:
        # Truncate traceback to avoid Telegram message limit (4096 chars)
        details = f"{error_str}\n\nTraceback:\n{tb}"[:3000]

    # Add failure count info for transient errors
    prefix = ""
    if is_transient:
        prefix = f"(after {_failure_counts[context]} consecutive failures)\n"
    
    message = f"🚨 **Error in {context}**\n{prefix}\n```\n{details}\n```"
    await send_telegram_message(message)


def reset_failure_count(context: str):
    """Reset the failure counter for a service after successful execution."""
    global _failure_counts
    if context in _failure_counts:
        _failure_counts[context] = 0
