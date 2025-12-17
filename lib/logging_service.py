from datetime import datetime, timezone
from lib.supabase_client import supabase
import logging
import json

logger = logging.getLogger(__name__)


def log_sync_event_sync(event_type: str, status: str, message: str, contact_id: str = None, details: dict = None):
    """
    Synchronous version of log_sync_event.
    Logs a sync event to the Supabase 'sync_logs' table and standard logger.
    
    Args:
        event_type: The type of event (e.g., 'sync_start', 'create_google', 'backup')
        status: The status/level (e.g., 'info', 'success', 'error', 'warning')
        message: Human readable message
        contact_id: Optional contact ID for context
        details: Optional dictionary with additional details
    """
    # 1. Print to console/file logs
    log_msg = f"[{event_type.upper()}] {message}"
    if status.lower() in ["error", "fatal"]:
        logger.error(log_msg)
    elif status.lower() == "warning":
        logger.warning(log_msg)
    else:
        logger.info(log_msg)

    # 2. Write to Supabase
    try:
        payload = {
            "event_type": event_type,
            "status": status,
            "message": message,
        }
        
        if details:
             payload["message"] += f" | Details: {json.dumps(details)}"

        supabase.table("sync_logs").insert(payload).execute()
        
    except Exception as e:
        logger.error(f"Failed to write to sync_logs: {e}")


async def log_sync_event(event_type: str, status: str, message: str, contact_id: str = None, details: dict = None):
    """
    Async version of log_sync_event (for backwards compatibility).
    Internally calls the sync version since Supabase client is sync anyway.
    """
    log_sync_event_sync(event_type, status, message, contact_id, details)
