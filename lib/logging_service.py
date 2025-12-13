from datetime import datetime, timezone
from lib.supabase_client import supabase
import logging
import json

logger = logging.getLogger(__name__)

async def log_sync_event(event_type: str, status: str, message: str, contact_id: str = None, details: dict = None):
    """
    Logs a sync event to the Supabase 'sync_logs' table and standard logger.
    Args:
        event_type: The type of event (e.g., 'sync_start', 'create_google', 'backup')
        status: The status/level (e.g., 'info', 'success', 'error', 'warning')
        message: Human readable message
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
            # Let DB handle created_at
        }
        # Note: We don't have contact_id or details columns in the SQL yet, 
        # but we can add them to the payload if we update the SQL.
        # For now, let's append details to message or ignore if columns don't exist.
        # Actually, the user asked for a JSON column in contacts, but didn't specify structure for logs.
        # My SQL created: event_type, status, message.
        # So I should stick to that.
        
        if details:
             payload["message"] += f" | Details: {json.dumps(details)}"

        # Execute synchronous Supabase call. 
        # In a high-perf async app, we'd use the async client or run_in_executor.
        # For this script, blocking briefly is acceptable.
        supabase.table("sync_logs").insert(payload).execute()
        
    except Exception as e:
        logger.error(f"Failed to write to sync_logs: {e}")
