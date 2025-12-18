"""
Health monitoring for sync operations.
Tracks consecutive failures and alerts if threshold exceeded.
"""
import logging
from datetime import datetime, timedelta, timezone
from lib.supabase_client import supabase
from lib.telegram_client import send_telegram_message

logger = logging.getLogger("HealthMonitor")

async def check_sync_health(service_name: str, failure_threshold: int = 5):
    """
    Check if a sync service has failed too many times consecutively.
    Send alert if threshold exceeded.
    
    Args:
        service_name: Name of the sync service (e.g., 'calendar_sync', 'gmail_sync')
        failure_threshold: Number of consecutive failures before alerting
    """
    try:
        # Get last N logs for this service
        response = supabase.table("sync_logs") \
            .select("status, created_at, message") \
            .eq("event_type", service_name) \
            .order("created_at", desc=True) \
            .limit(failure_threshold + 1) \
            .execute()
        
        logs = response.data
        if not logs or len(logs) < failure_threshold:
            return {"healthy": True}
        
        # Check if all recent attempts failed
        recent_failures = [log for log in logs if log.get("status") == "error"]
        
        if len(recent_failures) >= failure_threshold:
            # All recent attempts failed - send alert
            error_msg = logs[0].get("message", "Unknown error")
            alert = f"""⚠️ **Sync Health Alert**

**Service**: {service_name}
**Status**: {failure_threshold}+ consecutive failures
**Last Error**: {error_msg[:200]}

The sync service may need attention."""
            
            await send_telegram_message(alert)
            logger.warning(f"{service_name} has {len(recent_failures)} consecutive failures")
            return {"healthy": False, "consecutive_failures": len(recent_failures)}
        
        return {"healthy": True}
        
    except Exception as e:
        logger.error(f"Failed to check sync health: {e}")
        return {"healthy": True, "error": str(e)}


async def get_sync_statistics(hours: int = 24):
    """
    Get sync statistics for the last N hours.
    Returns success rate, error rate, etc.
    """
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        response = supabase.table("sync_logs") \
            .select("event_type, status") \
            .gte("created_at", cutoff.isoformat()) \
            .execute()
        
        logs = response.data
        if not logs:
            return {"total": 0, "success": 0, "error": 0, "rate": 0}
        
        total = len(logs)
        success = len([l for l in logs if l.get("status") == "success"])
        error = len([l for l in logs if l.get("status") == "error"])
        
        return {
            "total": total,
            "success": success,
            "error": error,
            "success_rate": round((success / total) * 100, 1) if total > 0 else 0
        }
        
    except Exception as e:
        logger.error(f"Failed to get sync statistics: {e}")
        return {"error": str(e)}
