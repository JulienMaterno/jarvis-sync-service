import logging
from datetime import datetime, timedelta, timezone
from lib.supabase_client import supabase
from lib.telegram_client import send_telegram_message

logger = logging.getLogger(__name__)

async def generate_daily_report():
    """
    Generates a daily summary of sync activities and sends it via Telegram.
    """
    try:
        logger.info("Generating daily report...")
        
        # 1. Query logs for the last 24 hours
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        response = supabase.table("sync_logs") \
            .select("*") \
            .gte("created_at", yesterday.isoformat()) \
            .execute()
        
        logs = response.data
        
        if not logs:
            await send_telegram_message("📊 **Daily Sync Report**\n\nNo activity recorded in the last 24 hours.")
            return

        # 2. Aggregate stats
        stats = {
            "total_ops": len(logs),
            "errors": 0,
            "contacts_synced": 0,
            "meetings_synced": 0,
            "tasks_synced": 0,
            "reflections_synced": 0,
            "emails_synced": 0,
            "calendar_events_synced": 0
        }

        error_messages = []

        for log in logs:
            event = log.get("event_type", "").lower()
            status = log.get("status", "").lower()
            msg = log.get("message", "")

            if status == "error":
                stats["errors"] += 1
                # Keep unique error messages (simplified)
                if len(error_messages) < 5: # Limit to 5 errors in summary
                    error_messages.append(f"- {msg[:50]}...")

            # Parse success messages for counts
            # This depends on how we logged them. 
            # e.g. "Synced 10 emails", "Created Supabase reflection: ..."
            
            if status == "success":
                if "gmail_sync" in event:
                    # Extract number from "Synced X emails"
                    try:
                        count = int([s for s in msg.split() if s.isdigit()][0])
                        stats["emails_synced"] += count
                    except: pass
                elif "calendar_sync" in event:
                    try:
                        count = int([s for s in msg.split() if s.isdigit()][0])
                        stats["calendar_events_synced"] += count
                    except: pass
            
            # For item-level logs (e.g. "Created Supabase reflection...")
            if "create_supabase_reflection" in event or "create_notion_reflection" in event:
                stats["reflections_synced"] += 1
            elif "create_supabase_task" in event or "create_notion_task" in event:
                stats["tasks_synced"] += 1
            elif "create_supabase_meeting" in event or "create_notion_meeting" in event:
                stats["meetings_synced"] += 1
            elif "contact" in event and ("create" in event or "update" in event):
                stats["contacts_synced"] += 1

        # 3. Format Message
        report = f"""📊 **Daily Sync Report**
_{yesterday.strftime('%Y-%m-%d')} to {datetime.now(timezone.utc).strftime('%Y-%m-%d')}_

**Summary**
✅ **Total Operations**: {stats['total_ops']}
❌ **Errors**: {stats['errors']}

**Sync Details**
📧 Emails: {stats['emails_synced']}
dV Calendar: {stats['calendar_events_synced']}
📝 Reflections: {stats['reflections_synced']}
✅ Tasks: {stats['tasks_synced']}
📅 Meetings: {stats['meetings_synced']}
busts Contacts: {stats['contacts_synced']}
"""

        if stats['errors'] > 0:
            report += "\n**Recent Errors**\n" + "\n".join(error_messages)

        # 4. Send
        await send_telegram_message(report)
        logger.info("Daily report sent.")
        return {"status": "success", "report": report}

    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        # Don't notify error about the report itself to avoid loops, just log it
        raise e
