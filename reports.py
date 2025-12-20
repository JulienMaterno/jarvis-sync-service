import logging
import os
import httpx
from datetime import datetime, timedelta, timezone
from lib.supabase_client import supabase
from lib.telegram_client import send_telegram_message

logger = logging.getLogger(__name__)

INTELLIGENCE_SERVICE_URL = os.getenv("INTELLIGENCE_SERVICE_URL", "https://jarvis-intelligence-service-776871804948.asia-southeast1.run.app")


async def generate_evening_journal_prompt():
    """
    Generates an AI-powered evening journal prompt by calling the Intelligence Service.
    The AI analyzes the day's activities and creates personalized highlights and prompts.
    """
    try:
        logger.info("Generating evening journal prompt via Intelligence Service...")
        
        today = datetime.now(timezone.utc).date()
        today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
        today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=timezone.utc)
        
        # Gather today's raw activity data
        activity_data = {
            "meetings": [],
            "calendar_events": [],
            "emails": [],
            "tasks_completed": [],
            "tasks_created": [],
            "reflections": [],
            "journals": []
        }
        
        # 1. Today's meetings
        meetings_resp = supabase.table("meetings") \
            .select("title, summary, people_mentioned, topics_discussed") \
            .gte("date", today_start.isoformat()) \
            .lte("date", today_end.isoformat()) \
            .execute()
        activity_data["meetings"] = meetings_resp.data or []
        
        # 2. Today's calendar events
        events_resp = supabase.table("calendar_events") \
            .select("summary, description, attendees, location") \
            .gte("start_time", today_start.isoformat()) \
            .lte("start_time", today_end.isoformat()) \
            .execute()
        activity_data["calendar_events"] = events_resp.data or []
        
        # 3. Today's emails
        emails_resp = supabase.table("emails") \
            .select("subject, sender, snippet") \
            .gte("date", today_start.isoformat()) \
            .lte("date", today_end.isoformat()) \
            .limit(20) \
            .execute()
        activity_data["emails"] = emails_resp.data or []
        
        # 4. Today's completed tasks
        tasks_completed_resp = supabase.table("tasks") \
            .select("title, description") \
            .not_.is_("completed_at", "null") \
            .gte("completed_at", today_start.isoformat()) \
            .lte("completed_at", today_end.isoformat()) \
            .execute()
        activity_data["tasks_completed"] = tasks_completed_resp.data or []
        
        # 5. Today's created tasks
        tasks_created_resp = supabase.table("tasks") \
            .select("title, description") \
            .gte("created_at", today_start.isoformat()) \
            .lte("created_at", today_end.isoformat()) \
            .execute()
        activity_data["tasks_created"] = tasks_created_resp.data or []
        
        # 6. Today's reflections
        reflections_resp = supabase.table("reflections") \
            .select("title, key_insights, tags") \
            .gte("created_at", today_start.isoformat()) \
            .lte("created_at", today_end.isoformat()) \
            .execute()
        activity_data["reflections"] = reflections_resp.data or []
        
        # 7. Today's journal entries (if any already exist)
        journals_resp = supabase.table("journals") \
            .select("highlights, mood, energy_level") \
            .eq("date", today.isoformat()) \
            .execute()
        activity_data["journals"] = journals_resp.data or []
        
        # Call Intelligence Service
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{INTELLIGENCE_SERVICE_URL}/api/v1/journal/evening-prompt",
                json={
                    "activity_data": activity_data,
                    "timezone": "Asia/Singapore"
                }
            )
            response.raise_for_status()
            result = response.json()
        
        # Send the AI-generated message via Telegram
        message = result.get("message", "Time to journal! 📓")
        await send_telegram_message(message)
        
        logger.info(f"Evening journal prompt sent. Highlights: {len(result.get('highlights', []))}")
        return {
            "status": "success", 
            "highlights": result.get("highlights", []),
            "prompts": result.get("reflection_prompts", [])
        }
        
    except httpx.HTTPError as e:
        logger.error(f"Intelligence Service error: {e}")
        # Fallback to simple message
        fallback_msg = """📓 **Evening Journal**

Time to reflect on your day! Take a moment to journal about what happened, what you learned, and what's on your mind.

_Reply with a voice note or text_ 🎙️"""
        await send_telegram_message(fallback_msg)
        return {"status": "fallback", "error": str(e)}
        
    except Exception as e:
        logger.error(f"Failed to generate evening prompt: {e}")
        raise e


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
📆 Calendar: {stats['calendar_events_synced']}
📝 Reflections: {stats['reflections_synced']}
✅ Tasks: {stats['tasks_synced']}
📅 Meetings: {stats['meetings_synced']}
👥 Contacts: {stats['contacts_synced']}
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
