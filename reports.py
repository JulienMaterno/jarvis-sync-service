import logging
import os
import httpx
from datetime import datetime, timedelta, timezone
from lib.supabase_client import supabase
from lib.telegram_client import send_telegram_message

logger = logging.getLogger(__name__)

INTELLIGENCE_SERVICE_URL = os.getenv("INTELLIGENCE_SERVICE_URL", "https://jarvis-intelligence-service-776871804948.asia-southeast1.run.app")


def get_activity_summary_for_journal() -> dict:
    """Get ActivityWatch summary for today to include in journal prompt."""
    try:
        today = datetime.now(timezone.utc).date()
        result = supabase.table("activity_summaries").select("*").eq(
            "date", str(today)
        ).execute()
        
        if result.data:
            return result.data[0]
    except Exception as e:
        logger.warning(f"Failed to get activity summary: {e}")
    
    return {}


def get_reading_data_for_journal() -> dict:
    """Get reading progress and highlights for today's journal prompt."""
    today = datetime.now(timezone.utc).date()
    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
    
    reading_data = {
        "currently_reading": [],
        "todays_highlights": [],
        "recent_finishes": []
    }
    
    try:
        # 1. Currently reading books with progress
        reading_resp = supabase.table("books") \
            .select("title, author, current_page, total_pages, started_at") \
            .eq("status", "Reading") \
            .is_("deleted_at", "null") \
            .execute()
        
        for book in reading_resp.data or []:
            total = book.get('total_pages') or 0
            current = book.get('current_page') or 0
            progress = round((current / total * 100), 1) if total > 0 else 0
            
            reading_data["currently_reading"].append({
                "title": book.get("title"),
                "author": book.get("author"),
                "progress_percent": progress,
                "current_page": current,
                "total_pages": total
            })
        
        # 2. Today's highlights
        highlights_resp = supabase.table("highlights") \
            .select("content, note, book_title, page_number, is_favorite") \
            .gte("highlighted_at", today_start.isoformat()) \
            .is_("deleted_at", "null") \
            .order("highlighted_at", desc=True) \
            .limit(10) \
            .execute()
        
        reading_data["todays_highlights"] = highlights_resp.data or []
        
        # 3. Recently finished books (last 7 days)
        week_ago = (today - timedelta(days=7)).isoformat()
        finished_resp = supabase.table("books") \
            .select("title, author, rating, finished_at") \
            .eq("status", "Finished") \
            .gte("finished_at", week_ago) \
            .is_("deleted_at", "null") \
            .execute()
        
        reading_data["recent_finishes"] = finished_resp.data or []
        
    except Exception as e:
        logger.warning(f"Failed to get reading data: {e}")
    
    return reading_data


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
            .select("title") \
            .not_.is_("completed_at", "null") \
            .gte("completed_at", today_start.isoformat()) \
            .lte("completed_at", today_end.isoformat()) \
            .execute()
        activity_data["tasks_completed"] = tasks_completed_resp.data or []
        
        # 5. Today's created tasks
        tasks_created_resp = supabase.table("tasks") \
            .select("title") \
            .gte("created_at", today_start.isoformat()) \
            .lte("created_at", today_end.isoformat()) \
            .execute()
        activity_data["tasks_created"] = tasks_created_resp.data or []
        
        # 6. Today's reflections
        reflections_resp = supabase.table("reflections") \
            .select("*") \
            .gte("created_at", today_start.isoformat()) \
            .lte("created_at", today_end.isoformat()) \
            .execute()
        activity_data["reflections"] = reflections_resp.data or []
        
        # 7. Today's journal entries (if any already exist)
        journals_resp = supabase.table("journals") \
            .select("*") \
            .eq("date", today.isoformat()) \
            .execute()
        activity_data["journals"] = journals_resp.data or []
        
        # 8. ActivityWatch screen time data
        activity_summary = get_activity_summary_for_journal()
        if activity_summary:
            activity_data["screen_time"] = {
                "total_active_hours": round(activity_summary.get("total_active_time", 0) / 3600, 1),
                "total_afk_hours": round(activity_summary.get("total_afk_time", 0) / 3600, 1),
                "productive_hours": round(activity_summary.get("productive_time", 0) / 3600, 1),
                "distracting_hours": round(activity_summary.get("distracting_time", 0) / 3600, 1),
                "top_apps": activity_summary.get("top_apps", [])[:5],
                "top_sites": activity_summary.get("top_sites", [])[:5],
            }
        
        # 9. Reading data - books and highlights
        reading_data = get_reading_data_for_journal()
        if reading_data.get("currently_reading") or reading_data.get("todays_highlights"):
            activity_data["reading"] = {
                "currently_reading": reading_data.get("currently_reading", []),
                "todays_highlights": reading_data.get("todays_highlights", []),
                "recently_finished": reading_data.get("recent_finishes", [])
            }
        
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
