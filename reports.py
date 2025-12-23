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
    Enhanced evening journal generation that:
    1. Collects ALL activities from last 24 hours
    2. Calls Intelligence Service for AI analysis
    3. Creates/updates journal entry in Supabase
    4. Sends interactive prompt to Telegram
    5. User can reply to append personal notes
    """
    try:
        logger.info("Generating enhanced evening journal prompt...")
        
        # Use 24-hour window
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=24)
        today = now.date()
        
        # =================================================================
        # 1. COLLECT ALL ACTIVITY DATA
        # =================================================================
        
        activity_data = {
            "meetings": [],
            "calendar_events": [],
            "emails": [],
            "tasks_completed": [],
            "tasks_created": [],
            "reflections": [],
            "highlights": [],  # Book highlights
            "reading": None,
            "screen_time": None,
            "contacts_added": [],
            "summary": {}
        }
        
        # Meetings (last 24h)
        meetings_resp = supabase.table("meetings") \
            .select("id, title, summary, contact_name, people_mentioned, topics_discussed, date, created_at") \
            .gte("created_at", cutoff.isoformat()) \
            .order("date", desc=True) \
            .execute()
        activity_data["meetings"] = meetings_resp.data or []
        
        # Calendar events (last 24h)
        events_resp = supabase.table("calendar_events") \
            .select("summary, description, attendees, location, start_time, end_time") \
            .gte("start_time", cutoff.isoformat()) \
            .lte("start_time", now.isoformat()) \
            .order("start_time") \
            .execute()
        activity_data["calendar_events"] = events_resp.data or []
        
        # Emails (last 24h, filtered)
        emails_resp = supabase.table("emails") \
            .select("subject, sender, snippet, date, contact_id") \
            .gte("date", cutoff.isoformat()) \
            .order("date", desc=True) \
            .limit(30) \
            .execute()
        
        # Filter out automated emails
        skip_keywords = {"unsubscribe", "newsletter", "noreply", "no-reply", 
                        "github", "notification", "automated", "donotreply"}
        filtered_emails = []
        for email in (emails_resp.data or []):
            subject = (email.get("subject") or "").lower()
            sender = (email.get("sender") or "").lower()
            if not any(kw in subject or kw in sender for kw in skip_keywords):
                filtered_emails.append(email)
        activity_data["emails"] = filtered_emails[:20]
        
        # Completed tasks (last 24h)
        tasks_completed_resp = supabase.table("tasks") \
            .select("id, title, description, priority, completed_at") \
            .not_.is_("completed_at", "null") \
            .gte("completed_at", cutoff.isoformat()) \
            .execute()
        activity_data["tasks_completed"] = tasks_completed_resp.data or []
        
        # Created tasks (last 24h)
        tasks_created_resp = supabase.table("tasks") \
            .select("id, title, description, priority, due_date, created_at") \
            .gte("created_at", cutoff.isoformat()) \
            .execute()
        activity_data["tasks_created"] = tasks_created_resp.data or []
        
        # Reflections (last 24h)
        reflections_resp = supabase.table("reflections") \
            .select("id, title, content, tags, created_at") \
            .gte("created_at", cutoff.isoformat()) \
            .order("created_at", desc=True) \
            .execute()
        activity_data["reflections"] = reflections_resp.data or []
        
        # Book highlights (last 24h) - NEW!
        highlights_resp = supabase.table("highlights") \
            .select("id, content, note, book_title, chapter, highlighted_at, is_favorite") \
            .gte("highlighted_at", cutoff.isoformat()) \
            .order("highlighted_at", desc=True) \
            .limit(20) \
            .execute()
        activity_data["highlights"] = highlights_resp.data or []
        
        # Reading progress - NEW!
        reading_data = get_reading_data_for_journal()
        if reading_data.get("currently_reading") or reading_data.get("todays_highlights"):
            activity_data["reading"] = {
                "currently_reading": reading_data.get("currently_reading", []),
                "todays_highlights": reading_data.get("todays_highlights", []),
                "recently_finished": reading_data.get("recent_finishes", [])
            }
        
        # Screen time (ActivityWatch)
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
        
        # New contacts (last 24h)
        contacts_resp = supabase.table("contacts") \
            .select("id, first_name, last_name, company, job_title, created_at") \
            .gte("created_at", cutoff.isoformat()) \
            .execute()
        activity_data["contacts_added"] = contacts_resp.data or []
        
        # Compute summary stats
        activity_data["summary"] = {
            "meetings_count": len(activity_data["meetings"]),
            "tasks_completed_count": len(activity_data["tasks_completed"]),
            "tasks_created_count": len(activity_data["tasks_created"]),
            "reflections_count": len(activity_data["reflections"]),
            "calendar_events_count": len(activity_data["calendar_events"]),
            "emails_count": len(activity_data["emails"]),
            "highlights_count": len(activity_data["highlights"]),
            "contacts_added_count": len(activity_data["contacts_added"]),
        }
        
        logger.info(f"Collected activity: {activity_data['summary']}")
        
        # =================================================================
        # 2. CALL INTELLIGENCE SERVICE FOR AI ANALYSIS
        # =================================================================
        
        async with httpx.AsyncClient(timeout=90.0) as client:
            # Try new endpoint first, fallback to legacy
            try:
                response = await client.post(
                    f"{INTELLIGENCE_SERVICE_URL}/api/v1/journal/evening-analysis",
                    json={
                        "activity_data": activity_data,
                        "timezone": "Asia/Singapore",
                        "user_name": "Bertan"
                    }
                )
                response.raise_for_status()
                result = response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    # Fallback to legacy endpoint
                    logger.info("Falling back to legacy journal endpoint")
                    response = await client.post(
                        f"{INTELLIGENCE_SERVICE_URL}/api/v1/journal/evening-prompt",
                        json={
                            "activity_data": activity_data,
                            "timezone": "Asia/Singapore"
                        }
                    )
                    response.raise_for_status()
                    result = response.json()
                else:
                    raise
        
        # =================================================================
        # 3. CREATE/UPDATE JOURNAL ENTRY IN SUPABASE
        # =================================================================
        
        journal_content = result.get("journal_content", "")
        if journal_content:
            # Check if journal exists for today
            existing_journal = supabase.table("journals") \
                .select("id, content") \
                .eq("date", today.isoformat()) \
                .execute()
            
            if existing_journal.data:
                # Update existing journal with AI content
                journal_id = existing_journal.data[0]["id"]
                current_content = existing_journal.data[0].get("content", "")
                
                # Prepend AI summary if not already there
                if "## AI Summary" not in current_content:
                    new_content = f"## AI Summary\n\n{journal_content}\n\n---\n\n{current_content}"
                    supabase.table("journals") \
                        .update({
                            "content": new_content,
                            "updated_at": now.isoformat(),
                            "last_sync_source": "supabase"
                        }) \
                        .eq("id", journal_id) \
                        .execute()
                    logger.info(f"Updated journal {journal_id} with AI summary")
            else:
                # Create new journal entry
                new_journal = {
                    "date": today.isoformat(),
                    "title": f"Journal - {today.strftime('%B %d, %Y')}",
                    "content": f"## AI Summary\n\n{journal_content}",
                    "last_sync_source": "supabase"
                }
                create_resp = supabase.table("journals").insert(new_journal).execute()
                if create_resp.data:
                    logger.info(f"Created new journal: {create_resp.data[0].get('id')}")
        
        # =================================================================
        # 4. SEND TO TELEGRAM
        # =================================================================
        
        message = result.get("message", "")
        if not message:
            # Fallback message construction
            message = f"""📓 **Evening Journal**
_{today.strftime('%A, %B %d, %Y')}_

Time to reflect on your day! Here's what I observed:

**Activities:** {activity_data['summary'].get('meetings_count', 0)} meetings, {activity_data['summary'].get('tasks_completed_count', 0)} tasks done

_Reply with a voice note or text to add your thoughts._"""
        
        await send_telegram_message(message)
        
        logger.info(f"Evening journal sent. Highlights: {len(result.get('highlights', []))}, Questions: {len(result.get('reflection_questions', result.get('reflection_prompts', [])))}")
        
        return {
            "status": "success",
            "highlights": result.get("highlights", []),
            "questions": result.get("reflection_questions", result.get("reflection_prompts", [])),
            "observations": result.get("observations", []),
            "activity_summary": activity_data["summary"]
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
