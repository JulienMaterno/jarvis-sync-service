import logging
from datetime import datetime, timedelta, timezone
from lib.supabase_client import supabase
from lib.telegram_client import send_telegram_message

logger = logging.getLogger(__name__)


async def generate_evening_journal_prompt():
    """
    Generates an evening journal prompt with topics based on the day's activities.
    Combines the daily report with journal suggestions.
    """
    try:
        logger.info("Generating evening journal prompt...")
        
        today = datetime.now(timezone.utc).date()
        today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
        today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=timezone.utc)
        
        # Gather today's activities
        topics = []
        people = set()
        
        # 1. Today's meetings
        meetings_resp = supabase.table("meetings") \
            .select("title, summary, people_mentioned") \
            .gte("date", today_start.isoformat()) \
            .lte("date", today_end.isoformat()) \
            .execute()
        
        for m in meetings_resp.data or []:
            if m.get("title"):
                topics.append(f"📅 Meeting: {m['title']}")
            if m.get("people_mentioned"):
                for p in m["people_mentioned"]:
                    people.add(p)
        
        # 2. Today's calendar events
        events_resp = supabase.table("calendar_events") \
            .select("summary, attendees") \
            .gte("start_time", today_start.isoformat()) \
            .lte("start_time", today_end.isoformat()) \
            .execute()
        
        for e in events_resp.data or []:
            if e.get("summary") and e["summary"] not in [t.split(": ", 1)[-1] for t in topics]:
                topics.append(f"🗓️ Event: {e['summary']}")
            if e.get("attendees"):
                for att in e["attendees"]:
                    if att.get("displayName"):
                        people.add(att["displayName"])
        
        # 3. Today's emails (important ones)
        emails_resp = supabase.table("emails") \
            .select("subject, sender") \
            .gte("date", today_start.isoformat()) \
            .lte("date", today_end.isoformat()) \
            .limit(10) \
            .execute()
        
        email_subjects = []
        for e in emails_resp.data or []:
            if e.get("subject"):
                # Skip newsletters and automated emails
                subject = e["subject"]
                skip_keywords = ["unsubscribe", "newsletter", "automated", "noreply", "no-reply"]
                if not any(kw in subject.lower() for kw in skip_keywords):
                    email_subjects.append(subject[:50])
        
        if email_subjects:
            topics.append(f"📧 Emails about: {', '.join(email_subjects[:3])}")
        
        # 4. Today's completed tasks
        tasks_resp = supabase.table("tasks") \
            .select("title") \
            .eq("completed", True) \
            .gte("updated_at", today_start.isoformat()) \
            .lte("updated_at", today_end.isoformat()) \
            .execute()
        
        completed_tasks = [t["title"] for t in (tasks_resp.data or []) if t.get("title")]
        if completed_tasks:
            topics.append(f"✅ Completed: {', '.join(completed_tasks[:3])}")
        
        # 5. Build the message
        now = datetime.now(timezone.utc)
        greeting = "Good evening" if now.hour >= 17 else "Hi"
        
        message = f"""📓 **{greeting}! Time for your evening journal**

🕐 _{now.strftime('%A, %B %d')} at {now.strftime('%H:%M')}_

"""
        
        if topics:
            message += "**Today's highlights to reflect on:**\n"
            for topic in topics[:6]:  # Limit to 6 topics
                message += f"• {topic}\n"
            message += "\n"
        
        if people:
            people_list = list(people)[:5]  # Limit to 5 people
            message += f"**People you connected with:** {', '.join(people_list)}\n\n"
        
        message += """**Journal prompts:**
• What went well today?
• What could have gone better?
• What are you grateful for?
• What's one thing you learned?
• What's on your mind for tomorrow?

_Reply with a voice note or text to journal!_ 🎙️"""

        await send_telegram_message(message)
        logger.info("Evening journal prompt sent.")
        return {"status": "success", "topics": len(topics), "people": len(people)}
        
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
