"""
==============================================================================
EVENING JOURNAL GENERATION SYSTEM
==============================================================================

This module implements a sophisticated evening journal system that:

1. COLLECTS all daily activity (last 24 hours):
   - Meetings, tasks, reflections
   - Calendar events, emails
   - Book highlights and reading progress
   - Screen time (ActivityWatch)

2. GENERATES an AI-powered journal entry:
   - Summarizes what happened
   - Highlights key moments
   - Asks thoughtful reflection questions

3. SENDS via Telegram for user feedback:
   - User can reply with voice/text
   - Responses are appended to the journal

4. CREATES/UPDATES journal in Supabase:
   - Journal entry stored in database
   - Synced to Notion via sync service

Architecture:
    Scheduler (9pm) → Sync Service → Intelligence Service → Telegram
                                           ↓
                                     Supabase (journal)
                                           ↓
                                     Notion (via sync)
"""

import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any
import httpx

logger = logging.getLogger("JarvisJournal")

# Environment
INTELLIGENCE_SERVICE_URL = os.environ.get(
    "INTELLIGENCE_SERVICE_URL",
    "https://jarvis-intelligence-service-776871804948.asia-southeast1.run.app"
)
TELEGRAM_BOT_URL = os.environ.get(
    "TELEGRAM_BOT_URL", 
    "https://jarvis-telegram-bot-776871804948.asia-southeast1.run.app"
)
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
INTERNAL_API_KEY = os.environ.get("INTERNAL_API_KEY", "")


class DailyActivityCollector:
    """
    Collects all activities from the last 24 hours from various sources.
    This is the "eyes" of the system - gathering everything that happened.
    """
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.lookback_hours = 24
    
    def set_lookback(self, hours: int):
        """Set how many hours to look back (default 24)."""
        self.lookback_hours = hours
    
    def collect_all(self) -> Dict[str, Any]:
        """Collect all activity data from the last 24 hours."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=self.lookback_hours)
        
        activity = {
            "collection_time": now.isoformat(),
            "lookback_hours": self.lookback_hours,
            "meetings": self._get_meetings(cutoff, now),
            "tasks_completed": self._get_completed_tasks(cutoff, now),
            "tasks_created": self._get_created_tasks(cutoff, now),
            "reflections": self._get_reflections(cutoff, now),
            "calendar_events": self._get_calendar_events(cutoff, now),
            "emails": self._get_emails(cutoff, now),
            "reading": self._get_reading_activity(cutoff, now),
            "highlights": self._get_book_highlights(cutoff, now),
            "contacts_added": self._get_new_contacts(cutoff, now),
            "existing_journals": self._get_existing_journals(now.date()),
        }
        
        # Add summary stats
        activity["summary"] = self._compute_summary(activity)
        
        return activity
    
    def _get_meetings(self, start: datetime, end: datetime) -> List[Dict]:
        """Get meetings from the time period."""
        try:
            response = self.supabase.table("meetings") \
                .select("id, title, summary, contact_name, people_mentioned, topics_discussed, date, created_at") \
                .gte("created_at", start.isoformat()) \
                .lte("created_at", end.isoformat()) \
                .order("date", desc=True) \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get meetings: {e}")
            return []
    
    def _get_completed_tasks(self, start: datetime, end: datetime) -> List[Dict]:
        """Get tasks completed in the time period."""
        try:
            response = self.supabase.table("tasks") \
                .select("id, title, description, priority, project, completed_at") \
                .not_.is_("completed_at", "null") \
                .gte("completed_at", start.isoformat()) \
                .lte("completed_at", end.isoformat()) \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get completed tasks: {e}")
            return []
    
    def _get_created_tasks(self, start: datetime, end: datetime) -> List[Dict]:
        """Get tasks created in the time period."""
        try:
            response = self.supabase.table("tasks") \
                .select("id, title, description, priority, due_date, project, created_at") \
                .gte("created_at", start.isoformat()) \
                .lte("created_at", end.isoformat()) \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get created tasks: {e}")
            return []
    
    def _get_reflections(self, start: datetime, end: datetime) -> List[Dict]:
        """Get reflections recorded in the time period."""
        try:
            response = self.supabase.table("reflections") \
                .select("id, title, content, tags, mood, energy_level, created_at") \
                .gte("created_at", start.isoformat()) \
                .lte("created_at", end.isoformat()) \
                .order("created_at", desc=True) \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get reflections: {e}")
            return []
    
    def _get_calendar_events(self, start: datetime, end: datetime) -> List[Dict]:
        """Get calendar events in the time period."""
        try:
            response = self.supabase.table("calendar_events") \
                .select("id, summary, description, location, attendees, start_time, end_time") \
                .gte("start_time", start.isoformat()) \
                .lte("start_time", end.isoformat()) \
                .order("start_time") \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get calendar events: {e}")
            return []
    
    def _get_emails(self, start: datetime, end: datetime, limit: int = 30) -> List[Dict]:
        """Get emails from the time period (filtered for meaningful ones)."""
        try:
            response = self.supabase.table("emails") \
                .select("id, subject, sender, snippet, date, contact_id") \
                .gte("date", start.isoformat()) \
                .lte("date", end.isoformat()) \
                .order("date", desc=True) \
                .limit(limit) \
                .execute()
            
            # Filter out automated/newsletter emails
            skip_keywords = {"unsubscribe", "newsletter", "noreply", "no-reply", 
                          "github", "notification", "automated", "donotreply"}
            emails = []
            for email in (response.data or []):
                subject = (email.get("subject") or "").lower()
                sender = (email.get("sender") or "").lower()
                if not any(kw in subject or kw in sender for kw in skip_keywords):
                    emails.append(email)
            
            return emails
        except Exception as e:
            logger.warning(f"Failed to get emails: {e}")
            return []
    
    def _get_reading_activity(self, start: datetime, end: datetime) -> Dict:
        """Get reading activity - books currently reading and recently finished."""
        reading = {
            "currently_reading": [],
            "recently_finished": [],
            "started_today": []
        }
        
        try:
            # Currently reading
            reading_resp = self.supabase.table("books") \
                .select("id, title, author, status, rating, current_page, total_pages, started_at") \
                .eq("status", "Reading") \
                .execute()
            
            for book in (reading_resp.data or []):
                progress = 0
                if book.get("total_pages") and book.get("current_page"):
                    progress = round((book["current_page"] / book["total_pages"]) * 100)
                reading["currently_reading"].append({
                    "id": book["id"],
                    "title": book.get("title"),
                    "author": book.get("author"),
                    "progress_percent": progress
                })
            
            # Recently finished (in last 24h)
            finished_resp = self.supabase.table("books") \
                .select("id, title, author, rating, finished_at") \
                .eq("status", "Finished") \
                .gte("finished_at", start.isoformat()) \
                .execute()
            reading["recently_finished"] = finished_resp.data or []
            
            # Started today
            started_resp = self.supabase.table("books") \
                .select("id, title, author") \
                .gte("started_at", start.isoformat()) \
                .execute()
            reading["started_today"] = started_resp.data or []
            
        except Exception as e:
            logger.warning(f"Failed to get reading activity: {e}")
        
        return reading
    
    def _get_book_highlights(self, start: datetime, end: datetime) -> List[Dict]:
        """Get book highlights from the time period."""
        try:
            response = self.supabase.table("highlights") \
                .select("id, content, note, book_title, chapter, highlighted_at, is_favorite") \
                .gte("highlighted_at", start.isoformat()) \
                .lte("highlighted_at", end.isoformat()) \
                .order("highlighted_at", desc=True) \
                .limit(20) \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get highlights: {e}")
            return []
    
    def _get_new_contacts(self, start: datetime, end: datetime) -> List[Dict]:
        """Get contacts added in the time period."""
        try:
            response = self.supabase.table("contacts") \
                .select("id, first_name, last_name, company, job_title, created_at") \
                .gte("created_at", start.isoformat()) \
                .lte("created_at", end.isoformat()) \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get new contacts: {e}")
            return []
    
    def _get_existing_journals(self, date) -> List[Dict]:
        """Get any existing journal entries for today."""
        try:
            response = self.supabase.table("journals") \
                .select("*") \
                .eq("date", date.isoformat()) \
                .execute()
            return response.data or []
        except Exception as e:
            logger.warning(f"Failed to get existing journals: {e}")
            return []
    
    def _compute_summary(self, activity: Dict) -> Dict:
        """Compute a summary of all activities."""
        return {
            "meetings_count": len(activity.get("meetings", [])),
            "tasks_completed_count": len(activity.get("tasks_completed", [])),
            "tasks_created_count": len(activity.get("tasks_created", [])),
            "reflections_count": len(activity.get("reflections", [])),
            "calendar_events_count": len(activity.get("calendar_events", [])),
            "emails_count": len(activity.get("emails", [])),
            "highlights_count": len(activity.get("highlights", [])),
            "contacts_added_count": len(activity.get("contacts_added", [])),
            "books_currently_reading": len(activity.get("reading", {}).get("currently_reading", [])),
            "books_finished_today": len(activity.get("reading", {}).get("recently_finished", [])),
        }


class JournalEntryManager:
    """
    Manages journal entries - creating, updating, and storing them.
    """
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
    
    def get_or_create_journal(self, date) -> Dict:
        """Get existing journal for date or create new one."""
        try:
            response = self.supabase.table("journals") \
                .select("*") \
                .eq("date", date.isoformat()) \
                .execute()
            
            if response.data:
                return response.data[0]
            
            # Create new journal entry
            new_journal = {
                "date": date.isoformat(),
                "title": f"Journal - {date.strftime('%B %d, %Y')}",
                "content": "",
                "last_sync_source": "supabase"
            }
            
            create_resp = self.supabase.table("journals") \
                .insert(new_journal) \
                .execute()
            
            return create_resp.data[0] if create_resp.data else new_journal
            
        except Exception as e:
            logger.error(f"Failed to get/create journal: {e}")
            raise
    
    def update_journal_content(self, journal_id: str, content: str, ai_summary: str = None) -> Dict:
        """Update journal with new content."""
        try:
            update_data = {
                "content": content,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "last_sync_source": "supabase"
            }
            
            if ai_summary:
                update_data["ai_summary"] = ai_summary
            
            response = self.supabase.table("journals") \
                .update(update_data) \
                .eq("id", journal_id) \
                .execute()
            
            return response.data[0] if response.data else {}
            
        except Exception as e:
            logger.error(f"Failed to update journal: {e}")
            raise
    
    def append_user_notes(self, journal_id: str, user_notes: str) -> Dict:
        """Append user's personal notes to the journal."""
        try:
            # First get current content
            response = self.supabase.table("journals") \
                .select("content") \
                .eq("id", journal_id) \
                .execute()
            
            current_content = response.data[0].get("content", "") if response.data else ""
            
            # Append user notes with timestamp
            timestamp = datetime.now(timezone.utc).strftime("%H:%M")
            separator = "\n\n---\n\n" if current_content else ""
            new_content = f"{current_content}{separator}## Personal Notes ({timestamp})\n\n{user_notes}"
            
            return self.update_journal_content(journal_id, new_content)
            
        except Exception as e:
            logger.error(f"Failed to append user notes: {e}")
            raise


class TelegramFeedbackLoop:
    """
    Handles the Telegram interaction for journal feedback.
    Sends the AI summary and collects user responses.
    """
    
    def __init__(self, bot_url: str, chat_id: str):
        self.bot_url = bot_url
        self.chat_id = chat_id
    
    async def send_journal_prompt(self, message: str, journal_id: str = None) -> bool:
        """Send the evening journal prompt to the user."""
        try:
            # Add callback context for the bot to track responses
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "context": {
                    "type": "evening_journal",
                    "journal_id": journal_id,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            }
            
            headers = {}
            if INTERNAL_API_KEY:
                headers["X-API-Key"] = INTERNAL_API_KEY
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.bot_url}/send_message",
                    json=payload,
                    headers=headers,
                )
                response.raise_for_status()
                return True
                
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False


def format_journal_message(
    ai_analysis: Dict,
    activity_summary: Dict,
    reading_data: Dict = None,
    highlights: List[Dict] = None
) -> str:
    """
    Format the evening journal message for Telegram.
    
    Creates a well-structured message with:
    - Day summary
    - Key highlights
    - Reading progress & book highlights
    - Reflection questions
    """
    now = datetime.now(timezone.utc)
    lines = []
    
    # Header
    lines.append(f"📓 **Evening Journal**")
    lines.append(f"_{now.strftime('%A, %B %d, %Y')}_")
    lines.append("")
    
    # Day at a Glance
    lines.append("**📊 Your Day at a Glance:**")
    summary = activity_summary
    summary_items = []
    
    if summary.get("meetings_count", 0) > 0:
        summary_items.append(f"🤝 {summary['meetings_count']} meeting(s)")
    if summary.get("tasks_completed_count", 0) > 0:
        summary_items.append(f"✅ {summary['tasks_completed_count']} task(s) completed")
    if summary.get("tasks_created_count", 0) > 0:
        summary_items.append(f"📝 {summary['tasks_created_count']} new task(s)")
    if summary.get("emails_count", 0) > 0:
        summary_items.append(f"📧 {summary['emails_count']} meaningful email(s)")
    if summary.get("reflections_count", 0) > 0:
        summary_items.append(f"💭 {summary['reflections_count']} reflection(s)")
    
    if summary_items:
        lines.extend([f"• {item}" for item in summary_items])
    else:
        lines.append("• A quiet day with no recorded activities")
    lines.append("")
    
    # AI-Generated Highlights
    ai_highlights = ai_analysis.get("highlights", [])
    if ai_highlights:
        lines.append("**✨ Key Moments:**")
        for highlight in ai_highlights[:5]:
            lines.append(f"• {highlight}")
        lines.append("")
    
    # Meetings summary
    meetings = ai_analysis.get("meetings", [])
    if meetings:
        lines.append("**🤝 Meetings:**")
        for meeting in meetings[:5]:
            lines.append(f"• {meeting}")
        lines.append("")
    
    # Reading Section
    if reading_data:
        currently_reading = reading_data.get("currently_reading", [])
        recently_finished = reading_data.get("recently_finished", [])
        
        if currently_reading or recently_finished or highlights:
            lines.append("**📚 Reading:**")
            
            if currently_reading:
                for book in currently_reading[:3]:
                    title = book.get("title", "Unknown")
                    progress = book.get("progress_percent", 0)
                    lines.append(f"• {title}: {progress}% complete")
            
            if recently_finished:
                for book in recently_finished[:2]:
                    title = book.get("title", "Unknown")
                    rating = book.get("rating")
                    rating_str = f" ({'⭐' * int(rating)})" if rating else ""
                    lines.append(f"• Finished: {title}{rating_str}")
            
            lines.append("")
    
    # Book Highlights
    if highlights and len(highlights) > 0:
        lines.append("**💡 Today's Book Highlights:**")
        for h in highlights[:3]:
            content = h.get("content", "")[:120]
            book = h.get("book_title", "")
            if content:
                lines.append(f'_"{content}..."_')
                if book:
                    lines.append(f"  — {book}")
                if h.get("note"):
                    lines.append(f"  📝 {h['note'][:80]}")
        lines.append("")
    
    # AI-Generated Reflection Questions (the key differentiator!)
    questions = ai_analysis.get("reflection_questions", [])
    if questions:
        lines.append("**🤔 Questions for Reflection:**")
        for q in questions[:4]:
            lines.append(f"• {q}")
        lines.append("")
    
    # Footer with call to action
    lines.append("---")
    lines.append("_Reply with a voice note or text to add your thoughts to today's journal._")
    
    return "\n".join(lines)
