import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from lib.google_calendar import GoogleCalendarClient
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

logger = logging.getLogger("CalendarSync")
logging.basicConfig(level=logging.INFO)

class CalendarSync:
    def __init__(self):
        self.google_client = GoogleCalendarClient()

    async def sync_events(self, days_past: int = 90, days_future: int = 180):
        """
        Syncs calendar events from Google to Supabase.
        """
        try:
            logger.info(f"Starting calendar sync (-{days_past}d to +{days_future}d)")
            
            time_min = datetime.now(timezone.utc) - timedelta(days=days_past)
            time_max = datetime.now(timezone.utc) + timedelta(days=days_future)
            
            events = await self.google_client.list_events(
                time_min=time_min,
                time_max=time_max
            )
            
            logger.info(f"Found {len(events)} events in Google Calendar")
            
            upsert_data = []
            for event in events:
                # Skip cancelled events if we want, but keeping them with status='cancelled' is better
                
                start = event.get('start', {})
                end = event.get('end', {})
                
                # Handle all-day events (date only) vs timed events (dateTime)
                start_time = start.get('dateTime') or start.get('date')
                end_time = end.get('dateTime') or end.get('date')
                
                # If it's just a date, it might not parse directly into timestamptz in some DBs without casting,
                # but Supabase/Postgres usually handles ISO strings well. 
                # For all-day events, 'date' is YYYY-MM-DD. We might want to append T00:00:00Z for consistency if needed.
                
                record = {
                    "google_event_id": event['id'],
                    "calendar_id": "primary",
                    "summary": event.get('summary', ''),
                    "description": event.get('description', ''),
                    "start_time": start_time,
                    "end_time": end_time,
                    "location": event.get('location', ''),
                    "status": event.get('status', ''),
                    "html_link": event.get('htmlLink', ''),
                    "attendees": event.get('attendees', []),
                    "creator": event.get('creator', {}),
                    "organizer": event.get('organizer', {}),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "last_sync_at": datetime.now(timezone.utc).isoformat()
                }
                upsert_data.append(record)

            if upsert_data:
                # Upsert in batches of 100 to avoid payload limits
                batch_size = 100
                for i in range(0, len(upsert_data), batch_size):
                    batch = upsert_data[i:i+batch_size]
                    response = supabase.table("calendar_events").upsert(
                        batch, on_conflict="google_event_id"
                    ).execute()
                    logger.info(f"Upserted batch {i//batch_size + 1}: {len(batch)} events")

            await log_sync_event("calendar_sync", "success", f"Synced {len(upsert_data)} events")
            return {"status": "success", "count": len(upsert_data)}

        except Exception as e:
            logger.error(f"Calendar sync failed: {str(e)}")
            await log_sync_event("calendar_sync", "error", str(e))
            raise e

async def run_calendar_sync():
    syncer = CalendarSync()
    return await syncer.sync_events()

if __name__ == "__main__":
    asyncio.run(run_calendar_sync())
