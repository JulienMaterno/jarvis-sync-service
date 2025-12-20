import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from lib.google_calendar import GoogleCalendarClient
from lib.supabase_client import supabase, find_contact_by_email
from lib.logging_service import log_sync_event

logger = logging.getLogger("CalendarSync")
logging.basicConfig(level=logging.INFO)

class CalendarSync:
    def __init__(self):
        self.google_client = GoogleCalendarClient()

    async def get_sync_token(self) -> Optional[str]:
        try:
            response = supabase.table("sync_state").select("value").eq("key", "calendar_sync_token").execute()
            if response.data:
                return response.data[0]["value"]
            return None
        except Exception:
            return None

    async def save_sync_token(self, token: str):
        try:
            supabase.table("sync_state").upsert({
                "key": "calendar_sync_token",
                "value": token,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).execute()
        except Exception as e:
            logger.warning(f"Failed to save sync token: {e}")

    async def sync_events(self, days_past: int = 90, days_future: int = 180):
        """
        Syncs calendar events from Google to Supabase.
        Uses syncToken for incremental syncs if available.
        """
        try:
            logger.info(f"Starting calendar sync...")
            
            sync_token = await self.get_sync_token()
            events = []
            next_sync_token = None
            full_sync = False
            token_invalid = False

            if sync_token:
                logger.info("Found sync token, attempting incremental sync")
                try:
                    result = await self.google_client.list_events(sync_token=sync_token)
                    
                    if result.get("expired"):
                        logger.info("Sync token expired, falling back to full sync")
                        full_sync = True
                        token_invalid = True
                    else:
                        events = result["events"]
                        next_sync_token = result.get("nextSyncToken")
                except Exception as e:
                    logger.warning(f"Incremental sync failed ({e}), falling back to full sync")
                    full_sync = True
                    token_invalid = True
            else:
                full_sync = True

            if full_sync:
                logger.info(f"Performing full sync (-{days_past}d to +{days_future}d)")
                time_min = datetime.now(timezone.utc) - timedelta(days=days_past)
                time_max = datetime.now(timezone.utc) + timedelta(days=days_future)
                
                try:
                    result = await self.google_client.list_events(
                        time_min=time_min,
                        time_max=time_max
                    )
                    events = result["events"]
                    next_sync_token = result.get("nextSyncToken")
                except ValueError as e:
                    # If we get a 400 Bad Request, clear any potentially corrupted sync token
                    if "400" in str(e) or "Bad Request" in str(e):
                        logger.warning(f"Got 400 error during full sync, clearing sync token: {e}")
                        try:
                            supabase.table("sync_state").delete().eq("key", "calendar_sync_token").execute()
                            logger.info("Cleared sync token due to 400 error")
                        except Exception as clear_err:
                            logger.error(f"Failed to clear sync token: {clear_err}")
                    raise

            logger.info(f"Found {len(events)} events in Google Calendar")
            
            upsert_data = []
            for event in events:
                # Skip cancelled events if we want, but keeping them with status='cancelled' is better
                
                start = event.get('start', {})
                end = event.get('end', {})
                
                # Handle all-day events (date only) vs timed events (dateTime)
                start_time = start.get('dateTime') or start.get('date')
                end_time = end.get('dateTime') or end.get('date')
                
                # Find contact from organizer or first attendee
                organizer = event.get('organizer', {})
                attendees = event.get('attendees', [])
                contact_id = find_contact_by_email(organizer.get('email'))
                if not contact_id and attendees:
                    for att in attendees:
                        contact_id = find_contact_by_email(att.get('email'))
                        if contact_id:
                            break
                
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
                    "last_sync_at": datetime.now(timezone.utc).isoformat(),
                    "contact_id": contact_id  # Auto-link to contact
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

            # Save sync token for next time
            if next_sync_token:
                await self.save_sync_token(next_sync_token)
                logger.info("Saved next sync token")
            elif token_invalid:
                # Clear invalid token from database
                try:
                    supabase.table("sync_state").delete().eq("key", "calendar_sync_token").execute()
                    logger.info("Cleared invalid sync token")
                except Exception as e:
                    logger.warning(f"Failed to clear invalid token: {e}")

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
