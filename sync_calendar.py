import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from lib.google_calendar import GoogleCalendarClient
from lib.supabase_client import supabase, find_contact_by_email, find_contact_by_name
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
                # When sync token expires, use shorter window to reduce API load
                # Only fetch recent past + near future (not 270 days)
                effective_past = 14 if token_invalid else days_past
                effective_future = 60 if token_invalid else days_future
                logger.info(f"Performing full sync (-{effective_past}d to +{effective_future}d){' (token expired, using shorter window)' if token_invalid else ''}")
                time_min = datetime.now(timezone.utc) - timedelta(days=effective_past)
                time_max = datetime.now(timezone.utc) + timedelta(days=effective_future)
                
                logger.info(f"Syncing calendar from {time_min} to {time_max}")
                
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

            # Build contact lookup cache: fetch all contacts once instead of per-event queries
            contact_cache_email = {}  # email -> contact_id
            contact_cache_name = {}   # "first last" -> contact_id
            try:
                contacts_resp = supabase.table("contacts").select("id,email,first_name,last_name").is_("deleted_at", "null").execute()
                for c in (contacts_resp.data or []):
                    if c.get("email"):
                        contact_cache_email[c["email"].strip().lower()] = c["id"]
                    fname = (c.get("first_name") or "").strip().lower()
                    lname = (c.get("last_name") or "").strip().lower()
                    if fname and lname:
                        contact_cache_name[f"{fname} {lname}"] = c["id"]
                    elif fname:
                        # Only store single-name if unique
                        if fname not in contact_cache_name:
                            contact_cache_name[fname] = c["id"]
                        else:
                            contact_cache_name[fname] = None  # Ambiguous, skip
                logger.info(f"Contact cache: {len(contact_cache_email)} emails, {len(contact_cache_name)} names")
            except Exception as e:
                logger.warning(f"Failed to build contact cache, falling back to per-event lookups: {e}")

            def lookup_contact_cached(email: str) -> Optional[str]:
                if not email:
                    return None
                clean = email.strip().lower()
                if '<' in clean and '>' in clean:
                    clean = clean[clean.find('<')+1:clean.find('>')]
                return contact_cache_email.get(clean)

            def lookup_contact_by_name_cached(name: str) -> Optional[str]:
                if not name:
                    return None
                clean = name.strip().lower()
                cid = contact_cache_name.get(clean)
                return cid if cid else None  # None means ambiguous or not found

            upsert_data = []
            for event in events:
                start = event.get('start', {})
                end = event.get('end', {})

                start_time = start.get('dateTime') or start.get('date')
                end_time = end.get('dateTime') or end.get('date')

                # Find contact from organizer or attendees using cached lookups
                organizer = event.get('organizer', {})
                attendees = event.get('attendees', [])

                contact_id = lookup_contact_cached(organizer.get('email'))
                if not contact_id and attendees:
                    for att in attendees:
                        if att.get('self'):
                            continue
                        contact_id = lookup_contact_cached(att.get('email'))
                        if contact_id:
                            break

                if not contact_id and attendees:
                    for att in attendees:
                        if att.get('self'):
                            continue
                        display_name = att.get('displayName')
                        if display_name:
                            contact_id = lookup_contact_by_name_cached(display_name)
                            if contact_id:
                                logger.info(f"Linked event '{event.get('summary', '')}' to contact via name: {display_name}")
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
                    "contact_id": contact_id
                }
                upsert_data.append(record)

            if upsert_data:
                # Upsert in batches of 100 to avoid payload limits
                batch_size = 100
                total_upserted = 0
                for i in range(0, len(upsert_data), batch_size):
                    batch = upsert_data[i:i+batch_size]
                    response = supabase.table("calendar_events").upsert(
                        batch, on_conflict="google_event_id"
                    ).execute()
                    total_upserted += len(batch)
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

            # Log and return results
            # Note: With upsert we can't distinguish created vs updated without pre-checking
            # But incremental sync (sync_token) means most are updates, full sync means mostly creates/updates
            await log_sync_event("calendar_sync", "success", f"Synced {len(upsert_data)} events")
            return {
                "status": "success", 
                "count": len(upsert_data),
                "sync_type": "incremental" if sync_token and not full_sync else "full",
                "events_processed": len(upsert_data)
            }

        except Exception as e:
            logger.error(f"Calendar sync failed: {str(e)}")
            await log_sync_event("calendar_sync", "error", str(e))
            raise e

async def run_calendar_sync():
    syncer = CalendarSync()
    return await syncer.sync_events()

if __name__ == "__main__":
    asyncio.run(run_calendar_sync())
