import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from lib.google_auth import get_access_token
from lib.utils import retry_on_error

GOOGLE_CALENDAR_API_BASE = "https://www.googleapis.com/calendar/v3"


def format_rfc3339(dt: datetime) -> str:
    """
    Format a datetime object to RFC3339 format for Google Calendar API.
    Ensures proper UTC format with 'Z' suffix (no +00:00 offset).
    Google Calendar API is strict - no microseconds, no +00:00 offset.
    """
    # Convert to UTC if timezone-aware
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    # Strip microseconds and timezone info, then format with Z suffix
    dt = dt.replace(microsecond=0, tzinfo=None)
    # Format without microseconds, with Z suffix for UTC
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

class GoogleCalendarClient:
    def __init__(self):
        self.access_token = None

    async def _ensure_token(self):
        if not self.access_token:
            self.access_token = await get_access_token()

    @retry_on_error()
    async def list_events(self, 
                         calendar_id: str = 'primary', 
                         time_min: Optional[datetime] = None, 
                         time_max: Optional[datetime] = None,
                         single_events: bool = True,
                         max_results: int = 2500,
                         sync_token: Optional[str] = None) -> Dict[str, Any]:
        """
        List events from a calendar.
        Returns {"events": [], "nextSyncToken": str}
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        params = {
            "maxResults": max_results
        }

        if sync_token:
            params["syncToken"] = sync_token
        else:
            # These parameters are incompatible with syncToken
            params["singleEvents"] = str(single_events).lower()
            params["orderBy"] = "startTime"
            if time_min:
                # Google Calendar API requires RFC3339 format with Z suffix for UTC
                params["timeMin"] = format_rfc3339(time_min)
            if time_max:
                params["timeMax"] = format_rfc3339(time_max)

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{GOOGLE_CALENDAR_API_BASE}/calendars/{calendar_id}/events",
                headers=headers,
                params=params
            )
            
            if response.status_code == 410: # Gone (Sync token expired)
                return {"events": [], "nextSyncToken": None, "expired": True}

            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.get(
                    f"{GOOGLE_CALENDAR_API_BASE}/calendars/{calendar_id}/events",
                    headers=headers,
                    params=params
                )
            
            if response.status_code == 400:
                # Log the actual error for debugging
                error_info = f"URL: {response.url}"
                try:
                    error_data = response.json()
                    error_info += f", Response: {error_data}"
                except Exception:
                    error_info += f", Response: {response.text}"
                raise ValueError(f"Bad Request (400) from Google Calendar API. {error_info}. Try clearing the calendar sync token.")
                
            response.raise_for_status()
            data = response.json()
            # Note: nextSyncToken is only returned on the last page of the result set.
            # If we had pagination, we'd need to loop. For now assuming < 2500 events.
            return {
                "events": data.get("items", []),
                "nextSyncToken": data.get("nextSyncToken"),
                "expired": False
            }

    @retry_on_error()
    async def get_event(self, calendar_id: str, event_id: str) -> Dict[str, Any]:
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{GOOGLE_CALENDAR_API_BASE}/calendars/{calendar_id}/events/{event_id}",
                headers=headers
            )
            response.raise_for_status()
            return response.json()
