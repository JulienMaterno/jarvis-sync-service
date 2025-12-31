import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from lib.google_auth import get_access_token
from lib.utils import retry_on_error

GOOGLE_CALENDAR_API_BASE = "https://www.googleapis.com/calendar/v3"


def format_rfc3339(dt: datetime) -> str:
    """Normalize datetime to RFC3339 `YYYY-MM-DDTHH:MM:SSZ` format."""

    if dt.tzinfo is None:
        # Assume naive datetimes are already UTC
        dt_utc = dt.replace(microsecond=0)
    else:
        dt_utc = dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)

    return dt_utc.isoformat(timespec="seconds") + "Z"

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

    @retry_on_error()
    async def create_event(
        self,
        summary: str,
        start_time: datetime,
        end_time: datetime,
        description: str = None,
        location: str = None,
        attendees: List[str] = None,
        calendar_id: str = 'primary',
        timezone_str: str = None
    ) -> Dict[str, Any]:
        """
        Create a new calendar event.
        
        Args:
            summary: Event title
            start_time: Event start datetime
            end_time: Event end datetime  
            description: Event description/notes
            location: Event location
            attendees: List of email addresses to invite
            calendar_id: Calendar to create event in (default: primary)
            timezone_str: Timezone for the event (e.g., 'Asia/Singapore')
            
        Returns:
            Created event data from Google Calendar API
        """
        await self._ensure_token()
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        # Build event body
        event_body = {
            "summary": summary,
            "start": {
                "dateTime": format_rfc3339(start_time),
                "timeZone": timezone_str or "UTC"
            },
            "end": {
                "dateTime": format_rfc3339(end_time),
                "timeZone": timezone_str or "UTC"
            }
        }
        
        if description:
            event_body["description"] = description
            
        if location:
            event_body["location"] = location
            
        if attendees:
            event_body["attendees"] = [{"email": email} for email in attendees]
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{GOOGLE_CALENDAR_API_BASE}/calendars/{calendar_id}/events",
                headers=headers,
                json=event_body
            )
            
            if response.status_code == 401:
                # Token expired, refresh and retry
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.post(
                    f"{GOOGLE_CALENDAR_API_BASE}/calendars/{calendar_id}/events",
                    headers=headers,
                    json=event_body
                )
            
            response.raise_for_status()
            return response.json()
