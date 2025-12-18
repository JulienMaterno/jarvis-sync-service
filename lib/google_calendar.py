import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from lib.google_auth import get_access_token
from lib.utils import retry_on_error

GOOGLE_CALENDAR_API_BASE = "https://www.googleapis.com/calendar/v3"

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
                # Ensure valid RFC3339 format. If aware, isoformat() includes offset.
                # If naive, we assume UTC and append Z.
                ts = time_min.isoformat()
                if time_min.tzinfo is None:
                    ts += 'Z'
                params["timeMin"] = ts
            if time_max:
                ts = time_max.isoformat()
                if time_max.tzinfo is None:
                    ts += 'Z'
                params["timeMax"] = ts

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
                try:
                    error_data = response.json()
                    raise ValueError(f"Bad Request from Google Calendar API: {error_data}")
                except:
                    raise ValueError(f"Bad Request from Google Calendar API: {response.text}")
                
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
