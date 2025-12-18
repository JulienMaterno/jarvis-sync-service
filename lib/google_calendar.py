import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from lib.google_auth import get_access_token

GOOGLE_CALENDAR_API_BASE = "https://www.googleapis.com/calendar/v3"

class GoogleCalendarClient:
    def __init__(self):
        self.access_token = None

    async def _ensure_token(self):
        if not self.access_token:
            self.access_token = await get_access_token()

    async def list_events(self, 
                         calendar_id: str = 'primary', 
                         time_min: Optional[datetime] = None, 
                         time_max: Optional[datetime] = None,
                         single_events: bool = True,
                         max_results: int = 2500) -> Dict[str, Any]:
        """
        List events from a calendar.
        Returns {"events": [], "nextSyncToken": str}
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        params = {
            "singleEvents": str(single_events).lower(),
            "orderBy": "startTime",
            "maxResults": max_results
        }
        
        if time_min:
            params["timeMin"] = time_min.isoformat() + 'Z'
        if time_max:
            params["timeMax"] = time_max.isoformat() + 'Z'

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{GOOGLE_CALENDAR_API_BASE}/calendars/{calendar_id}/events",
                headers=headers,
                params=params
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.get(
                    f"{GOOGLE_CALENDAR_API_BASE}/calendars/{calendar_id}/events",
                    headers=headers,
                    params=params
                )
                
            response.raise_for_status()
            data = response.json()
            # Note: nextSyncToken is only returned on the last page of the result set.
            # If we had pagination, we'd need to loop. For now assuming < 2500 events.
            return {
                "events": data.get("items", []),
                "nextSyncToken": data.get("nextSyncToken") 
            }

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
