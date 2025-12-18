#!/usr/bin/env python3
"""Test calendar API call"""

import asyncio
from datetime import datetime, timedelta, timezone
from lib.google_calendar import GoogleCalendarClient

async def test_calendar():
    print("Testing calendar API...\n")
    
    client = GoogleCalendarClient()
    
    # Test with proper time_min
    time_min = datetime.now(timezone.utc) - timedelta(days=90)
    time_max = datetime.now(timezone.utc) + timedelta(days=180)
    
    print(f"time_min: {time_min.isoformat()}")
    print(f"time_max: {time_max.isoformat()}")
    
    try:
        result = await client.list_events(
            time_min=time_min,
            time_max=time_max,
            max_results=10  # Just get 10 to test
        )
        print(f"\n✅ Success! Got {len(result['events'])} events")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_calendar())
