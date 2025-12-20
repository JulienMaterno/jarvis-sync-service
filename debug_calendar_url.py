from datetime import datetime, timezone, timedelta
import urllib.parse

def format_rfc3339(dt: datetime) -> str:
    """
    Format a datetime object to RFC3339 format for Google Calendar API.
    Ensures proper UTC format with 'Z' suffix (no +00:00 offset).
    Google Calendar API is strict - no microseconds, no +00:00 offset.
    """
    try:
        # Convert to UTC if timezone-aware
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc)
        
        # Create a new naive datetime in UTC
        dt_utc = datetime(
            year=dt.year,
            month=dt.month,
            day=dt.day,
            hour=dt.hour,
            minute=dt.minute,
            second=dt.second
        )
        
        # Format with Z suffix
        formatted_date = dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        return formatted_date
    except Exception as e:
        print(f"Error in format_rfc3339: {e}")
        # Fallback
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

def simulate_request():
    days_past = 90
    days_future = 180
    
    time_min = datetime.now(timezone.utc) - timedelta(days=days_past)
    time_max = datetime.now(timezone.utc) + timedelta(days=days_future)
    
    print(f"Original time_min: {time_min}")
    print(f"Original time_max: {time_max}")
    
    formatted_min = format_rfc3339(time_min)
    formatted_max = format_rfc3339(time_max)
    
    print(f"Formatted time_min: {formatted_min}")
    print(f"Formatted time_max: {formatted_max}")
    
    params = {
        "singleEvents": "true",
        "orderBy": "startTime",
        "maxResults": 2500,
        "timeMin": formatted_min,
        "timeMax": formatted_max
    }
    
    # Simulate URL encoding
    query_string = urllib.parse.urlencode(params)
    print(f"Query string: {query_string}")
    
    # Check for the bad pattern
    if "%2B00%3A00" in query_string or "+00:00" in query_string:
        print("FAIL: Found +00:00 in query string")
    elif "." in formatted_min and "Z" in formatted_min:
         print("FAIL: Found microseconds in query string")
    else:
        print("SUCCESS: URL looks correct")

if __name__ == "__main__":
    simulate_request()
