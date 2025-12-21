from datetime import datetime, timezone, timedelta
import urllib.parse

def format_rfc3339(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt_utc = dt.replace(microsecond=0)
    else:
        dt_utc = dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)

    return dt_utc.isoformat(timespec="seconds") + "Z"

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
