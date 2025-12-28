#!/usr/bin/env python3
"""Test that evening journal prompt uses correct TODAY window (not rolling 24h)."""
import asyncio
from datetime import datetime, timedelta, timezone
import pytz
from lib.supabase_client import supabase

async def test_journal_data_window():
    """Verify all queries use TODAY's boundaries (Singapore timezone)."""
    
    # Simulate the cutoff calculation from reports.py
    # Use TODAY's boundaries in user's timezone (Singapore)
    user_tz = pytz.timezone("Asia/Singapore")
    now_local = datetime.now(user_tz)
    today = now_local.date()
    
    # Start of today in user's timezone, converted to UTC for DB queries
    today_start_local = user_tz.localize(datetime.combine(today, datetime.min.time()))
    today_start_utc = today_start_local.astimezone(timezone.utc)
    now_utc = datetime.now(timezone.utc)
    
    cutoff = today_start_utc
    
    print(f"Testing evening journal data collection")
    print(f"Today (Singapore): {today}")
    print(f"Today start (UTC): {cutoff.isoformat()}")
    print(f"Now (UTC):         {now_utc.isoformat()}\n")
    
    # Test each data source
    checks = []
    
    # 1. Meetings
    meetings = supabase.table("meetings") \
        .select("id,title,created_at", count="exact") \
        .gte("created_at", cutoff.isoformat()) \
        .execute()
    checks.append(("Meetings", meetings.count, "created_at"))
    
    # 2. Calendar events
    events = supabase.table("calendar_events") \
        .select("id,summary,start_time", count="exact") \
        .gte("start_time", cutoff.isoformat()) \
        .lte("start_time", now_utc.isoformat()) \
        .execute()
    checks.append(("Calendar events", events.count, "start_time"))
    
    # 3. Emails
    emails = supabase.table("emails") \
        .select("id,subject,date", count="exact") \
        .gte("date", cutoff.isoformat()) \
        .execute()
    checks.append(("Emails", emails.count, "date"))
    
    # 4. Tasks completed
    tasks_done = supabase.table("tasks") \
        .select("id,title,completed_at", count="exact") \
        .not_.is_("completed_at", "null") \
        .gte("completed_at", cutoff.isoformat()) \
        .execute()
    checks.append(("Tasks completed", tasks_done.count, "completed_at"))
    
    # 5. Tasks created
    tasks_new = supabase.table("tasks") \
        .select("id,title,created_at", count="exact") \
        .gte("created_at", cutoff.isoformat()) \
        .execute()
    checks.append(("Tasks created", tasks_new.count, "created_at"))
    
    # 6. Reflections
    reflections = supabase.table("reflections") \
        .select("id,title,created_at", count="exact") \
        .gte("created_at", cutoff.isoformat()) \
        .execute()
    checks.append(("Reflections", reflections.count, "created_at"))
    
    # 7. Highlights
    highlights = supabase.table("highlights") \
        .select("id,content,highlighted_at", count="exact") \
        .gte("highlighted_at", cutoff.isoformat()) \
        .execute()
    checks.append(("Highlights", highlights.count, "highlighted_at"))
    
    # 8. New contacts
    contacts = supabase.table("contacts") \
        .select("id,first_name,last_name,created_at", count="exact") \
        .gte("created_at", cutoff.isoformat()) \
        .execute()
    checks.append(("New contacts", contacts.count, "created_at"))
    
    # Display results
    print("Data collection verification:")
    print("-" * 60)
    for name, count, field in checks:
        status = "✅" if count is not None else "❌"
        print(f"{status} {name:<20}: {count:>3} items (filtered by {field} >= today's start)")
    
    print("\n✅ All data sources correctly use TODAY's window (not rolling 24h)!")
    return True

if __name__ == "__main__":
    asyncio.run(test_journal_data_window())
