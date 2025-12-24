#!/usr/bin/env python3
"""Check recent sync errors."""
from lib.supabase_client import supabase
from datetime import datetime, timedelta, timezone

# Get error logs from last 24 hours
cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

result = supabase.table('sync_logs') \
    .select('event_type,status,message,created_at') \
    .eq('status', 'error') \
    .gte('created_at', cutoff) \
    .order('created_at', desc=True) \
    .limit(30) \
    .execute()

print(f'\nFound {len(result.data)} errors in last 24h:\n')
print(f"{'Time':<20} | {'Event Type':<30} | {'Message':<80}")
print("-" * 135)

for log in result.data:
    time_str = log['created_at'][:19]
    event = log['event_type'][:30]
    msg = log.get('message', 'No message')[:80]
    print(f"{time_str:<20} | {event:<30} | {msg}")

# Get summary by event type
print("\n\nError summary by type:")
print("-" * 50)
summary_result = supabase.rpc('get_error_summary_24h').execute()
if hasattr(summary_result, 'data') and summary_result.data:
    for row in summary_result.data:
        print(f"  {row['event_type']:<30}: {row['count']} errors")
else:
    # Manual grouping if RPC doesn't exist
    from collections import Counter
    event_counts = Counter(log['event_type'] for log in result.data)
    for event_type, count in event_counts.most_common():
        print(f"  {event_type:<30}: {count} errors")
