#!/usr/bin/env python3
"""Analyze sync log statuses."""
from lib.supabase_client import supabase
from datetime import datetime, timedelta, timezone
from collections import Counter

# Get logs from last 24 hours
cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

result = supabase.table('sync_logs') \
    .select('event_type,status,message') \
    .gte('created_at', cutoff) \
    .execute()

logs = result.data
print(f"\nTotal logs in last 24h: {len(logs)}\n")

# Count by status
status_counts = Counter(log['status'] for log in logs)
print("Status breakdown:")
print("-" * 40)
for status, count in status_counts.most_common():
    pct = (count / len(logs) * 100) if logs else 0
    print(f"  {status:<15}: {count:>4} ({pct:>5.1f}%)")

# Show sample of non-success statuses
print("\n\nSample non-success entries:")
print("-" * 80)
non_success = [l for l in logs if l['status'] != 'success'][:10]
for log in non_success:
    print(f"  {log['status']:<10} | {log['event_type']:<30} | {log.get('message', '')[:40]}")

# Calculate "real" success rate (success vs error only)
success_count = status_counts.get('success', 0)
error_count = status_counts.get('error', 0)
actionable_total = success_count + error_count

if actionable_total > 0:
    real_success_rate = (success_count / actionable_total) * 100
    print(f"\n\nSuccess rate (success vs error only): {real_success_rate:.1f}%")
    print(f"  Success: {success_count}")
    print(f"  Error: {error_count}")
