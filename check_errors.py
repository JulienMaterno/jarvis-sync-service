#!/usr/bin/env python3
"""Check recent sync errors from Supabase"""

from datetime import datetime, timedelta, timezone
from lib.supabase_client import supabase

def check_errors():
    print("Checking sync errors from the last 24 hours...\n")
    
    # Query errors from last 24 hours
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    response = supabase.table("sync_logs") \
        .select("*") \
        .gte("created_at", yesterday.isoformat()) \
        .eq("status", "error") \
        .order("created_at", desc=True) \
        .limit(50) \
        .execute()
    
    errors = response.data
    
    if not errors:
        print("✅ No errors found!")
        return
    
    print(f"Found {len(errors)} errors:\n")
    print("=" * 80)
    
    # Group errors by type
    error_types = {}
    for error in errors:
        event_type = error.get("event_type", "unknown")
        message = error.get("message", "")
        
        if event_type not in error_types:
            error_types[event_type] = []
        error_types[event_type].append({
            "time": error.get("created_at"),
            "message": message
        })
    
    # Display grouped errors
    for event_type, error_list in error_types.items():
        print(f"\n🔴 {event_type} ({len(error_list)} errors)")
        print("-" * 80)
        for i, err in enumerate(error_list[:5], 1):  # Show first 5
            print(f"{i}. [{err['time']}]")
            print(f"   {err['message'][:200]}")
        if len(error_list) > 5:
            print(f"   ... and {len(error_list) - 5} more")

if __name__ == "__main__":
    check_errors()
