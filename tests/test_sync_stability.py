#!/usr/bin/env python3
"""Test sync stability in both directions."""

import os
import sys
from dotenv import load_dotenv
load_dotenv()

from lib.supabase_client import supabase

def test_supabase_to_notion():
    """Create a test meeting in Supabase, then run sync to push to Notion."""
    print("\n=== TEST 1: Supabase → Notion ===")
    
    # Create test meeting
    test_meeting = {
        'title': 'TEST - Delete Me After Sync Test',
        'summary': 'Test meeting to verify Supabase to Notion sync',
        'date': '2025-01-10T10:00:00Z',
        'topics_discussed': ['sync-test'],
    }
    
    result = supabase.table('meetings').insert(test_meeting).execute()
    meeting_id = result.data[0]['id']
    print(f"✅ Created test meeting in Supabase: {meeting_id[:8]}...")
    print(f"   Title: {test_meeting['title']}")
    print(f"\n→ Run: python -m syncs.meetings_sync --full")
    print(f"→ Then check Notion for the new meeting")
    print(f"→ Delete it from Notion to test Notion → Supabase deletion")
    
    return meeting_id


def check_test_meeting(meeting_id: str = None):
    """Check status of test meeting."""
    print("\n=== Checking test meeting status ===")
    
    if meeting_id:
        result = supabase.table('meetings').select('*').eq('id', meeting_id).execute()
    else:
        result = supabase.table('meetings').select('*').ilike('title', '%TEST - Delete Me%').execute()
    
    if result.data:
        for m in result.data:
            print(f"  ID: {m['id'][:8]}...")
            print(f"  Title: {m['title']}")
            print(f"  notion_page_id: {m.get('notion_page_id', 'None')}")
            print(f"  deleted_at: {m.get('deleted_at', 'None')}")
    else:
        print("  No test meeting found")


def cleanup_test():
    """Hard delete test meeting."""
    print("\n=== Cleaning up test meeting ===")
    
    result = supabase.table('meetings').delete().ilike('title', '%TEST - Delete Me%').execute()
    if result.data:
        print(f"✅ Deleted {len(result.data)} test meeting(s)")
    else:
        print("  No test meeting to delete")


def show_current_counts():
    """Show current sync state."""
    print("\n=== Current Sync State ===")
    
    tables = ['meetings', 'reflections', 'journals', 'tasks']
    for table in tables:
        total = supabase.table(table).select('id', count='exact').execute()
        active = supabase.table(table).select('id', count='exact').is_('deleted_at', 'null').execute()
        linked = supabase.table(table).select('id', count='exact').is_('deleted_at', 'null').not_.is_('notion_page_id', 'null').execute()
        
        print(f"  {table}: {active.count} active, {linked.count} linked to Notion, {total.count - active.count} soft-deleted")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python test_sync_stability.py create    - Create test meeting in Supabase")
        print("  python test_sync_stability.py check     - Check test meeting status")
        print("  python test_sync_stability.py cleanup   - Delete test meeting")
        print("  python test_sync_stability.py counts    - Show current sync state")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == 'create':
        test_supabase_to_notion()
    elif cmd == 'check':
        meeting_id = sys.argv[2] if len(sys.argv) > 2 else None
        check_test_meeting(meeting_id)
    elif cmd == 'cleanup':
        cleanup_test()
    elif cmd == 'counts':
        show_current_counts()
    else:
        print(f"Unknown command: {cmd}")
