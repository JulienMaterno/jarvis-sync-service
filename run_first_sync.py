"""
Run first Beeper sync to populate database
"""
import asyncio
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.supabase_client import supabase
from sync_beeper import run_beeper_sync

async def main():
    print("="*60)
    print("FIRST BEEPER SYNC")
    print("="*60)
    print("This will sync all chats and messages from the last 30 days")
    print("="*60 + "\n")
    
    try:
        result = await run_beeper_sync(supabase, full_sync=True)
        
        print("\n" + "="*60)
        print("SYNC RESULTS")
        print("="*60)
        print(f"Chats synced: {result['chats_synced']}")
        print(f"  - Created: {result['chats_created']}")
        print(f"  - Updated: {result['chats_updated']}")
        print(f"\nMessages synced: {result['messages_synced']}")
        print(f"  - New: {result['messages_new']}")
        print(f"  - Skipped (duplicates): {result['messages_skipped']}")
        print(f"\nContacts linked: {result['contacts_linked']}")
        
        if result['errors']:
            print(f"\nErrors: {len(result['errors'])}")
            for error in result['errors'][:5]:  # Show first 5
                print(f"  - {error}")
        
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Sync failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
