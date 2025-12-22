"""Clear calendar sync token and test sync."""
from lib.supabase_client import supabase
import asyncio

async def main():
    # Check current token
    token_resp = supabase.table('sync_state').select('*').eq('key', 'calendar_sync_token').execute()
    if token_resp.data:
        print(f"Current token: {token_resp.data[0]['value'][:50]}...")
        print(f"Updated at: {token_resp.data[0]['updated_at']}")
        
        # Delete the bad token
        supabase.table('sync_state').delete().eq('key', 'calendar_sync_token').execute()
        print("\n✅ Deleted bad sync token")
    else:
        print("No sync token found")
    
    # Now run a test calendar sync
    print("\n🔄 Running fresh calendar sync...")
    from sync_calendar import run_calendar_sync
    result = await run_calendar_sync()
    print(f"Result: {result}")

asyncio.run(main())
