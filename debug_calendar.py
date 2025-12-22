"""Debug calendar sync issue."""
import asyncio
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Check sync token
print("=== SYNC TOKEN ===")
result = supabase.table("sync_state").select("*").eq("key", "calendar_sync_token").execute()
if result.data:
    token = result.data[0].get('value', '')
    print(f"Token exists: {token[:50]}..." if len(token) > 50 else f"Token: {token}")
else:
    print("No sync token found")

# Check last error details
print("\n=== LAST ERROR DETAILS ===")
errors = supabase.table('sync_logs').select('*').eq('event_type', 'calendar_sync').eq('status', 'error').order('created_at', desc=True).limit(1).execute()
if errors.data:
    err = errors.data[0]
    print(f"Time: {err.get('created_at')}")
    print(f"Message: {err.get('message')}")
    print(f"Details: {err.get('details')}")

# Clear the token to fix the issue
print("\n=== CLEARING SYNC TOKEN ===")
try:
    supabase.table("sync_state").delete().eq("key", "calendar_sync_token").execute()
    print("Sync token cleared!")
except Exception as e:
    print(f"Error clearing: {e}")
