"""Quick check of beeper sync logs."""
from lib.supabase_client import supabase

r = supabase.table('sync_logs').select('*').eq('event_type', 'beeper_sync').order('created_at', desc=True).limit(5).execute()

print("Recent Beeper Sync Logs:")
for l in r.data:
    msg = l.get("message", "")[:70]
    print(f"  {l['created_at'][:19]} - {l['status']:8} - {msg}")
