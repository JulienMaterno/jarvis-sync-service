"""Check recent calendar sync logs."""
from lib.supabase_client import supabase

resp = supabase.table('sync_logs').select('*').eq('event_type', 'calendar_sync').order('created_at', desc=True).limit(10).execute()
print("Recent calendar sync logs:")
print("-" * 50)
for log in resp.data:
    msg = (log.get('message') or '')[:100]
    print(f"{log['created_at']}: {log['status']} - {msg}")
