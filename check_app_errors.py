"""Check applications sync errors in detail."""
import os
import json
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

c = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Get detailed logs
logs = c.table('sync_logs').select('*').ilike('event_type', '%application%').order('created_at', desc=True).limit(3).execute()
print('=== Recent Application Sync Logs ===')
for l in logs.data:
    ts = l.get('created_at', '')[:19]
    msg = l.get('message', 'No message')
    details = l.get('details', {})
    print(f'{ts}: {msg}')
    if details:
        print(f'  Details: {json.dumps(details, indent=2, default=str)[:500]}')
    print()

# Also check for any error-related events
print('\n=== All Recent Error Logs ===')
errors = c.table('sync_logs').select('*').eq('status', 'error').order('created_at', desc=True).limit(5).execute()
for l in errors.data:
    ts = l.get('created_at', '')[:19]
    event = l.get('event_type', 'unknown')
    msg = l.get('message', 'No message')[:100]
    print(f'{ts} [{event}] {msg}')
