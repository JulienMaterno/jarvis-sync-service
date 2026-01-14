"""Test if the database trigger correctly sets last_sync_source."""
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

c = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Get one application
app = c.table('applications').select('id, name, status, last_sync_source, notes').limit(1).execute().data[0]
print(f'BEFORE: {app["name"]}')
print(f'  status: {app["status"]}')
print(f'  last_sync_source: {app["last_sync_source"]}')

# Update notes (without changing last_sync_source explicitly)
test_note = f'Trigger test {datetime.now().isoformat()}'
c.table('applications').update({'notes': test_note}).eq('id', app['id']).execute()

# Check again
app2 = c.table('applications').select('id, name, status, last_sync_source, notes').eq('id', app['id']).execute().data[0]
print(f'\nAFTER update:')
print(f'  last_sync_source: {app2["last_sync_source"]} (should be "supabase" if trigger works)')
print(f'  notes: {app2["notes"][:60]}')
