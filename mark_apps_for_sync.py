"""Quick script to mark applications for Notion sync and run sync."""
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client
import os
from datetime import datetime, timezone

c = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

# Mark ALL applications as needing sync to Notion
print("Marking applications for sync...")
result = c.table('applications').update({
    'last_sync_source': 'supabase'
}).not_.is_('id', 'null').execute()

print(f'Marked {len(result.data)} applications for Notion sync')
print("\nNow run: python -m syncs.applications_sync --full")
