"""Check applications sync status."""
import os
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client

c = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Check sync logs for applications
print("=== Recent Applications Sync Logs ===")
logs = c.table('sync_logs').select('*').ilike('event_type', '%application%').order('created_at', desc=True).limit(10).execute()
for l in logs.data:
    ts = l.get('created_at', '')[:19]
    status = l.get('status', 'unknown')
    msg = l.get('message', 'No message')[:100]
    print(f"{ts} [{status}] {msg}")

print("\n=== Applications Count ===")
apps = c.table('applications').select('id', count='exact').execute()
print(f"Total in Supabase: {apps.count}")

# Check how many have notion_page_id
apps_full = c.table('applications').select('id, name, notion_page_id, last_sync_source').execute()
with_notion = sum(1 for a in apps_full.data if a.get('notion_page_id'))
print(f"With Notion ID: {with_notion}")
print(f"Without Notion ID: {apps.count - with_notion}")

# Show ones without Notion ID
print("\n=== Applications Missing Notion ID ===")
missing = [a for a in apps_full.data if not a.get('notion_page_id')]
for a in missing[:10]:
    print(f"  - {a['name']} (source: {a.get('last_sync_source')})")
