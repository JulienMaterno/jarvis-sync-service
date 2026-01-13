"""Test creating MIT application in Notion."""
import os
from dotenv import load_dotenv
load_dotenv()

from lib.notion_client import NotionClient
from lib.sync_base import NotionPropertyBuilder
from supabase import create_client
import httpx

notion = NotionClient(os.getenv('NOTION_API_TOKEN'))
# Use hardcoded default from applications_sync.py
db_id = os.getenv('NOTION_APPLICATIONS_DB_ID', 'bfb77dff-9721-47b6-9bab-0cd0b315a298')

print(f"Database ID: {db_id}")

# Get the real MIT data from Supabase
c = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
apps = c.table('applications').select('*').eq('name', 'MIT Solve Global Climate Challenge').execute()
app = apps.data[0]

print(f"\nApplication data:")
for k, v in app.items():
    if v and k not in ('content', 'notes'):
        print(f"  {k}: {str(v)[:60]}")
print(f"  notes length: {len(app.get('notes') or '')}")

# Build properties exactly like the sync does
from syncs.applications_sync import ApplicationsSyncService
sync = ApplicationsSyncService()
properties = sync.convert_to_source(app)

print(f"\nProperties being sent:")
for k, v in properties.items():
    if k == 'Notes':
        text = v.get('rich_text', [{}])[0].get('text', {}).get('content', '')
        print(f"  Notes: {len(text)} chars (truncated? {len(text) == 2000})")
    else:
        print(f"  {k}: {str(v)[:80]}")

try:
    result = notion.create_page({'database_id': db_id}, properties)
    print(f'\nSUCCESS! Page created: {result["id"]}')
    print('URL:', result.get('url'))
except httpx.HTTPStatusError as e:
    print(f'\nHTTP Error: {e}')
    print(f'Response body: {e.response.text}')
except Exception as e:
    print(f'\nERROR: {e}')
    print(f'Error type: {type(e)}')
