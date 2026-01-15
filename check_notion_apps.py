"""Check Notion applications count"""
import requests
import os
from dotenv import load_dotenv

load_dotenv()

NOTION_API_KEY = os.getenv('NOTION_API_TOKEN')  # Note: env var is NOTION_API_TOKEN
db_id = os.getenv('NOTION_APPLICATIONS_DB_ID', 'bfb77dff-9721-47b6-9bab-0cd0b315a298')

print(f'DB ID: {db_id}')
key_preview = NOTION_API_KEY[:10] if NOTION_API_KEY else "NOT SET"
print(f'API Key (first 10 chars): {key_preview}')

headers = {
    'Authorization': f'Bearer {NOTION_API_KEY}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}

all_pages = []
has_more = True
start_cursor = None

while has_more:
    body = {'page_size': 100}
    if start_cursor:
        body['start_cursor'] = start_cursor
    
    resp = requests.post(f'https://api.notion.com/v1/databases/{db_id}/query', headers=headers, json=body)
    if resp.status_code != 200:
        print(f'Error: {resp.status_code} - {resp.json()}')
        break
    
    data = resp.json()
    all_pages.extend(data.get('results', []))
    has_more = data.get('has_more', False)
    start_cursor = data.get('next_cursor')

print(f'Total applications in Notion: {len(all_pages)}')
archived = len([p for p in all_pages if p.get('archived', False)])
print(f'  - Archived: {archived}')
print(f'  - Not archived: {len(all_pages) - archived}')

# Check Supabase for comparison
from supabase import create_client
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
apps = sb.table('applications').select('id', count='exact').execute()
print(f'\nSupabase applications: {apps.count}')

# Check how many have notion_page_id
apps_with_notion = sb.table('applications').select('id', count='exact').not_.is_('notion_page_id', 'null').execute()
apps_without_notion = sb.table('applications').select('id', count='exact').is_('notion_page_id', 'null').execute()
print(f'  - With notion_page_id: {apps_with_notion.count}')
print(f'  - Without notion_page_id: {apps_without_notion.count}')
