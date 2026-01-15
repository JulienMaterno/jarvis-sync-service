"""Detailed check of Notion applications"""
import requests
import os
from dotenv import load_dotenv

load_dotenv()

NOTION_API_KEY = os.getenv('NOTION_API_TOKEN')
db_id = 'bfb77dff-9721-47b6-9bab-0cd0b315a298'

headers = {
    'Authorization': f'Bearer {NOTION_API_KEY}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}

# Query ALL items (including archived)
all_pages = []
has_more = True
start_cursor = None
page_num = 0

while has_more:
    page_num += 1
    body = {'page_size': 100}
    if start_cursor:
        body['start_cursor'] = start_cursor
    
    resp = requests.post(f'https://api.notion.com/v1/databases/{db_id}/query', headers=headers, json=body)
    data = resp.json()
    results = data.get('results', [])
    all_pages.extend(results)
    has_more = data.get('has_more', False)
    start_cursor = data.get('next_cursor')
    print(f'Page {page_num}: fetched {len(results)} items, total so far: {len(all_pages)}, has_more={has_more}')

print(f'\n=== SUMMARY ===')
print(f'Total items from API: {len(all_pages)}')
print(f'Archived: {len([p for p in all_pages if p.get("archived")])}')
print(f'Not archived: {len([p for p in all_pages if not p.get("archived")])}')
print(f'In trash: {len([p for p in all_pages if p.get("in_trash")])}')

# Check Supabase count
from supabase import create_client
sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
apps = sb.table('applications').select('id', count='exact').execute()
print(f'\nSupabase count: {apps.count}')

# Check how many in Supabase have notion_page_id that exists in Notion
notion_ids = set(p['id'] for p in all_pages)
apps_data = sb.table('applications').select('id, notion_page_id, name').execute()
missing_in_notion = []
for app in apps_data.data:
    if app['notion_page_id']:
        # Notion IDs are sometimes with/without hyphens
        notion_id_clean = app['notion_page_id'].replace('-', '')
        found = any(notion_id_clean == p['id'].replace('-', '') for p in all_pages)
        if not found:
            missing_in_notion.append(app)

print(f'\nSupabase apps with notion_page_id NOT found in Notion: {len(missing_in_notion)}')
if missing_in_notion[:5]:
    print('First 5:')
    for app in missing_in_notion[:5]:
        print(f"  - {app['name'][:50]} (notion_id: {app['notion_page_id'][:20]}...)")
