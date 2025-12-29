"""Debug tasks sync issues."""
import os
from dotenv import load_dotenv
load_dotenv()

import httpx

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}'
}

# Check recent tasks
response = httpx.get(
    f'{SUPABASE_URL}/rest/v1/tasks',
    headers=headers,
    params={
        'select': 'id,title,status,last_sync_source,notion_page_id,created_at',
        'order': 'created_at.desc',
        'limit': '15'
    }
)

tasks = response.json()
print("Recent tasks:")
print("-" * 100)
for t in tasks:
    title = (t.get('title') or 'Untitled')[:40]
    status = t.get('status') or 'None'
    sync_src = t.get('last_sync_source') or 'None'
    has_notion = 'Yes' if t.get('notion_page_id') else 'No'
    created = t.get('created_at', '')[:10]
    print(f"{title:40} | {status:10} | sync: {sync_src:10} | notion: {has_notion} | {created}")

# Check for duplicate titles
print("\n\nChecking for duplicate titles...")
response = httpx.get(
    f'{SUPABASE_URL}/rest/v1/tasks',
    headers=headers,
    params={
        'select': 'title',
        'deleted_at': 'is.null'
    }
)

all_tasks = response.json()
titles = [t['title'] for t in all_tasks if t.get('title')]
from collections import Counter
dupes = {k: v for k, v in Counter(titles).items() if v > 1}

if dupes:
    print("DUPLICATE TITLES FOUND:")
    for title, count in sorted(dupes.items(), key=lambda x: -x[1])[:10]:
        print(f"  - '{title[:50]}' appears {count} times")
else:
    print("No duplicate titles found.")
