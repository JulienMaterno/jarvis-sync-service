"""Query Notion Tasks database schema."""
import os
from dotenv import load_dotenv
load_dotenv()
import httpx

token = os.environ.get('NOTION_API_TOKEN')
db_id = '2b3cd3f1-eb28-8004-a33a-d26b8bb3fa58'

headers = {
    'Authorization': f'Bearer {token}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}

# Get database schema
resp = httpx.get(f'https://api.notion.com/v1/databases/{db_id}', headers=headers)
data = resp.json()

print('=== NOTION TASKS DATABASE SCHEMA ===')
title_arr = data.get('title', [{}])
if title_arr:
    print(f"Title: {title_arr[0].get('plain_text', 'Unknown')}")
print()
print('Properties:')
for name, prop in data.get('properties', {}).items():
    prop_type = prop.get('type', 'unknown')
    print(f'  {name}: {prop_type}')
    if prop_type == 'select':
        options = [o.get('name') for o in prop.get('select', {}).get('options', [])]
        print(f'    Options: {options}')
    elif prop_type == 'status':
        status_config = prop.get('status', {})
        options = [o.get('name') for o in status_config.get('options', [])]
        print(f'    Options: {options}')

# Get a sample task
print()
print('=== SAMPLE TASK ===')
query_resp = httpx.post(
    f'https://api.notion.com/v1/databases/{db_id}/query',
    headers=headers,
    json={"page_size": 1}
)
tasks = query_resp.json().get('results', [])
if tasks:
    task = tasks[0]
    props = task.get('properties', {})
    for name, prop in props.items():
        prop_type = prop.get('type')
        if prop_type == 'title':
            val = prop.get('title', [{}])[0].get('plain_text', '') if prop.get('title') else ''
        elif prop_type == 'rich_text':
            val = prop.get('rich_text', [{}])[0].get('plain_text', '') if prop.get('rich_text') else ''
        elif prop_type == 'date':
            date_obj = prop.get('date')
            val = date_obj.get('start') if date_obj else None
        elif prop_type == 'select':
            sel = prop.get('select')
            val = sel.get('name') if sel else None
        elif prop_type == 'status':
            stat = prop.get('status')
            val = stat.get('name') if stat else None
        elif prop_type == 'checkbox':
            val = prop.get('checkbox')
        elif prop_type == 'relation':
            val = [r.get('id') for r in prop.get('relation', [])]
        else:
            val = f"<{prop_type}>"
        print(f'  {name}: {val}')
