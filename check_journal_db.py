"""Quick script to find and inspect the Journal database in Notion."""

import os
import httpx
import json
from dotenv import load_dotenv

load_dotenv()

token = os.environ.get('NOTION_API_TOKEN')
headers = {
    'Authorization': f'Bearer {token}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}

# Search for Journal database
print("Searching for Journal database...")
response = httpx.post(
    'https://api.notion.com/v1/search',
    headers=headers,
    json={'query': 'Journal', 'filter': {'value': 'database', 'property': 'object'}}
)
data = response.json()

for db in data.get('results', []):
    title = ''.join([t.get('plain_text', '') for t in db.get('title', [])])
    db_id = db['id']
    print(f"\nDB: {title}")
    print(f"ID: {db_id}")
    
    # Get database schema
    print("\nProperties:")
    for prop_name, prop_data in db.get('properties', {}).items():
        prop_type = prop_data.get('type')
        print(f"  - {prop_name}: {prop_type}")
    
    # Get a sample entry
    print("\nSample entries:")
    query_response = httpx.post(
        f'https://api.notion.com/v1/databases/{db_id}/query',
        headers=headers,
        json={'page_size': 3}
    )
    entries = query_response.json().get('results', [])
    for entry in entries:
        props = entry.get('properties', {})
        # Try to get title/name
        for pname, pval in props.items():
            if pval.get('type') == 'title':
                title_text = ''.join([t.get('plain_text', '') for t in pval.get('title', [])])
                print(f"  Entry: {title_text}")
                break
