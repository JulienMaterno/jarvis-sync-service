"""Check Notion for Books and Highlights databases."""
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')

headers = {
    'Authorization': f'Bearer {NOTION_API_TOKEN}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}

client = httpx.Client(headers=headers, timeout=30.0)

# Search for databases
print("Searching for databases in Notion...")
print("=" * 60)

resp = client.post('https://api.notion.com/v1/search', json={
    'filter': {'property': 'object', 'value': 'database'},
    'page_size': 100
})

if resp.status_code == 200:
    data = resp.json()
    databases = data.get('results', [])
    print(f"Found {len(databases)} databases:\n")
    
    for db in databases:
        title_parts = db.get('title', [])
        title = ''.join([t.get('plain_text', '') for t in title_parts]) if title_parts else 'Untitled'
        db_id = db['id']
        print(f"📊 {title}")
        print(f"   ID: {db_id}")
        
        # Show properties
        props = db.get('properties', {})
        prop_names = list(props.keys())[:10]
        print(f"   Properties: {', '.join(prop_names)}")
        print()
else:
    print(f"Error: {resp.status_code} - {resp.text}")
