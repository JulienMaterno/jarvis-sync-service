"""Search for all Notion databases including those with specific names."""
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

# Search for "content" and "highlights" and "books"
search_terms = ['content', 'highlights', 'books', 'book', 'reading']

for term in search_terms:
    print(f"\n🔍 Searching for '{term}'...")
    print("-" * 40)
    
    resp = client.post('https://api.notion.com/v1/search', json={
        'query': term,
        'page_size': 20
    })
    
    if resp.status_code == 200:
        data = resp.json()
        results = data.get('results', [])
        
        for item in results:
            obj_type = item.get('object')
            if obj_type == 'database':
                title_parts = item.get('title', [])
                title = ''.join([t.get('plain_text', '') for t in title_parts]) if title_parts else 'Untitled'
                print(f"  📊 Database: {title}")
                print(f"     ID: {item['id']}")
            elif obj_type == 'page':
                # Get page title
                props = item.get('properties', {})
                title = 'Untitled'
                for prop_name, prop_val in props.items():
                    if prop_val.get('type') == 'title':
                        title_parts = prop_val.get('title', [])
                        if title_parts:
                            title = ''.join([t.get('plain_text', '') for t in title_parts])
                            break
                print(f"  📄 Page: {title[:50]}")
                print(f"     ID: {item['id']}")
        
        if not results:
            print(f"  No results found for '{term}'")
    else:
        print(f"  Error: {resp.status_code}")

# Also try to list shared databases
print("\n\n📋 Listing ALL accessible databases...")
print("=" * 60)

resp = client.post('https://api.notion.com/v1/search', json={
    'filter': {'property': 'object', 'value': 'database'},
    'page_size': 100
})

if resp.status_code == 200:
    data = resp.json()
    for db in data.get('results', []):
        title_parts = db.get('title', [])
        title = ''.join([t.get('plain_text', '') for t in title_parts]) if title_parts else 'Untitled'
        print(f"  {title}: {db['id']}")
