"""Simple check for all databases."""
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

with httpx.Client(headers=headers, timeout=30.0) as client:
    # Get ALL databases
    response = client.post(
        'https://api.notion.com/v1/search',
        json={"filter": {"property": "object", "value": "database"}, "page_size": 100}
    )
    response.raise_for_status()
    databases = response.json().get('results', [])
    
    print(f"\nFound {len(databases)} databases:\n")
    for db in databases:
        title_parts = db.get('title', [])
        title = ''.join([t.get('plain_text', '') for t in title_parts]) or "(Untitled)"
        print(f"  - {title}: {db['id']}")
    
    # Also get all pages to see what's accessible
    print("\n\nSearching for 'Reading List'...")
    response = client.post(
        'https://api.notion.com/v1/search',
        json={"query": "Reading List", "page_size": 20}
    )
    response.raise_for_status()
    results = response.json().get('results', [])
    print(f"Found {len(results)} results")
    for r in results:
        print(f"  - {r['object']}: {r['id']}")
