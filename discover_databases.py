"""
Discover all Notion databases accessible to the integration.
"""
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

NOTION_API_TOKEN = os.getenv('NOTION_API_TOKEN')
if not NOTION_API_TOKEN:
    print('NOTION_API_TOKEN not set')
    exit(1)

headers = {
    'Authorization': f'Bearer {NOTION_API_TOKEN}',
    'Notion-Version': '2022-06-28'
}

# Search for databases
response = httpx.post(
    'https://api.notion.com/v1/search',
    headers=headers,
    json={
        'filter': {'property': 'object', 'value': 'database'},
        'page_size': 100
    },
    timeout=30.0
)
response.raise_for_status()
data = response.json()

results = data.get('results', [])
print(f'Found {len(results)} databases:')
print('-' * 80)

for db in results:
    title_parts = db.get('title', [])
    title = ''.join([t.get('plain_text', '') for t in title_parts]) if title_parts else '(untitled)'
    db_id = db['id']
    print(f'{title:<40} {db_id}')
