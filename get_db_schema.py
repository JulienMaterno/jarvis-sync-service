"""
Get the schema of a Notion database.
"""
import os
import sys
import json
import httpx
from dotenv import load_dotenv

load_dotenv()

NOTION_API_TOKEN = os.getenv('NOTION_API_TOKEN')
if not NOTION_API_TOKEN:
    print('NOTION_API_TOKEN not set')
    exit(1)

def get_database_schema(database_id: str):
    headers = {
        'Authorization': f'Bearer {NOTION_API_TOKEN}',
        'Notion-Version': '2022-06-28'
    }
    
    response = httpx.get(
        f'https://api.notion.com/v1/databases/{database_id}',
        headers=headers,
        timeout=30.0
    )
    response.raise_for_status()
    data = response.json()
    
    title_parts = data.get('title', [])
    title = ''.join([t.get('plain_text', '') for t in title_parts]) if title_parts else '(untitled)'
    
    print(f'\n{"="*80}')
    print(f'DATABASE: {title}')
    print(f'ID: {database_id}')
    print(f'{"="*80}')
    
    properties = data.get('properties', {})
    print(f'\nProperties ({len(properties)}):')
    print('-' * 60)
    
    for name, prop in sorted(properties.items()):
        prop_type = prop.get('type', 'unknown')
        extra = ''
        
        if prop_type == 'select':
            options = prop.get('select', {}).get('options', [])
            option_names = [o.get('name', '') for o in options[:5]]
            extra = f' -> {option_names}'
        elif prop_type == 'multi_select':
            options = prop.get('multi_select', {}).get('options', [])
            option_names = [o.get('name', '') for o in options[:5]]
            extra = f' -> {option_names}'
        elif prop_type == 'relation':
            rel_db = prop.get('relation', {}).get('database_id', '')[:8]
            extra = f' -> db:{rel_db}...'
        elif prop_type == 'formula':
            extra = f' (formula)'
        elif prop_type == 'rollup':
            extra = f' (rollup)'
            
        print(f'  {name:<30} {prop_type:<15} {extra}')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python get_db_schema.py <database_id>')
        print('\nKnown databases:')
        print('  Applications:     bfb77dff-9721-47b6-9bab-0cd0b315a298')
        print('  LinkedIn Posts:   2d1068b5-e624-81f2-8be0-fd6783c4763f')
        exit(1)
    
    get_database_schema(sys.argv[1])
