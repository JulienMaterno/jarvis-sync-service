"""
Get sample records from a Notion database.
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

def get_sample_records(database_id: str, limit: int = 3):
    headers = {
        'Authorization': f'Bearer {NOTION_API_TOKEN}',
        'Notion-Version': '2022-06-28'
    }
    
    # Query for records
    response = httpx.post(
        f'https://api.notion.com/v1/databases/{database_id}/query',
        headers=headers,
        json={'page_size': limit},
        timeout=30.0
    )
    response.raise_for_status()
    data = response.json()
    
    results = data.get('results', [])
    print(f'\nFound {len(results)} sample records:\n')
    
    for i, page in enumerate(results, 1):
        page_id = page['id']
        props = page.get('properties', {})
        
        print(f'\n--- Record {i} (id: {page_id[:8]}...) ---')
        
        # Get title
        for name, prop in props.items():
            if prop.get('type') == 'title':
                title_parts = prop.get('title', [])
                title = ''.join([t.get('plain_text', '') for t in title_parts])
                print(f'Title: {title[:60]}...' if len(title) > 60 else f'Title: {title}')
                break
        
        # Print other properties
        for name, prop in sorted(props.items()):
            prop_type = prop.get('type')
            value = None
            
            if prop_type == 'title':
                continue  # Already printed
            elif prop_type == 'rich_text':
                text_parts = prop.get('rich_text', [])
                value = ''.join([t.get('plain_text', '') for t in text_parts])[:100]
            elif prop_type == 'select':
                sel = prop.get('select')
                value = sel.get('name') if sel else None
            elif prop_type == 'multi_select':
                opts = prop.get('multi_select', [])
                value = [o.get('name') for o in opts]
            elif prop_type == 'date':
                date_obj = prop.get('date')
                value = date_obj.get('start') if date_obj else None
            elif prop_type == 'url':
                value = prop.get('url')
            elif prop_type == 'number':
                value = prop.get('number')
            elif prop_type == 'checkbox':
                value = prop.get('checkbox')
            elif prop_type == 'relation':
                rels = prop.get('relation', [])
                value = [r.get('id', '')[:8] for r in rels]
            
            if value is not None and value != '' and value != []:
                if isinstance(value, str) and len(value) > 80:
                    value = value[:80] + '...'
                print(f'  {name}: {value}')
        
        # Get page content
        blocks_response = httpx.get(
            f'https://api.notion.com/v1/blocks/{page_id}/children',
            headers=headers,
            timeout=30.0
        )
        if blocks_response.status_code == 200:
            blocks = blocks_response.json().get('results', [])
            if blocks:
                print(f'  [Content: {len(blocks)} blocks]')
                for block in blocks[:3]:
                    block_type = block.get('type')
                    if block_type == 'paragraph':
                        text = ''.join([t.get('plain_text', '') for t in block.get('paragraph', {}).get('rich_text', [])])
                        if text:
                            print(f'    - {text[:60]}...' if len(text) > 60 else f'    - {text}')
                    elif block_type == 'heading_1':
                        text = ''.join([t.get('plain_text', '') for t in block.get('heading_1', {}).get('rich_text', [])])
                        print(f'    # {text}')
                    elif block_type == 'heading_2':
                        text = ''.join([t.get('plain_text', '') for t in block.get('heading_2', {}).get('rich_text', [])])
                        print(f'    ## {text}')
                    elif block_type == 'bulleted_list_item':
                        text = ''.join([t.get('plain_text', '') for t in block.get('bulleted_list_item', {}).get('rich_text', [])])
                        print(f'    • {text[:50]}')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python get_sample_records.py <database_id>')
        print('\nKnown databases:')
        print('  Applications:     bfb77dff-9721-47b6-9bab-0cd0b315a298')
        print('  LinkedIn Posts:   2d1068b5-e624-81f2-8be0-fd6783c4763f')
        exit(1)
    
    get_sample_records(sys.argv[1])
