#!/usr/bin/env python3
"""
Check for duplicates in Supabase tables and compare with Notion counts.
"""
import os
import httpx
from dotenv import load_dotenv
from collections import Counter

load_dotenv()

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')

# Notion Database IDs
NOTION_DBS = {
    'meetings': '2d1068b5-e624-8154-a7b2-e8c349e7a196',
    'tasks': '2d1068b5-e624-81e5-ad71-e58029bd481d',
    'journals': '2d1068b5-e624-81cc-9553-d2375854d951',
    'reflections': '2d1068b5-e624-81d9-aefc-fafbe8ec92bd',
    'contacts': '2d1068b5-e624-81e8-9c1c-f1d45c33e420',
    'books': '16a068b5-e624-8158-b858-dd72af14183f',
    'highlights': '16a068b5-e624-81e9-a7ef-ecbf84c577ef',
}

supabase_headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
}

notion_headers = {
    'Authorization': f'Bearer {NOTION_API_TOKEN}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}


def get_supabase_count(table: str) -> int:
    """Get count of records in Supabase table."""
    headers = {**supabase_headers, 'Prefer': 'count=exact'}
    response = httpx.head(f'{SUPABASE_URL}/rest/v1/{table}?select=*', headers=headers)
    content_range = response.headers.get('content-range', '0/*')
    return int(content_range.split('/')[1]) if '/' in content_range else 0


def get_notion_count(db_id: str) -> int:
    """Get count of pages in Notion database."""
    count = 0
    start_cursor = None
    
    while True:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor
        
        response = httpx.post(
            f'https://api.notion.com/v1/databases/{db_id}/query',
            headers=notion_headers,
            json=body,
            timeout=30.0
        )
        
        if response.status_code != 200:
            print(f"  Error querying Notion: {response.status_code}")
            return -1
        
        data = response.json()
        count += len(data.get('results', []))
        
        if not data.get('has_more'):
            break
        start_cursor = data.get('next_cursor')
    
    return count


def check_duplicates_by_notion_id(table: str) -> dict:
    """Check for duplicate notion_page_id values in a table."""
    response = httpx.get(
        f'{SUPABASE_URL}/rest/v1/{table}?select=notion_page_id',
        headers=supabase_headers,
        timeout=60.0
    )
    
    if response.status_code != 200:
        return {'error': response.text}
    
    records = response.json()
    notion_ids = [r.get('notion_page_id') for r in records if r.get('notion_page_id')]
    
    # Count occurrences
    id_counts = Counter(notion_ids)
    duplicates = {k: v for k, v in id_counts.items() if v > 1}
    
    return {
        'total_records': len(records),
        'with_notion_id': len(notion_ids),
        'without_notion_id': len(records) - len(notion_ids),
        'duplicate_count': len(duplicates),
        'duplicates': duplicates
    }


def check_duplicates_by_title(table: str, title_field: str = 'title') -> dict:
    """Check for duplicate titles in a table."""
    response = httpx.get(
        f'{SUPABASE_URL}/rest/v1/{table}?select={title_field}',
        headers=supabase_headers,
        timeout=60.0
    )
    
    if response.status_code != 200:
        return {'error': response.text}
    
    records = response.json()
    titles = [r.get(title_field) for r in records if r.get(title_field)]
    
    # Count occurrences
    title_counts = Counter(titles)
    duplicates = {k: v for k, v in title_counts.items() if v > 1}
    
    return {
        'duplicate_titles': len(duplicates),
        'examples': dict(list(duplicates.items())[:5])  # Show first 5
    }


def main():
    print("=" * 70)
    print("SUPABASE vs NOTION COMPARISON & DUPLICATE CHECK")
    print("=" * 70)
    
    tables_to_check = ['meetings', 'tasks', 'journals', 'reflections', 'contacts', 'books', 'highlights']
    
    print("\n1. RECORD COUNT COMPARISON")
    print("-" * 70)
    print(f"{'Table':<15} {'Supabase':<12} {'Notion':<12} {'Diff':<10} {'Status'}")
    print("-" * 70)
    
    for table in tables_to_check:
        supabase_count = get_supabase_count(table)
        notion_db_id = NOTION_DBS.get(table)
        
        if notion_db_id:
            notion_count = get_notion_count(notion_db_id)
            diff = supabase_count - notion_count
            status = "OK" if abs(diff) <= 5 else ("MISSING" if diff < 0 else "EXTRA")
            print(f"{table:<15} {supabase_count:<12} {notion_count:<12} {diff:+<10} {status}")
        else:
            print(f"{table:<15} {supabase_count:<12} {'N/A':<12} {'-':<10}")
    
    print("\n2. DUPLICATE CHECK (by notion_page_id)")
    print("-" * 70)
    
    for table in tables_to_check:
        result = check_duplicates_by_notion_id(table)
        if 'error' in result:
            print(f"{table}: ERROR - {result['error'][:50]}")
        else:
            dup_count = result['duplicate_count']
            without_id = result['without_notion_id']
            status = "OK" if dup_count == 0 else f"DUPLICATES FOUND"
            print(f"{table:<15} - Duplicates: {dup_count}, Without Notion ID: {without_id} - {status}")
            
            if dup_count > 0:
                print(f"               Duplicate IDs: {list(result['duplicates'].keys())[:3]}...")
    
    print("\n3. DUPLICATE CHECK (by title)")
    print("-" * 70)
    
    title_fields = {
        'meetings': 'title',
        'tasks': 'title', 
        'journals': 'title',
        'reflections': 'title',
        'contacts': 'first_name',
        'books': 'title',
        'highlights': 'content'
    }
    
    for table in tables_to_check:
        title_field = title_fields.get(table, 'title')
        result = check_duplicates_by_title(table, title_field)
        if 'error' in result:
            print(f"{table}: ERROR - {result['error'][:50]}")
        else:
            dup_count = result['duplicate_titles']
            if dup_count > 0:
                print(f"{table:<15} - {dup_count} duplicate titles:")
                for title, count in list(result['examples'].items())[:3]:
                    print(f"               '{title[:40]}...' x{count}" if len(str(title)) > 40 else f"               '{title}' x{count}")
            else:
                print(f"{table:<15} - No duplicate titles")
    
    print("\n" + "=" * 70)
    print("CHECK COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()
