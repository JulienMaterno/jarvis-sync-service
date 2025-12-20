#!/usr/bin/env python3
"""Check meeting database sync status between Supabase and Notion"""

import os
from dotenv import load_dotenv
import httpx
from collections import Counter

load_dotenv()

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_MEETING_DB_ID = '297cd3f1-eb28-810f-86f0-f142f7e3a5ca'

def check_supabase():
    print("=" * 60)
    print("SUPABASE MEETINGS")
    print("=" * 60)
    
    headers = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}', 'Prefer': 'count=exact'}
    
    # Get total count
    resp = httpx.get(f'{SUPABASE_URL}/rest/v1/meetings?select=id', headers=headers)
    total = resp.headers.get('content-range', '0').split('/')[-1]
    print(f"Total meetings: {total}")
    
    # Get all meetings
    resp = httpx.get(f'{SUPABASE_URL}/rest/v1/meetings?select=id,title,date,notion_page_id,last_sync_source,source_file&order=date.desc', 
                     headers={'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}'})
    meetings = resp.json()
    
    if isinstance(meetings, dict) and 'error' in meetings:
        print(f"Error: {meetings}")
        return []
    
    # Check source types
    from_audio = sum(1 for m in meetings if m.get('source_file'))
    manual = sum(1 for m in meetings if not m.get('source_file'))
    print(f"\nBy origin:")
    print(f"  - From audio pipeline: {from_audio}")
    print(f"  - Manual/other: {manual}")
    
    # Check sync status
    synced = sum(1 for m in meetings if m.get('notion_page_id'))
    not_synced = sum(1 for m in meetings if not m.get('notion_page_id'))
    print(f"\nSync status:")
    print(f"  - Synced to Notion: {synced}")
    print(f"  - Not synced: {not_synced}")
    
    print(f"\nLatest 5 meetings:")
    for m in meetings[:5]:
        synced_icon = "✅" if m.get('notion_page_id') else "❌"
        origin = "🎙️" if m.get('source_file') else "📝"
        print(f"  {synced_icon} {origin} {m.get('date', 'no-date')}: {(m.get('title') or '(no title)')[:40]}")
    
    # Check oldest unsynced
    unsynced = [m for m in meetings if not m.get('notion_page_id')]
    if unsynced:
        print(f"\nOldest unsynced meetings:")
        for m in sorted(unsynced, key=lambda x: x.get('date') or '')[:5]:
            origin = "🎙️" if m.get('source_file') else "📝"
            print(f"  ❌ {origin} {m.get('date', 'no-date')}: {(m.get('title') or '(no title)')[:40]}")
    
    return meetings

def check_notion():
    print("\n" + "=" * 60)
    print("NOTION MEETINGS")
    print("=" * 60)
    
    headers = {
        'Authorization': f'Bearer {NOTION_API_TOKEN}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }
    
    # Query all pages
    all_pages = []
    start_cursor = None
    
    while True:
        body = {'page_size': 100}
        if start_cursor:
            body['start_cursor'] = start_cursor
            
        resp = httpx.post(
            f'https://api.notion.com/v1/databases/{NOTION_MEETING_DB_ID}/query',
            headers=headers,
            json=body,
            timeout=30.0
        )
        data = resp.json()
        
        if 'results' not in data:
            print(f"Error: {data}")
            break
            
        all_pages.extend(data['results'])
        
        if not data.get('has_more'):
            break
        start_cursor = data.get('next_cursor')
    
    print(f"Total meetings: {len(all_pages)}")
    
    # Check sync status (has supabase_id)
    synced = 0
    not_synced = 0
    for page in all_pages:
        props = page.get('properties', {})
        supabase_id = props.get('supabase_id', {}).get('rich_text', [])
        if supabase_id:
            synced += 1
        else:
            not_synced += 1
    
    print(f"\nSync status:")
    print(f"  - Has supabase_id: {synced}")
    print(f"  - No supabase_id: {not_synced}")
    
    return all_pages

def compare(supabase_meetings, notion_pages):
    print("\n" + "=" * 60)
    print("COMPARISON")
    print("=" * 60)
    
    # Get Notion page IDs from Supabase
    supabase_notion_ids = set(m.get('notion_page_id') for m in supabase_meetings if m.get('notion_page_id'))
    notion_ids = set(p['id'].replace('-', '') for p in notion_pages)
    
    # Normalize IDs (remove dashes)
    supabase_notion_ids_normalized = set(id.replace('-', '') for id in supabase_notion_ids if id)
    
    print(f"Supabase meetings with notion_page_id: {len(supabase_notion_ids)}")
    print(f"Notion pages: {len(notion_ids)}")
    
    # In Supabase but not in Notion (orphaned references)
    orphaned = supabase_notion_ids_normalized - notion_ids
    if orphaned:
        print(f"\n⚠️  Orphaned references (in Supabase but page gone from Notion): {len(orphaned)}")
    
    # In Notion but not referenced in Supabase
    not_in_supabase = notion_ids - supabase_notion_ids_normalized
    if not_in_supabase:
        print(f"\n⚠️  Notion pages not in Supabase: {len(not_in_supabase)}")

if __name__ == "__main__":
    supabase_meetings = check_supabase()
    notion_pages = check_notion()
    compare(supabase_meetings, notion_pages)
