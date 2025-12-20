#!/usr/bin/env python3
"""Check reflections data in Supabase and Notion."""
import os
from dotenv import load_dotenv
load_dotenv()
import httpx

url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY')

client = httpx.Client(headers={'apikey': key, 'Authorization': f'Bearer {key}'}, timeout=30)
resp = client.get(f'{url}/rest/v1/reflections?select=id,title,content,sections,notion_page_id&limit=10&order=created_at.desc')
reflections = resp.json()

print("=== SUPABASE REFLECTIONS ===\n")
for r in reflections:
    title = r.get('title', 'Untitled') or 'Untitled'
    content = r.get('content') or ''
    sections = r.get('sections') or []
    notion_id = r.get('notion_page_id')
    
    print(f"Title: {title[:60]}")
    print(f"  Content: {len(content)} chars")
    print(f"  Sections: {len(sections)} sections")
    if sections:
        for s in sections[:2]:
            print(f"    - {s.get('heading', 'No heading')}: {len(s.get('content', ''))} chars")
    print(f"  Notion linked: {'Yes' if notion_id else 'No'}")
    print()
