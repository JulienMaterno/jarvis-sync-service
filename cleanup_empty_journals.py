#!/usr/bin/env python3
"""Find and delete empty journal entries in Supabase."""

import os
import httpx
from dotenv import load_dotenv

load_dotenv()

url = os.environ['SUPABASE_URL']
key = os.environ['SUPABASE_KEY']
headers = {'apikey': key, 'Authorization': f'Bearer {key}'}

# Get all journals
r = httpx.get(f'{url}/rest/v1/journals?select=id,date,content,mood,source_file,notion_page_id&order=date.desc', headers=headers)
journals = r.json()

# Find empty ones (no content AND no source_file from voice)
empty = [j for j in journals if not j.get('content') and not j.get('source_file')]

print(f"Found {len(empty)} empty journal entries:")
for j in empty:
    print(f"  {j['date']} - mood: {j.get('mood', 'none')}, notion_id: {j.get('notion_page_id', 'none')[:8] if j.get('notion_page_id') else 'none'}...")

if empty:
    confirm = input("\nDelete these empty journals? (y/n): ")
    if confirm.lower() == 'y':
        for j in empty:
            r = httpx.delete(f"{url}/rest/v1/journals?id=eq.{j['id']}", headers=headers)
            if r.status_code in [200, 204]:
                print(f"  Deleted: {j['date']}")
            else:
                print(f"  Failed to delete {j['date']}: {r.status_code}")
        print("Done!")
    else:
        print("Cancelled.")
