#!/usr/bin/env python3
"""Check today's transcripts and meetings"""
import os
from dotenv import load_dotenv
from supabase import create_client
from datetime import datetime

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

today = datetime.now().strftime('%Y-%m-%d')

print("=" * 60)
print(f"TRANSCRIPTS from {today}:")
print("=" * 60)
result = supabase.table('transcripts').select('id,source_file,content,created_at').gte('created_at', f'{today}T00:00:00').order('created_at').execute()
for t in result.data:
    content = t.get('content', '') or ''
    print(f"  {t['source_file'][:40]:40} | {len(content):6} chars | ID: {t['id'][:8]}")
print(f"Total: {len(result.data)} transcripts")

print("\n" + "=" * 60)
print(f"MEETINGS from {today}:")
print("=" * 60)
result = supabase.table('meetings').select('id,title,date,created_at').gte('created_at', f'{today}T00:00:00').order('created_at').execute()
for m in result.data:
    title = (m.get('title') or 'Untitled')[:50]
    print(f"  {title:50} | ID: {m['id'][:8]}")
print(f"Total: {len(result.data)} meetings")

print("\n" + "=" * 60)
print(f"REFLECTIONS from {today}:")
print("=" * 60)
result = supabase.table('reflections').select('id,title,tags,created_at').gte('created_at', f'{today}T00:00:00').order('created_at').execute()
for r in result.data:
    title = (r.get('title') or 'Untitled')[:50]
    tags = r.get('tags') or []
    print(f"  {title:50} | {tags}")
print(f"Total: {len(result.data)} reflections")
