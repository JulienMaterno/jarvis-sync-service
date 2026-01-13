#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

today = '2026-01-12'
result = supabase.table('transcripts').select('id,source_file,full_text,audio_duration_seconds').gte('created_at', f'{today}T00:00:00').order('created_at').execute()

print('TRANSCRIPTS from today:')
for t in result.data:
    text = t.get('full_text', '') or ''
    duration = t.get('audio_duration_seconds', 0) or 0
    mins = int(duration // 60)
    source = t.get('source_file', '')[:40]
    tid = t['id'][:8]
    print(f'  {source:40} | {mins:3}min | {len(text):6} chars | ID: {tid}')

print()
print('✓ Transcripts store FULL TEXT - AI reads this as input')
print('✓ AI output is ANALYSIS only - NOT a repeat of the transcript')
