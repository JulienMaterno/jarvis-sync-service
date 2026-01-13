"""Analyze transcript segments to understand speaker distribution"""
import os
from dotenv import load_dotenv
load_dotenv()
from supabase import create_client

sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

result = sb.table('transcripts').select('segments').ilike('source_file', '%131528%').execute()

if result.data:
    segments = result.data[0].get('segments', [])
    if segments:
        # Count by speaker/channel
        left_count = 0
        right_count = 0
        unknown_count = 0
        
        for s in segments:
            channel = s.get('channel')
            if channel == 'left':
                left_count += 1
            elif channel == 'right':
                right_count += 1
            else:
                unknown_count += 1
        
        print(f'Total segments: {len(segments)}')
        print(f'Left channel (Aaron): {left_count}')
        print(f'Right channel (Other Person): {right_count}')
        print(f'Unknown channel: {unknown_count}')
        
        # Show first few from each
        print()
        print('Sample LEFT segments (Aaron):')
        left_segs = [s for s in segments if s.get('channel') == 'left'][:3]
        for s in left_segs:
            text = s.get('text', '')[:100]
            print(f"  {s.get('start', 0):.1f}s: {text}...")
        
        print()
        print('Sample RIGHT segments (Other Person):')
        right_segs = [s for s in segments if s.get('channel') == 'right'][:3]
        for s in right_segs:
            text = s.get('text', '')[:100]
            print(f"  {s.get('start', 0):.1f}s: {text}...")
        
        # Check the full_text for speaker tags
        print()
        print("Looking at full_text speaker tags...")
        
        result2 = sb.table('transcripts').select('full_text').ilike('source_file', '%131528%').execute()
        full_text = result2.data[0].get('full_text', '')
        
        aaron_count = full_text.count('[Aaron]')
        other_count = full_text.count('[Other Person]')
        print(f"[Aaron] tags in full_text: {aaron_count}")
        print(f"[Other Person] tags in full_text: {other_count}")
else:
    print('Not found')
