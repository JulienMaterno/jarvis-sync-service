#!/usr/bin/env python3
"""Check Beeper chats in database."""

import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_KEY')
)

# Get chats count
chats = supabase.table('beeper_chats').select('id, platform, is_archived').execute()
print(f'\n=== DATABASE STATS ===\n')
print(f'Total chats: {len(chats.data)}')

# Count by platform
from collections import Counter
platforms = Counter(c['platform'] for c in chats.data)
print('\nBy platform:')
for platform, count in platforms.most_common():
    print(f'  {platform}: {count}')

archived_count = sum(1 for c in chats.data if c['is_archived'])
print(f'\nArchived: {archived_count}')
print(f'Active: {len(chats.data) - archived_count}')

# Get messages count
messages = supabase.table('beeper_messages').select('id', count='exact').execute()
print(f'\nTotal messages: {messages.count}')
