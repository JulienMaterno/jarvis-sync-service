"""Quick script to check Beeper data in Supabase."""
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')

supabase = create_client(url, key)

# Count beeper_chats
chats_result = supabase.table('beeper_chats').select('id', count='exact').execute()
chats_count = chats_result.count if chats_result.count is not None else len(chats_result.data)

# Count beeper_messages
messages_result = supabase.table('beeper_messages').select('id', count='exact').execute()
messages_count = messages_result.count if messages_result.count is not None else len(messages_result.data)

# Get last sync time
last_sync_result = supabase.table('beeper_chats').select('last_synced_at').order('last_synced_at', desc=True).limit(1).execute()

print('=== Beeper Data Summary ===')
print(f'beeper_chats rows: {chats_count}')
print(f'beeper_messages rows: {messages_count}')

if last_sync_result.data and last_sync_result.data[0].get('last_synced_at'):
    print(f"Last sync: {last_sync_result.data[0]['last_synced_at']}")
else:
    print('Last sync: No sync timestamp found')

# Get all columns from beeper_chats to see the schema
print('\n=== Sample beeper_chats (all columns) ===')
sample_chats = supabase.table('beeper_chats').select('*').limit(3).execute()
for chat in sample_chats.data:
    print(f"  Chat: {chat}")

print('\n=== Sample beeper_messages (all columns) ===')
sample_msgs = supabase.table('beeper_messages').select('*').order('timestamp', desc=True).limit(3).execute()
for msg in sample_msgs.data:
    print(f"  Msg: {msg}")
