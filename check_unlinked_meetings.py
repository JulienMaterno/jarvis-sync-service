"""Check meetings that have unlinked contacts."""
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

print('=== MEETINGS WITH UNLINKED CONTACTS ===')
print('(Meetings that have contact_name but no contact_id)')
print()

# Query for meetings with contact_name but no contact_id
result = supabase.table('meetings').select(
    'id,title,date,contact_name,contact_id'
).not_.is_('contact_name', 'null').is_('contact_id', 'null').order('date', desc=True).limit(20).execute()

for meeting in result.data:
    name = meeting.get('contact_name', 'Unknown')
    title = meeting.get('title', 'No title')[:40]
    date = (meeting.get('date') or '')[:10]
    print(f"[{date}] {title} - Missing: '{name}'")

print(f"\nTotal unlinked meetings found: {len(result.data)}")
