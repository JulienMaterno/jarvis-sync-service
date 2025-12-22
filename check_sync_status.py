"""Check most recent sync status."""
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

print('=== MOST RECENT SYNCS ===')

# Calendar
cal = supabase.table('sync_logs').select('*').eq('event_type', 'calendar_sync').order('created_at', desc=True).limit(3).execute()
for log in cal.data:
    t = log.get('created_at', '')[:19]
    s = log.get('status', '')
    m = (log.get('message') or '')[:60]
    print(f"Calendar: {t} - {s} - {m}")

# Gmail
gmail = supabase.table('sync_logs').select('*').eq('event_type', 'gmail_sync').order('created_at', desc=True).limit(3).execute()
for log in gmail.data:
    t = log.get('created_at', '')[:19]
    s = log.get('status', '')
    m = (log.get('message') or '')[:60]
    print(f"Gmail: {t} - {s} - {m}")

# Meetings
meetings = supabase.table('sync_logs').select('*').ilike('event_type', '%meeting%').order('created_at', desc=True).limit(3).execute()
for log in meetings.data:
    t = log.get('created_at', '')[:19]
    e = log.get('event_type', '')
    s = log.get('status', '')
    print(f"Meeting: {t} - {e} - {s}")

# Contacts
contacts = supabase.table('sync_logs').select('*').ilike('event_type', '%contact%').order('created_at', desc=True).limit(3).execute()
for log in contacts.data:
    t = log.get('created_at', '')[:19]
    e = log.get('event_type', '')
    m = (log.get('message') or '')[:60]
    print(f"Contact: {t} - {e} - {m}")
