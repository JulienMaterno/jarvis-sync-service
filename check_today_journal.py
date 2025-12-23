"""Check today's journal entry."""
from supabase import create_client
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

today = datetime.now(timezone.utc).date().isoformat()
result = supabase.table('journals').select('id, date, title, content').eq('date', today).execute()

if result.data:
    j = result.data[0]
    print(f"Journal for {today}:")
    print(f"  ID: {j['id']}")
    print(f"  Title: {j['title']}")
    content = j.get('content', '')[:1000] if j.get('content') else 'No content'
    print(f"  Content (first 1000 chars):\n{content}...")
else:
    print(f"No journal found for {today}")
