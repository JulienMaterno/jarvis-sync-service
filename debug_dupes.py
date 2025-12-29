"""Debug duplicate tasks."""
import os
from dotenv import load_dotenv
load_dotenv()
import httpx

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
headers = {'apikey': SUPABASE_KEY, 'Authorization': f'Bearer {SUPABASE_KEY}'}

# Check a specific duplicate
response = httpx.get(
    f'{SUPABASE_URL}/rest/v1/tasks',
    headers=headers,
    params={
        'select': 'id,title,notion_page_id,created_at,origin_type,origin_id',
        'title': 'eq.Send follow-up after Domex chat'
    }
)
tasks = response.json()
print(f'Found {len(tasks)} copies of "Send follow-up after Domex chat":')
print(tasks)
