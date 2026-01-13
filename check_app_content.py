"""Quick check of application content fields."""
from lib.supabase_client import supabase

r = supabase.table('applications').select('name,content,context,notes').limit(2).execute()

for app in r.data:
    print(f"\n=== {app['name']} ===")
    print(f"Content type: {type(app.get('content'))}")
    print(f"Content value: {repr(app.get('content'))[:200]}")
    print(f"Context type: {type(app.get('context'))}")  
    print(f"Context value: {repr(app.get('context'))[:200]}")
    print(f"Notes type: {type(app.get('notes'))}")
    print(f"Notes value: {repr(app.get('notes'))[:200]}")
