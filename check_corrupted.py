"""Check corrupted content count."""
from lib.supabase_client import supabase

r = supabase.table('applications').select('content').execute()
bad = [x for x in r.data if x.get('content') and '["' in str(x.get('content'))]
print(f'Applications with corrupted content: {len(bad)}/{len(r.data)}')

# Show some examples
for app in bad[:3]:
    print(f"  - {repr(app.get('content'))}")
