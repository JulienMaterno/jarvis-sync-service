"""Test sync log error detection and fallback."""
from lib.supabase_client import supabase

log_data = {
    'event_type': 'test_fallback',
    'status': 'success',
    'message': 'testing fallback',
    'details': {'foo': 'bar'}
}

try:
    result = supabase.table('sync_logs').insert(log_data).execute()
    print("Inserted with details - column exists!")
except Exception as e:
    print(f"First attempt failed: {e}")
    print(f"Contains 'details': {'details' in str(e).lower()}")
    print(f"Contains 'column': {'column' in str(e).lower()}")
    
    # Try fallback
    if 'details' in str(e).lower() or 'column' in str(e).lower():
        print("Trying fallback without details...")
        log_data.pop('details', None)
        try:
            result = supabase.table('sync_logs').insert(log_data).execute()
            print("Fallback succeeded!")
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
