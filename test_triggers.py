"""Test if SQL triggers are working correctly."""
from supabase import create_client
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

db = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

def test_trigger(table_name: str, id_field: str = 'id'):
    """Test trigger on a specific table."""
    print(f"\n{'='*50}")
    print(f"Testing {table_name} trigger...")
    print('='*50)
    
    # Get a record
    record = db.table(table_name).select('*').limit(1).execute()
    if not record.data:
        print(f"  No records in {table_name}")
        return None
    
    rec = record.data[0]
    rec_id = rec[id_field]
    
    print(f"  BEFORE: last_sync_source = {rec.get('last_sync_source', 'N/A')}")
    print(f"  BEFORE: updated_at = {rec.get('updated_at', 'N/A')}")
    
    # Update with a test field that exists on all tables
    if 'notes' in rec:
        update_field = 'notes'
        update_value = f"Trigger test {datetime.now().isoformat()}"
    elif 'description' in rec:
        update_field = 'description'  
        update_value = rec.get('description', '') or ''
        if not update_value.endswith(' (trigger test)'):
            update_value += ' (trigger test)'
    else:
        # Find any text field
        for field in rec:
            if isinstance(rec[field], str) and field not in ['id', 'notion_page_id', 'last_sync_source']:
                update_field = field
                update_value = rec[field]
                break
        else:
            print(f"  Cannot find a field to update")
            return None
    
    # Do the update (WITHOUT explicitly setting last_sync_source)
    db.table(table_name).update({
        update_field: update_value
    }).eq(id_field, rec_id).execute()
    
    # Check result
    after = db.table(table_name).select('*').eq(id_field, rec_id).single().execute()
    after_rec = after.data
    
    print(f"  AFTER:  last_sync_source = {after_rec.get('last_sync_source', 'N/A')}")
    print(f"  AFTER:  updated_at = {after_rec.get('updated_at', 'N/A')}")
    
    if after_rec.get('last_sync_source') == 'supabase':
        print("  ✓ TRIGGER WORKING!")
        return True
    else:
        print(f"  ✗ TRIGGER NOT WORKING (expected 'supabase', got '{after_rec.get('last_sync_source')}')")
        return False

# Test all sync-enabled tables
tables = ['applications', 'meetings', 'tasks', 'reflections', 'journals', 'contacts', 'documents']

print("\n" + "="*60)
print("  TESTING SQL TRIGGERS FOR AUTO-SYNC")
print("="*60)

results = {}
for table in tables:
    try:
        results[table] = test_trigger(table)
    except Exception as e:
        print(f"\n  ERROR testing {table}: {e}")
        results[table] = None

print("\n" + "="*60)
print("  SUMMARY")
print("="*60)
for table, status in results.items():
    if status is True:
        print(f"  ✓ {table}")
    elif status is False:
        print(f"  ✗ {table} - TRIGGER NOT WORKING")
    else:
        print(f"  ? {table} - Could not test")

working = sum(1 for s in results.values() if s is True)
total = len([s for s in results.values() if s is not None])
print(f"\n  {working}/{total} triggers working")
