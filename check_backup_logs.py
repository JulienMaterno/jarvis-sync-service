"""Check recent backup logs."""
from lib.supabase_client import supabase

logs = supabase.table('sync_logs').select('*').eq('event_type', 'backup_full').order('created_at', desc=True).limit(10).execute()

print("=== Recent Backup Logs ===\n")
for log in logs.data:
    created = log.get("created_at", "")[:19]
    status = log.get("status", "")
    message = log.get("message", "")
    print(f"{created}  [{status}]  {message}")

print("\n=== Storage Status ===")
# Check Supabase storage
try:
    files = supabase.storage.from_("backups").list()
    backup_files = [f for f in files if f.get("name", "").startswith("full_backup")]
    print(f"Supabase Storage: {len(backup_files)} backup files found")
    for f in sorted(backup_files, key=lambda x: x.get("name", ""))[-5:]:
        print(f"  - {f.get('name')}")
except Exception as e:
    print(f"Supabase Storage error: {e}")
