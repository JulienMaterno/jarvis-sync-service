"""Check backup contents"""
from supabase import create_client
import os
import json
import gzip
from dotenv import load_dotenv

load_dotenv()

sb = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Get the latest backup file content
backup_bytes = sb.storage.from_('backups').download('full_backup_20260114_190005.json.gz')
data = json.loads(gzip.decompress(backup_bytes).decode())

print('=== LATEST BACKUP SUMMARY (2026-01-14) ===')
print(f"Timestamp: {data.get('timestamp', 'N/A')}")
print()
print("Data keys:", data.keys())
tables_data = data.get('tables', {})
if isinstance(tables_data, dict):
    for table_name, table_info in tables_data.items():
        if isinstance(table_info, dict):
            print(f"{table_name}: {table_info.get('count', len(table_info.get('data', [])))} rows")
        else:
            print(f"{table_name}: {len(table_info) if isinstance(table_info, list) else '?'} rows")
else:
    for table in tables_data:
        print(f"{table['table']}: {table['count']} rows")
