"""
Backup critical Supabase tables to Google Cloud Storage.
Run daily via Cloud Scheduler or manually.

Usage:
    python backup_to_gcs.py                    # Backup all tables
    python backup_to_gcs.py --tables contacts meetings  # Specific tables
    python backup_to_gcs.py --dry-run          # Preview without uploading
"""
import os
import json
import gzip
from datetime import datetime, timezone
from typing import Optional
from supabase import create_client

# Try to import GCS, fall back to local storage
try:
    from google.cloud import storage
    HAS_GCS = True
except ImportError:
    HAS_GCS = False
    print("⚠️  google-cloud-storage not installed, will save locally")

# Configuration
BACKUP_BUCKET = os.getenv("BACKUP_BUCKET", "jarvis-478401-backups")
BACKUP_PREFIX = "supabase-backups"

# Tables to backup (in priority order)
CRITICAL_TABLES = [
    # Core personal data
    "contacts",
    "meetings", 
    "tasks",
    "journals",
    "reflections",
    "transcripts",
    "documents",
    
    # Knowledge & memory
    "knowledge_chunks",
    "mem0_memories",
    
    # Books & reading
    "books",
    "highlights",
    "authors",
    
    # Communications
    "emails",
    "beeper_chats",
    "beeper_messages",
    
    # Calendar & events
    "calendar_events",
    
    # Applications & LinkedIn
    "applications",
    "linkedin_connections",
    "linkedin_posts",
    
    # Finance
    "revolut_transactions",
    
    # Activity
    "activity_events",
    "activity_summaries",
]

# Tables to skip (logs, caches, system tables)
SKIP_TABLES = [
    "sync_logs",
    "pipeline_logs", 
    "sync_state",
    "sync_audit",
    "letta_memory_cache",
    "alembic_version",
    "mem0migrations",
    "gocardless_state",
    "scheduled_briefings",
    "pending_clarifications",
    "conversation_topics",
]


def get_supabase_client():
    """Create Supabase client."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY required")
    return create_client(url, key)


def backup_table(client, table_name: str, batch_size: int = 1000) -> dict:
    """Backup a single table, returning all records."""
    all_records = []
    offset = 0
    
    while True:
        try:
            response = client.table(table_name).select("*").range(offset, offset + batch_size - 1).execute()
            records = response.data
            
            if not records:
                break
                
            all_records.extend(records)
            offset += batch_size
            
            if len(records) < batch_size:
                break
                
        except Exception as e:
            print(f"  ❌ Error fetching {table_name}: {e}")
            break
    
    return {
        "table": table_name,
        "record_count": len(all_records),
        "backed_up_at": datetime.now(timezone.utc).isoformat(),
        "records": all_records
    }


def upload_to_gcs(data: bytes, blob_name: str, bucket_name: str) -> str:
    """Upload data to Google Cloud Storage."""
    if not HAS_GCS:
        # Save locally instead
        local_path = f"./backups/{blob_name}"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(data)
        return f"local://{local_path}"
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type="application/gzip")
    return f"gs://{bucket_name}/{blob_name}"


def run_backup(
    tables: Optional[list] = None,
    dry_run: bool = False,
    compress: bool = True
) -> dict:
    """Run backup of specified tables."""
    
    print("=" * 60)
    print(f"SUPABASE BACKUP - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    client = get_supabase_client()
    
    # Determine which tables to backup
    tables_to_backup = tables or CRITICAL_TABLES
    
    results = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "tables": {},
        "total_records": 0,
        "errors": []
    }
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for table_name in tables_to_backup:
        if table_name in SKIP_TABLES:
            print(f"⏭️  Skipping {table_name} (in skip list)")
            continue
            
        print(f"\n📦 Backing up {table_name}...")
        
        try:
            backup_data = backup_table(client, table_name)
            record_count = backup_data["record_count"]
            
            if record_count == 0:
                print(f"  ⚪ {table_name}: 0 records (skipping)")
                continue
            
            print(f"  ✅ {table_name}: {record_count:,} records")
            results["total_records"] += record_count
            
            if not dry_run:
                # Serialize to JSON
                json_data = json.dumps(backup_data, default=str, indent=2)
                
                # Compress
                if compress:
                    compressed = gzip.compress(json_data.encode("utf-8"))
                    blob_name = f"{BACKUP_PREFIX}/{timestamp}/{table_name}.json.gz"
                    location = upload_to_gcs(compressed, blob_name, BACKUP_BUCKET)
                else:
                    blob_name = f"{BACKUP_PREFIX}/{timestamp}/{table_name}.json"
                    location = upload_to_gcs(json_data.encode("utf-8"), blob_name, BACKUP_BUCKET)
                
                print(f"  📤 Uploaded to {location}")
                results["tables"][table_name] = {
                    "records": record_count,
                    "location": location
                }
            else:
                results["tables"][table_name] = {
                    "records": record_count,
                    "location": "(dry run)"
                }
                
        except Exception as e:
            error_msg = f"{table_name}: {str(e)}"
            print(f"  ❌ Error: {e}")
            results["errors"].append(error_msg)
    
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    
    # Summary
    print("\n" + "=" * 60)
    print("BACKUP SUMMARY")
    print("=" * 60)
    print(f"Tables backed up: {len(results['tables'])}")
    print(f"Total records: {results['total_records']:,}")
    print(f"Errors: {len(results['errors'])}")
    
    if dry_run:
        print("\n⚠️  DRY RUN - No data was uploaded")
    
    return results


def list_backups(limit: int = 10) -> list:
    """List recent backups from GCS."""
    if not HAS_GCS:
        print("GCS not available, checking local backups...")
        import glob
        return glob.glob("./backups/**/*.json.gz", recursive=True)[:limit]
    
    client = storage.Client()
    bucket = client.bucket(BACKUP_BUCKET)
    blobs = bucket.list_blobs(prefix=BACKUP_PREFIX)
    
    # Group by timestamp
    backups = {}
    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) >= 3:
            timestamp = parts[1]
            if timestamp not in backups:
                backups[timestamp] = []
            backups[timestamp].append(blob.name)
    
    # Sort by timestamp descending
    sorted_backups = sorted(backups.items(), reverse=True)[:limit]
    
    print(f"\n📋 Recent backups in gs://{BACKUP_BUCKET}:")
    for timestamp, files in sorted_backups:
        print(f"\n  {timestamp}:")
        for f in files[:5]:
            print(f"    - {f.split('/')[-1]}")
        if len(files) > 5:
            print(f"    ... and {len(files) - 5} more")
    
    return sorted_backups


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Backup Supabase to GCS")
    parser.add_argument("--tables", nargs="+", help="Specific tables to backup")
    parser.add_argument("--dry-run", action="store_true", help="Preview without uploading")
    parser.add_argument("--list", action="store_true", help="List recent backups")
    parser.add_argument("--no-compress", action="store_true", help="Don't compress output")
    
    args = parser.parse_args()
    
    if args.list:
        list_backups()
    else:
        run_backup(
            tables=args.tables,
            dry_run=args.dry_run,
            compress=not args.no_compress
        )
