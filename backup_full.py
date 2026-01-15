"""
Full Database Backup System

Backs up ALL critical Supabase tables to:
1. Google Cloud Storage (for long-term retention)
2. Supabase Storage bucket (for quick access)

Run daily via Cloud Scheduler: POST /backup/full

Tables backed up:
- contacts (CRM)
- meetings (meeting records)
- tasks (action items)
- journals (daily journals)
- reflections (topic reflections)
- transcripts (voice memo transcripts)
- calendar_events (Google Calendar)
- emails (Gmail)
- beeper_chats (messaging)
- beeper_messages (chat history)
- books (reading list)
- highlights (book highlights)
- applications (grants/fellowships)
- linkedin_posts (LinkedIn content)
"""

import asyncio
import json
import gzip
import os
from datetime import datetime, timezone
from typing import Dict, List, Any

# Import from existing lib
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

# GCS imports (optional - for cloud storage)
try:
    from google.cloud import storage
    HAS_GCS = True
except ImportError:
    HAS_GCS = False
    print("Warning: google-cloud-storage not installed. GCS backup disabled.")

# Tables to back up (in priority order - most important first)
CRITICAL_TABLES = [
    "contacts",           # CRM - irreplaceable
    "meetings",           # Meeting summaries - irreplaceable
    "tasks",              # Action items
    "journals",           # Daily journals
    "reflections",        # Topic reflections
    "transcripts",        # Voice memo transcripts
]

IMPORTANT_TABLES = [
    "calendar_events",    # Can be re-synced from Google
    "emails",             # Can be re-synced from Gmail
    "beeper_chats",       # Messaging
    "beeper_messages",    # Chat history
]

KNOWLEDGE_TABLES = [
    "documents",          # RAG documents
    "knowledge_chunks",   # RAG embeddings/chunks
    "chat_messages",      # Chat history with AI
]

SYSTEM_TABLES = [
    "sync_logs",          # Sync operation logs
    "pipeline_logs",      # Audio pipeline logs
]

OPTIONAL_TABLES = [
    "books",              # Reading list
    "highlights",         # Book highlights
    "applications",       # Grant applications
    "linkedin_posts",     # LinkedIn content
]

ALL_TABLES = CRITICAL_TABLES + IMPORTANT_TABLES + KNOWLEDGE_TABLES + SYSTEM_TABLES + OPTIONAL_TABLES

# Supabase Storage bucket for backups
SUPABASE_BUCKET = "backups"

# GCS bucket name (set via env var)
GCS_BUCKET = os.getenv("BACKUP_GCS_BUCKET", "jarvis-478401-backups")


async def backup_table(table_name: str) -> Dict[str, Any]:
    """
    Backup a single table.
    Returns the data and row count.
    """
    try:
        data = []
        page_size = 1000
        start = 0
        
        while True:
            response = supabase.table(table_name).select("*").range(start, start + page_size - 1).execute()
            batch = response.data
            data.extend(batch)
            
            if len(batch) < page_size:
                break
            start += page_size
        
        return {
            "table": table_name,
            "count": len(data),
            "data": data,
            "status": "success"
        }
    except Exception as e:
        return {
            "table": table_name,
            "count": 0,
            "data": [],
            "status": "error",
            "error": str(e)
        }


async def upload_to_supabase_storage(filename: str, content: bytes, content_type: str) -> bool:
    """Upload backup to Supabase Storage."""
    try:
        supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=filename,
            file=content,
            file_options={"content-type": content_type, "upsert": "true"}
        )
        return True
    except Exception as e:
        print(f"Supabase Storage upload failed: {e}")
        return False


async def upload_to_gcs(filename: str, content: bytes, content_type: str) -> bool:
    """Upload backup to Google Cloud Storage."""
    if not HAS_GCS:
        return False
    
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(f"database-backups/{filename}")
        blob.upload_from_string(content, content_type=content_type)
        return True
    except Exception as e:
        print(f"GCS upload failed: {e}")
        return False


async def run_full_backup() -> Dict[str, Any]:
    """
    Run a full backup of all tables.
    
    Returns summary with counts and any errors.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    results = {
        "timestamp": timestamp,
        "tables": {},
        "total_rows": 0,
        "errors": [],
        "storage": {
            "supabase": False,
            "gcs": False
        }
    }
    
    # Backup all tables
    all_data = {}
    
    for table in ALL_TABLES:
        print(f"Backing up {table}...")
        result = await backup_table(table)
        
        results["tables"][table] = {
            "count": result["count"],
            "status": result["status"]
        }
        
        if result["status"] == "success":
            results["total_rows"] += result["count"]
            all_data[table] = result["data"]
        else:
            results["errors"].append(f"{table}: {result.get('error', 'Unknown error')}")
    
    # Create single JSON backup file (compressed)
    backup_content = json.dumps({
        "timestamp": timestamp,
        "tables": all_data,
        "metadata": {
            "total_tables": len(all_data),
            "total_rows": results["total_rows"]
        }
    }, default=str)
    
    # Compress with gzip
    compressed = gzip.compress(backup_content.encode('utf-8'))
    filename = f"full_backup_{timestamp}.json.gz"
    
    print(f"Backup size: {len(backup_content):,} bytes → {len(compressed):,} bytes (compressed)")
    
    # Upload to Supabase Storage
    if await upload_to_supabase_storage(filename, compressed, "application/gzip"):
        results["storage"]["supabase"] = True
        await log_sync_event("backup_full", "success", f"Uploaded to Supabase: {filename}")
    
    # Upload to GCS (if available)
    if await upload_to_gcs(filename, compressed, "application/gzip"):
        results["storage"]["gcs"] = True
        await log_sync_event("backup_full", "success", f"Uploaded to GCS: {filename}")
    
    # Also save individual critical tables (uncompressed for easy access)
    for table in CRITICAL_TABLES:
        if table in all_data:
            table_json = json.dumps(all_data[table], default=str, indent=2)
            table_filename = f"{table}_{timestamp}.json"
            await upload_to_supabase_storage(table_filename, table_json.encode('utf-8'), "application/json")
    
    # Log final result
    status = "success" if not results["errors"] else "partial"
    await log_sync_event(
        "backup_full", 
        status, 
        f"Full backup: {results['total_rows']} rows from {len(all_data)} tables. Errors: {len(results['errors'])}"
    )
    
    return results


async def list_backups() -> List[Dict[str, Any]]:
    """List available backups from Supabase Storage."""
    try:
        files = supabase.storage.from_(SUPABASE_BUCKET).list()
        backups = [
            {
                "name": f.get("name"),
                "size": f.get("metadata", {}).get("size", 0),
                "created": f.get("created_at")
            }
            for f in (files or [])
            if f.get("name", "").startswith("full_backup_") or f.get("name", "").endswith(".json")
        ]
        return sorted(backups, key=lambda x: x.get("created", ""), reverse=True)
    except Exception as e:
        print(f"Failed to list backups: {e}")
        return []


async def restore_table(table_name: str, backup_filename: str) -> Dict[str, Any]:
    """
    Restore a table from backup.
    
    WARNING: This will INSERT data (not replace). 
    Manual cleanup may be needed first.
    """
    try:
        # Download backup
        data = supabase.storage.from_(SUPABASE_BUCKET).download(backup_filename)
        
        # Decompress if gzip
        if backup_filename.endswith('.gz'):
            data = gzip.decompress(data)
        
        backup = json.loads(data)
        
        # Get table data
        if table_name in backup.get("tables", {}):
            rows = backup["tables"][table_name]
        elif isinstance(backup, list):
            rows = backup  # Direct table backup
        else:
            return {"error": f"Table {table_name} not found in backup"}
        
        # Insert rows (batch)
        inserted = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i+100]
            # Remove id to let Supabase generate new ones (or handle conflicts)
            for row in batch:
                row.pop('id', None)
            
            supabase.table(table_name).insert(batch).execute()
            inserted += len(batch)
        
        return {
            "status": "success",
            "table": table_name,
            "rows_restored": inserted
        }
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--list":
        backups = asyncio.run(list_backups())
        print("Available backups:")
        for b in backups[:20]:
            print(f"  {b['name']} ({b['size']:,} bytes) - {b['created']}")
    else:
        result = asyncio.run(run_full_backup())
        print(f"\nBackup complete!")
        print(f"Total rows: {result['total_rows']:,}")
        print(f"Supabase Storage: {'✓' if result['storage']['supabase'] else '✗'}")
        print(f"GCS: {'✓' if result['storage']['gcs'] else '✗'}")
        if result['errors']:
            print(f"Errors: {result['errors']}")
