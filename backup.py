import asyncio
import json
import csv
import os
from datetime import datetime
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

try:
    from google.cloud import storage
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

BACKUP_DIR = "backups"

async def upload_to_gcs(filename: str, content: str, content_type: str):
    """
    Uploads content to Google Cloud Storage.
    """
    bucket_name = os.environ.get("GCS_BACKUP_BUCKET")
    if not bucket_name:
        print("Skipping GCS upload: GCS_BACKUP_BUCKET not set.")
        return

    if not GCS_AVAILABLE:
        print("Skipping GCS upload: google-cloud-storage not installed.")
        return

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.upload_from_string(content, content_type=content_type)
        print(f"Uploaded {filename} to GCS bucket {bucket_name}")
        await log_sync_event("backup_upload", "success", f"Uploaded {filename} to GCS")
    except Exception as e:
        print(f"GCS upload failed: {e}")
        await log_sync_event("backup_upload", "error", f"GCS upload failed: {str(e)}")

async def backup_contacts():
    """
    Fetches all contacts from Supabase and saves them to a local JSON and CSV file.
    Also uploads to GCS if configured.
    """
    try:
        print("Starting backup...")
        
        # Ensure backup directory exists
        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
            
        # Fetch all contacts
        contacts = []
        page_size = 1000
        start = 0
        while True:
            response = supabase.table("contacts").select("*").range(start, start + page_size - 1).execute()
            batch = response.data
            contacts.extend(batch)
            if len(batch) < page_size:
                break
            start += page_size
            
        print(f"Fetched {len(contacts)} contacts.")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Save as JSON
        json_filename = f"contacts_backup_{timestamp}.json"
        json_path = os.path.join(BACKUP_DIR, json_filename)
        json_content = json.dumps(contacts, indent=2, default=str)
        
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(json_content)
        print(f"Saved JSON backup to {json_path}")
        
        # Upload JSON to GCS
        await upload_to_gcs(json_filename, json_content, "application/json")
        
        # 2. Save as CSV
        if contacts:
            csv_filename = f"contacts_backup_{timestamp}.csv"
            csv_path = os.path.join(BACKUP_DIR, csv_filename)
            # Get all keys from the first contact (or union of all keys if schema varies)
            keys = set()
            for c in contacts:
                keys.update(c.keys())
            fieldnames = sorted(list(keys))
            
            import io
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(contacts)
            csv_content = output.getvalue()
            
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                f.write(csv_content)
            print(f"Saved CSV backup to {csv_path}")
            
            # Upload CSV to GCS
            await upload_to_gcs(csv_filename, csv_content, "text/csv")
            
        await log_sync_event("backup", "success", f"Backup created: {len(contacts)} contacts")
        return {"status": "success", "count": len(contacts), "timestamp": timestamp}
        
    except Exception as e:
        print(f"Backup failed: {e}")
        await log_sync_event("backup", "error", f"Backup failed: {str(e)}")
        raise e


if __name__ == "__main__":
    asyncio.run(backup_contacts())
