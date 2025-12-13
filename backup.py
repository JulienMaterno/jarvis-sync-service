import asyncio
import json
import csv
import os
from datetime import datetime
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

BACKUP_DIR = "backups"
SUPABASE_BUCKET = "backups"

async def upload_to_supabase_storage(filename: str, content: bytes, content_type: str):
    """
    Uploads content to Supabase Storage.
    """
    try:
        # Note: Supabase Storage operations are synchronous in the Python client
        # We use 'upsert': 'true' to overwrite if exists (though timestamps make filenames unique)
        res = supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=filename,
            file=content,
            file_options={"content-type": content_type, "upsert": "true"}
        )
        print(f"Uploaded {filename} to Supabase Storage bucket '{SUPABASE_BUCKET}'")
        await log_sync_event("backup_upload", "success", f"Uploaded {filename} to Supabase Storage")
    except Exception as e:
        print(f"Supabase Storage upload failed: {e}")
        await log_sync_event("backup_upload", "error", f"Supabase Storage upload failed: {str(e)}")

async def backup_contacts():
    """
    Fetches all contacts from Supabase and saves them to a local JSON and CSV file.
    Also uploads to Supabase Storage.
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
        
        # Upload JSON to Supabase Storage
        # Convert string to bytes for upload
        await upload_to_supabase_storage(json_filename, json_content.encode('utf-8'), "application/json")
        
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
            
            # Upload CSV to Supabase Storage
            await upload_to_supabase_storage(csv_filename, csv_content.encode('utf-8'), "text/csv")
            
        await log_sync_event("backup", "success", f"Backup created: {len(contacts)} contacts")
        return {"status": "success", "count": len(contacts), "timestamp": timestamp}
        
    except Exception as e:
        print(f"Backup failed: {e}")
        await log_sync_event("backup", "error", f"Backup failed: {str(e)}")
        raise e


if __name__ == "__main__":
    asyncio.run(backup_contacts())
