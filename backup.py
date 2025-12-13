import asyncio
import json
import csv
import os
from datetime import datetime
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

BACKUP_DIR = "backups"

async def backup_contacts():
    """
    Fetches all contacts from Supabase and saves them to a local JSON and CSV file.
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
        json_filename = os.path.join(BACKUP_DIR, f"contacts_backup_{timestamp}.json")
        with open(json_filename, "w", encoding="utf-8") as f:
            json.dump(contacts, f, indent=2, default=str)
        print(f"Saved JSON backup to {json_filename}")
        
        # 2. Save as CSV
        if contacts:
            csv_filename = os.path.join(BACKUP_DIR, f"contacts_backup_{timestamp}.csv")
            # Get all keys from the first contact (or union of all keys if schema varies)
            keys = set()
            for c in contacts:
                keys.update(c.keys())
            fieldnames = sorted(list(keys))
            
            with open(csv_filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(contacts)
            print(f"Saved CSV backup to {csv_filename}")
            
        await log_sync_event("backup", "success", f"Backup created: {len(contacts)} contacts")
        
    except Exception as e:
        print(f"Backup failed: {e}")
        await log_sync_event("backup", "error", f"Backup failed: {str(e)}")

if __name__ == "__main__":
    asyncio.run(backup_contacts())
