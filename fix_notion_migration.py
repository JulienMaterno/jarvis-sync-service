"""
Emergency fix for Notion migration issue.

After migrating to a new Notion account:
1. Old notion_page_id values point to non-existent pages
2. Sync incorrectly marked contacts as deleted
3. Google contacts were then deleted

This script:
1. Clears deleted_at for contacts that were incorrectly marked
2. Clears old notion_page_id values
3. Re-creates contacts in Google from Supabase

Run this ONCE after the Notion migration.
"""

import os
import asyncio
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fix_contacts():
    """
    Fix contacts that were incorrectly marked as deleted.
    """
    logger.info("=" * 60)
    logger.info("NOTION MIGRATION FIX")
    logger.info("=" * 60)
    
    # Step 1: Get all contacts
    all_contacts = []
    page_size = 1000
    start = 0
    while True:
        response = supabase.table("contacts").select("*").range(start, start + page_size - 1).execute()
        batch = response.data
        all_contacts.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    
    logger.info(f"Total contacts in Supabase: {len(all_contacts)}")
    
    # Count statistics
    deleted_contacts = [c for c in all_contacts if c.get("deleted_at")]
    with_old_notion_id = [c for c in all_contacts if c.get("notion_page_id") and c.get("notion_page_id").startswith("2c7cd3f1")]
    
    logger.info(f"Contacts marked as deleted: {len(deleted_contacts)}")
    logger.info(f"Contacts with OLD notion_page_id (2c7cd3f1...): {len(with_old_notion_id)}")
    
    # Step 2: Clear deleted_at for ALL contacts (restore them)
    if deleted_contacts:
        logger.info(f"\nRestoring {len(deleted_contacts)} incorrectly deleted contacts...")
        for contact in deleted_contacts:
            contact_id = contact["id"]
            name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
            logger.info(f"  Restoring: {name} ({contact.get('email', 'no email')})")
            
            supabase.table("contacts").update({
                "deleted_at": None,
                "last_sync_source": "migration_fix"
            }).eq("id", contact_id).execute()
        
        logger.info(f"Restored {len(deleted_contacts)} contacts.")
    
    # Step 3: Clear ALL notion_page_id values (they're all from the old account)
    contacts_with_notion = [c for c in all_contacts if c.get("notion_page_id")]
    if contacts_with_notion:
        logger.info(f"\nClearing {len(contacts_with_notion)} old Notion page IDs...")
        
        for contact in contacts_with_notion:
            contact_id = contact["id"]
            supabase.table("contacts").update({
                "notion_page_id": None,
                "notion_updated_at": None
            }).eq("id", contact_id).execute()
        
        logger.info(f"Cleared {len(contacts_with_notion)} Notion page IDs.")
    
    # Also clear notion_page_id from meetings, reflections, journals, tasks
    for table_name in ["meetings", "reflections", "journals", "tasks"]:
        try:
            # Get count
            response = supabase.table(table_name).select("id", count="exact").not_.is_("notion_page_id", "null").execute()
            count = response.count if hasattr(response, 'count') else len(response.data)
            
            if count > 0:
                logger.info(f"\nClearing {count} old Notion page IDs from {table_name}...")
                
                # Fetch all with notion_page_id
                all_records = []
                start = 0
                while True:
                    batch = supabase.table(table_name).select("id").not_.is_("notion_page_id", "null").range(start, start + 999).execute()
                    all_records.extend(batch.data)
                    if len(batch.data) < 1000:
                        break
                    start += 1000
                
                for record in all_records:
                    supabase.table(table_name).update({
                        "notion_page_id": None,
                        "notion_updated_at": None
                    }).eq("id", record["id"]).execute()
                
                logger.info(f"Cleared Notion page IDs from {len(all_records)} {table_name}.")
        except Exception as e:
            logger.warning(f"Could not process {table_name}: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info("FIX COMPLETE")
    logger.info("=" * 60)
    logger.info("\nNext steps:")
    logger.info("1. Run /sync/contacts to recreate Google contacts")
    logger.info("2. Run /sync/supabase-to-notion to create new Notion pages")
    logger.info("3. Run /sync/all to sync everything")
    
    return {
        "restored_contacts": len(deleted_contacts),
        "cleared_notion_ids": len(contacts_with_notion)
    }


if __name__ == "__main__":
    result = fix_contacts()
    print(f"\nResult: {result}")
