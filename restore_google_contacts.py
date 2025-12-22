"""
Emergency contact recovery script.

This bypasses the safety valve and pushes all contacts from Supabase to Google.
Only run this when recovering from a disaster where Google contacts were deleted.
"""

import os
import asyncio
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from lib.google_contacts import (
    get_access_token, 
    get_contact_groups, 
    create_contact_group,
    create_contact,
)
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def ensure_group_exists(token, location_name, name_to_id_map):
    """Creates location group if it doesn't exist."""
    if not location_name:
        return
    if location_name not in name_to_id_map:
        logger.info(f"Creating group: {location_name}")
        try:
            new_group_id = await create_contact_group(token, location_name)
            if new_group_id:
                name_to_id_map[location_name] = new_group_id
        except Exception as e:
            logger.error(f"Error creating group {location_name}: {e}")


async def restore_contacts_to_google():
    """
    Pushes all Supabase contacts (without google_resource_name) to Google.
    """
    logger.info("=" * 60)
    logger.info("CONTACT RECOVERY: Supabase -> Google")
    logger.info("=" * 60)
    
    # 1. Get Access Token & Groups
    token = await get_access_token()
    group_mapping = await get_contact_groups(token)
    name_to_id_map = {v: k for k, v in group_mapping.items()}
    
    # 2. Get all contacts from Supabase that need to be pushed to Google
    # (no google_resource_name AND not deleted)
    all_contacts = []
    page_size = 1000
    start = 0
    while True:
        response = supabase.table("contacts").select("*").is_("google_resource_name", "null").is_("deleted_at", "null").range(start, start + page_size - 1).execute()
        batch = response.data
        all_contacts.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    
    logger.info(f"Found {len(all_contacts)} contacts to push to Google")
    
    if len(all_contacts) == 0:
        logger.info("No contacts to restore!")
        return {"created": 0, "errors": 0}
    
    created = 0
    errors = 0
    
    for contact in all_contacts:
        try:
            name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
            email = contact.get('email', 'no email')
            logger.info(f"Creating in Google: {name} ({email})")
            
            # Ensure location group exists
            await ensure_group_exists(token, contact.get("location"), name_to_id_map)
            
            # Create in Google
            new_google_contact = await create_contact(token, contact, name_to_id_map)
            new_resource_name = new_google_contact["resourceName"]
            
            # Update Supabase with the new Google ID
            supabase.table("contacts").update({
                "google_resource_name": new_resource_name,
                "last_sync_source": "google"
            }).eq("id", contact["id"]).execute()
            
            created += 1
            
            # Rate limiting
            if created % 10 == 0:
                logger.info(f"Progress: {created}/{len(all_contacts)}")
                await asyncio.sleep(0.5)  # Avoid rate limits
                
        except Exception as e:
            logger.error(f"Error creating contact {contact.get('email')}: {e}")
            errors += 1
            # Continue with next contact
    
    logger.info("=" * 60)
    logger.info(f"RECOVERY COMPLETE: Created {created}, Errors {errors}")
    logger.info("=" * 60)
    
    return {"created": created, "errors": errors}


if __name__ == "__main__":
    result = asyncio.run(restore_contacts_to_google())
    print(f"\nResult: {result}")
