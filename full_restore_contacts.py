"""
Clear old Google resource names and push contacts to Google.
"""
import os
import asyncio
import logging
from dotenv import load_dotenv
load_dotenv()

from lib.google_contacts import (
    get_access_token, 
    get_contact_groups, 
    create_contact_group,
    create_contact,
)
from lib.supabase_client import supabase

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


async def full_restore():
    """
    1. Clear all google_resource_name in Supabase
    2. Push all contacts to Google
    """
    logger.info("=" * 60)
    logger.info("FULL CONTACT RESTORE")
    logger.info("=" * 60)
    
    # Step 1: Clear all google_resource_name
    logger.info("Step 1: Clearing old Google resource names...")
    
    # Get all contacts with google_resource_name
    all_contacts = []
    page_size = 1000
    start = 0
    while True:
        response = supabase.table("contacts").select("*").is_("deleted_at", "null").range(start, start + page_size - 1).execute()
        batch = response.data
        all_contacts.extend(batch)
        if len(batch) < page_size:
            break
        start += page_size
    
    logger.info(f"Found {len(all_contacts)} active contacts")
    
    # Clear google_resource_name for all
    for contact in all_contacts:
        if contact.get("google_resource_name"):
            supabase.table("contacts").update({
                "google_resource_name": None
            }).eq("id", contact["id"]).execute()
    
    logger.info(f"Cleared google_resource_name for all contacts")
    
    # Step 2: Push all to Google
    logger.info("\nStep 2: Pushing contacts to Google...")
    
    token = await get_access_token()
    group_mapping = await get_contact_groups(token)
    name_to_id_map = {v: k for k, v in group_mapping.items()}
    
    created = 0
    errors = 0
    
    for i, contact in enumerate(all_contacts):
        try:
            name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
            email = contact.get('email', 'no email')
            
            if i % 20 == 0:
                logger.info(f"Progress: {i}/{len(all_contacts)} - Creating: {name}")
            
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
                await asyncio.sleep(0.3)  # Avoid rate limits
                
        except Exception as e:
            logger.error(f"Error creating contact {contact.get('email')}: {e}")
            errors += 1
            await asyncio.sleep(1)  # Longer wait on error
    
    logger.info("=" * 60)
    logger.info(f"RESTORE COMPLETE: Created {created}, Errors {errors}")
    logger.info("=" * 60)
    
    return {"created": created, "errors": errors}


if __name__ == "__main__":
    result = asyncio.run(full_restore())
    print(f"\nResult: {result}")
