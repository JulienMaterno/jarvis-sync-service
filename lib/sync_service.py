from lib.google_contacts import get_access_token, get_all_contacts, get_contact_groups, transform_contact
from lib.supabase_client import supabase
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def sync_google_contacts_to_supabase():
    """
    Orchestrates the sync from Google Contacts to Supabase.
    """
    try:
        logger.info("Starting Google Contacts sync...")
        
        # 1. Get Access Token
        token = await get_access_token()
        logger.info("Obtained Google access token.")
        
        # 2. Get Contact Groups (for mapping labels)
        group_mapping = await get_contact_groups(token)
        logger.info(f"Fetched {len(group_mapping)} contact groups.")
        
        # 3. Fetch all contacts
        google_contacts = await get_all_contacts(token)
        logger.info(f"Fetched {len(google_contacts)} contacts from Google.")
        
        synced_count = 0
        errors_count = 0
        
        # 4. Transform and Upsert
        for gc in google_contacts:
            try:
                contact_data = transform_contact(gc, group_mapping)
                
                # Upsert to Supabase
                # Assuming 'google_resource_name' is a unique key or primary key in Supabase
                # If it's not the primary key, we need to ensure the table has a unique constraint on it
                # and use on_conflict.
                response = supabase.table("contacts").upsert(
                    contact_data, 
                    on_conflict="google_resource_name"
                ).execute()
                
                synced_count += 1
                
            except Exception as e:
                logger.error(f"Error syncing contact {gc.get('resourceName')}: {e}")
                errors_count += 1
                
        logger.info(f"Sync complete. Synced: {synced_count}, Errors: {errors_count}")
        return {"synced": synced_count, "errors": errors_count}

    except Exception as e:
        logger.error(f"Fatal error during sync: {e}")
        raise e
