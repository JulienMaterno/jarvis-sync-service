from lib.google_contacts import (
    get_access_token, 
    get_all_contacts, 
    get_contact_groups, 
    transform_contact,
    create_contact,
    update_contact,
    delete_contact
)
from lib.supabase_client import supabase
import logging
from datetime import datetime, timezone, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def sync_contacts():
    """
    Bi-directional sync between Google Contacts and Supabase.
    Uses 'Last Write Wins' based on timestamps.
    
    REQUIRES: 'deleted_at' timestamp column in Supabase 'contacts' table.
    """
    try:
        logger.info("Starting bi-directional sync...")
        
        # 1. Get Access Token & Groups
        token = await get_access_token()
        group_mapping = await get_contact_groups(token)
        # Invert mapping for writing (Name -> ID)
        name_to_id_map = {v: k for k, v in group_mapping.items()}
        
        # 2. Fetch all from Google
        google_contacts_list = await get_all_contacts(token)
        # Map by resourceName for easy lookup
        google_contacts_map = {gc["resourceName"]: gc for gc in google_contacts_list}
        logger.info(f"Fetched {len(google_contacts_list)} contacts from Google.")
        
        # 3. Fetch all from Supabase (including soft-deleted) with Pagination
        supabase_contacts = []
        page_size = 1000
        start = 0
        while True:
            response = supabase.table("contacts").select("*").range(start, start + page_size - 1).execute()
            batch = response.data
            supabase_contacts.extend(batch)
            if len(batch) < page_size:
                break
            start += page_size
            
        logger.info(f"Fetched {len(supabase_contacts)} contacts from Supabase.")
        
        # SAFETY VALVE: If Google returns empty or very few contacts but Supabase has many, 
        # abort to prevent mass deletion.
        if len(supabase_contacts) > 10 and len(google_contacts_list) < (len(supabase_contacts) * 0.1):
            raise Exception(f"Safety Valve Triggered: Google returned {len(google_contacts_list)} contacts, but Supabase has {len(supabase_contacts)}. Aborting sync to prevent mass deletion.")

        synced_count = 0
        errors_count = 0
        
        # 4. Process Supabase Contacts (Source of Truth)
        for sb_contact in supabase_contacts:
            resource_name = sb_contact.get("google_resource_name")
            deleted_at = sb_contact.get("deleted_at")
            
            try:
                if deleted_at:
                    # Case: Soft-deleted in Supabase -> Delete from Google
                    if resource_name and resource_name in google_contacts_map:
                        logger.info(f"Deleting Google contact {resource_name} (Deleted in Supabase)")
                        await delete_contact(token, resource_name)
                        # Remove from map so we don't re-ingest it
                        del google_contacts_map[resource_name]
                        synced_count += 1
                    continue
                        
                # Case: Active in Supabase
                if not resource_name:
                    # Case: New in Supabase (Notion/Manual) -> Create in Google
                    logger.info(f"Creating contact in Google: {sb_contact.get('email')}")
                    new_contact = await create_contact(token, sb_contact, name_to_id_map)
                    new_resource_name = new_contact["resourceName"]
                    
                    # Update Supabase with the new ID
                    supabase.table("contacts").update({
                        "google_resource_name": new_resource_name,
                        "last_sync_source": "google"
                    }).eq("id", sb_contact["id"]).execute()
                    
                    synced_count += 1
                    
                elif resource_name in google_contacts_map:
                    # Case: Exists in both -> Compare timestamps
                    google_contact = google_contacts_map[resource_name]
                    etag = google_contact.get("etag")
                    
                    # Transform current Google data to Supabase format for comparison
                    current_google_data = transform_contact(google_contact, group_mapping)
                    
                    # Timestamps
                    sb_updated_at = sb_contact.get("updated_at")
                    google_updated_at = current_google_data.get("_google_updated_at")
                    
                    # Determine winner
                    update_direction = None # None, 'to_google', 'to_supabase'
                    
                    if sb_updated_at and google_updated_at:
                        # Parse timestamps
                        # Supabase: ISO8601 (e.g. 2023-10-27T10:00:00+00:00)
                        # Google: RFC3339 (e.g. 2023-10-27T10:00:00.123Z)
                        sb_dt = datetime.fromisoformat(sb_updated_at.replace('Z', '+00:00'))
                        # Google often has 'Z' at the end
                        google_dt = datetime.fromisoformat(google_updated_at.replace('Z', '+00:00'))
                        
                        # Buffer for self-updates (5 seconds)
                        last_source = sb_contact.get("last_sync_source")
                        
                        if last_source == "google":
                            # Last update came from Google. Only update Google if Supabase is significantly newer
                            if sb_dt > google_dt + timedelta(seconds=5):
                                update_direction = 'to_google'
                            elif google_dt > sb_dt:
                                # Google is newer (user edit in Google)
                                update_direction = 'to_supabase'
                        else:
                            # Last update came from Supabase/Notion. Only update Supabase if Google is significantly newer
                            if google_dt > sb_dt + timedelta(seconds=5):
                                update_direction = 'to_supabase'
                            elif sb_dt > google_dt:
                                update_direction = 'to_google'
                    elif not google_updated_at:
                        update_direction = 'to_google'
                    
                    # If timestamps are close or inconclusive, check content equality to avoid unnecessary writes
                    if not update_direction:
                        # Fallback: Check content. If different, default to Supabase (Hub) -> Google
                        # But only if we didn't decide 'to_supabase' already
                        pass

                    # Execute Update
                    if update_direction == 'to_supabase':
                        logger.info(f"Updating Supabase contact {sb_contact.get('email')} from Google (Google is newer)")
                        # Remove internal fields
                        update_data = {k: v for k, v in current_google_data.items() if not k.startswith('_')}
                        update_data["last_sync_source"] = "google"
                        # Explicitly update updated_at to prevent infinite loops if no DB trigger exists
                        update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
                        
                        supabase.table("contacts").update(update_data).eq("id", sb_contact["id"]).execute()
                        synced_count += 1
                        
                    elif update_direction == 'to_google' or update_direction is None:
                        # Check if content actually changed before pushing to Google
                        fields_to_check = [
                            "first_name", "last_name", "email", "phone", "phone_secondary",
                            "company", "job_title", "notes", "birthday", "linkedin_url", 
                            "location", "subscribed"
                        ]
                        
                        needs_update = False
                        for field in fields_to_check:
                            sb_val = sb_contact.get(field)
                            google_val = current_google_data.get(field)
                            
                            if sb_val != google_val:
                                if (sb_val is None and google_val == "") or (sb_val == "" and google_val is None):
                                    continue
                                needs_update = True
                                break
                        
                        if needs_update:
                            logger.info(f"Updating Google contact {resource_name} (Supabase is newer/different)")
                            await update_contact(token, resource_name, sb_contact, etag, google_contact, name_to_id_map)
                            synced_count += 1
                    
                    # Remove from google_contacts_map so we know we processed it
                    del google_contacts_map[resource_name]
                    
                else:
                    # Case: In Supabase (with Google ID) but NOT in Google
                    # This means it was DELETED in Google.
                    # Action: Propagate deletion to Supabase (Soft Delete)
                    logger.warning(f"Contact {resource_name} missing in Google. Soft-deleting in Supabase.")
                    supabase.table("contacts").update({
                        "deleted_at": datetime.now(timezone.utc).isoformat(),
                        "last_sync_source": "google"
                    }).eq("id", sb_contact["id"]).execute()
                    synced_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing Supabase contact {sb_contact.get('id')}: {e}")
                errors_count += 1

        # 5. Process remaining Google Contacts
        #    - These are in Google but NOT in Supabase (or at least not linked)
        for resource_name, google_contact in google_contacts_map.items():
            try:
                logger.info(f"Ingesting new contact from Google: {resource_name}")
                raw_data = transform_contact(google_contact, group_mapping)
                
                # Filter out internal fields (starting with _)
                contact_data = {k: v for k, v in raw_data.items() if not k.startswith('_')}
                contact_data["last_sync_source"] = "google"
                
                # Insert into Supabase
                # We use upsert here just in case, but insert is fine too
                supabase.table("contacts").upsert(
                    contact_data, 
                    on_conflict="google_resource_name"
                ).execute()
                synced_count += 1
                
            except Exception as e:
                logger.error(f"Error ingesting Google contact {resource_name}: {e}")
                errors_count += 1
                
        logger.info(f"Sync complete. Synced: {synced_count}, Errors: {errors_count}")
        return {"synced": synced_count, "errors": errors_count}

    except Exception as e:
        logger.error(f"Fatal error during sync: {e}")
        raise e

# Legacy alias if needed, or we can remove it
sync_google_contacts_to_supabase = sync_contacts
