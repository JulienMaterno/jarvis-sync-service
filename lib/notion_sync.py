import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from lib.notion_client import notion, notion_database_id
from lib.supabase_client import supabase

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Core fields that map directly to Supabase columns
CORE_FIELDS = {
    "Name", "Mail", "Birthday", "Company", "Position", "LinkedIn URL", "Location", "Subscribed?", "Phone Number"
}

def get_all_notion_contacts(filter_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetches all pages from the Notion CRM database.
    """
    results = []
    has_more = True
    start_cursor = None

    while has_more:
        response = notion.query_database(
            database_id=notion_database_id,
            page_size=100,
            start_cursor=start_cursor,
            filter=filter_params
        )
        
        results.extend(response.get("results", []))
        
        has_more = response.get("has_more")
        start_cursor = response.get("next_cursor")
        
    return results

def extract_property_value(prop: Dict[str, Any]) -> Any:
    """
    Helper to extract raw value from Notion property based on type.
    """
    prop_type = prop.get("type")
    if prop_type == "title":
        return prop.get("title", [{}])[0].get("plain_text", "") if prop.get("title") else ""
    elif prop_type == "rich_text":
        return prop.get("rich_text", [{}])[0].get("plain_text", "") if prop.get("rich_text") else ""
    elif prop_type == "email":
        return prop.get("email")
    elif prop_type == "phone_number":
        return prop.get("phone_number")
    elif prop_type == "url":
        return prop.get("url")
    elif prop_type == "select":
        return prop.get("select", {}).get("name") if prop.get("select") else None
    elif prop_type == "multi_select":
        return [opt.get("name") for opt in prop.get("multi_select", [])]
    elif prop_type == "date":
        return prop.get("date", {}).get("start") if prop.get("date") else None
    elif prop_type == "checkbox":
        return prop.get("checkbox")
    elif prop_type == "number":
        return prop.get("number")
    return None

def transform_notion_to_supabase(page: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps Notion page properties to Supabase columns.
    Handles dynamic properties via JSONB.
    """
    props = page.get("properties", {})
    
    # Name
    title_prop = props.get("Name", {}).get("title", [])
    full_name = title_prop[0].get("plain_text", "") if title_prop else ""
    
    parts = full_name.split(" ", 1)
    first_name = parts[0]
    last_name = parts[1] if len(parts) > 1 else ""
    
    # Email
    email_prop = props.get("Mail", {}).get("email")
    email = email_prop if email_prop else None
    
    # Birthday
    birthday_prop = props.get("Birthday", {}).get("date")
    birthday = birthday_prop.get("start") if birthday_prop else None
    
    # Company
    company_prop = props.get("Company", {}).get("rich_text", [])
    company = company_prop[0].get("plain_text") if company_prop else None
    
    # Job Title
    job_prop = props.get("Position", {}).get("rich_text", [])
    job_title = job_prop[0].get("plain_text") if job_prop else None
    
    # LinkedIn
    linkedin_prop = props.get("LinkedIn URL", {}).get("url")
    linkedin_url = linkedin_prop if linkedin_prop else None
    
    # Location (Dynamic)
    location_prop = props.get("Location", {}).get("select")
    location = location_prop.get("name") if location_prop else None
    
    # Subscribed
    subscribed_prop = props.get("Subscribed?", {}).get("checkbox")
    subscribed = subscribed_prop if subscribed_prop is not None else False

    # Phone
    phone_prop = props.get("Phone Number", {}).get("phone_number")
    phone = phone_prop if phone_prop else None

    # Dynamic Properties (JSONB)
    notion_properties = {}
    for key, prop in props.items():
        if key not in CORE_FIELDS:
            val = extract_property_value(prop)
            if val is not None and val != "":
                notion_properties[key] = val

    return {
        "notion_page_id": page.get("id"),
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "phone": phone,
        "birthday": birthday,
        "company": company,
        "job_title": job_title,
        "linkedin_url": linkedin_url,
        "location": location,
        "subscribed": subscribed,
        "notion_properties": notion_properties,
        "notion_updated_at": page.get("last_edited_time"),
        "last_sync_source": "notion"
    }

def transform_supabase_to_notion(contact: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps Supabase contact to Notion page properties.
    """
    props = {}
    
    # Name
    full_name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
    if full_name:
        props["Name"] = {"title": [{"text": {"content": full_name}}]}
        
    # Email
    if contact.get("email"):
        props["Mail"] = {"email": contact["email"]}

    # Phone
    if contact.get("phone"):
        props["Phone Number"] = {"phone_number": contact["phone"]}

    # Birthday
    if contact.get("birthday"):
        props["Birthday"] = {"date": {"start": contact["birthday"]}}
        
    # Company
    if contact.get("company"):
        props["Company"] = {"rich_text": [{"text": {"content": contact["company"]}}]}
        
    # Job Title
    if contact.get("job_title"):
        props["Position"] = {"rich_text": [{"text": {"content": contact["job_title"]}}]}
        
    # LinkedIn
    if contact.get("linkedin_url"):
        props["LinkedIn URL"] = {"url": contact["linkedin_url"]}
        
    # Location (Dynamic)
    sb_loc = contact.get("location")
    if sb_loc:
        props["Location"] = {"select": {"name": sb_loc}}
        
    # Subscribed
    if contact.get("subscribed") is not None:
        props["Subscribed?"] = {"checkbox": contact["subscribed"]}
        
    # Dynamic Properties (JSONB)
    # Note: We can't easily create new columns in Notion via API if they don't exist.
    # But if they exist, we can update them.
    # For now, we assume the columns exist in Notion if they are in the JSON.
    # We need to know the TYPE to format the payload correctly.
    # Since we don't store the type in JSON, this is tricky.
    # Strategy: We only sync back what we know. 
    # If the user added a column in Notion, it synced to JSON.
    # If we want to sync it back, we need to know how to format it.
    # For this iteration, we will SKIP syncing JSON back to Notion to avoid errors,
    # unless we implement a schema fetcher.
    # Given the prompt "This one should then be synced to supabase as well but not to google contacts",
    # it implies One-Way for extra columns (Notion -> Supabase).
    # So we don't need to unpack JSON back to Notion.
    
    return props
        
    return props

def sync_notion_deletions_to_supabase(last_synced_at: Optional[str]):
    """
    Scans for archived Notion pages and soft-deletes them in Supabase.
    This queries the database directly to check archived status, since archived pages
    might not update their last_edited_time, causing incremental syncs to miss deletions.
    """
    logger.info("Checking for deleted (archived) pages in Notion...")
    
    deleted_count = 0
    
    # Strategy: Get all contacts from Supabase that have a notion_page_id,
    # then check each one in Notion to see if it's archived.
    # This is more reliable than using the search API with timestamps.
    
    res = supabase.table("contacts").select("id, notion_page_id, deleted_at").not_.is_("notion_page_id", "null").execute()
    contacts_with_notion = res.data
    
    logger.info(f"Checking {len(contacts_with_notion)} contacts with Notion page IDs...")
    
    for contact in contacts_with_notion:
        page_id = contact.get("notion_page_id")
        already_deleted = contact.get("deleted_at")
        
        # Skip if already marked as deleted in Supabase
        if already_deleted:
            continue
            
        try:
            # Retrieve the page from Notion
            page = notion.retrieve_page(page_id)
            
            # Check if it's archived
            if page.get("archived"):
                logger.info(f"Found archived Notion page {page_id}. Soft-deleting in Supabase.")
                supabase.table("contacts").update({
                    "deleted_at": datetime.now(timezone.utc).isoformat(),
                    "last_sync_source": "notion"
                }).eq("id", contact["id"]).execute()
                deleted_count += 1
                
        except Exception as e:
            # If we get a 404 or "object not found", it means the page was deleted
            error_msg = str(e).lower()
            if "404" in error_msg or "could not find" in error_msg or "object not found" in error_msg:
                logger.info(f"Notion page {page_id} not found (deleted). Soft-deleting in Supabase.")
                supabase.table("contacts").update({
                    "deleted_at": datetime.now(timezone.utc).isoformat(),
                    "last_sync_source": "notion"
                }).eq("id", contact["id"]).execute()
                deleted_count += 1
            else:
                logger.error(f"Error checking Notion page {page_id}: {e}")
            
    logger.info(f"Processed {deleted_count} Notion deletions.")

def sync_notion_to_supabase(full_sync: bool = False, check_deletions: bool = None):
    """
    Syncs contacts from Notion to Supabase.
    
    Args:
        full_sync: If True, sync all contacts. If False, only sync recently changed.
        check_deletions: If True, check for archived Notion pages. Defaults to full_sync value.
                        Set to False for scheduled incremental syncs to improve performance.
    
    Returns:
        Dict with synced, created, skipped, errors counts
    """
    logger.info(f"Starting Notion -> Supabase sync (mode: {'full' if full_sync else 'incremental'})...")
    
    # Default: only check deletions on full sync (saves ~60s of API calls)
    if check_deletions is None:
        check_deletions = full_sync
    
    # Get current counts for safety valve
    existing_supabase = supabase.table("contacts").select("id").is_("deleted_at", "null").execute()
    existing_count = len(existing_supabase.data) if existing_supabase.data else 0
    
    # Get last sync timestamp for incremental mode
    last_synced_at = None
    if not full_sync:
        res = supabase.table("contacts").select("notion_updated_at").order("notion_updated_at", desc=True).limit(1).execute()
        last_synced_at = res.data[0]["notion_updated_at"] if res.data and res.data[0].get("notion_updated_at") else None
    
    # 1. Handle Deletions (Archived in Notion -> Soft delete in Supabase)
    # Skip for incremental syncs to save API calls (127 contacts = 127 API calls = ~60s)
    if check_deletions:
        sync_notion_deletions_to_supabase(last_synced_at)
    else:
        logger.info("Skipping deletion check for incremental sync (run full sync periodically)")
    
    # Build filter for Notion query
    filter_params = None
    if last_synced_at and not full_sync:
        logger.info(f"Incremental sync: Fetching Notion pages modified after {last_synced_at}")
        filter_params = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": last_synced_at}
        }
    else:
        logger.info("Full sync: Fetching all Notion pages")

    notion_contacts = get_all_notion_contacts(filter_params)
    logger.info(f"Fetched {len(notion_contacts)} contacts from Notion.")
    
    # SAFETY VALVE: Prevent accidental mass deletion
    if full_sync and existing_count > 10 and len(notion_contacts) < (existing_count * 0.1):
        msg = f"SAFETY VALVE: Notion returned {len(notion_contacts)} contacts, but Supabase has {existing_count}. Aborting to prevent data loss."
        logger.error(msg)
        raise Exception(msg)
    
    synced = 0
    created = 0
    skipped = 0
    errors = 0
    
    for page in notion_contacts:
        try:
            page_id = page.get("id")
            last_edited = page.get("last_edited_time")
            
            # Check if exists in Supabase
            res = supabase.table("contacts").select("*").eq("notion_page_id", page_id).execute()
            existing = res.data[0] if res.data else None
            
            contact_data = transform_notion_to_supabase(page)
            
            if existing:
                # Skip if already soft-deleted in Supabase - don't update deleted contacts
                if existing.get("deleted_at"):
                    logger.debug(f"Skipping soft-deleted contact {existing.get('id')}")
                    skipped += 1
                    continue
                    
                # Compare timestamps with 5-second buffer to prevent ping-pong
                # notion_updated_at in Supabase stores when Notion was last edited
                sb_notion_updated = existing.get("notion_updated_at")
                
                should_update = False
                if not sb_notion_updated:
                    # No previous sync - always update
                    should_update = True
                else:
                    # Use 5-second buffer to prevent ping-pong loops
                    # This applies regardless of last_sync_source to handle race conditions
                    from datetime import datetime, timedelta, timezone
                    try:
                        notion_dt = datetime.fromisoformat(last_edited.replace('Z', '+00:00'))
                        sb_dt = datetime.fromisoformat(sb_notion_updated.replace('Z', '+00:00'))
                        # Update if Notion is newer by more than 5 seconds
                        if notion_dt > sb_dt + timedelta(seconds=5):
                            should_update = True
                    except Exception:
                        # Fallback to string comparison
                        should_update = last_edited > sb_notion_updated
                
                if should_update:
                    contact_data['last_sync_source'] = 'notion'
                    logger.info(f"Updating Supabase contact from Notion: {contact_data.get('email')}")
                    supabase.table("contacts").update(contact_data).eq("id", existing["id"]).execute()
                    synced += 1
                else:
                    skipped += 1
            else:
                # Check if email exists (to link existing contact)
                email = contact_data.get("email")
                if email:
                    res_email = supabase.table("contacts").select("*").eq("email", email).is_("deleted_at", "null").execute()
                    existing_by_email = res_email.data[0] if res_email.data else None
                    
                    if existing_by_email:
                        contact_data['last_sync_source'] = 'notion'
                        logger.info(f"Linking existing Supabase contact {email} to Notion page {page_id}")
                        supabase.table("contacts").update(contact_data).eq("id", existing_by_email["id"]).execute()
                        synced += 1
                        continue

                # Create new
                contact_data['last_sync_source'] = 'notion'
                logger.info(f"Creating new contact in Supabase from Notion: {contact_data.get('email')}")
                supabase.table("contacts").insert(contact_data).execute()
                created += 1
                
        except Exception as e:
            logger.error(f"Error syncing Notion page {page.get('id')}: {e}")
            errors += 1
            
    logger.info(f"Notion → Supabase: {synced} updated, {created} created, {skipped} skipped, {errors} errors")
    return {"synced": synced, "created": created, "skipped": skipped, "errors": errors}

def sync_supabase_to_notion(full_sync: bool = False):
    """
    Syncs contacts from Supabase to Notion.
    
    Args:
        full_sync: If True, sync all contacts. If False, uses timestamp comparison.
    
    Returns:
        Dict with synced, created, archived, skipped, errors counts
    """
    logger.info(f"Starting Supabase -> Notion sync (mode: {'full' if full_sync else 'incremental'})...")
    
    # 1. Handle Deletions (Soft deleted in Supabase -> Archive in Notion)
    # If a contact has deleted_at set AND still has a notion_page_id, we need to archive it
    res = supabase.table("contacts").select("*").not_.is_("deleted_at", "null").not_.is_("notion_page_id", "null").execute()
    deleted_contacts = res.data or []
    
    deleted_count = 0
    for contact in deleted_contacts:
        try:
            page_id = contact.get("notion_page_id")
            
            # If contact is soft-deleted and still has notion_page_id, always try to archive
            logger.info(f"Archiving Notion page {page_id} (soft-deleted in Supabase)")
            result = notion.archive_page(page_id)
            
            # Clear the notion_page_id to indicate archival is complete
            update_data = {
                "notion_page_id": None,  # Clear link - archival is done
                "notion_updated_at": datetime.now(timezone.utc).isoformat()
            }
            supabase.table("contacts").update(update_data).eq("id", contact["id"]).execute()
            deleted_count += 1
            logger.info(f"Successfully archived and cleared link for contact {contact.get('id')}")
                
        except Exception as e:
            error_msg = str(e).lower()
            if "400" in error_msg or "404" in error_msg or "archived" in error_msg:
                # Page already gone - clear the link
                logger.info(f"Notion page {contact.get('notion_page_id')} already archived/deleted, clearing link")
                supabase.table("contacts").update({
                    "notion_page_id": None,
                    "notion_updated_at": datetime.now(timezone.utc).isoformat()
                }).eq("id", contact["id"]).execute()
                deleted_count += 1
            else:
                logger.error(f"Error archiving Notion page {contact.get('notion_page_id')}: {e}")

    logger.info(f"Archived {deleted_count} pages in Notion.")


    # 2. Handle Updates/Creates
    # Fetch active contacts
    res = supabase.table("contacts").select("*").is_("deleted_at", "null").execute()
    contacts = res.data or []
    logger.info(f"Fetched {len(contacts)} active contacts from Supabase.")
    
    # Get Notion contact count for safety valve
    notion_contacts = get_all_notion_contacts()
    notion_count = len(notion_contacts)
    
    # SAFETY VALVE: Prevent accidental mass creation
    if full_sync and notion_count > 10 and len(contacts) < (notion_count * 0.1):
        msg = f"SAFETY VALVE: Supabase has {len(contacts)} contacts, but Notion has {notion_count}. Aborting to prevent data loss."
        logger.error(msg)
        raise Exception(msg)
    
    synced = 0
    created = 0
    skipped = 0
    errors = 0
    
    for contact in contacts:
        try:
            page_id = contact.get("notion_page_id")
            props = transform_supabase_to_notion(contact)
            
            if page_id:
                sb_updated_at = contact.get("updated_at")
                notion_updated_at = contact.get("notion_updated_at")
                
                should_update = False
                if sb_updated_at and notion_updated_at:
                    # Parse timestamps
                    sb_dt = datetime.fromisoformat(sb_updated_at.replace('Z', '+00:00'))
                    notion_dt = datetime.fromisoformat(notion_updated_at.replace('Z', '+00:00'))
                    
                    if contact.get("last_sync_source") == "supabase":
                        # If last sync was from supabase, only update if significantly newer (buffer for self-update)
                        if sb_dt > notion_dt + timedelta(seconds=5):
                            should_update = True
                    else:
                        if sb_dt > notion_dt:
                            should_update = True
                elif not notion_updated_at:
                    should_update = True
                    
                if should_update:
                    logger.info(f"Updating Notion page {page_id}")
                    try:
                        # Rate limit
                        import time
                        time.sleep(0.34) # ~3 requests per second max
                        updated_page = notion.update_page(page_id=page_id, properties=props)
                        
                        # Update notion_updated_at in Supabase to avoid loop
                        # Use the actual last_edited_time from Notion response
                        supabase.table("contacts").update({
                            "notion_updated_at": updated_page.get("last_edited_time"),
                            "last_sync_source": "supabase"
                        }).eq("id", contact["id"]).execute()
                        
                        synced += 1
                    except Exception as e:
                        # Check if error is due to page being archived/deleted
                        # Notion API returns 400 or 404 for archived pages sometimes depending on context
                        error_msg = str(e).lower()
                        if "archived" in error_msg or "could not find" in error_msg or "404" in error_msg or "400" in error_msg:
                            logger.warning(f"Notion page {page_id} appears to be deleted/archived. Clearing link in Supabase.")
                            supabase.table("contacts").update({
                                "notion_page_id": None,
                                "notion_updated_at": datetime.now(timezone.utc).isoformat(),
                                "last_sync_source": "notion"
                            }).eq("id", contact["id"]).execute()
                            skipped += 1
                        else:
                            raise e
                else:
                    skipped += 1
            else:
                # Create new page in Notion
                logger.info(f"Creating new page in Notion for {contact.get('email')}")
                # Rate limit
                import time
                time.sleep(0.34)
                new_page = notion.create_page(
                    parent={"database_id": notion_database_id},
                    properties=props
                )
                new_page_id = new_page.get("id")
                
                # Save back to Supabase
                supabase.table("contacts").update({
                    "notion_page_id": new_page_id,
                    "notion_updated_at": new_page.get("last_edited_time"),
                    "last_sync_source": "supabase"
                }).eq("id", contact["id"]).execute()
                
                created += 1
                
        except Exception as e:
            logger.error(f"Error syncing contact {contact.get('id')} to Notion: {e}")
            errors += 1
    
    logger.info(f"Supabase → Notion: {synced} updated, {created} created, {skipped} skipped, {deleted_count} archived, {errors} errors")
    return {"synced": synced, "created": created, "skipped": skipped, "archived": deleted_count, "errors": errors}
