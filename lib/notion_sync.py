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
    "Name", "Mail", "Birthday", "Company", "Position", "LinkedIn URL", "Location", "Subscribed?"
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
    Uses incremental search if last_synced_at is provided.
    """
    logger.info("Checking for deleted (archived) pages in Notion...")
    
    has_more = True
    start_cursor = None
    deleted_count = 0
    
    # Sort by last_edited_time desc so we can stop early
    sort_param = {"direction": "descending", "timestamp": "last_edited_time"}
    
    while has_more:
        response = notion.search(
            filter={"property": "object", "value": "page"},
            sort=sort_param,
            page_size=100,
            start_cursor=start_cursor
        )
        
        pages = response.get("results", [])
        if not pages:
            break
            
        for page in pages:
            last_edited = page.get("last_edited_time")
            
            # Optimization: Stop if we reached pages older than last sync
            if last_synced_at and last_edited < last_synced_at:
                has_more = False
                break
                
            # Check if it belongs to our DB
            parent = page.get("parent", {})
            p_db_id = parent.get("database_id")
            
            # Normalize IDs for comparison
            if p_db_id and p_db_id.replace("-", "") == notion_database_id.replace("-", ""):
                if page.get("archived"):
                    # This page is archived and recent. Soft delete in Supabase.
                    page_id = page.get("id")
                    
                    # Check if it exists and is not already deleted
                    res = supabase.table("contacts").select("id, deleted_at").eq("notion_page_id", page_id).execute()
                    if res.data:
                        contact = res.data[0]
                        if not contact.get("deleted_at"):
                            logger.info(f"Found archived Notion page {page_id}. Soft-deleting in Supabase.")
                            supabase.table("contacts").update({
                                "deleted_at": datetime.now(timezone.utc).isoformat(),
                                "last_sync_source": "notion"
                            }).eq("id", contact["id"]).execute()
                            deleted_count += 1
        
        if has_more:
            has_more = response.get("has_more")
            start_cursor = response.get("next_cursor")
            
    logger.info(f"Processed {deleted_count} Notion deletions.")

def sync_notion_to_supabase():
    """
    Syncs contacts from Notion to Supabase.
    """
    logger.info("Starting Notion -> Supabase sync...")
    
    # Optimization: Get the latest notion_updated_at from Supabase to filter Notion query
    # This ensures we only fetch changed rows
    res = supabase.table("contacts").select("notion_updated_at").order("notion_updated_at", desc=True).limit(1).execute()
    last_synced_at = res.data[0]["notion_updated_at"] if res.data and res.data[0]["notion_updated_at"] else None
    
    # 1. Handle Deletions (Archived in Notion -> Soft delete in Supabase)
    sync_notion_deletions_to_supabase(last_synced_at)
    
    filter_params = None
    if last_synced_at:
        logger.info(f"Incremental sync: Fetching Notion pages modified after {last_synced_at}")
        filter_params = {
            "timestamp": "last_edited_time",
            "last_edited_time": {
                "after": last_synced_at
            }
        }
    else:
        logger.info("Full sync: Fetching all Notion pages")

    notion_contacts = get_all_notion_contacts(filter_params)
    logger.info(f"Fetched {len(notion_contacts)} contacts from Notion.")
    
    synced = 0
    created = 0
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
                # Compare timestamps
                sb_notion_updated = existing.get("notion_updated_at")
                
                # Since we filtered by last_edited_time > last_synced_at, we know it's newer.
                # But double check just in case of race conditions or manual edits
                if not sb_notion_updated or last_edited > sb_notion_updated:
                    logger.info(f"Updating Supabase contact from Notion: {contact_data.get('email')}")
                    supabase.table("contacts").update(contact_data).eq("id", existing["id"]).execute()
                    synced += 1
            else:
                # Check if email exists (to link existing contact)
                email = contact_data.get("email")
                if email:
                    res_email = supabase.table("contacts").select("*").eq("email", email).execute()
                    existing_by_email = res_email.data[0] if res_email.data else None
                    
                    if existing_by_email:
                        logger.info(f"Linking existing Supabase contact {email} to Notion page {page_id}")
                        supabase.table("contacts").update(contact_data).eq("id", existing_by_email["id"]).execute()
                        synced += 1
                        continue

                # Create new
                logger.info(f"Creating new contact in Supabase from Notion: {contact_data.get('email')}")
                supabase.table("contacts").insert(contact_data).execute()
                created += 1
                
        except Exception as e:
            logger.error(f"Error syncing Notion page {page.get('id')}: {e}")
            errors += 1
            
    return {"synced": synced, "created": created, "errors": errors}

def sync_supabase_to_notion():
    """
    Syncs contacts from Supabase to Notion.
    """
    logger.info("Starting Supabase -> Notion sync...")
    
    # 1. Handle Deletions (Soft deleted in Supabase -> Archive in Notion)
    res = supabase.table("contacts").select("*").not_.is_("deleted_at", "null").not_.is_("notion_page_id", "null").execute()
    deleted_contacts = res.data
    
    deleted_count = 0
    for contact in deleted_contacts:
        try:
            page_id = contact.get("notion_page_id")
            deleted_at = contact.get("deleted_at")
            notion_updated_at = contact.get("notion_updated_at")
            
            # Only archive if deletion happened after last sync
            if not notion_updated_at or deleted_at > notion_updated_at:
                logger.info(f"Archiving Notion page {page_id} (deleted in Supabase)")
                notion.archive_page(page_id)
                
                # Update timestamp to prevent re-syncing
                supabase.table("contacts").update({
                    "notion_updated_at": datetime.now(timezone.utc).isoformat()
                }).eq("id", contact["id"]).execute()
                deleted_count += 1
                
        except Exception as e:
            logger.error(f"Error archiving Notion page {contact.get('notion_page_id')}: {e}")

    logger.info(f"Archived {deleted_count} pages in Notion.")

    # 2. Handle Updates/Creates
    # Fetch active contacts
    res = supabase.table("contacts").select("*").is_("deleted_at", "null").execute()
    contacts = res.data
    logger.info(f"Fetched {len(contacts)} active contacts from Supabase.")
    
    synced = 0
    created = 0
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
                            logger.warning(f"Notion page {page_id} appears to be deleted/archived. Soft-deleting in Supabase.")
                            supabase.table("contacts").update({
                                "deleted_at": datetime.now(timezone.utc).isoformat(),
                                "last_sync_source": "notion"
                            }).eq("id", contact["id"]).execute()
                        else:
                            raise e
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
            
    return {"synced": synced, "created": created, "archived": deleted_count, "errors": errors}
