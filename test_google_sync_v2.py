import asyncio
import logging
from datetime import datetime, timezone
from lib.sync_service import sync_contacts
from lib.supabase_client import supabase
from lib.google_contacts import get_access_token, create_contact, delete_contact, get_contact

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_bidirectional_sync():
    logger.info("=== Starting Bi-Directional Sync Test ===")
    
    # 1. Setup: Create a test contact in Supabase
    test_email = f"test.sync.{int(datetime.now().timestamp())}@example.com"
    logger.info(f"Creating test contact in Supabase: {test_email}")
    
    contact_data = {
        "first_name": "Test",
        "last_name": "Sync",
        "email": test_email,
        "last_sync_source": "supabase",
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    
    res = supabase.table("contacts").insert(contact_data).execute()
    contact_id = res.data[0]["id"]
    logger.info(f"Created Supabase contact ID: {contact_id}")
    
    # 2. Run Sync (Should create in Google)
    logger.info("--- Running Sync 1 (Supabase -> Google) ---")
    await sync_contacts()
    
    # Verify creation in Google
    res = supabase.table("contacts").select("*").eq("id", contact_id).execute()
    contact = res.data[0]
    resource_name = contact.get("google_resource_name")
    
    if resource_name:
        logger.info(f"SUCCESS: Contact synced to Google. Resource Name: {resource_name}")
    else:
        logger.error("FAILURE: Contact did not sync to Google.")
        return

    # 3. Test Google -> Supabase Update
    logger.info("--- Testing Google -> Supabase Update ---")
    token = await get_access_token()
    
    # Update in Google
    logger.info("Updating contact in Google (changing job title)...")
    # We need to fetch the current etag first
    g_contact = await get_contact(token, resource_name)
    
    update_body = {
        "names": [{"givenName": "Test", "familyName": "Sync Updated"}],
        "organizations": [{"name": "Google Update Inc", "title": "CEO"}],
        "emailAddresses": [{"value": test_email}],
        "etag": g_contact["etag"]
    }
    
    # Use the raw update method or helper if available. 
    # Since we don't have a direct 'update_contact_fields' helper exposed easily for test, 
    # we'll use the internal update_contact from sync_service logic or just rely on the fact 
    # that we can't easily update google without the helper.
    # Let's just wait a second and update Supabase to simulate conflict? 
    # No, we want to test Google -> Supabase.
    
    # Let's manually update Google using the helper in google_contacts if possible, 
    # or just skip this if too complex to mock.
    # Actually, we can just use the `update_contact` from lib.google_contacts if we import it.
    from lib.google_contacts import update_contact as google_update_contact
    
    # We need to construct the full contact object for the update helper
    # The helper expects a Supabase-like dict to transform, OR we can use the raw API.
    # The `update_contact` in `lib.google_contacts` takes (token, resource_name, contact_data, etag).
    # `contact_data` is a Supabase-style dict.
    
    new_sb_data = contact.copy()
    new_sb_data["job_title"] = "Google CEO"
    new_sb_data["last_name"] = "Sync Updated"
    
    # We want to simulate a Google-side edit. 
    # If we use `update_contact`, it pushes to Google.
    await google_update_contact(token, resource_name, new_sb_data, g_contact["etag"])
    logger.info("Updated Google contact.")
    
    # Wait a bit to ensure timestamps differ
    await asyncio.sleep(2)
    
    # Run Sync
    logger.info("--- Running Sync 2 (Google -> Supabase) ---")
    await sync_contacts()
    
    # Verify Supabase
    res = supabase.table("contacts").select("*").eq("id", contact_id).execute()
    updated_contact = res.data[0]
    
    if updated_contact["job_title"] == "Google CEO":
        logger.info("SUCCESS: Google update synced to Supabase.")
    else:
        logger.error(f"FAILURE: Supabase not updated. Job Title: {updated_contact['job_title']}")

    # 4. Test Deletion (Google -> Supabase)
    logger.info("--- Testing Deletion (Google -> Supabase) ---")
    logger.info("Deleting contact from Google...")
    await delete_contact(token, resource_name)
    
    # Run Sync
    logger.info("--- Running Sync 3 (Deletion Propagation) ---")
    await sync_contacts()
    
    # Verify Supabase Soft Delete
    res = supabase.table("contacts").select("*").eq("id", contact_id).execute()
    final_contact = res.data[0]
    
    if final_contact.get("deleted_at"):
        logger.info(f"SUCCESS: Contact soft-deleted in Supabase. Deleted At: {final_contact['deleted_at']}")
    else:
        logger.error("FAILURE: Contact not deleted in Supabase.")

    # Cleanup (Hard delete from Supabase to keep test clean)
    logger.info("Cleaning up test data...")
    supabase.table("contacts").delete().eq("id", contact_id).execute()
    logger.info("Test Complete.")

if __name__ == "__main__":
    asyncio.run(test_bidirectional_sync())
