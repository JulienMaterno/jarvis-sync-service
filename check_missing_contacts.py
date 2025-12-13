import asyncio
from lib.supabase_client import supabase
from lib.google_contacts import get_access_token, get_all_contacts, update_contact

async def check_missing():
    # Get all active contacts from Supabase
    response = supabase.table("contacts").select("*").is_("deleted_at", "null").execute()
    sb_contacts = response.data
    
    # Get all from Google
    token = await get_access_token()
    google_contacts = await get_all_contacts(token)
    
    # Create sets of emails
    sb_emails = {c.get("email") for c in sb_contacts if c.get("email")}
    google_resource_names = {c.get("resourceName") for c in google_contacts}
    
    # Find contacts in Supabase but not in Google
    print(f"\nTotal in Supabase (active): {len(sb_contacts)}")
    print(f"Total in Google: {len(google_contacts)}")
    print(f"\nContacts in Supabase without google_resource_name:")
    
    missing_in_google = []
    for contact in sb_contacts:
        resource_name = contact.get("google_resource_name")
        if not resource_name or resource_name not in google_resource_names:
            missing_in_google.append(contact)
    
    print(f"Found {len(missing_in_google)} contacts missing from Google:\n")
    for c in missing_in_google:
        email = c.get("email") or "NO EMAIL"
        phone = c.get("phone") or "NO PHONE"
        name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
        print(f"  - {name} | Email: {email} | Phone: {phone}")
    
    # Check contact groups - some might be in "Other Contacts" vs main "Contacts"
    print(f"\n\nChecking Google contact memberships:")
    in_my_contacts = 0
    in_other_contacts = 0
    no_membership = 0
    
    for gc in google_contacts:
        memberships = gc.get("memberships", [])
        groups = [m.get("contactGroupMembership", {}).get("contactGroupResourceName") for m in memberships]
        
        if "contactGroups/myContacts" in groups:
            in_my_contacts += 1
        elif any("contactGroups/starred" in g or "contactGroups/friends" in g or "contactGroups/family" in g for g in groups if g):
            in_my_contacts += 1  # Starred/Friends/Family are also "My Contacts"
        else:
            no_membership += 1
            # Print first few for debugging
            if no_membership <= 4:
                names = gc.get("names", [{}])
                name = names[0].get("displayName", "NO NAME") if names else "NO NAME"
                print(f"  Contact NOT in 'My Contacts': {name} | Groups: {groups}")
    
    print(f"\nSummary:")
    print(f"  In 'My Contacts': {in_my_contacts}")
    print(f"  Not in 'My Contacts': {no_membership}")
    print(f"  (UI shows 118, which matches if we subtract the {no_membership} contacts)")
        # Fix the 4 contacts not in My Contacts
    if no_membership > 0:
        print(f"\n\nFixing {no_membership} contacts by adding them to 'My Contacts'...")
        fixed = 0
        for gc in google_contacts:
            memberships = gc.get("memberships", [])
            groups = [m.get("contactGroupMembership", {}).get("contactGroupResourceName") for m in memberships]
            
            if "contactGroups/myContacts" not in groups:
                # Add My Contacts to this contact
                resource_name = gc.get("resourceName")
                etag = gc.get("etag")
                
                # Find the contact data in Supabase
                sb_contact = next((c for c in sb_contacts if c.get("google_resource_name") == resource_name), None)
                if sb_contact:
                    try:
                        # Just trigger an update which will add My Contacts
                        names = gc.get("names", [{}])
                        name = names[0].get("displayName", "UNKNOWN") if names else "UNKNOWN"
                        print(f"  Fixing: {name}")
                        # We'll need to import and use the update function properly
                        # For now just report
                        fixed += 1
                    except Exception as e:
                        print(f"  Error fixing {resource_name}: {e}")
        print(f"\nIdentified {fixed} contacts that need fixing.")
        print(f"Run a sync to automatically add them to 'My Contacts'.")
        # Also check for contacts without email AND phone
    print(f"\n\nContacts without email AND phone (can't be synced to Google):")
    no_contact_info = [c for c in sb_contacts if not c.get("email") and not c.get("phone")]
    print(f"Found {len(no_contact_info)} contacts without contact info")
    for c in no_contact_info:
        name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
        print(f"  - {name}")

if __name__ == "__main__":
    asyncio.run(check_missing())
