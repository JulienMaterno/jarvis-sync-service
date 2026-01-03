"""Check current Beeper sync status."""
from collections import defaultdict
from lib.supabase_client import supabase


def check():
    sb = supabase
    
    # Total chats
    chats = sb.table("beeper_chats").select("*").execute()
    print(f"Total beeper_chats: {len(chats.data)}")
    
    # Linked vs unlinked
    linked = [c for c in chats.data if c.get("contact_id")]
    unlinked = [c for c in chats.data if not c.get("contact_id")]
    print(f"Linked to contact: {len(linked)}")
    print(f"Not linked: {len(unlinked)}")
    
    # Platform breakdown
    print("\nBy platform:")
    by_platform = defaultdict(lambda: {"linked": 0, "unlinked": 0})
    for c in chats.data:
        key = "linked" if c.get("contact_id") else "unlinked"
        by_platform[c["platform"]][key] += 1
    
    for platform, counts in sorted(by_platform.items()):
        print(f"  {platform}: {counts['linked']} linked, {counts['unlinked']} unlinked")
    
    # Cross-platform check - same contact on multiple platforms
    contact_platforms = defaultdict(set)
    contact_names = {}
    for c in linked:
        contact_platforms[c["contact_id"]].add(c["platform"])
        contact_names[c["contact_id"]] = c.get("chat_name", "?")
    
    multi_platform = {cid: plats for cid, plats in contact_platforms.items() if len(plats) > 1}
    print(f"\nContacts linked to multiple platforms: {len(multi_platform)}")
    for cid, plats in list(multi_platform.items())[:5]:
        print(f"  - {contact_names.get(cid, 'unknown')}: {plats}")
    
    # Show some unlinked chats with names
    print(f"\nSample unlinked chats (could be linked):")
    for c in unlinked[:12]:
        name = c.get("chat_name") or "(empty)"
        remote = c.get("remote_user_name") or "(empty)"
        platform = c["platform"]
        print(f"  [{platform:10}] name: {name[:30]:30} | remote: {remote[:25]}")
    
    # Get contacts to see what we're matching against
    contacts = sb.table("contacts").select("id,first_name,last_name,phone").execute()
    print(f"\nTotal contacts in DB: {len(contacts.data)}")
    
    # Show sample contacts with phone for matching reference
    print("\nSample contacts (for matching):")
    for c in contacts.data[:10]:
        name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
        phone = c.get("phone") or "(no phone)"
        print(f"  - {name[:30]:30} | phone: {phone}")


if __name__ == "__main__":
    check()
