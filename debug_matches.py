"""Debug why some matches aren't linking."""
from lib.supabase_client import supabase


def check():
    # Get contacts with first names matching the unlinked
    test_names = ["yolanda", "eshan", "irwan"]
    
    contacts = supabase.table("contacts").select("id, first_name, last_name").execute()
    
    for test in test_names:
        print(f"\nLooking for '{test}':")
        for c in contacts.data:
            first = (c.get("first_name") or "").lower()
            last = (c.get("last_name") or "").lower()
            full = f"{first} {last}".strip()
            
            if test in first or test in last or test in full:
                print(f"  Found contact: {c['first_name']} {c['last_name']} (id: {c['id'][:8]}...)")
    
    # Show the chat names for context
    print("\n\nActual LinkedIn chat names:")
    chats = supabase.table("beeper_chats").select(
        "chat_name, remote_user_name"
    ).eq("platform", "linkedin").is_(
        "contact_id", "null"
    ).eq("chat_type", "dm").execute()
    
    for c in chats.data:
        name = (c.get("remote_user_name") or c.get("chat_name") or "").lower()
        if any(t in name for t in test_names):
            print(f"  Chat: {c.get('remote_user_name')} / {c.get('chat_name')}")


if __name__ == "__main__":
    check()
