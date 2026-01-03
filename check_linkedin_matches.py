"""Check for potential LinkedIn matches."""
from lib.supabase_client import supabase


def check():
    # Get unlinked LinkedIn DMs
    unlinked = supabase.table("beeper_chats").select(
        "chat_name, remote_user_name"
    ).eq("platform", "linkedin").is_(
        "contact_id", "null"
    ).eq("chat_type", "dm").execute()
    
    # Get all contacts
    contacts = supabase.table("contacts").select("first_name, last_name").execute()
    contact_names = []
    for c in contacts.data:
        first = c.get("first_name") or ""
        last = c.get("last_name") or ""
        full = f"{first} {last}".strip().lower()
        contact_names.append(full)
    
    print(f"Unlinked LinkedIn DMs: {len(unlinked.data)}")
    print(f"Total contacts: {len(contacts.data)}")
    print("\nChecking for potential matches:")
    
    for chat in unlinked.data:
        name = chat.get("remote_user_name") or chat.get("chat_name") or ""
        name_lower = name.lower().strip()
        
        # Check if any contact contains this name or vice versa
        matches = []
        for cn in contact_names:
            if name_lower in cn or cn in name_lower:
                matches.append(cn)
        
        if matches:
            print(f"  '{name}' -> possible contacts: {matches}")
        else:
            print(f"  '{name}' -> NO MATCH (probably not in contacts)")


if __name__ == "__main__":
    check()
