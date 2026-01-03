"""
Deep dive: Compare ALL unlinked WhatsApp chats with ALL contacts with phones.
Use aggressive phone normalization to find potential matches.
"""
from lib.supabase_client import supabase
import re


def normalize_phone(phone):
    """Aggressively normalize phone for comparison."""
    if not phone:
        return ""
    # Remove everything except digits
    digits = re.sub(r'[^\d]', '', phone)
    # Remove leading zeros
    digits = digits.lstrip('0')
    # Take last 9 digits (handles country code differences)
    if len(digits) >= 9:
        return digits[-9:]
    return digits


def check():
    # Get all contacts with phones
    contacts = supabase.table("contacts").select(
        "id, first_name, last_name, phone"
    ).not_.is_("phone", "null").execute()
    
    contact_phones = {}
    for c in contacts.data:
        phone = c.get("phone")
        if phone:
            norm = normalize_phone(phone)
            if norm:
                contact_phones[norm] = c
    
    print(f"Contacts with normalized phones: {len(contact_phones)}")
    
    # Get all UNLINKED WhatsApp chats
    unlinked_wa = supabase.table("beeper_chats").select(
        "beeper_chat_id, chat_name, remote_phone, remote_user_name"
    ).eq("platform", "whatsapp").is_("contact_id", "null").eq("chat_type", "dm").execute()
    
    print(f"Unlinked WhatsApp DM chats: {len(unlinked_wa.data)}")
    
    # Check each unlinked chat
    potential_matches = []
    for chat in unlinked_wa.data:
        phone_sources = []
        
        # Try remote_phone
        if chat.get("remote_phone"):
            phone_sources.append(("remote_phone", chat["remote_phone"]))
        
        # Try chat_name if it looks like a phone
        cn = chat.get("chat_name", "")
        if cn and cn.replace("+", "").replace(" ", "").replace("-", "").isdigit():
            phone_sources.append(("chat_name", cn))
        
        for source, phone in phone_sources:
            norm = normalize_phone(phone)
            if norm and norm in contact_phones:
                contact = contact_phones[norm]
                potential_matches.append({
                    "chat_id": chat["beeper_chat_id"],
                    "chat_name": chat.get("chat_name"),
                    "chat_phone": phone,
                    "chat_phone_source": source,
                    "contact_name": f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip(),
                    "contact_phone": contact.get("phone"),
                    "contact_id": contact["id"]
                })
    
    if potential_matches:
        print(f"\n🚨 FOUND {len(potential_matches)} POTENTIAL MISSED LINKINGS!")
        print("\nThese chats have phone numbers matching contacts but weren't linked:")
        for m in potential_matches:
            print(f"\n  Chat: {m['chat_name']}")
            print(f"    Chat phone ({m['chat_phone_source']}): {m['chat_phone']}")
            print(f"    Contact: {m['contact_name']}")
            print(f"    Contact phone: {m['contact_phone']}")
        
        # Try to understand why they didn't match
        print("\n\nInvestigating why these didn't match...")
        for m in potential_matches[:3]:  # Check first 3
            print(f"\n  Checking: {m['contact_name']}")
            print(f"    Contact phone raw: '{m['contact_phone']}'")
            print(f"    Chat phone raw: '{m['chat_phone']}'")
            contact_norm = re.sub(r'[^\d]', '', m['contact_phone'] or '')
            chat_norm = re.sub(r'[^\d]', '', m['chat_phone'] or '')
            print(f"    Contact phone normalized (full): {contact_norm}")
            print(f"    Chat phone normalized (full): {chat_norm}")
    else:
        print("\n✅ No potential matches found - phone number matching is working correctly!")
    
    # Show some examples of unlinked chats that are just phone numbers
    print("\n\nUnlinked WhatsApp chats that are phone numbers (no contact match):")
    phone_only_chats = [c for c in unlinked_wa.data if c.get("chat_name", "").replace("+", "").replace(" ", "").replace("-", "").isdigit()]
    for chat in phone_only_chats[:10]:
        print(f"  - {chat.get('chat_name')}")
    
    if len(phone_only_chats) > 10:
        print(f"  ... and {len(phone_only_chats) - 10} more")


if __name__ == "__main__":
    check()
