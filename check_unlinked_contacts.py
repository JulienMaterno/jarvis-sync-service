"""
Check for contacts with phone numbers that are NOT linked to any WhatsApp chat.
These are potential missed linkings.
"""
from lib.supabase_client import supabase


def check():
    # Get all contacts with phone numbers
    contacts = supabase.table("contacts").select(
        "id, first_name, last_name, phone"
    ).not_.is_("phone", "null").execute()
    
    contacts_with_phone = [c for c in contacts.data if c.get("phone") and c["phone"].strip()]
    print(f"Contacts with phone numbers: {len(contacts_with_phone)}")
    
    # Get all WhatsApp chats that are linked to contacts
    linked_whatsapp = supabase.table("beeper_chats").select(
        "contact_id, chat_name, remote_phone"
    ).eq("platform", "whatsapp").not_.is_("contact_id", "null").execute()
    
    linked_contact_ids = {c["contact_id"] for c in linked_whatsapp.data}
    print(f"Contacts linked to WhatsApp chats: {len(linked_contact_ids)}")
    
    # Find contacts with phones that are NOT linked to any WhatsApp chat
    unlinked = []
    for contact in contacts_with_phone:
        if contact["id"] not in linked_contact_ids:
            unlinked.append(contact)
    
    print(f"\n⚠️ Contacts with phone but NO WhatsApp link: {len(unlinked)}")
    
    if unlinked:
        print("\nThese contacts have phone numbers but no linked WhatsApp chat:")
        for c in unlinked[:20]:  # Show first 20
            name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
            phone = c.get("phone", "?")
            print(f"  - {name:30} | phone: {phone}")
        
        if len(unlinked) > 20:
            print(f"  ... and {len(unlinked) - 20} more")
        
        # Now check if any of these phones exist in WhatsApp chats (unlinked)
        print("\n\nChecking if their phones exist in unlinked WhatsApp chats...")
        
        # Get all unlinked WhatsApp chats
        unlinked_wa = supabase.table("beeper_chats").select(
            "beeper_chat_id, chat_name, remote_phone, remote_user_name"
        ).eq("platform", "whatsapp").is_("contact_id", "null").execute()
        
        # Normalize phones for comparison
        def normalize(phone):
            if not phone:
                return ""
            import re
            return re.sub(r'[^\d]', '', phone)
        
        wa_phones = {}
        for chat in unlinked_wa.data:
            rp = chat.get("remote_phone")
            cn = chat.get("chat_name", "")
            # Chat name might BE the phone number
            if rp:
                wa_phones[normalize(rp)] = chat
            if cn and cn.replace("+", "").replace(" ", "").isdigit():
                wa_phones[normalize(cn)] = chat
        
        print(f"Unlinked WhatsApp chats with phone info: {len(wa_phones)}")
        
        # Check each unlinked contact
        missed = []
        for contact in unlinked:
            contact_phone = normalize(contact.get("phone", ""))
            if contact_phone in wa_phones:
                missed.append({
                    "contact": contact,
                    "chat": wa_phones[contact_phone]
                })
        
        if missed:
            print(f"\n🚨 FOUND {len(missed)} MISSED LINKINGS!")
            print("These contacts SHOULD be linked but weren't:")
            for m in missed:
                name = f"{m['contact'].get('first_name', '')} {m['contact'].get('last_name', '')}".strip()
                phone = m['contact'].get('phone')
                chat_name = m['chat'].get('chat_name')
                print(f"  - Contact: {name} ({phone})")
                print(f"    Chat: {chat_name}")
                print()
        else:
            print("\n✅ No missed linkings found - all contacts with matching WhatsApp chats are linked")
    else:
        print("\n✅ All contacts with phone numbers are linked to WhatsApp chats!")


if __name__ == "__main__":
    check()
