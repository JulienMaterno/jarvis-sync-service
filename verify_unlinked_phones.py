"""Verify that unlinked phone numbers are truly not in contacts."""
from lib.supabase_client import supabase
import re

# Sample phone numbers from unlinked chats
test_phones = ['+61406062528', '+6287890803300', '+491626178525', '+66979949412']

# Get all contacts
contacts = supabase.table('contacts').select('first_name, last_name, phone').execute()

for test in test_phones:
    test_digits = re.sub(r'[^\d]', '', test)
    print(f"Looking for: {test} (digits: {test_digits})")
    found = False
    for c in contacts.data:
        cp = c.get('phone') or ''
        cp_digits = re.sub(r'[^\d]', '', cp)
        if test_digits[-9:] == cp_digits[-9:]:
            name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
            print(f"  FOUND: {name} - {cp}")
            found = True
    if not found:
        print("  NOT in contacts")
    print()
