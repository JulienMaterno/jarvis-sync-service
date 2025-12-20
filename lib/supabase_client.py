import os
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

url: str = os.environ.get("SUPABASE_URL", "")
key: str = os.environ.get("SUPABASE_KEY", "")

if not url or not key:
    # In a real app, we might raise an error here, but for now we'll just print a warning
    # or let the client creation fail if it validates immediately.
    print("Warning: SUPABASE_URL or SUPABASE_KEY not found in environment variables.")

supabase: Client = create_client(url, key)


def find_contact_by_email(email_address: str) -> Optional[str]:
    """
    Find a contact ID by email address.
    Handles "Name <email>" format automatically.
    """
    if not email_address:
        return None
    
    # Clean email: extract just the address if in "Name <email>" format
    email_clean = email_address.strip().lower()
    if '<' in email_clean and '>' in email_clean:
        start = email_clean.find('<') + 1
        end = email_clean.find('>')
        email_clean = email_clean[start:end]
    
    try:
        response = supabase.table("contacts").select("id").ilike("email", email_clean).limit(1).execute()
        if response.data:
            return response.data[0]["id"]
    except Exception:
        pass
    
    return None
