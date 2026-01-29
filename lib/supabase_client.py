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


def find_contact_by_name(display_name: str) -> Optional[str]:
    """
    Find a contact ID by display name (from calendar attendee).
    Uses fuzzy matching: exact first+last name, then unique first name.

    Args:
        display_name: Name like "Nick Hazell" or "Nick"

    Returns:
        Contact ID if found with high confidence, None otherwise
    """
    if not display_name:
        return None

    name_clean = display_name.strip()
    parts = name_clean.lower().split()
    if not parts:
        return None

    first_name = parts[0]
    last_name = parts[-1] if len(parts) > 1 else None

    try:
        # Strategy 1: Exact match on first + last name
        if last_name and first_name != last_name:
            response = supabase.table("contacts").select("id").ilike(
                "first_name", first_name
            ).ilike(
                "last_name", last_name
            ).is_("deleted_at", "null").limit(1).execute()

            if response.data:
                return response.data[0]["id"]

        # Strategy 2: First name only (if unique match)
        response = supabase.table("contacts").select("id").ilike(
            "first_name", first_name
        ).is_("deleted_at", "null").limit(5).execute()

        if len(response.data) == 1:
            return response.data[0]["id"]

    except Exception:
        pass

    return None
