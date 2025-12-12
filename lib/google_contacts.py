import os
import httpx
from typing import Dict, Any, List, Optional

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_PEOPLE_API_BASE = "https://people.googleapis.com/v1"

async def get_access_token() -> str:
    """
    Exchanges the refresh token for a new access token.
    """
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Missing Google OAuth credentials in environment variables.")

    async with httpx.AsyncClient() as client:
        response = await client.post(GOOGLE_TOKEN_URL, data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        })
        response.raise_for_status()
        data = response.json()
        return data["access_token"]

async def get_contact_groups(access_token: str) -> Dict[str, str]:
    """
    Fetches contact groups to map resource names to human-readable names.
    Returns a dict: {resourceName: formattedName}
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    groups_mapping = {}
    
    async with httpx.AsyncClient() as client:
        # We might need pagination here too if there are many groups, 
        # but usually there are few.
        response = await client.get(
            f"{GOOGLE_PEOPLE_API_BASE}/contactGroups",
            headers=headers,
            params={"pageSize": 1000}
        )
        response.raise_for_status()
        data = response.json()
        
        for group in data.get("contactGroups", []):
            resource_name = group.get("resourceName")
            # prefer formattedName (System groups like 'myContacts' have formattedName 'Contacts')
            # but for user created labels, name and formattedName might be similar.
            # The prompt asks to check for "Australia", "Subscribed" etc.
            name = group.get("formattedName") or group.get("name")
            if resource_name and name:
                groups_mapping[resource_name] = name
                
    return groups_mapping

async def get_all_contacts(access_token: str) -> List[Dict[str, Any]]:
    """
    Fetches all connections from Google People API.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    contacts = []
    page_token = None
    
    fields = "names,emailAddresses,phoneNumbers,birthdays,organizations,urls,memberships,biographies"
    
    async with httpx.AsyncClient() as client:
        while True:
            params = {
                "personFields": fields,
                "pageSize": 100,
            }
            if page_token:
                params["pageToken"] = page_token

            response = await client.get(
                f"{GOOGLE_PEOPLE_API_BASE}/people/me/connections",
                headers=headers,
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            connections = data.get("connections", [])
            contacts.extend(connections)
            
            page_token = data.get("nextPageToken")
            if not page_token:
                break
                
    return contacts

def transform_contact(google_contact: Dict[str, Any], group_mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Transforms a Google Contact object to the Supabase schema.
    """
    resource_name = google_contact.get("resourceName")
    
    # Names
    names = google_contact.get("names", [])
    first_name = names[0].get("givenName") if names else None
    last_name = names[0].get("familyName") if names else None
    
    # Birthday
    birthdays = google_contact.get("birthdays", [])
    birthday_str = None
    if birthdays:
        date = birthdays[0].get("date", {})
        year = date.get("year")
        month = date.get("month")
        day = date.get("day")
        if year and month and day:
            birthday_str = f"{year}-{month:02d}-{day:02d}"
            
    # Emails
    emails = google_contact.get("emailAddresses", [])
    email = emails[0].get("value") if emails else None
    
    # Phones
    phones = google_contact.get("phoneNumbers", [])
    phone = phones[0].get("value") if phones else None
    phone_secondary = phones[1].get("value") if len(phones) > 1 else None
    
    # Organizations
    orgs = google_contact.get("organizations", [])
    company = orgs[0].get("name") if orgs else None
    job_title = orgs[0].get("title") if orgs else None
    
    # URLs (LinkedIn)
    urls = google_contact.get("urls", [])
    linkedin_url = None
    for u in urls:
        val = u.get("value", "").lower()
        if "linkedin" in val:
            linkedin_url = u.get("value")
            break
            
    # Biographies
    bios = google_contact.get("biographies", [])
    notes = bios[0].get("value") if bios else None
    
    # Memberships -> Subscribed & Location
    memberships = google_contact.get("memberships", [])
    subscribed = False
    location = None
    
    valid_locations = {'Australia', 'China', 'Germany', 'SEA', 'Other'}
    
    for m in memberships:
        group_info = m.get("contactGroupMembership", {})
        group_resource_name = group_info.get("contactGroupResourceName")
        
        if group_resource_name in group_mapping:
            group_name = group_mapping[group_resource_name]
            
            if group_name == "Subscribed":
                subscribed = True
            elif group_name in valid_locations:
                location = group_name
                
    return {
        "google_resource_name": resource_name,
        "first_name": first_name,
        "last_name": last_name,
        "birthday": birthday_str,
        "email": email,
        "phone": phone,
        "phone_secondary": phone_secondary,
        "company": company,
        "job_title": job_title,
        "linkedin_url": linkedin_url,
        "notes": notes,
        "subscribed": subscribed,
        "location": location
    }
