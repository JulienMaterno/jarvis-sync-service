import os
import httpx
import asyncio
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

async def get_contact(access_token: str, resource_name: str) -> Dict[str, Any]:
    """
    Fetches a single contact by resource name.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    fields = "names,emailAddresses,phoneNumbers,birthdays,organizations,urls,memberships,biographies,metadata,addresses"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{GOOGLE_PEOPLE_API_BASE}/{resource_name}",
            headers=headers,
            params={"personFields": fields}
        )
        response.raise_for_status()
        return response.json()

async def get_all_contacts(access_token: str) -> List[Dict[str, Any]]:
    """
    Fetches all connections from Google People API.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    contacts = []
    page_token = None
    
    fields = "names,emailAddresses,phoneNumbers,birthdays,organizations,urls,memberships,biographies,metadata,addresses"
    
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
    etag = google_contact.get("etag")
    
    # Metadata for update time
    metadata = google_contact.get("metadata", {})
    sources = metadata.get("sources", [])
    updated_at = None
    for source in sources:
        if source.get("type") == "CONTACT":
            updated_at = source.get("updateTime")
            break
    
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
    
    # Also check addresses for location if not found in groups
    addresses = google_contact.get("addresses", [])
    address_location = None
    if addresses:
        # Just take the first formatted address or country
        address_location = addresses[0].get("country") or addresses[0].get("formattedValue")

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
    
    # Fallback to address if no group location
    if not location and address_location:
        # Simple mapping or just use it if it matches valid locations
        if address_location in valid_locations:
            location = address_location
        # Else maybe map it? For now leave as None if not in valid set
                
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
        "location": location,
        "_etag": etag,
        "_google_updated_at": updated_at
    }

def transform_to_google_body(data: Dict[str, Any]) -> Dict[str, Any]:
    body = {}
    
    # Names
    if data.get("first_name") or data.get("last_name"):
        body["names"] = [{
            "givenName": data.get("first_name", ""),
            "familyName": data.get("last_name", "")
        }]
        
    # Emails
    if data.get("email"):
        body["emailAddresses"] = [{"value": data["email"]}]
        
    # Phones
    phones = []
    if data.get("phone"):
        phones.append({"value": data["phone"]})
    if data.get("phone_secondary"):
        phones.append({"value": data["phone_secondary"]})
    if phones:
        body["phoneNumbers"] = phones
        
    # Orgs
    if data.get("company") or data.get("job_title"):
        body["organizations"] = [{
            "name": data.get("company", ""),
            "title": data.get("job_title", "")
        }]
        
    # Bio/Notes
    if data.get("notes"):
        body["biographies"] = [{"value": data["notes"]}]
        
    # Birthday
    if data.get("birthday"):
        # Assumes YYYY-MM-DD
        try:
            parts = data["birthday"].split("-")
            if len(parts) == 3:
                body["birthdays"] = [{
                    "date": {
                        "year": int(parts[0]),
                        "month": int(parts[1]),
                        "day": int(parts[2])
                    }
                }]
        except:
            pass
            
    # Addresses (Location)
    if data.get("location"):
        body["addresses"] = [{"country": data["location"]}]

    return body

def calculate_memberships(
    target_location: Optional[str], 
    target_subscribed: bool,
    existing_memberships: List[Dict[str, Any]],
    name_to_id_map: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Calculates the new list of memberships (labels).
    Preserves existing labels that are NOT location/subscribed related.
    """
    new_memberships = []
    
    # Known location labels to remove if switching
    known_locations = {'Australia', 'China', 'Germany', 'SEA', 'Other'}
    
    # 1. Keep existing memberships that are NOT in our managed set
    for m in existing_memberships:
        group_info = m.get("contactGroupMembership", {})
        resource_name = group_info.get("contactGroupResourceName")
        
        # We need to know the NAME of this group to decide if we keep it.
        # But we only have ID -> Name map (inverted).
        # Wait, we have name_to_id_map. We need id_to_name_map to check existing.
        # Actually, we can just check if the resource_name matches any of our managed IDs.
        
        is_managed = False
        for loc in known_locations:
            if name_to_id_map.get(loc) == resource_name:
                is_managed = True
                break
        
        if name_to_id_map.get("Subscribed") == resource_name:
            is_managed = True
            
        if not is_managed:
            new_memberships.append(m)
            
    # 2. Add new Location label
    if target_location and target_location in known_locations:
        group_id = name_to_id_map.get(target_location)
        if group_id:
            new_memberships.append({
                "contactGroupMembership": {
                    "contactGroupResourceName": group_id
                }
            })
            
    # 3. Add Subscribed label
    if target_subscribed:
        group_id = name_to_id_map.get("Subscribed")
        if group_id:
            new_memberships.append({
                "contactGroupMembership": {
                    "contactGroupResourceName": group_id
                }
            })
            
    return new_memberships

async def create_contact(
    access_token: str, 
    contact_data: Dict[str, Any],
    name_to_id_map: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    # Rate limit
    await asyncio.sleep(0.2) 
    headers = {"Authorization": f"Bearer {access_token}"}
    body = transform_to_google_body(contact_data)
    
    # Handle Memberships for Create
    if name_to_id_map:
        memberships = calculate_memberships(
            contact_data.get("location"),
            contact_data.get("subscribed", False),
            [], # No existing memberships
            name_to_id_map
        )
        if memberships:
            body["memberships"] = memberships

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{GOOGLE_PEOPLE_API_BASE}/people:createContact",
            headers=headers,
            json=body
        )
        response.raise_for_status()
        return response.json()

async def update_contact(
    access_token: str, 
    resource_name: str, 
    contact_data: Dict[str, Any], 
    etag: str,
    raw_google_contact: Optional[Dict[str, Any]] = None,
    name_to_id_map: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    # Rate limit
    await asyncio.sleep(0.2)
    headers = {"Authorization": f"Bearer {access_token}"}
    body = transform_to_google_body(contact_data)
    body["etag"] = etag
    
    # We need to specify which fields to update. 
    # For simplicity, we'll update all fields we support.
    update_fields = "names,emailAddresses,phoneNumbers,organizations,biographies,birthdays,addresses"
    
    # Handle Memberships for Update
    if raw_google_contact and name_to_id_map:
        existing_memberships = raw_google_contact.get("memberships", [])
        new_memberships = calculate_memberships(
            contact_data.get("location"),
            contact_data.get("subscribed", False),
            existing_memberships,
            name_to_id_map
        )
        body["memberships"] = new_memberships
        update_fields += ",memberships"
    
    async with httpx.AsyncClient() as client:
        response = await client.patch(
            f"{GOOGLE_PEOPLE_API_BASE}/{resource_name}:updateContact",
            headers=headers,
            params={"updatePersonFields": update_fields},
            json=body
        )
        response.raise_for_status()
        return response.json()

async def delete_contact(access_token: str, resource_name: str) -> None:
    """
    Deletes a contact from Google Contacts.
    """
    # Rate limit
    await asyncio.sleep(0.2)
    headers = {"Authorization": f"Bearer {access_token}"}
    
    async with httpx.AsyncClient() as client:
        response = await client.delete(
            f"{GOOGLE_PEOPLE_API_BASE}/{resource_name}:deleteContact",
            headers=headers
        )
        response.raise_for_status()

