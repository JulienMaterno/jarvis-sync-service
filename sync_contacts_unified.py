#!/usr/bin/env python3
"""
===================================================================================
CONTACTS SYNC SERVICE - Multi-Source (Google + Notion ↔ Supabase)
===================================================================================

Syncs contacts from both Google Contacts AND Notion to Supabase.
This is the most complex sync pattern - three-way with multiple sources.

Data Flow:
    Google Contacts ←→ Supabase ←→ Notion

Priority:
    - Google is source of truth for contact details (phone, email)
    - Notion is source of truth for CRM data (notes, tags, meetings)
    - Supabase is the central hub linking both

Usage:
    python sync_contacts_unified.py                    # Full bidirectional sync
    python sync_contacts_unified.py --google-only      # Only sync Google
    python sync_contacts_unified.py --notion-only      # Only sync Notion
    python sync_contacts_unified.py --schema           # Show schemas

Database: contacts (Supabase) ↔ Contacts (Notion) ↔ Google People API
Direction: MULTI-SOURCE (three-way)
"""

import os
import sys
import json
from datetime import datetime, timezone

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

from lib.sync_base import (
    TwoWaySyncService,
    NotionPropertyExtractor as Extract,
    NotionPropertyBuilder as Build,
    NotionClient,
    SupabaseClient,
    SyncLogger,
    SyncResult,
    SyncStats,
    create_cli_parser,
    setup_logger,
    NOTION_API_TOKEN,
    SUPABASE_URL,
    SUPABASE_KEY,
    retry_on_error
)
from typing import Dict, List, Optional, Tuple
import httpx

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_CONTACTS_DATABASE_ID = os.environ.get('NOTION_CRM_DATABASE_ID', '2d1068b5-e624-81e8-9c1c-f1d45c33e420')
SUPABASE_TABLE = 'contacts'

# Google API config
GOOGLE_TOKEN_JSON = os.environ.get('GOOGLE_TOKEN_JSON')


# ============================================================================
# GOOGLE CONTACTS CLIENT
# ============================================================================

class GoogleContactsClient:
    """Client for Google People API."""
    
    def __init__(self, token_json: str):
        self.logger = setup_logger('GoogleContacts')
        
        if not token_json:
            self.logger.warning("GOOGLE_TOKEN_JSON not set - Google sync disabled")
            self.enabled = False
            return
        
        self.enabled = True
        token_data = json.loads(token_json)
        self.access_token = token_data.get('access_token')
        self.refresh_token = token_data.get('refresh_token')
        self.client_id = token_data.get('client_id')
        self.client_secret = token_data.get('client_secret')
    
    def _refresh_access_token(self) -> bool:
        """Refresh the OAuth access token."""
        try:
            response = httpx.post(
                'https://oauth2.googleapis.com/token',
                data={
                    'client_id': self.client_id,
                    'client_secret': self.client_secret,
                    'refresh_token': self.refresh_token,
                    'grant_type': 'refresh_token'
                }
            )
            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('access_token')
                self.logger.info("Successfully refreshed Google access token")
                return True
            else:
                self.logger.error(f"Failed to refresh token: {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Token refresh error: {e}")
            return False
    
    @retry_on_error(max_retries=2)
    def list_contacts(self, max_results: int = 1000) -> List[Dict]:
        """List all contacts from Google."""
        if not self.enabled:
            return []
        
        contacts = []
        page_token = None
        
        while True:
            params = {
                'personFields': 'names,emailAddresses,phoneNumbers,organizations,addresses,birthdays,urls,biographies',
                'pageSize': min(max_results, 100)
            }
            if page_token:
                params['pageToken'] = page_token
            
            response = httpx.get(
                'https://people.googleapis.com/v1/people/me/connections',
                headers={'Authorization': f'Bearer {self.access_token}'},
                params=params,
                timeout=30.0
            )
            
            if response.status_code == 401:
                if self._refresh_access_token():
                    continue
                break
            
            response.raise_for_status()
            data = response.json()
            
            contacts.extend(data.get('connections', []))
            
            page_token = data.get('nextPageToken')
            if not page_token or len(contacts) >= max_results:
                break
        
        return contacts
    
    @retry_on_error(max_retries=2)
    def create_contact(self, contact_data: Dict) -> Optional[Dict]:
        """Create a new contact in Google."""
        if not self.enabled:
            return None
        
        body = self._build_google_contact(contact_data)
        
        response = httpx.post(
            'https://people.googleapis.com/v1/people:createContact',
            headers={
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            },
            json=body,
            timeout=30.0
        )
        
        if response.status_code == 401:
            if self._refresh_access_token():
                return self.create_contact(contact_data)
        
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=2)
    def update_contact(self, resource_name: str, contact_data: Dict, etag: str) -> Optional[Dict]:
        """Update an existing Google contact."""
        if not self.enabled:
            return None
        
        body = self._build_google_contact(contact_data)
        body['etag'] = etag
        
        response = httpx.patch(
            f'https://people.googleapis.com/v1/{resource_name}:updateContact',
            headers={
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            },
            params={'updatePersonFields': 'names,emailAddresses,phoneNumbers,organizations'},
            json=body,
            timeout=30.0
        )
        
        if response.status_code == 401:
            if self._refresh_access_token():
                return self.update_contact(resource_name, contact_data, etag)
        
        response.raise_for_status()
        return response.json()
    
    def _build_google_contact(self, data: Dict) -> Dict:
        """Build Google People API contact format."""
        body = {}
        
        if data.get('first_name') or data.get('last_name'):
            body['names'] = [{
                'givenName': data.get('first_name', ''),
                'familyName': data.get('last_name', '')
            }]
        
        if data.get('email'):
            body['emailAddresses'] = [{'value': data['email']}]
        
        if data.get('phone'):
            body['phoneNumbers'] = [{'value': data['phone']}]
        
        if data.get('company') or data.get('job_title'):
            body['organizations'] = [{
                'name': data.get('company', ''),
                'title': data.get('job_title', '')
            }]
        
        return body
    
    @staticmethod
    def parse_google_contact(contact: Dict) -> Dict:
        """Parse Google contact into Supabase format."""
        names = contact.get('names', [{}])[0]
        emails = contact.get('emailAddresses', [{}])[0]
        phones = contact.get('phoneNumbers', [{}])[0]
        orgs = contact.get('organizations', [{}])[0]
        addresses = contact.get('addresses', [{}])[0]
        birthdays = contact.get('birthdays', [{}])[0]
        urls = contact.get('urls', [{}])[0]
        bios = contact.get('biographies', [{}])[0]
        
        # Parse birthday if present
        birthday = None
        if birthdays.get('date'):
            bd = birthdays['date']
            if bd.get('year') and bd.get('month') and bd.get('day'):
                birthday = f"{bd['year']}-{bd['month']:02d}-{bd['day']:02d}"
        
        return {
            'first_name': names.get('givenName', ''),
            'last_name': names.get('familyName', ''),
            'email': emails.get('value'),
            'phone': phones.get('value'),
            'company': orgs.get('name'),
            'job_title': orgs.get('title'),
            'location': addresses.get('formattedValue'),
            'birthday': birthday,
            'linkedin_url': urls.get('value') if 'linkedin' in urls.get('value', '').lower() else None,
            'notes': bios.get('value'),
            'google_contact_id': contact.get('resourceName'),
            'google_etag': contact.get('etag')
        }


# ============================================================================
# CONTACTS SYNC SERVICE
# ============================================================================

class ContactsSyncService(TwoWaySyncService):
    """
    Multi-source sync for Contacts: Google + Notion ↔ Supabase
    
    Notion Property Mapping:
    - Name (title) → first_name + last_name
    - Company (rich_text) → company
    - Email (email) → email
    - Phone (phone) → phone
    - Job Title (rich_text) → job_title
    - Birthday (date) → birthday
    - LinkedIn (url) → linkedin_url
    - Location (rich_text) → location
    - Tags (multi_select) → stored in dynamic_properties
    """
    
    def __init__(self):
        super().__init__(
            service_name='contacts_sync',
            notion_database_id=NOTION_CONTACTS_DATABASE_ID,
            supabase_table=SUPABASE_TABLE
        )
        self.google = GoogleContactsClient(GOOGLE_TOKEN_JSON)
    
    def convert_from_source(self, notion_record: Dict) -> Dict:
        """Convert Notion contact page to Supabase format."""
        props = notion_record.get('properties', {})
        
        # Parse name (might be "First Last" in title)
        full_name = Extract.title(props, 'Name')
        parts = full_name.split(' ', 1)
        first_name = parts[0] if parts else ''
        last_name = parts[1] if len(parts) > 1 else ''
        
        return {
            'first_name': first_name,
            'last_name': last_name,
            'email': Extract.email(props, 'Email'),
            'phone': Extract.phone(props, 'Phone'),
            'company': Extract.rich_text(props, 'Company'),
            'job_title': Extract.rich_text(props, 'Job Title'),
            'birthday': Extract.date(props, 'Birthday'),
            'linkedin_url': Extract.url(props, 'LinkedIn'),
            'location': Extract.rich_text(props, 'Location'),
            'notes': Extract.rich_text(props, 'Notes'),
            'dynamic_properties': {
                'tags': Extract.multi_select(props, 'Tags')
            }
        }
    
    def convert_to_source(self, supabase_record: Dict) -> Dict:
        """Convert Supabase contact to Notion properties."""
        full_name = f"{supabase_record.get('first_name', '')} {supabase_record.get('last_name', '')}".strip()
        
        props = {
            'Name': Build.title(full_name),
            'Email': Build.email(supabase_record.get('email')),
            'Phone': Build.phone(supabase_record.get('phone')),
            'Company': Build.rich_text(supabase_record.get('company')),
            'Job Title': Build.rich_text(supabase_record.get('job_title')),
            'LinkedIn': Build.url(supabase_record.get('linkedin_url')),
            'Location': Build.rich_text(supabase_record.get('location')),
            'Notes': Build.rich_text(supabase_record.get('notes')),
        }
        
        if supabase_record.get('birthday'):
            props['Birthday'] = Build.date(supabase_record['birthday'])
        
        # Handle tags from dynamic_properties
        dyn = supabase_record.get('dynamic_properties') or {}
        if dyn.get('tags'):
            props['Tags'] = Build.multi_select(dyn['tags'])
        
        return props
    
    def sync_google(self) -> SyncResult:
        """Sync Google Contacts to/from Supabase."""
        if not self.google.enabled:
            self.logger.warning("Google sync disabled - no token configured")
            return SyncResult(success=True, direction="google_disabled", stats=SyncStats())
        
        stats = SyncStats()
        start_time = __import__('time').time()
        
        try:
            # Fetch Google contacts
            google_contacts = self.google.list_contacts()
            self.logger.info(f"Found {len(google_contacts)} Google contacts")
            
            # Get existing Supabase contacts
            existing = {r.get('google_contact_id'): r for r in self.supabase.select_all() if r.get('google_contact_id')}
            by_email = {r.get('email'): r for r in self.supabase.select_all() if r.get('email')}
            
            # Safety valve
            is_safe, msg = self.check_safety_valve(len(google_contacts), len(existing), "Google → Supabase")
            if not is_safe:
                self.logger.error(msg)
                return SyncResult(success=False, direction="google_to_supabase", error_message=msg)
            
            # Process each Google contact
            for g_contact in google_contacts:
                try:
                    parsed = GoogleContactsClient.parse_google_contact(g_contact)
                    google_id = parsed.get('google_contact_id')
                    
                    # Find existing record
                    existing_record = existing.get(google_id) or by_email.get(parsed.get('email'))
                    
                    if existing_record:
                        # Merge: keep Notion-originated fields, update Google fields
                        merged = {
                            'first_name': parsed.get('first_name') or existing_record.get('first_name'),
                            'last_name': parsed.get('last_name') or existing_record.get('last_name'),
                            'email': parsed.get('email') or existing_record.get('email'),
                            'phone': parsed.get('phone') or existing_record.get('phone'),
                            'company': parsed.get('company') or existing_record.get('company'),
                            'job_title': parsed.get('job_title') or existing_record.get('job_title'),
                            'location': parsed.get('location') or existing_record.get('location'),
                            'birthday': parsed.get('birthday') or existing_record.get('birthday'),
                            'linkedin_url': parsed.get('linkedin_url') or existing_record.get('linkedin_url'),
                            'google_contact_id': google_id,
                            'last_sync_source': 'google',
                            'updated_at': datetime.now(timezone.utc).isoformat()
                        }
                        self.supabase.update(existing_record['id'], merged)
                        stats.updated += 1
                    else:
                        # Create new
                        parsed['last_sync_source'] = 'google'
                        parsed['created_at'] = datetime.now(timezone.utc).isoformat()
                        parsed['updated_at'] = datetime.now(timezone.utc).isoformat()
                        self.supabase.insert(parsed)
                        stats.created += 1
                
                except Exception as e:
                    self.logger.error(f"Error processing Google contact: {e}")
                    stats.errors += 1
            
            elapsed = __import__('time').time() - start_time
            return SyncResult(
                success=True,
                direction="google_to_supabase",
                stats=stats,
                elapsed_seconds=elapsed
            )
            
        except Exception as e:
            self.logger.error(f"Google sync failed: {e}")
            return SyncResult(success=False, direction="google_to_supabase", error_message=str(e))
    
    def full_sync(self) -> SyncResult:
        """
        Full three-way sync:
        1. Google → Supabase (import new contacts, update existing)
        2. Notion → Supabase (import new contacts, update existing)
        3. Supabase → Notion (push contacts without notion_page_id)
        """
        self.logger.info("Starting full three-way contacts sync")
        
        # Phase 1: Google → Supabase
        self.logger.info("Phase 1: Google → Supabase")
        google_result = self.sync_google()
        
        # Phase 2: Notion ↔ Supabase (bidirectional)
        self.logger.info("Phase 2: Notion ↔ Supabase")
        notion_result = self.sync(full_sync=True)
        
        # Combine results
        combined_stats = SyncStats(
            created=google_result.stats.created + notion_result.stats.created,
            updated=google_result.stats.updated + notion_result.stats.updated,
            errors=google_result.stats.errors + notion_result.stats.errors
        )
        
        return SyncResult(
            success=google_result.success and notion_result.success,
            direction="three_way",
            stats=combined_stats,
            elapsed_seconds=google_result.elapsed_seconds + notion_result.elapsed_seconds
        )


# ============================================================================
# MAIN
# ============================================================================

def show_schema():
    """Display schemas from both sources."""
    print("\n📇 CONTACTS DATABASE SCHEMAS")
    print("=" * 60)
    
    # Notion schema
    print("\n🔷 NOTION CONTACTS:")
    notion = NotionClient(NOTION_API_TOKEN)
    try:
        schema = notion.get_database_schema(NOTION_CONTACTS_DATABASE_ID)
        print(f"   Title: {schema.get('title', [{}])[0].get('plain_text', 'Untitled')}")
        print(f"   ID: {schema.get('id')}")
        print(f"\n   Properties:")
        for name, prop in schema.get('properties', {}).items():
            print(f"     • {name:20} ({prop.get('type')})")
    except Exception as e:
        print(f"   ❌ Failed to get Notion schema: {e}")
    
    # Supabase schema
    print("\n🔶 SUPABASE CONTACTS:")
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY, 'contacts')
    try:
        records = supabase.select_all()
        if records:
            print(f"   Records: {len(records)}")
            print(f"   Columns: {', '.join(records[0].keys())}")
        else:
            print("   (No records)")
    except Exception as e:
        print(f"   ❌ Failed to query Supabase: {e}")
    
    # Google info
    print("\n🔵 GOOGLE CONTACTS:")
    google = GoogleContactsClient(GOOGLE_TOKEN_JSON)
    if google.enabled:
        try:
            contacts = google.list_contacts(max_results=5)
            print(f"   Status: ✅ Connected")
            print(f"   Sample: Found {len(contacts)} contacts (limited query)")
        except Exception as e:
            print(f"   Status: ⚠️ Error: {e}")
    else:
        print("   Status: ❌ Not configured (GOOGLE_TOKEN_JSON missing)")


def run_sync(google_only: bool = False, notion_only: bool = False) -> Dict:
    """Run the contacts sync."""
    service = ContactsSyncService()
    
    if google_only:
        result = service.sync_google()
    elif notion_only:
        result = service.sync(full_sync=True)
    else:
        result = service.full_sync()
    
    return result.to_dict()


if __name__ == '__main__':
    parser = create_cli_parser('Contacts')
    parser.add_argument('--google-only', action='store_true', help='Only sync Google Contacts')
    parser.add_argument('--notion-only', action='store_true', help='Only sync Notion Contacts')
    args = parser.parse_args()
    
    if args.schema:
        show_schema()
    else:
        result = run_sync(google_only=args.google_only, notion_only=args.notion_only)
        
        if result.get('success'):
            stats = result.get('stats', {})
            print(f"\n✅ Contacts sync complete!")
            print(f"   Direction: {result.get('direction')}")
            print(f"   Created: {stats.get('created', 0)}")
            print(f"   Updated: {stats.get('updated', 0)}")
            print(f"   Errors: {stats.get('errors', 0)}")
            print(f"   Time: {result.get('elapsed_seconds', 0):.1f}s")
        else:
            print(f"\n❌ Sync failed: {result.get('error_message')}")
