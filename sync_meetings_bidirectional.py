"""
Bidirectional Notion ↔ Supabase Meeting Sync Service

Syncs meetings between Notion and Supabase:
- Notion → Supabase: Manual entries, AI meeting notes metadata
- Supabase → Notion: Meetings from voice pipeline

Handles:
- CRM person relations (contact_id in Supabase, People relation in Notion)
- "unsupported" blocks (AI meeting notes) - ignores but doesn't delete
- Page content extraction for readable blocks
- Last-write-wins conflict resolution

Usage:
    python sync_meetings_bidirectional.py --full    # Full sync
    python sync_meetings_bidirectional.py           # Incremental
"""

import os
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
import httpx
from lib.utils import retry_on_error_sync

load_dotenv()

# Import logging service - use sync version to avoid event loop issues
try:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from lib.logging_service import log_sync_event_sync
    HAS_LOGGING_SERVICE = True
except ImportError:
    HAS_LOGGING_SERVICE = False
    def log_sync_event_sync(event_type, status, message, **kwargs):
        """Fallback if logging service not available"""
        pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('MeetingSync')

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_MEETING_DB_ID = os.environ.get('NOTION_MEETING_DB_ID', '297cd3f1-eb28-810f-86f0-f142f7e3a5ca')
NOTION_CRM_DB_ID = os.environ.get('NOTION_CRM_DATABASE_ID', '2c7cd3f1eb2880269e53ed4d45e99b69')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# ============================================================================
# NOTION CLIENT
# ============================================================================

class NotionClient:
    """Notion API client with content extraction."""
    
    def __init__(self, token: str):
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Notion-Version': '2022-06-28',
            'Content-Type': 'application/json'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
        self._crm_cache: Dict[str, Dict] = {}
        self._crm_by_email: Dict[str, str] = {}  # email -> notion_page_id
    
    @retry_on_error_sync()
    def query_database(
        self, 
        database_id: str, 
        filter: Optional[Dict] = None,
        sorts: Optional[List[Dict]] = None,
        page_size: int = 100,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """Query all pages from a database with pagination.
        
        Args:
            limit: If set, stop after getting this many results (no pagination)
        """
        results = []
        start_cursor = None
        
        while True:
            body = {"page_size": min(page_size, limit) if limit else page_size}
            if filter:
                body["filter"] = filter
            if sorts:
                body["sorts"] = sorts
            if start_cursor:
                body["start_cursor"] = start_cursor
            
            response = self.client.post(
                f'https://api.notion.com/v1/databases/{database_id}/query',
                json=body
            )
            response.raise_for_status()
            data = response.json()
            
            results.extend(data.get('results', []))
            
            # Stop if we have enough or no more pages
            if limit and len(results) >= limit:
                return results[:limit]
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return results
    
    @retry_on_error_sync()
    def get_page(self, page_id: str) -> Dict:
        """Get a single page by ID."""
        response = self.client.get(f'https://api.notion.com/v1/pages/{page_id}')
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def create_page(self, database_id: str, properties: Dict, content_blocks: List[Dict] = None) -> Dict:
        """Create a new page in a database."""
        body = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        if content_blocks:
            body["children"] = content_blocks
        
        response = self.client.post(
            'https://api.notion.com/v1/pages',
            json=body
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def update_page(self, page_id: str, properties: Dict) -> Dict:
        """Update page properties (not content)."""
        response = self.client.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            json={"properties": properties}
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def get_page_blocks(self, page_id: str) -> List[Dict]:
        """Get all blocks from a page."""
        blocks = []
        start_cursor = None
        
        while True:
            url = f'https://api.notion.com/v1/blocks/{page_id}/children'
            if start_cursor:
                url += f'?start_cursor={start_cursor}'
            
            response = self.client.get(url)
            response.raise_for_status()
            data = response.json()
            
            blocks.extend(data.get('results', []))
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return blocks
    
    @retry_on_error_sync()
    def append_blocks(self, page_id: str, blocks: List[Dict]) -> List[Dict]:
        """Append blocks to a page."""
        response = self.client.patch(
            f'https://api.notion.com/v1/blocks/{page_id}/children',
            json={"children": blocks}
        )
        response.raise_for_status()
        return response.json().get('results', [])
    
    def get_block_children(self, block_id: str) -> List[Dict]:
        """Get children of a block (for nested content)."""
        blocks = []
        start_cursor = None
        
        while True:
            url = f'https://api.notion.com/v1/blocks/{block_id}/children'
            if start_cursor:
                url += f'?start_cursor={start_cursor}'
            
            response = self.client.get(url)
            # Handle errors gracefully - some blocks don't support children
            if response.status_code in [400, 404]:
                return []
            response.raise_for_status()
            data = response.json()
            
            blocks.extend(data.get('results', []))
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return blocks
    
    def extract_page_content(self, page_id: str, max_depth: int = 3) -> Tuple[str, bool]:
        """
        Extract readable text content from a page.
        
        Returns:
            Tuple of (content_text, has_unsupported_blocks)
        """
        blocks = self.get_page_blocks(page_id)
        return self._extract_blocks_text(blocks, max_depth=max_depth)
    
    def _extract_blocks_text(self, blocks: List[Dict], depth: int = 0, max_depth: int = 3) -> Tuple[str, bool]:
        """Recursively extract text from blocks."""
        text_parts = []
        has_unsupported = False
        indent = "  " * depth
        
        for block in blocks:
            block_type = block.get('type')
            block_id = block.get('id')
            has_children = block.get('has_children', False)
            
            # Handle unsupported blocks (AI meeting notes, etc.)
            if block_type == 'unsupported':
                has_unsupported = True
                # Try to get children anyway
                if has_children and depth < max_depth:
                    children = self.get_block_children(block_id)
                    child_text, _ = self._extract_blocks_text(children, depth + 1, max_depth)
                    if child_text:
                        text_parts.append(child_text)
                continue
            
            # Extract text based on block type
            text = self._get_block_text(block)
            if text:
                prefix = self._get_block_prefix(block_type)
                text_parts.append(f"{indent}{prefix}{text}")
            
            # Recursively get children (for toggles, etc.)
            if has_children and depth < max_depth:
                children = self.get_block_children(block_id)
                child_text, child_unsupported = self._extract_blocks_text(children, depth + 1, max_depth)
                if child_text:
                    text_parts.append(child_text)
                if child_unsupported:
                    has_unsupported = True
        
        return '\n'.join(filter(None, text_parts)), has_unsupported
    
    def _get_block_text(self, block: Dict) -> Optional[str]:
        """Extract plain text from a single block."""
        block_type = block.get('type')
        
        text_block_types = [
            'paragraph', 'heading_1', 'heading_2', 'heading_3',
            'bulleted_list_item', 'numbered_list_item', 'to_do',
            'toggle', 'quote', 'callout'
        ]
        
        if block_type in text_block_types:
            rich_text = block.get(block_type, {}).get('rich_text', [])
            return ''.join([t.get('plain_text', '') for t in rich_text])
        
        return None
    
    def _get_block_prefix(self, block_type: str) -> str:
        """Get display prefix for block type."""
        prefixes = {
            'heading_1': '# ',
            'heading_2': '## ',
            'heading_3': '### ',
            'bulleted_list_item': '• ',
            'numbered_list_item': '- ',
            'to_do': '☐ ',
            'quote': '> ',
            'callout': '💡 ',
            'toggle': '▶ ',
        }
        return prefixes.get(block_type, '')
    
    def get_crm_contact(self, page_id: str) -> Optional[Dict]:
        """Get CRM contact details by page ID."""
        if page_id in self._crm_cache:
            return self._crm_cache[page_id]
        
        try:
            page = self.get_page(page_id)
            props = page.get('properties', {})
            
            name_prop = props.get('Name', {}).get('title', [])
            name = name_prop[0].get('plain_text', '') if name_prop else ''
            email = props.get('Mail', {}).get('email')
            
            contact = {
                'notion_id': page_id,
                'name': name,
                'email': email,
                'url': page.get('url')
            }
            
            self._crm_cache[page_id] = contact
            if email:
                self._crm_by_email[email.lower()] = page_id
            return contact
            
        except Exception as e:
            logger.warning(f"Failed to get CRM contact {page_id}: {e}")
            return None
    
    def find_crm_by_email(self, email: str) -> Optional[str]:
        """Find CRM page ID by email."""
        if not email:
            return None
        
        email_lower = email.lower()
        if email_lower in self._crm_by_email:
            return self._crm_by_email[email_lower]
        
        # Search in Notion CRM
        try:
            results = self.query_database(
                NOTION_CRM_DB_ID,
                filter={
                    "property": "Mail",
                    "email": {"equals": email}
                }
            )
            if results:
                page_id = results[0]['id']
                self._crm_by_email[email_lower] = page_id
                return page_id
        except Exception as e:
            logger.warning(f"Failed to find CRM by email {email}: {e}")
        
        return None
    
    def find_crm_by_name(self, name: str) -> Optional[str]:
        """Find CRM page ID by name."""
        if not name:
            return None
        
        try:
            results = self.query_database(
                NOTION_CRM_DB_ID,
                filter={
                    "property": "Name",
                    "title": {"contains": name.split()[0]}  # First name
                }
            )
            
            # Try exact match
            for page in results:
                title_prop = page['properties'].get('Name', {}).get('title', [])
                page_name = title_prop[0].get('plain_text', '') if title_prop else ''
                if page_name.lower() == name.lower():
                    return page['id']
            
            # Return first if only one result
            if len(results) == 1:
                return results[0]['id']
                
        except Exception as e:
            logger.warning(f"Failed to find CRM by name {name}: {e}")
        
        return None
    
    def cache_crm_contacts(self):
        """Pre-cache all CRM contacts for faster lookup."""
        logger.info("Caching CRM contacts...")
        contacts = self.query_database(NOTION_CRM_DB_ID)
        for page in contacts:
            page_id = page['id']
            props = page.get('properties', {})
            
            name_prop = props.get('Name', {}).get('title', [])
            name = name_prop[0].get('plain_text', '') if name_prop else ''
            email = props.get('Mail', {}).get('email')
            
            self._crm_cache[page_id] = {
                'notion_id': page_id,
                'name': name,
                'email': email
            }
            if email:
                self._crm_by_email[email.lower()] = page_id
        
        logger.info(f"Cached {len(self._crm_cache)} CRM contacts")


# ============================================================================
# SUPABASE CLIENT
# ============================================================================

class SupabaseClient:
    """Supabase API client for meetings and contacts."""
    
    def __init__(self, url: str, key: str):
        self.base_url = url
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
        self._contact_cache: Dict[str, Dict] = {}  # id -> contact
    
    def get_all_meetings(self) -> List[Dict]:
        """Get all meetings from Supabase."""
        meetings = []
        offset = 0
        limit = 1000
        
        while True:
            response = self.client.get(
                f'{self.base_url}/rest/v1/meetings',
                params={
                    'select': '*',
                    'order': 'created_at.desc',
                    'offset': str(offset),
                    'limit': str(limit)
                }
            )
            response.raise_for_status()
            batch = response.json()
            meetings.extend(batch)
            
            if len(batch) < limit:
                break
            offset += limit
        
        return meetings
    
    def get_meeting_by_notion_id(self, notion_page_id: str) -> Optional[Dict]:
        """Get meeting by Notion page ID."""
        response = self.client.get(
            f'{self.base_url}/rest/v1/meetings',
            params={
                'select': '*',
                'notion_page_id': f'eq.{notion_page_id}',
                'limit': '1'
            }
        )
        response.raise_for_status()
        results = response.json()
        return results[0] if results else None
    
    def create_meeting(self, data: Dict) -> Dict:
        """Create a new meeting."""
        response = self.client.post(
            f'{self.base_url}/rest/v1/meetings',
            json=data
        )
        response.raise_for_status()
        return response.json()[0]
    
    def update_meeting(self, meeting_id: str, data: Dict) -> Dict:
        """Update an existing meeting."""
        response = self.client.patch(
            f'{self.base_url}/rest/v1/meetings',
            json=data,
            params={'id': f'eq.{meeting_id}'}
        )
        response.raise_for_status()
        results = response.json()
        return results[0] if results else data
    
    def get_contact(self, contact_id: str) -> Optional[Dict]:
        """Get contact by ID."""
        if contact_id in self._contact_cache:
            return self._contact_cache[contact_id]
        
        response = self.client.get(
            f'{self.base_url}/rest/v1/contacts',
            params={
                'select': 'id,first_name,last_name,email,notion_page_id',
                'id': f'eq.{contact_id}',
                'limit': '1'
            }
        )
        response.raise_for_status()
        results = response.json()
        if results:
            self._contact_cache[contact_id] = results[0]
            return results[0]
        return None
    
    def find_contact_by_name(self, name: str) -> Optional[Dict]:
        """Find contact by name."""
        if not name:
            return None
        
        parts = name.strip().split()
        first_name = parts[0]
        last_name = parts[-1] if len(parts) > 1 else None
        
        # Try exact match
        if last_name:
            response = self.client.get(
                f'{self.base_url}/rest/v1/contacts',
                params={
                    'select': 'id,first_name,last_name,email,notion_page_id',
                    'first_name': f'ilike.{first_name}',
                    'last_name': f'ilike.{last_name}',
                    'deleted_at': 'is.null',
                    'limit': '1'
                }
            )
            response.raise_for_status()
            results = response.json()
            if results:
                return results[0]
        
        # Fallback to first name
        response = self.client.get(
            f'{self.base_url}/rest/v1/contacts',
            params={
                'select': 'id,first_name,last_name,email,notion_page_id',
                'first_name': f'ilike.{first_name}',
                'deleted_at': 'is.null',
                'limit': '5'
            }
        )
        response.raise_for_status()
        results = response.json()
        
        if len(results) == 1:
            return results[0]
        
        return None
    
    def cache_contacts(self):
        """Pre-cache all contacts."""
        logger.info("Caching Supabase contacts...")
        response = self.client.get(
            f'{self.base_url}/rest/v1/contacts',
            params={
                'select': 'id,first_name,last_name,email,notion_page_id',
                'deleted_at': 'is.null'
            }
        )
        response.raise_for_status()
        for contact in response.json():
            self._contact_cache[contact['id']] = contact
        logger.info(f"Cached {len(self._contact_cache)} contacts")


# ============================================================================
# SYNC LOGIC
# ============================================================================

def notion_to_supabase_meeting(page: Dict, notion: NotionClient, supabase: SupabaseClient) -> Dict:
    """Convert Notion meeting page to Supabase format."""
    props = page.get('properties', {})
    page_id = page.get('id')
    
    # Title
    title_prop = props.get('Meeting', {}).get('title', [])
    title = title_prop[0].get('plain_text', 'Untitled') if title_prop else 'Untitled'
    
    # Date
    date_prop = props.get('Date', {}).get('date')
    date = date_prop.get('start') if date_prop else None
    
    # Location  
    location_prop = props.get('Location', {}).get('rich_text', [])
    location = location_prop[0].get('plain_text', '') if location_prop else None
    
    # People relation (links to CRM)
    person_ids = props.get('People', {}).get('relation', [])
    contact_name = None
    contact_id = None
    
    if person_ids:
        crm_contact = notion.get_crm_contact(person_ids[0].get('id'))
        if crm_contact:
            contact_name = crm_contact.get('name')
            # Find Supabase contact
            if crm_contact.get('email'):
                sb_contact = supabase.find_contact_by_name(contact_name)
                if sb_contact:
                    contact_id = sb_contact.get('id')
    
    # Extract page content
    content, has_unsupported = notion.extract_page_content(page_id)
    
    # Build summary from content if available
    summary = content[:2000] if content else None  # Limit summary length
    
    return {
        'notion_page_id': page_id,
        'title': title,
        'date': date,
        'location': location,
        'contact_id': contact_id,
        'contact_name': contact_name,
        'summary': summary,
        'source_file': 'notion-sync',
        'notion_updated_at': page.get('last_edited_time'),
        '_has_unsupported_blocks': has_unsupported  # Internal flag
    }


def supabase_to_notion_meeting(meeting: Dict, notion: NotionClient) -> Tuple[Dict, List[Dict]]:
    """Convert Supabase meeting to Notion format (properties + content blocks)."""
    
    # Build properties
    properties = {
        "Meeting": {
            "title": [{"text": {"content": meeting.get('title', 'Untitled')}}]
        }
    }
    
    # Date
    if meeting.get('date'):
        properties["Date"] = {
            "date": {"start": meeting['date'][:10]}  # Just the date part
        }
    
    # Location
    if meeting.get('location'):
        properties["Location"] = {
            "rich_text": [{"text": {"content": meeting['location']}}]
        }
    
    # People relation - find CRM contact
    contact_name = meeting.get('contact_name')
    if contact_name:
        crm_page_id = notion.find_crm_by_name(contact_name)
        if crm_page_id:
            properties["People"] = {
                "relation": [{"id": crm_page_id}]
            }
    
    # Build content blocks
    blocks = []
    
    # Summary
    if meeting.get('summary'):
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": meeting['summary'][:2000]}}]
            }
        })
    
    # Topics discussed - handle complex JSON structures
    topics = meeting.get('topics_discussed', [])
    if topics:
        blocks.append({
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [{"type": "text", "text": {"content": "Topics Discussed"}}]
            }
        })
        for topic in topics:
            # Handle both string and dict formats
            if isinstance(topic, dict):
                topic_text = topic.get('topic', '')
                details = topic.get('details', [])
                if topic_text:
                    blocks.append({
                        "object": "block",
                        "type": "bulleted_list_item",
                        "bulleted_list_item": {
                            "rich_text": [{"type": "text", "text": {"content": topic_text[:2000]}}]
                        }
                    })
                # Add details as sub-items (paragraph since Notion API doesn't support nested lists directly)
                for detail in details[:5]:  # Limit to 5 details per topic
                    if detail:
                        blocks.append({
                            "object": "block",
                            "type": "paragraph",
                            "paragraph": {
                                "rich_text": [{"type": "text", "text": {"content": f"  • {detail[:2000]}"}}]
                            }
                        })
            elif isinstance(topic, str) and topic:
                blocks.append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{"type": "text", "text": {"content": topic[:2000]}}]
                    }
                })
    
    # Follow-up items - handle complex JSON structures
    follow_ups = meeting.get('follow_up_items', [])
    if follow_ups:
        blocks.append({
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [{"type": "text", "text": {"content": "Follow-up Items"}}]
            }
        })
        for item in follow_ups:
            # Handle both string and dict formats
            if isinstance(item, dict):
                topic = item.get('topic', item.get('item', ''))
                context = item.get('context', '')
                item_text = topic
                if context:
                    item_text = f"{topic}: {context}"
                if item_text:
                    blocks.append({
                        "object": "block",
                        "type": "to_do",
                        "to_do": {
                            "rich_text": [{"type": "text", "text": {"content": item_text[:2000]}}],
                            "checked": False
                        }
                    })
            elif isinstance(item, str) and item:
                blocks.append({
                    "object": "block",
                    "type": "to_do",
                    "to_do": {
                        "rich_text": [{"type": "text", "text": {"content": item[:2000]}}],
                        "checked": False
                    }
                })
    
    # Key points - handle complex JSON structures
    key_points = meeting.get('key_points', [])
    if key_points:
        blocks.append({
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [{"type": "text", "text": {"content": "Key Points"}}]
            }
        })
        for point in key_points:
            # Handle both string and dict formats
            if isinstance(point, dict):
                point_text = point.get('point', point.get('text', str(point)))
            else:
                point_text = str(point) if point else ''
            
            if point_text:
                blocks.append({
                    "object": "block",
                    "type": "bulleted_list_item",
                    "bulleted_list_item": {
                        "rich_text": [{"type": "text", "text": {"content": point_text[:2000]}}]
                    }
                })
    
    return properties, blocks


def sync_notion_to_supabase(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync meetings from Notion → Supabase.
    
    Incremental mode: Only processes meetings edited in Notion in the last `since_hours`.
    Full mode: Processes all meetings, but still uses timestamp comparison for updates.
    """
    created = 0
    updated = 0
    skipped = 0
    errors = 0
    
    # Build filter for incremental
    notion_filter = None
    if not full_sync:
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        notion_filter = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": since.isoformat()}
        }
        logger.info(f"Incremental mode: checking meetings edited since {since.isoformat()}")
    else:
        logger.info("Full sync mode: checking all meetings")
    
    # Query Notion (filtered if incremental)
    meetings = notion.query_database(
        NOTION_MEETING_DB_ID,
        filter=notion_filter,
        sorts=[{"property": "Date", "direction": "descending"}]
    )
    logger.info(f"Found {len(meetings)} meetings in Notion to process")
    
    # Get existing Supabase meetings for safety valve
    existing_supabase = supabase.get_all_meetings()
    logger.info(f"Supabase has {len(existing_supabase)} total meetings")
    
    # SAFETY VALVE: If Notion returns empty or very few but Supabase has many, abort
    if full_sync and len(existing_supabase) > 10 and len(meetings) < (len(existing_supabase) * 0.1):
        msg = f"Safety Valve: Notion returned {len(meetings)} meetings, but Supabase has {len(existing_supabase)}. Aborting to prevent data loss."
        logger.error(msg)
        raise Exception(msg)
    
    # Warn if unusually high changes in incremental mode
    if not full_sync and len(meetings) > 50:
        logger.warning(f"High number of changes ({len(meetings)}) in incremental mode - consider running full sync")
    
    for page in meetings:
        page_id = page.get('id')
        notion_updated = page.get('last_edited_time', '')
        
        try:
            # Check if exists in Supabase
            existing = supabase.get_meeting_by_notion_id(page_id)
            
            if existing:
                # Compare timestamps with "Last Write Wins" + timestamp buffer
                existing_notion_updated = existing.get('notion_updated_at', '')
                last_sync_source = existing.get('last_sync_source', '')
                
                # Parse timestamps for buffer comparison
                try:
                    notion_dt = datetime.fromisoformat(notion_updated.replace('Z', '+00:00'))
                    if existing_notion_updated:
                        existing_dt = datetime.fromisoformat(existing_notion_updated.replace('Z', '+00:00'))
                    else:
                        existing_dt = None
                except:
                    notion_dt = None
                    existing_dt = None
                
                # Skip if Supabase already has this version
                # Use 5-second buffer if last update came from Notion to avoid ping-pong
                if existing_notion_updated and notion_dt and existing_dt:
                    if last_sync_source == 'notion':
                        # Last update from Notion - only update if significantly newer
                        if notion_dt <= existing_dt + timedelta(seconds=5):
                            skipped += 1
                            continue
                    else:
                        # Last update from Supabase - standard comparison
                        if notion_updated <= existing_notion_updated:
                            skipped += 1
                            continue
                elif existing_notion_updated and existing_notion_updated >= notion_updated:
                    skipped += 1
                    continue
                
                # Parse meeting data from Notion
                meeting_data = notion_to_supabase_meeting(page, notion, supabase)
                has_unsupported = meeting_data.pop('_has_unsupported_blocks', False)
                
                # Preserve existing content if we have unsupported blocks
                if has_unsupported and not meeting_data.get('summary') and existing.get('summary'):
                    meeting_data['summary'] = existing['summary']
                
                # Content comparison to avoid unnecessary updates
                fields_to_check = [
                    'title', 'date', 'location', 'contact_id', 'contact_name', 'summary'
                ]
                needs_update = False
                for field in fields_to_check:
                    new_val = meeting_data.get(field)
                    existing_val = existing.get(field)
                    
                    # Normalize empty values
                    if (new_val is None and existing_val == "") or (new_val == "" and existing_val is None):
                        continue
                    if new_val != existing_val:
                        needs_update = True
                        logger.debug(f"Field '{field}' changed: {existing_val!r} → {new_val!r}")
                        break
                
                if needs_update:
                    logger.info(f"Updating: {meeting_data['title']}")
                    meeting_data['last_sync_source'] = 'notion'  # Track sync direction
                    supabase.update_meeting(existing['id'], meeting_data)
                    updated += 1
                    log_sync_event_sync(
                        "update_supabase_meeting", "success",
                        f"Updated meeting '{meeting_data['title']}' from Notion"
                    )
                else:
                    logger.debug(f"Skipping (content unchanged): {meeting_data['title']}")
                    skipped += 1
            else:
                # New meeting - create in Supabase
                meeting_data = notion_to_supabase_meeting(page, notion, supabase)
                meeting_data.pop('_has_unsupported_blocks', False)
                meeting_data['last_sync_source'] = 'notion'  # Track sync direction
                
                logger.info(f"Creating: {meeting_data['title']}")
                supabase.create_meeting(meeting_data)
                created += 1
                log_sync_event_sync(
                    "create_supabase_meeting", "success",
                    f"Created meeting '{meeting_data['title']}' from Notion"
                )
                
        except Exception as e:
            logger.error(f"Error syncing Notion page {page_id} ({page.get('properties', {}).get('Meeting', {}).get('title', [{}])[0].get('plain_text', 'Unknown')}): {e}")
            errors += 1
    
    logger.info(f"Notion → Supabase: {created} created, {updated} updated, {skipped} skipped, {errors} errors")
    return created, updated, skipped


def sync_supabase_to_notion(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync meetings from Supabase → Notion.
    
    Incremental mode: Only processes meetings without notion_page_id OR updated recently.
    Full mode: Processes all meetings, but still uses timestamp comparison for updates.
    """
    created = 0
    updated = 0
    skipped = 0
    errors = 0
    
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours) if not full_sync else None
    
    # Get meetings from Supabase
    # In incremental mode, only get meetings that need syncing
    all_meetings = supabase.get_all_meetings()
    
    # Get Notion meetings count for safety valve
    if full_sync:
        notion_meetings = notion.query_database(
            NOTION_MEETING_DB_ID,
            sorts=[{"property": "Date", "direction": "descending"}]
        )
        logger.info(f"Notion has {len(notion_meetings)} total meetings")
        
        # SAFETY VALVE: If Supabase has many meetings but Notion is empty, abort
        if len(all_meetings) > 10 and len(notion_meetings) < (len(all_meetings) * 0.1):
            msg = f"Safety Valve: Supabase has {len(all_meetings)} meetings, but Notion has {len(notion_meetings)}. Aborting to prevent data loss."
            logger.error(msg)
            raise Exception(msg)
    
    if not full_sync:
        # Filter to meetings that either:
        # 1. Have no notion_page_id (need to be created)
        # 2. Have been updated recently (may need to update Notion)
        meetings = []
        for m in all_meetings:
            notion_page_id = m.get('notion_page_id')
            updated_at = m.get('updated_at', '')
            
            if not notion_page_id:
                # Not linked to Notion - needs creation
                meetings.append(m)
            elif since and updated_at:
                # Check if updated recently
                try:
                    updated_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                    if updated_dt > since:
                        meetings.append(m)
                except:
                    pass
        logger.info(f"Incremental mode: {len(meetings)} meetings need syncing (of {len(all_meetings)} total)")
    else:
        meetings = all_meetings
        logger.info(f"Full sync mode: processing all {len(meetings)} meetings")
    
    for meeting in meetings:
        meeting_id = meeting.get('id')
        notion_page_id = meeting.get('notion_page_id')
        
        try:
            if notion_page_id:
                # Already linked - check if we need to update Notion
                meeting_updated = meeting.get('updated_at', '')
                notion_updated = meeting.get('notion_updated_at', '')
                last_sync_source = meeting.get('last_sync_source', '')
                
                # Parse timestamps for buffer comparison
                try:
                    meeting_dt = datetime.fromisoformat(meeting_updated.replace('Z', '+00:00'))
                    notion_dt = datetime.fromisoformat(notion_updated.replace('Z', '+00:00')) if notion_updated else None
                except:
                    meeting_dt = None
                    notion_dt = None
                
                # Skip if Notion is already up-to-date
                # Use 5-second buffer if last update came from Supabase to avoid ping-pong
                if notion_updated and meeting_dt and notion_dt:
                    if last_sync_source == 'supabase':
                        # Last update from Supabase - only update if significantly newer
                        if meeting_dt <= notion_dt + timedelta(seconds=5):
                            skipped += 1
                            continue
                    else:
                        # Last update from Notion - standard comparison
                        if meeting_updated <= notion_updated:
                            skipped += 1
                            continue
                elif notion_updated and meeting_updated <= notion_updated:
                    skipped += 1
                    continue
                
                # Get current Notion page to compare content
                try:
                    current_page = notion.get_page(notion_page_id)
                    current_props = current_page.get('properties', {})
                    
                    # Build new properties
                    new_props, _ = supabase_to_notion_meeting(meeting, notion)
                    
                    # Content comparison - check if properties actually changed
                    needs_update = False
                    
                    # Helper to safely get first item from list
                    def safe_first(lst, default={}):
                        return lst[0] if lst else default
                    
                    # Compare title
                    current_title_list = current_props.get('Meeting', {}).get('title', [])
                    current_title = safe_first(current_title_list).get('plain_text', '')
                    new_title_list = new_props.get('Meeting', {}).get('title', [])
                    new_title = safe_first(new_title_list).get('text', {}).get('content', '')
                    if current_title != new_title:
                        needs_update = True
                    
                    # Compare date
                    current_date = current_props.get('Date', {}).get('date', {}).get('start', '') if current_props.get('Date', {}).get('date') else ''
                    new_date = new_props.get('Date', {}).get('date', {}).get('start', '') if 'Date' in new_props and new_props.get('Date', {}).get('date') else ''
                    if current_date != new_date:
                        needs_update = True
                    
                    # Compare location
                    current_loc_list = current_props.get('Location', {}).get('rich_text', [])
                    current_location = safe_first(current_loc_list).get('plain_text', '')
                    new_loc_list = new_props.get('Location', {}).get('rich_text', []) if 'Location' in new_props else []
                    new_location = safe_first(new_loc_list).get('text', {}).get('content', '')
                    if current_location != new_location:
                        needs_update = True
                    
                    # Compare People relation
                    current_people = [p['id'] for p in current_props.get('People', {}).get('relation', [])]
                    new_people = [p['id'] for p in new_props.get('People', {}).get('relation', [])] if 'People' in new_props else []
                    if current_people != new_people:
                        needs_update = True
                    
                    if needs_update:
                        logger.info(f"Updating Notion: {meeting['title']}")
                        updated_page = notion.update_page(notion_page_id, new_props)
                        
                        # Update Supabase with new notion_updated_at and last_sync_source
                        supabase.update_meeting(meeting_id, {
                            'notion_updated_at': updated_page.get('last_edited_time'),
                            'last_sync_source': 'supabase'  # Track sync direction
                        })
                        updated += 1
                        log_sync_event_sync(
                            "update_notion_meeting", "success",
                            f"Updated Notion page for '{meeting['title']}'"
                        )
                    else:
                        logger.debug(f"Skipping (content unchanged): {meeting['title']}")
                        skipped += 1
                        
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        logger.warning(f"Notion page {notion_page_id} not found - unlinking from Supabase")
                        supabase.update_meeting(meeting_id, {'notion_page_id': None, 'notion_updated_at': None})
                        skipped += 1
                    else:
                        raise
            else:
                # Not linked to Notion - create new page
                props, blocks = supabase_to_notion_meeting(meeting, notion)
                logger.info(f"Creating Notion: {meeting['title']}")
                
                try:
                    new_page = notion.create_page(NOTION_MEETING_DB_ID, props, blocks[:100])
                    new_page_id = new_page['id']
                    
                    # Link Supabase meeting to Notion page with last_sync_source
                    supabase.update_meeting(meeting_id, {
                        'notion_page_id': new_page_id,
                        'notion_updated_at': new_page.get('last_edited_time'),
                        'last_sync_source': 'supabase'  # Track sync direction
                    })
                    created += 1
                    log_sync_event_sync(
                        "create_notion_meeting", "success",
                        f"Created Notion page for '{meeting['title']}'"
                    )
                except Exception as create_error:
                    # If creation fails with blocks, try without blocks
                    logger.warning(f"Failed with blocks, trying without: {create_error}")
                    new_page = notion.create_page(NOTION_MEETING_DB_ID, props, [])
                    new_page_id = new_page['id']
                    
                    # Try to append blocks separately
                    if blocks:
                        try:
                            notion.append_blocks(new_page_id, blocks[:50])
                        except Exception as block_error:
                            logger.warning(f"Could not append blocks: {block_error}")
                    
                    supabase.update_meeting(meeting_id, {
                        'notion_page_id': new_page_id,
                        'notion_updated_at': new_page.get('last_edited_time')
                    })
                    created += 1
                
        except Exception as e:
            logger.error(f"Error syncing Supabase meeting {meeting_id} ({meeting.get('title', 'Unknown')}): {e}")
            import traceback
            logger.debug(traceback.format_exc())
            errors += 1
    
    logger.info(f"Supabase → Notion: {created} created, {updated} updated, {skipped} skipped, {errors} errors")
    return created, updated, skipped


def run_sync(full_sync: bool = False, since_hours: int = 24):
    """Run bidirectional sync."""
    start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("BIDIRECTIONAL MEETING SYNC")
    logger.info(f"Mode: {'FULL' if full_sync else f'INCREMENTAL ({since_hours}h)'}")
    logger.info("=" * 60)
    
    # Log sync start
    log_sync_event_sync(
        "meeting_sync_start", "info",
        f"Starting {'full' if full_sync else 'incremental'} meeting sync"
    )
    
    try:
        # Initialize clients
        notion = NotionClient(NOTION_API_TOKEN)
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        # Cache contacts for faster lookups
        logger.info("Caching CRM contacts...")
        notion.cache_crm_contacts()
        supabase.cache_contacts()
        
        # Notion → Supabase
        logger.info("\n--- NOTION → SUPABASE ---")
        n2s_created, n2s_updated, n2s_skipped = sync_notion_to_supabase(
            notion, supabase, full_sync, since_hours
        )
        
        # Supabase → Notion
        logger.info("\n--- SUPABASE → NOTION ---")
        s2n_created, s2n_updated, s2n_skipped = sync_supabase_to_notion(
            notion, supabase, full_sync, since_hours
        )
        
        # Summary
        elapsed = time.time() - start_time
        total_ops = n2s_created + n2s_updated + s2n_created + s2n_updated
        
        logger.info("\n" + "=" * 60)
        logger.info("SYNC COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Notion → Supabase: {n2s_created} created, {n2s_updated} updated, {n2s_skipped} skipped")
        logger.info(f"Supabase → Notion: {s2n_created} created, {s2n_updated} updated, {s2n_skipped} skipped")
        logger.info(f"Total operations: {total_ops} in {elapsed:.1f}s")
        logger.info("=" * 60)
        
        # Log sync completion
        log_sync_event_sync(
            "meeting_sync_complete", "success",
            f"Sync complete: {total_ops} operations in {elapsed:.1f}s"
        )
        
        return {
            'success': True,
            'notion_to_supabase': {'created': n2s_created, 'updated': n2s_updated, 'skipped': n2s_skipped},
            'supabase_to_notion': {'created': s2n_created, 'updated': s2n_updated, 'skipped': s2n_skipped},
            'total_operations': total_ops,
            'elapsed_seconds': elapsed
        }
        
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Sync failed after {elapsed:.1f}s: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        return {
            'success': False,
            'error': str(e),
            'elapsed_seconds': elapsed
        }


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Bidirectional Notion ↔ Supabase meeting sync')
    parser.add_argument('--full', action='store_true', help='Full sync (all meetings)')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back for incremental')
    parser.add_argument('--direction', choices=['both', 'notion-to-supabase', 'supabase-to-notion'], 
                       default='both', help='Sync direction')
    args = parser.parse_args()
    
    if not NOTION_API_TOKEN:
        logger.error("NOTION_API_TOKEN not set")
        return
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL or SUPABASE_KEY not set")
        return
    
    run_sync(full_sync=args.full, since_hours=args.hours)


if __name__ == '__main__':
    main()
