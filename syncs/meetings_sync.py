"""
===================================================================================
MEETINGS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion Meetings DB and Supabase meetings table
- CRM contact linking (People relation in Notion ↔ contact_id in Supabase)
- Content extraction handling "unsupported" blocks (AI meeting notes)
- Complex JSONB structures (topics_discussed, follow_up_items, key_points)
- Automatic deletion sync (both directions)
- Safety valves to prevent data loss

Notion Properties:
- Meeting (title): Meeting title
- Date (date): Meeting date
- Location (rich_text): Meeting location
- People (relation): Link to CRM database

Usage:
    python -m syncs.meetings_sync --full    # Full sync
    python -m syncs.meetings_sync           # Incremental (last 24h)
"""

import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
import httpx

# Load environment
load_dotenv()

from lib.sync_base import (
    TwoWaySyncService,
    NotionClient as BaseNotionClient,
    SupabaseClient as BaseSupabaseClient,
    NotionPropertyExtractor,
    NotionPropertyBuilder,
    ContentBlockBuilder,
    SyncResult,
    SyncStats,
    create_cli_parser,
    setup_logger,
    retry_on_error,
    NOTION_API_TOKEN,
    SUPABASE_URL,
    SUPABASE_KEY
)

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_MEETING_DB_ID = os.environ.get('NOTION_MEETING_DB_ID', '297cd3f1-eb28-810f-86f0-f142f7e3a5ca')
NOTION_CRM_DB_ID = os.environ.get('NOTION_CRM_DATABASE_ID', '2c7cd3f1eb2880269e53ed4d45e99b69')


# ============================================================================
# EXTENDED NOTION CLIENT WITH CRM SUPPORT
# ============================================================================

class MeetingsNotionClient(BaseNotionClient):
    """Extended Notion client with CRM contact lookup for meetings."""
    
    def __init__(self, token: str):
        super().__init__(token)
        self._crm_cache: Dict[str, Dict] = {}
        self._crm_by_email: Dict[str, str] = {}  # email -> notion_page_id
        self._crm_by_name: Dict[str, str] = {}  # lowercase name -> notion_page_id
    
    def cache_crm_contacts(self):
        """Pre-cache all CRM contacts for faster lookup."""
        contacts = self.query_database(NOTION_CRM_DB_ID)
        for page in contacts:
            page_id = page['id']
            props = page.get('properties', {})
            
            name = NotionPropertyExtractor.title(props, 'Name')
            email = props.get('Mail', {}).get('email')
            
            self._crm_cache[page_id] = {
                'notion_id': page_id,
                'name': name,
                'email': email
            }
            if email:
                self._crm_by_email[email.lower()] = page_id
            if name:
                self._crm_by_name[name.lower()] = page_id
    
    def get_crm_contact(self, page_id: str) -> Optional[Dict]:
        """Get CRM contact details by page ID."""
        if page_id in self._crm_cache:
            return self._crm_cache[page_id]
        
        try:
            page = self.get_page(page_id)
            props = page.get('properties', {})
            
            name = NotionPropertyExtractor.title(props, 'Name')
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
            if name:
                self._crm_by_name[name.lower()] = page_id
            return contact
            
        except Exception:
            return None
    
    def find_crm_by_name(self, name: str) -> Optional[str]:
        """Find CRM page ID by name."""
        if not name:
            return None
        
        # Check cache first
        if name.lower() in self._crm_by_name:
            return self._crm_by_name[name.lower()]
        
        # Search in Notion CRM
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
                page_name = NotionPropertyExtractor.title(page.get('properties', {}), 'Name')
                if page_name.lower() == name.lower():
                    self._crm_by_name[name.lower()] = page['id']
                    return page['id']
            
            # Return first if only one result
            if len(results) == 1:
                self._crm_by_name[name.lower()] = results[0]['id']
                return results[0]['id']
                
        except Exception:
            pass
        
        return None
    
    def extract_meeting_content(self, page_id: str, max_depth: int = 3) -> Tuple[str, bool]:
        """
        Extract readable text content from a meeting page.
        Handles "unsupported" blocks (AI meeting notes) gracefully.
        
        Returns:
            Tuple of (content_text, has_unsupported_blocks)
        """
        blocks = self.get_all_blocks(page_id)
        return self._extract_meeting_blocks_text(blocks, max_depth=max_depth)
    
    def _extract_meeting_blocks_text(self, blocks: List[Dict], depth: int = 0, max_depth: int = 3) -> Tuple[str, bool]:
        """Recursively extract text from blocks, handling unsupported types."""
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
                    child_text, _ = self._extract_meeting_blocks_text(children, depth + 1, max_depth)
                    if child_text:
                        text_parts.append(child_text)
                continue
            
            # Extract text from readable block types
            text = self._get_meeting_block_text(block)
            if text:
                prefix = self._get_meeting_block_prefix(block_type)
                text_parts.append(f"{indent}{prefix}{text}")
            
            # Recursively get children (for toggles, etc.)
            if has_children and depth < max_depth:
                children = self.get_block_children(block_id)
                child_text, child_unsupported = self._extract_meeting_blocks_text(children, depth + 1, max_depth)
                if child_text:
                    text_parts.append(child_text)
                if child_unsupported:
                    has_unsupported = True
        
        return '\n'.join(filter(None, text_parts)), has_unsupported
    
    def _get_meeting_block_text(self, block: Dict) -> Optional[str]:
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
    
    def _get_meeting_block_prefix(self, block_type: str) -> str:
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


# ============================================================================
# EXTENDED SUPABASE CLIENT WITH CONTACT LOOKUP
# ============================================================================

class MeetingsSupabaseClient(BaseSupabaseClient):
    """Extended Supabase client with contact lookup for meetings."""
    
    def __init__(self, url: str, key: str):
        super().__init__(url, key, "meetings")
        self._contact_cache: Dict[str, Dict] = {}  # id -> contact
    
    def cache_contacts(self):
        """Pre-cache all contacts for faster lookup."""
        response = self.client.get(
            f'{self.base_url}/contacts',
            params={
                'select': 'id,first_name,last_name,email,notion_page_id',
                'deleted_at': 'is.null'
            }
        )
        response.raise_for_status()
        for contact in response.json():
            self._contact_cache[contact['id']] = contact
    
    def get_contact(self, contact_id: str) -> Optional[Dict]:
        """Get contact by ID."""
        if contact_id in self._contact_cache:
            return self._contact_cache[contact_id]
        
        response = self.client.get(
            f'{self.base_url}/contacts',
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
                f'{self.base_url}/contacts',
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
        
        # Fallback to first name only
        response = self.client.get(
            f'{self.base_url}/contacts',
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


# ============================================================================
# MEETINGS SYNC SERVICE
# ============================================================================

class MeetingsSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Meetings between Notion and Supabase.
    
    Notion Properties:
    - Meeting (title): Meeting title
    - Date (date): Meeting date
    - Location (rich_text): Meeting location
    - People (relation): Link to CRM contacts
    - Content in page body blocks
    
    Supabase Fields:
    - title (text): Meeting title
    - date (timestamptz): Meeting date
    - location (text): Location
    - contact_id (uuid): Foreign key to contacts
    - contact_name (text): Denormalized contact name
    - summary (text): Meeting summary/notes
    - topics_discussed, follow_up_items, key_points (jsonb): Structured data
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """
    
    def __init__(self):
        # Use custom clients with CRM/contact support
        self.notion = MeetingsNotionClient(NOTION_API_TOKEN)
        self.supabase = MeetingsSupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        self.notion_database_id = NOTION_MEETING_DB_ID
        self.service_name = "MeetingsSync"
        self.logger = setup_logger("MeetingsSync")
        
        # Initialize parent class attributes
        from lib.sync_base import SyncDirection, SyncLogger
        self.sync_direction = SyncDirection.TWO_WAY
        self.sync_logger = SyncLogger(self.service_name)
    
    def get_source_id(self, source_record: Dict) -> str:
        return source_record.get('id', '')
    
    def check_safety_valve(self, source_count: int, dest_count: int, direction: str) -> Tuple[bool, str]:
        """Check if sync should proceed based on record counts."""
        from lib.sync_base import SAFETY_VALVE_THRESHOLD, SAFETY_VALVE_MIN_RECORDS
        
        if dest_count <= SAFETY_VALVE_MIN_RECORDS:
            return True, ""
        
        if source_count < dest_count * SAFETY_VALVE_THRESHOLD:
            return False, f"Safety Valve Triggered: Source has {source_count}, destination has {dest_count}. Aborting {direction}."
        
        return True, ""
    
    # Uses base class compare_timestamps with proper ISO parsing and 5-second buffer
    
    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion meeting to Supabase format.
        Notion → Supabase
        
        Note: Content and contact linking are handled in _sync_notion_to_supabase override.
        """
        props = notion_record.get('properties', {})
        
        # Extract title
        title = NotionPropertyExtractor.title(props, 'Meeting')
        if not title:
            title = 'Untitled'
        
        # Extract date
        date = NotionPropertyExtractor.date(props, 'Date')
        
        # Extract location
        location = NotionPropertyExtractor.rich_text(props, 'Location')
        
        return {
            'title': title,
            'date': date,
            'location': location if location else None,
            'source_file': 'notion-sync',
        }
    
    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase meeting to Notion properties format.
        Supabase → Notion
        
        Note: Content blocks and People relation are handled separately.
        """
        properties = {
            'Meeting': NotionPropertyBuilder.title(supabase_record.get('title', 'Untitled')[:100]),
        }
        
        if supabase_record.get('date'):
            # Just the date part
            date_str = supabase_record['date'][:10] if len(supabase_record['date']) > 10 else supabase_record['date']
            properties['Date'] = NotionPropertyBuilder.date(date_str)
        
        if supabase_record.get('location'):
            properties['Location'] = NotionPropertyBuilder.rich_text(supabase_record['location'][:200])
        
        return properties
    
    def _build_meeting_content_blocks(self, meeting: Dict) -> List[Dict]:
        """
        Build Notion content blocks from meeting data.

        Priority:
        1. Use 'sections' field (JSONB) if present - structured format [{heading, content}]
        2. Fallback to complex JSONB structures (topics_discussed, follow_up_items, key_points)
        3. Fallback to 'summary' field if nothing else

        Sections format provides clean structure and enables reliable appending.
        """
        blocks = []
        builder = ContentBlockBuilder()

        # PRIORITY: Use sections if present
        sections = meeting.get('sections', [])
        if sections:
            for section in sections:
                heading = section.get('heading', '')
                content = section.get('content', '')

                if heading:
                    blocks.append(builder.heading_2(heading))

                if content:
                    blocks.extend(builder.chunked_paragraphs(content))

            return blocks

        # FALLBACK: Use summary and structured fields
        # Summary
        if meeting.get('summary'):
            blocks.extend(builder.chunked_paragraphs(meeting['summary']))

        # Topics discussed - use NUMBERED list with NESTED bullet children for details
        topics = meeting.get('topics_discussed', [])
        if topics:
            blocks.append(builder.heading_3('Topics Discussed'))
            for topic in topics:
                if isinstance(topic, dict):
                    topic_text = topic.get('topic', '')
                    details = topic.get('details', [])
                    if topic_text:
                        # Create bullet children for details
                        detail_children = []
                        for detail in details[:7]:  # Max 7 details per topic
                            if detail:
                                detail_children.append(builder.bulleted_list_item(detail[:2000]))

                        # Create numbered item with nested bullet children
                        blocks.append(builder.numbered_list_item(
                            topic_text[:2000],
                            children=detail_children if detail_children else None
                        ))
                elif isinstance(topic, str) and topic:
                    blocks.append(builder.numbered_list_item(topic[:2000]))

        # Follow-up items
        follow_ups = meeting.get('follow_up_items', [])
        if follow_ups:
            blocks.append(builder.heading_3('Follow-up Items'))
            for item in follow_ups:
                if isinstance(item, dict):
                    topic = item.get('topic', item.get('item', ''))
                    context = item.get('context', '')
                    item_text = f"{topic}: {context}" if context else topic
                    if item_text:
                        blocks.append(builder.to_do(item_text[:2000]))
                elif isinstance(item, str) and item:
                    blocks.append(builder.to_do(item[:2000]))

        # Key points
        key_points = meeting.get('key_points', [])
        if key_points:
            blocks.append(builder.heading_3('Key Points'))
            for point in key_points:
                if isinstance(point, dict):
                    point_text = point.get('point', point.get('text', str(point)))
                else:
                    point_text = str(point) if point else ''

                if point_text:
                    blocks.append(builder.bulleted_list_item(point_text[:2000]))

        return blocks
    
    def _sync_notion_deletions(self) -> int:
        """Override to use meetings-specific logic."""
        deleted_count = 0
        
        all_records = self.supabase.select_all()
        linked_records = [r for r in all_records if r.get('notion_page_id') and not r.get('deleted_at')]
        
        if not linked_records:
            self.logger.info("No linked meetings to check for Notion deletions")
            return 0
        
        self.logger.info(f"Checking {len(linked_records)} linked meetings for Notion deletions...")
        
        try:
            all_notion_pages = self.notion.query_database(self.notion_database_id)
            notion_page_ids = {p['id'] for p in all_notion_pages}
            self.logger.info(f"Found {len(notion_page_ids)} pages in Notion Meetings database")
        except Exception as e:
            self.logger.error(f"Failed to query Notion database: {e}")
            return 0
        
        for record in linked_records:
            notion_page_id = record.get('notion_page_id')
            
            if notion_page_id not in notion_page_ids:
                record_id = record.get('id')
                title = record.get('title', 'Untitled')
                
                try:
                    self.supabase.soft_delete(record_id)
                    self.supabase.update(record_id, {
                        'notion_page_id': None,
                        'notion_updated_at': None
                    })
                    deleted_count += 1
                    self.logger.info(f"Soft-deleted meeting '{title}' (Notion page was deleted)")
                except Exception as e:
                    self.logger.error(f"Failed to soft-delete meeting {record_id}: {e}")
        
        if deleted_count > 0:
            self.logger.info(f"Soft-deleted {deleted_count} meetings (Notion pages were deleted)")
        else:
            self.logger.info("No Notion deletions detected")
        
        return deleted_count
    
    def _sync_notion_to_supabase(self, full_sync: bool, since_hours: int, metrics=None) -> SyncResult:
        """
        Override to include content extraction and CRM contact linking.
        Notion → Supabase with content and contacts
        """
        from lib.sync_base import SyncMetrics
        stats = SyncStats()
        start_time = __import__('time').time()
        
        try:
            # Cache contacts for faster lookup
            self.notion.cache_crm_contacts()
            self.supabase.cache_contacts()
            
            # Build filter
            filter_query = None
            if not full_sync:
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
                filter_query = {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"after": cutoff}
                }
            
            # Fetch from Notion
            notion_records = self.notion.query_database(self.notion_database_id, filter=filter_query)
            self.logger.info(f"Found {len(notion_records)} meetings in Notion")
            
            if metrics:
                metrics.notion_api_calls += 1
                metrics.source_total = len(notion_records)
                metrics.records_read += len(notion_records)
            
            # Get existing for comparison
            existing_map = {}
            for r in self.supabase.select_all():
                if r.get('notion_page_id'):
                    existing_map[r['notion_page_id']] = r
            
            if metrics:
                metrics.supabase_api_calls += 1
                metrics.destination_total = len(existing_map)
            
            # Safety valve
            is_safe, msg = self.check_safety_valve(len(notion_records), len(existing_map), "Notion → Supabase")
            if not is_safe and full_sync:
                self.logger.error(msg)
                return SyncResult(success=False, direction="notion_to_supabase", error_message=msg)
            
            # Process records
            for notion_record in notion_records:
                try:
                    notion_id = self.get_source_id(notion_record)
                    existing_record = existing_map.get(notion_id)
                    
                    # Skip if Supabase has local changes that need to sync TO Notion
                    if existing_record and existing_record.get('last_sync_source') == 'supabase':
                        self.logger.info(f"Skipping '{existing_record.get('title', 'Untitled')}' - has local Supabase changes pending sync to Notion")
                        stats.skipped += 1
                        continue
                    
                    # Compare timestamps if record exists
                    if existing_record:
                        comparison = self.compare_timestamps(
                            notion_record.get('last_edited_time'),
                            existing_record.get('notion_updated_at')
                        )
                        if comparison <= 0:
                            stats.skipped += 1
                            continue
                        stats.updated += 1
                    else:
                        stats.created += 1
                    
                    # Convert properties
                    data = self.convert_from_source(notion_record)
                    
                    # Handle CRM contact linking
                    props = notion_record.get('properties', {})
                    person_ids = props.get('People', {}).get('relation', [])
                    
                    if person_ids:
                        crm_contact = self.notion.get_crm_contact(person_ids[0].get('id'))
                        if crm_contact:
                            data['contact_name'] = crm_contact.get('name')
                            # Find matching Supabase contact
                            sb_contact = self.supabase.find_contact_by_name(crm_contact.get('name'))
                            if sb_contact:
                                data['contact_id'] = sb_contact.get('id')
                    
                    # Extract content from page blocks
                    try:
                        content, has_unsupported = self.notion.extract_meeting_content(notion_id)
                        data['summary'] = content[:2000] if content else None
                    except Exception as e:
                        self.logger.warning(f"Failed to extract content: {e}")

                    # Extract structured sections (heading_2 + content)
                    try:
                        sections = self.notion.extract_page_sections(notion_id)
                        if sections:
                            data['sections'] = sections
                            self.logger.info(f"Extracted {len(sections)} sections from meeting '{data.get('title', 'Untitled')}'")
                    except Exception as e:
                        self.logger.warning(f"Failed to extract sections: {e}")
                    
                    # Add sync metadata
                    data['notion_page_id'] = notion_id
                    data['notion_updated_at'] = notion_record.get('last_edited_time')
                    data['last_sync_source'] = 'notion'
                    data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    
                    self.supabase.upsert(data, conflict_column='notion_page_id')
                    
                except Exception as e:
                    self.logger.error(f"Error syncing meeting from Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="notion_to_supabase",
                stats=stats,
                elapsed_seconds=__import__('time').time() - start_time
            )
            
        except Exception as e:
            return SyncResult(success=False, direction="notion_to_supabase", error_message=str(e))
    
    def _sync_supabase_to_notion(self, full_sync: bool, since_hours: int, metrics=None) -> SyncResult:
        """
        Override to include content block creation and CRM linking.
        Supabase → Notion with content blocks and People relation
        """
        stats = SyncStats()
        start_time = __import__('time').time()
        
        try:
            # Cache contacts
            self.notion.cache_crm_contacts()
            
            # Get Supabase records that need syncing (active only, not soft-deleted)
            if full_sync:
                supabase_records = self.supabase.get_all_active()
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                all_records = self.supabase.select_updated_since(cutoff)
                # Filter out soft-deleted records
                supabase_records = [r for r in all_records if not r.get('deleted_at')]
            
            if metrics:
                metrics.supabase_api_calls += 1
            
            # Filter to records that need syncing to Notion
            # A record needs syncing if:
            # 1. It has no notion_page_id (new record), OR
            # 2. last_sync_source is 'supabase' (explicitly marked), OR
            # 3. updated_at > notion_updated_at (updated locally since last sync)
            records_to_sync = []
            for r in supabase_records:
                needs_sync = False
                
                if not r.get('notion_page_id'):
                    needs_sync = True
                elif r.get('last_sync_source') == 'supabase':
                    needs_sync = True
                else:
                    comparison = self.compare_timestamps(
                        r.get('updated_at'),
                        r.get('notion_updated_at')
                    )
                    if comparison > 0:
                        needs_sync = True
                        self.logger.info(f"Meeting '{r.get('title')}' has local changes")
                
                if needs_sync:
                    records_to_sync.append(r)
            
            self.logger.info(f"Found {len(records_to_sync)} meetings to sync to Notion")
            
            # Safety valve
            notion_records = self.notion.query_database(self.notion_database_id)
            if metrics:
                metrics.notion_api_calls += 1
            
            is_safe, msg = self.check_safety_valve(len(records_to_sync), len(notion_records), "Supabase → Notion")
            if not is_safe:
                self.logger.warning(msg)
            
            for record in records_to_sync:
                try:
                    notion_page_id = record.get('notion_page_id')
                    notion_props = self.convert_to_source(record)
                    
                    # Add People relation if we have a contact_name
                    if record.get('contact_name'):
                        crm_page_id = self.notion.find_crm_by_name(record['contact_name'])
                        if crm_page_id:
                            notion_props['People'] = {'relation': [{'id': crm_page_id}]}
                    
                    if notion_page_id:
                        # Update existing page
                        updated_page = self.notion.update_page(notion_page_id, notion_props)
                        if metrics:
                            metrics.notion_api_calls += 1
                        
                        # Note: We don't update content blocks for existing pages to avoid
                        # overwriting AI meeting notes or other manual edits
                        
                        # Update Supabase with new Notion timestamp to prevent re-sync loops
                        # This is CRITICAL: without this, `last_sync_source` stays 'supabase'
                        # and future Notion edits would be skipped!
                        self.supabase.update(record['id'], {
                            'notion_updated_at': updated_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        
                        stats.updated += 1
                    else:
                        # Create new page with content
                        blocks = self._build_meeting_content_blocks(record)
                        new_page = self.notion.create_page(
                            self.notion_database_id, 
                            notion_props, 
                            children=blocks
                        )
                        if metrics:
                            metrics.notion_api_calls += 1
                        
                        # Update Supabase with new Notion ID
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page['id'],
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        stats.created += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing meeting to Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=__import__('time').time() - start_time
            )
            
        except Exception as e:
            return SyncResult(success=False, direction="supabase_to_notion", error_message=str(e))
    
    def sync(self, full_sync: bool = False, since_hours: int = 24) -> SyncResult:
        """Override to use meetings-specific sync logic with metrics."""
        import time as time_module
        from lib.sync_base import SyncMetrics
        
        metrics = SyncMetrics()
        start_time = time_module.time()
        
        # Step 0a: Sync Notion deletions → Supabase (soft-delete)
        self.logger.info("Phase 0a: Sync Notion Deletions → Supabase")
        phase_start = time_module.time()
        notion_deletions = self._sync_notion_deletions()
        metrics.notion_deletions_duration = time_module.time() - phase_start
        
        # Step 0b: Sync Supabase deletions → Notion (archive)
        self.logger.info("Phase 0b: Sync Supabase Deletions → Notion")
        phase_start = time_module.time()
        supabase_deletions = self._sync_supabase_deletions()
        metrics.supabase_deletions_duration = time_module.time() - phase_start
        
        # Step 1: Notion → Supabase
        self.logger.info("Phase 1: Notion → Supabase")
        phase_start = time_module.time()
        result1 = self._sync_notion_to_supabase(full_sync, since_hours, metrics)
        metrics.notion_to_supabase_duration = time_module.time() - phase_start
        
        # Step 2: Supabase → Notion  
        self.logger.info("Phase 2: Supabase → Notion")
        phase_start = time_module.time()
        result2 = self._sync_supabase_to_notion(full_sync, since_hours, metrics)
        metrics.supabase_to_notion_duration = time_module.time() - phase_start
        
        # Combine results
        combined_stats = SyncStats(
            created=result1.stats.created + result2.stats.created,
            updated=result1.stats.updated + result2.stats.updated,
            deleted=result1.stats.deleted + result2.stats.deleted + notion_deletions + supabase_deletions,
            skipped=result1.stats.skipped + result2.stats.skipped,
            errors=result1.stats.errors + result2.stats.errors
        )
        
        # Finalize metrics
        metrics.finish()
        metrics.records_written = combined_stats.created + combined_stats.updated + combined_stats.deleted
        
        elapsed = time_module.time() - start_time
        result = SyncResult(
            success=result1.success and result2.success,
            direction="bidirectional",
            stats=combined_stats,
            metrics=metrics,
            elapsed_seconds=elapsed
        )
        
        self.sync_logger.log_complete(result)
        return result


# ============================================================================
# ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run the meetings sync and return results."""
    service = MeetingsSyncService()
    result = service.sync(full_sync=full_sync, since_hours=since_hours)
    
    return {
        'success': result.success,
        'direction': result.direction,
        'created': result.stats.created,
        'updated': result.stats.updated,
        'deleted': result.stats.deleted,
        'skipped': result.stats.skipped,
        'errors': result.stats.errors,
        'elapsed_seconds': result.elapsed_seconds
    }


if __name__ == "__main__":
    parser = create_cli_parser("Meetings")
    args = parser.parse_args()
    
    result = run_sync(full_sync=args.full, since_hours=args.hours)
    print(f"\nResult: {result}")
