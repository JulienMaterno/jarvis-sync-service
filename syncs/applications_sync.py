"""
===================================================================================
APPLICATIONS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion Applications DB and Supabase applications table
- Full page content extraction (questions, answers, notes)
- Application type and status tracking
- Deadline and grant amount fields

Notion Database: Applications (bfb77dff-9721-47b6-9bab-0cd0b315a298)

Notion Properties:
- Name (title): Application name (e.g., "Antler", "EF", "Cosmos Grant")
- Type (select): Grant, Fellowship, Program, Accelerator, Residency
- Status (select): Not Started, Researching, In Progress, Applied, Accepted
- Institution (rich_text): Organization name
- Website (url): Application URL
- Grant Amount (rich_text): Amount range or specific value
- Deadline (date): Application deadline
- Context (rich_text): Brief context about the application
- Notes (rich_text): Additional notes
- [Page content]: Questions, answers, and detailed notes

Usage:
    python -m syncs.applications_sync --full    # Full sync
    python -m syncs.applications_sync           # Incremental (last 24h)
    python -m syncs.applications_sync --schema  # Show database schema
"""

import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment
load_dotenv()

from lib.sync_base import (
    TwoWaySyncService,
    NotionPropertyExtractor,
    NotionPropertyBuilder,
    ContentBlockBuilder,
    SyncResult,
    SyncStats,
    SyncMetrics,
    create_cli_parser,
    setup_logger,
    format_exception
)

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_APPLICATIONS_DB_ID = os.environ.get(
    'NOTION_APPLICATIONS_DB_ID', 
    'bfb77dff-9721-47b6-9bab-0cd0b315a298'
)

# Valid application types (Notion will auto-create new select options if needed)
APPLICATION_TYPES = [
    'Grant', 'Fellowship', 'Program', 'Accelerator', 'Residency',
    'Prize/Fellowship',  # Composite types
    'Competition', 'Award', 'Prize'  # Additional types
]

# Valid statuses (Notion will auto-create new select options if needed)
APPLICATION_STATUSES = [
    'TO_APPLY', 'RESEARCHING', 'INACTIVE', 'NOT_ELIGIBLE', 'OTHER',  # New standardized statuses
    'Not Started', 'Researching', 'In Progress', 'Applied', 'Accepted',  # Legacy
    'Deadline Likely Passed', 'Deadline Passed', 'Rejected', 'Withdrawn',  # Terminal states
    'Interview', 'Shortlisted', 'Waitlisted'  # Intermediate states
]

# Common locations (for reference - not enforced, any value accepted)
APPLICATION_LOCATIONS = ['Remote', 'Singapore', 'USA', 'Europe', 'UK', 'Global', 'Asia', 'Other']


# ============================================================================
# APPLICATIONS SYNC SERVICE
# ============================================================================

class ApplicationsSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Applications between Notion and Supabase.
    
    Notion Properties:
    - Name (title): Application name
    - Type (select): Type of application
    - Status (select): Current status
    - Institution (rich_text): Organization
    - Website (url): Link
    - Grant Amount (rich_text): Amount/range
    - Deadline (date): Due date
    - Context (rich_text): Brief context
    - Notes (rich_text): Additional notes
    - Page content: Full content (questions/answers)
    
    Supabase Fields:
    - name (text): Application name
    - application_type (text): Type
    - status (text): Status
    - institution (text): Organization
    - website (text): URL
    - grant_amount (text): Amount
    - deadline (date): Deadline
    - context (text): Context
    - notes (text): Notes
    - content (text): Full page content
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """
    
    def __init__(self):
        super().__init__(
            service_name="ApplicationsSync",
            notion_database_id=NOTION_APPLICATIONS_DB_ID,
            supabase_table="applications"
        )
        self.logger = setup_logger("ApplicationsSync")
    
    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion application to Supabase format.
        Notion → Supabase
        """
        props = notion_record.get('properties', {})
        
        # Extract all properties
        name = NotionPropertyExtractor.title(props, 'Name')
        if not name:
            name = 'Untitled Application'
        
        result = {
            'name': name,
            'application_type': NotionPropertyExtractor.select(props, 'Type'),
            'status': NotionPropertyExtractor.select(props, 'Status') or 'Not Started',
            'institution': NotionPropertyExtractor.rich_text(props, 'Institution'),
            'website': NotionPropertyExtractor.url(props, 'Website'),
            'grant_amount': NotionPropertyExtractor.rich_text(props, 'Grant Amount'),
            'deadline': NotionPropertyExtractor.date(props, 'Deadline'),
            'location': NotionPropertyExtractor.select(props, 'Location'),  # New field
            'context': NotionPropertyExtractor.rich_text(props, 'Context'),
            'notes': NotionPropertyExtractor.rich_text(props, 'Notes'),
        }
        
        return result
    
    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase application to Notion properties format.
        Supabase → Notion
        """
        name = supabase_record.get('name', 'Untitled Application')
        
        properties = {
            'Name': NotionPropertyBuilder.title(name[:100]),  # Notion title limit
        }
        
        # Type (select) - accept ANY value, Notion auto-creates options
        app_type = supabase_record.get('application_type')
        if app_type:
            properties['Type'] = NotionPropertyBuilder.select(app_type)
        
        # Status (select) - accept ANY value, Notion auto-creates options
        status = supabase_record.get('status')
        if status:
            properties['Status'] = NotionPropertyBuilder.select(status)
        
        # Institution (rich_text)
        institution = supabase_record.get('institution')
        if institution:
            properties['Institution'] = NotionPropertyBuilder.rich_text(institution)
        
        # Website (url)
        website = supabase_record.get('website')
        if website:
            properties['Website'] = NotionPropertyBuilder.url(website)
        
        # Grant Amount (rich_text)
        grant_amount = supabase_record.get('grant_amount')
        if grant_amount:
            properties['Grant Amount'] = NotionPropertyBuilder.rich_text(grant_amount)
        
        # Deadline (date)
        deadline = supabase_record.get('deadline')
        if deadline:
            properties['Deadline'] = NotionPropertyBuilder.date(deadline)
        
        # Location (select) - any value, Notion handles select options
        location = supabase_record.get('location')
        if location:
            properties['Location'] = NotionPropertyBuilder.select(location)
        
        # Context (rich_text)
        context = supabase_record.get('context')
        if context:
            properties['Context'] = NotionPropertyBuilder.rich_text(context)
        
        # Notes (rich_text)
        notes = supabase_record.get('notes')
        if notes:
            properties['Notes'] = NotionPropertyBuilder.rich_text(notes)

        return properties

    def _build_content_blocks(self, application: Dict) -> List[Dict]:
        """
        Build Notion content blocks from application data.

        Priority:
        1. Use 'sections' field (JSONB) if present - structured format
        2. Fallback to 'content' field (markdown) if no sections

        Sections format: [{heading: str, content: str}]
        This provides clean structure and enables reliable appending.
        """
        blocks = []
        builder = ContentBlockBuilder()

        # PRIORITY: Use sections if present
        sections = application.get('sections', [])
        if sections:
            for section in sections:
                heading = section.get('heading', '')
                content = section.get('content', '')

                if heading:
                    blocks.append(builder.heading_2(heading))

                if content:
                    blocks.extend(builder.chunked_paragraphs(content))

            return blocks

        # FALLBACK: Parse markdown content field
        content = application.get('content')
        if not content:
            return []

        import re
        lines = content.split('\n')
        current_paragraph = []

        def flush_paragraph():
            """Helper to flush accumulated paragraph lines."""
            if current_paragraph:
                para_text = '\n'.join(current_paragraph)
                blocks.extend(builder.chunked_paragraphs(para_text))
                current_paragraph.clear()

        for line in lines:
            stripped = line.strip()

            # Empty line - flush paragraph
            if not stripped:
                flush_paragraph()
                continue

            # Headings: # ## ###
            if stripped.startswith('# '):
                flush_paragraph()
                blocks.append(builder.heading_1(stripped[2:]))
                continue
            if stripped.startswith('## '):
                flush_paragraph()
                blocks.append(builder.heading_2(stripped[3:]))
                continue
            if stripped.startswith('### '):
                flush_paragraph()
                blocks.append(builder.heading_3(stripped[4:]))
                continue

            # Bullet points: - or *
            if stripped.startswith('- ') or stripped.startswith('* '):
                flush_paragraph()
                blocks.append(builder.bulleted_list_item(stripped[2:]))
                continue

            # Numbered lists: 1. 2. 3. etc
            match = re.match(r'^(\d+)\.\s+(.+)$', stripped)
            if match:
                flush_paragraph()
                blocks.append(builder.numbered_list_item(match.group(2)))
                continue

            # Regular line - accumulate into paragraph
            current_paragraph.append(stripped)

        # Flush remaining paragraph
        flush_paragraph()

        return blocks

    def _sync_notion_to_supabase(self, full_sync: bool, since_hours: int, metrics=None) -> SyncResult:
        """
        Override to include content extraction.
        Notion → Supabase with page content
        """
        stats = SyncStats()
        start_time = __import__('time').time()
        
        try:
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
            self.logger.info(f"Found {len(notion_records)} applications in Notion")
            
            if metrics:
                metrics.notion_api_calls += 1
                metrics.source_total = len(notion_records)
            
            # Get existing by notion_page_id
            existing_by_notion_id = {}
            for r in self.supabase.select_all():
                if r.get('notion_page_id'):
                    existing_by_notion_id[r['notion_page_id']] = r
            
            if metrics:
                metrics.supabase_api_calls += 1
            
            # Safety valve
            is_safe, msg = self.check_safety_valve(len(notion_records), len(existing_by_notion_id), "Notion → Supabase")
            if not is_safe and full_sync:
                self.logger.error(msg)
                return SyncResult(success=False, direction="notion_to_supabase", error_message=msg)
            
            # Process records
            for notion_record in notion_records:
                try:
                    notion_id = self.get_source_id(notion_record)
                    data = self.convert_from_source(notion_record)
                    
                    if data is None:
                        stats.skipped += 1
                        continue
                    
                    existing_record = existing_by_notion_id.get(notion_id)
                    
                    # Skip if Supabase has local changes pending sync to Notion
                    if existing_record and existing_record.get('last_sync_source') == 'supabase':
                        self.logger.info(f"Skipping application '{data.get('name')}' - has local changes pending")
                        stats.skipped += 1
                        continue
                    
                    # Compare timestamps
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
                    
                    # Extract page content (returns tuple: text, has_unsupported)
                    try:
                        content_text, has_unsupported = self.notion.extract_page_content(notion_id)
                        data['content'] = content_text
                        if has_unsupported:
                            self.logger.info(f"Application '{data.get('name')}' has unsupported Notion blocks")
                    except Exception as e:
                        self.logger.warning(f"Failed to extract content: {e}")
                        data['content'] = ''
                    
                    # Add sync metadata
                    data['notion_page_id'] = notion_id
                    data['notion_updated_at'] = notion_record.get('last_edited_time')
                    data['last_sync_source'] = 'notion'
                    data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    
                    # Upsert
                    if existing_record:
                        self.supabase.update(existing_record['id'], data)
                    else:
                        self.supabase.insert(data)
                    
                except Exception as e:
                    self.logger.error(f"Error syncing application from Notion: {e}")
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
        Override to handle content block creation.
        Supabase → Notion with page content (Q&A)
        """
        stats = SyncStats()
        start_time = __import__('time').time()
        if metrics is None:
            metrics = SyncMetrics()

        try:
            # Get Supabase records that need syncing
            if full_sync:
                supabase_records = self.supabase.get_all_active()
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                all_records = self.supabase.select_updated_since(cutoff)
                supabase_records = [r for r in all_records if not r.get('deleted_at')]

            metrics.supabase_api_calls += 1

            # Filter to records that need syncing to Notion
            records_to_sync = self.filter_records_needing_notion_sync(supabase_records, name_field='name')
            self.logger.info(f"Found {len(records_to_sync)} applications to sync to Notion")

            for record in records_to_sync:
                try:
                    notion_page_id = record.get('notion_page_id')
                    notion_props = self.convert_to_source(record)
                    blocks = self._build_content_blocks(record)

                    if notion_page_id:
                        # Update existing page properties
                        updated_page = self.notion.update_page(notion_page_id, notion_props)
                        metrics.notion_api_calls += 1

                        # Update content blocks if we have content (SAFE PATTERN)
                        if blocks:
                            try:
                                # Batch blocks to avoid Notion API limits
                                MAX_BLOCKS_PER_REQUEST = 100
                                block_batches = [blocks[i:i + MAX_BLOCKS_PER_REQUEST]
                                               for i in range(0, len(blocks), MAX_BLOCKS_PER_REQUEST)]

                                # SAFETY: Try to add first batch BEFORE deleting anything
                                first_batch_success = False
                                try:
                                    self.notion.append_blocks(notion_page_id, block_batches[0])
                                    first_batch_success = True
                                    metrics.notion_api_calls += 1
                                except Exception as e:
                                    self.logger.error(f"Failed to add content blocks (skipping delete to preserve data): {e}")
                                    raise  # Don't delete if we can't add

                                # Only delete existing blocks AFTER we confirmed new content works
                                if first_batch_success:
                                    existing_blocks = self.notion.get_all_blocks(notion_page_id)
                                    # Skip the blocks we just added (they're at the end)
                                    blocks_to_delete = [b for b in existing_blocks
                                                       if b.get('id') not in [nb.get('id') for nb in block_batches[0]]]
                                    for block in blocks_to_delete:
                                        try:
                                            self.notion.delete_block(block['id'])
                                        except:
                                            pass
                                    metrics.notion_api_calls += 1

                                    # Add remaining batches
                                    for batch in block_batches[1:]:
                                        try:
                                            self.notion.append_blocks(notion_page_id, batch)
                                            metrics.notion_api_calls += 1
                                        except Exception as e:
                                            self.logger.warning(f"Failed to add batch of {len(batch)} blocks: {e}")

                            except Exception as e:
                                self.logger.warning(f"Failed to update content blocks: {e}")

                        # Update Supabase with new timestamp
                        self.supabase.update(record['id'], {
                            'notion_updated_at': updated_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        metrics.supabase_api_calls += 1
                        stats.updated += 1
                    else:
                        # Create new page with content blocks
                        new_page = self.notion.create_page(
                            self.notion_database_id,
                            notion_props,
                            children=blocks
                        )
                        metrics.notion_api_calls += 1

                        # Update Supabase with new Notion ID
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page['id'],
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        metrics.supabase_api_calls += 1
                        stats.created += 1

                except Exception as e:
                    self.logger.error(f"Error syncing application '{record.get('name')}' to Notion: {format_exception(e)}")
                    self.logger.error(f"  Properties sent: {notion_props}")
                    stats.errors += 1

            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=__import__('time').time() - start_time
            )

        except Exception as e:
            return SyncResult(success=False, direction="supabase_to_notion", error_message=format_exception(e))


# ============================================================================
# ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run the applications sync."""
    service = ApplicationsSyncService()
    result = service.sync(full_sync=full_sync, since_hours=since_hours)
    return result.to_dict()


if __name__ == '__main__':
    parser = create_cli_parser("ApplicationsSync")
    args = parser.parse_args()
    
    if args.schema:
        service = ApplicationsSyncService()
        schema = service.notion.get_database_schema(NOTION_APPLICATIONS_DB_ID)
        print(f"\nNotion Database Schema:")
        print(f"ID: {NOTION_APPLICATIONS_DB_ID}")
        for name, prop in schema.items():
            print(f"  {name}: {prop.get('type')}")
    else:
        result = run_sync(full_sync=args.full, since_hours=args.hours)
        print(f"\nSync Result: {result}")
