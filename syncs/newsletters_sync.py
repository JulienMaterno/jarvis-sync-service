"""
===================================================================================
NEWSLETTERS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion Newsletters DB and Supabase newsletters table
- Content extraction and block creation
- Status tracking (draft, ready, sent)
- Simple newsletter workflow

Notion Properties:
- Name (title): Newsletter name (e.g., "Exploring Out Loud #6")
- Status (select): draft, ready, sent
- Date (date): Send/publish date
- Content in page body blocks

Usage:
    python -m syncs.newsletters_sync --full    # Full sync
    python -m syncs.newsletters_sync           # Incremental (last 24h)
"""

import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment
load_dotenv()

from lib.sync_base import (
    TwoWaySyncService,
    NotionClient,
    SupabaseClient,
    NotionPropertyExtractor,
    NotionPropertyBuilder,
    ContentBlockBuilder,
    SyncResult,
    SyncStats,
    SyncMetrics,
    create_cli_parser,
    setup_logger,
    NOTION_API_TOKEN,
    SUPABASE_URL,
    SUPABASE_KEY,
    MAX_BLOCKS_PER_REQUEST
)

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_NEWSLETTERS_DB_ID = os.environ.get('NOTION_NEWSLETTERS_DB_ID', '')

if not NOTION_NEWSLETTERS_DB_ID:
    raise ValueError("NOTION_NEWSLETTERS_DB_ID environment variable is required")


# ============================================================================
# NEWSLETTERS SYNC SERVICE
# ============================================================================

class NewslettersSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Newsletters between Notion and Supabase.

    Notion Properties:
    - Name (title): Newsletter name
    - Status (select): draft, ready, sent
    - Date (date): Send/publish date
    - Content in page body blocks

    Supabase Fields:
    - name (text): Newsletter name
    - status (text): draft, ready, sent
    - date (date): Send/publish date
    - content (text): Full text content
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """

    def __init__(self):
        super().__init__(
            service_name="NewslettersSync",
            notion_database_id=NOTION_NEWSLETTERS_DB_ID,
            supabase_table="newsletters"
        )
        self.logger = setup_logger("NewslettersSync")

    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion newsletter to Supabase format.
        Notion → Supabase
        """
        props = notion_record.get('properties', {})

        # Extract name
        name = NotionPropertyExtractor.title(props, 'Name')

        # Extract status
        status_raw = NotionPropertyExtractor.select(props, 'Status')
        status = status_raw.lower() if status_raw else 'draft'

        # Validate status
        if status not in ['draft', 'ready', 'sent']:
            status = 'draft'

        # Extract date
        date = NotionPropertyExtractor.date(props, 'Date')

        return {
            'name': name,
            'status': status,
            'date': date,
        }

    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase newsletter to Notion properties format.
        Supabase → Notion
        """
        properties = {}

        # Name
        if supabase_record.get('name'):
            properties['Name'] = NotionPropertyBuilder.title(supabase_record['name'])

        # Status
        if supabase_record.get('status'):
            # Capitalize first letter for Notion select
            status = supabase_record['status']
            status_display = status.capitalize() if status in ['draft', 'ready', 'sent'] else 'Draft'
            properties['Status'] = NotionPropertyBuilder.select(status_display)

        # Date
        if supabase_record.get('date'):
            properties['Date'] = NotionPropertyBuilder.date(supabase_record['date'])

        return properties

    def _build_content_blocks(self, newsletter: Dict) -> List[Dict]:
        """
        Build Notion content blocks from newsletter data.
        """
        blocks = []
        builder = ContentBlockBuilder()

        # Add content as paragraphs
        content = newsletter.get('content', '')
        if content:
            # Split by double newlines for paragraphs
            paragraphs = content.split('\n\n')
            for para in paragraphs:
                if para.strip():
                    # Check if it's a markdown heading
                    if para.strip().startswith('## '):
                        heading_text = para.strip()[3:].strip()
                        blocks.append(builder.heading_2(heading_text))
                    elif para.strip().startswith('### '):
                        heading_text = para.strip()[4:].strip()
                        blocks.append(builder.heading_3(heading_text))
                    else:
                        # Regular paragraph
                        blocks.append(builder.paragraph(para.strip()))

        return blocks

    def _sync_supabase_to_notion(self, full_sync: bool, since_hours: int, metrics=None) -> SyncResult:
        """
        Override to include content block creation.
        Supabase → Notion with content
        """
        from datetime import datetime, timezone, timedelta
        import time as time_module

        stats = SyncStats()
        start_time = time_module.time()

        try:
            # Get Supabase records to sync
            if full_sync:
                supabase_records = self.supabase.get_all_active()
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                supabase_records = self.supabase.get_updated_since(cutoff)

            self.logger.info(f"Found {len(supabase_records)} records to sync to Notion")

            if metrics:
                metrics.source_total = len(supabase_records)
                metrics.records_read += len(supabase_records)

            # Build lookup for existing Notion pages
            notion_records = self.notion.query_database(self.notion_database_id)
            if metrics:
                metrics.notion_api_calls += 1

            notion_lookup = {r['id']: r for r in notion_records}

            for record in supabase_records:
                try:
                    # Convert to Notion properties format
                    properties = self.convert_to_source(record)

                    notion_page_id = record.get('notion_page_id')

                    # Determine if create or update
                    if notion_page_id and notion_page_id in notion_lookup:
                        # Update existing page
                        self.notion.update_page(notion_page_id, properties)
                        if metrics:
                            metrics.notion_api_calls += 1

                        # Update content blocks
                        content_blocks = self._build_content_blocks(record)
                        if content_blocks:
                            self.notion.replace_page_content(notion_page_id, content_blocks)
                            if metrics:
                                metrics.notion_api_calls += 1

                        stats.updated += 1
                        self.logger.info(f"Updated Notion page: {record.get('name', record.get('id'))}")
                    else:
                        # Create new page
                        content_blocks = self._build_content_blocks(record)
                        new_page = self.notion.create_page(
                            database_id=self.notion_database_id,
                            properties=properties,
                            children=content_blocks
                        )
                        if metrics:
                            metrics.notion_api_calls += 1

                        new_page_id = new_page['id']

                        # Update Supabase with notion_page_id
                        self.supabase.update(
                            record_id=record['id'],
                            data={
                                'notion_page_id': new_page_id,
                                'last_sync_source': 'notion',
                                'updated_at': datetime.now(timezone.utc).isoformat()
                            }
                        )
                        if metrics:
                            metrics.target_writes += 1

                        stats.created += 1
                        self.logger.info(f"Created Notion page: {record.get('name', record.get('id'))}")

                except Exception as e:
                    self.logger.error(f"Error syncing to Notion: {e}")
                    stats.errors += 1

            elapsed = time_module.time() - start_time
            return SyncResult(
                success=stats.errors == 0,
                direction='supabase_to_notion',
                stats=stats,
                elapsed_seconds=elapsed
            )

        except Exception as e:
            self.logger.error(f"Supabase → Notion sync failed: {e}")
            elapsed = time_module.time() - start_time
            return SyncResult(
                success=False,
                direction='supabase_to_notion',
                stats=stats,
                elapsed_seconds=elapsed,
                error_message=str(e)
            )


# ============================================================================
# CLI ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict[str, Any]:
    """
    Main entry point for newsletters sync.

    Args:
        full_sync: If True, sync all records. If False, only recent changes.
        since_hours: For incremental sync, how many hours to look back.

    Returns:
        Dict with sync results
    """
    service = NewslettersSyncService()
    result = service.sync(full_sync=full_sync, since_hours=since_hours)
    return result.to_dict()


if __name__ == '__main__':
    parser = create_cli_parser("Newsletters bidirectional sync")
    args = parser.parse_args()

    result = run_sync(full_sync=args.full, since_hours=args.hours)

    # Print results
    print(f"\nResult: {result}")
