"""
===================================================================================
DOCUMENTS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion Documents DB and Supabase documents table
- Content extraction from page body
- Type categorization (cv, application, legal, etc.)
- Tag support
- Safety valves to prevent data loss

Notion Properties (expected):
- Name (title): Document title
- Type (select): cv, application, legal, reference, etc.
- Tags (multi_select): Tag labels
- File URL (url): Optional link to original file
- Content in page body blocks

Supabase Fields:
- title (text): Document title
- type (text): Document type
- content (text): Full page content
- content_hash (text): Hash for change detection
- filename (text): Original filename
- file_url (text): URL to source file
- metadata (jsonb): Additional structured data
- tags (text[]): Tags
- word_count, char_count: Content metrics
- notion_page_id, notion_updated_at, last_sync_source (sync tracking)

Usage:
    python -m syncs.documents_sync --full    # Full sync
    python -m syncs.documents_sync           # Incremental (last 24h)
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
    SUPABASE_KEY
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# You need to add NOTION_DOCUMENTS_DB_ID to .env file
NOTION_DOCUMENTS_DB_ID = os.environ.get('NOTION_DOCUMENTS_DB_ID', '')

# Document types (must match Notion select options)
DOCUMENT_TYPES = [
    "cv", "application", "legal", "reference", "template", 
    "proposal", "contract", "guide", "notes", "other"
]


# ============================================================================
# DOCUMENTS SYNC SERVICE
# ============================================================================

class DocumentsSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Documents between Notion and Supabase.
    
    Notion Properties:
    - Name (title): Document title
    - Type (select): Document type (cv, application, legal, etc.)
    - Tags (multi_select): Tag labels
    - File URL (url): Optional link to source file
    - Content in page body blocks
    
    Supabase Fields:
    - title (text): Document title
    - type (text): Document type
    - content (text): Full page content
    - content_hash (text): Hash for change detection
    - filename (text): Original filename if uploaded
    - file_url (text): URL to source file
    - metadata (jsonb): Additional data
    - tags (text[]): Tags
    - word_count, char_count: Content metrics
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """
    
    def __init__(self):
        if not NOTION_DOCUMENTS_DB_ID:
            raise ValueError("NOTION_DOCUMENTS_DB_ID not set! Add it to .env file")
        
        super().__init__(
            service_name="DocumentsSync",
            notion_database_id=NOTION_DOCUMENTS_DB_ID,
            supabase_table="documents"
        )
        self.logger = setup_logger("DocumentsSync")
    
    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion document to Supabase format.
        Notion → Supabase
        """
        props = notion_record.get('properties', {})
        
        # Extract title
        title = NotionPropertyExtractor.title(props, 'Name')
        if not title:
            title = 'Untitled Document'
        
        # Extract type (select)
        doc_type = NotionPropertyExtractor.select(props, 'Type')
        
        # Extract tags (multi_select)
        tags = NotionPropertyExtractor.multi_select(props, 'Tags')
        
        # Extract file URL if present
        file_url = NotionPropertyExtractor.url(props, 'File URL')
        
        return {
            'title': title,
            'type': doc_type,
            'tags': tags if tags else None,
            'file_url': file_url,
        }
    
    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase document to Notion properties format.
        Supabase → Notion
        """
        properties = {}
        
        # Title (required)
        title = supabase_record.get('title', 'Untitled Document')
        properties['Name'] = NotionPropertyBuilder.title(title[:100])  # Notion title limit
        
        # Type (select)
        doc_type = supabase_record.get('type')
        if doc_type:
            properties['Type'] = NotionPropertyBuilder.select(doc_type)
        
        # Tags (multi_select)
        tags = supabase_record.get('tags')
        if tags:
            properties['Tags'] = NotionPropertyBuilder.multi_select(tags)
        
        # File URL
        file_url = supabase_record.get('file_url')
        if file_url:
            properties['File URL'] = NotionPropertyBuilder.url(file_url)
        
        return properties
    
    def _build_content_blocks(self, document: Dict) -> List[Dict]:
        """
        Build Notion content blocks from document content.
        """
        blocks = []
        builder = ContentBlockBuilder()
        
        content = document.get('content', '')
        if content:
            # Split content into manageable blocks
            blocks.extend(builder.chunked_paragraphs(content))
        
        return blocks
    
    def _calculate_content_metrics(self, content: str) -> Dict[str, int]:
        """Calculate word and character counts."""
        if not content:
            return {'word_count': 0, 'char_count': 0}
        
        return {
            'word_count': len(content.split()),
            'char_count': len(content)
        }
    
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
            self.logger.info(f"Found {len(notion_records)} documents in Notion")
            
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
                        self.logger.info(f"Skipping '{data.get('title')}' - has local changes pending")
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
                    
                    # Extract page content
                    try:
                        content_text, has_unsupported = self.notion.extract_page_content(notion_id)
                        data['content'] = content_text
                        
                        # Calculate metrics
                        content_metrics = self._calculate_content_metrics(content_text)
                        data['word_count'] = content_metrics['word_count']
                        data['char_count'] = content_metrics['char_count']
                        
                        if has_unsupported:
                            self.logger.info(f"Document '{data.get('title')}' has unsupported Notion blocks")
                    except Exception as e:
                        self.logger.warning(f"Failed to extract content: {e}")
                        data['content'] = ''
                        data['word_count'] = 0
                        data['char_count'] = 0
                    
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
                    
                    if metrics:
                        metrics.supabase_api_calls += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing document from Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="notion_to_supabase",
                stats=stats,
                elapsed_seconds=__import__('time').time() - start_time
            )
            
        except Exception as e:
            self.logger.error(f"Notion→Supabase sync failed: {e}")
            return SyncResult(success=False, direction="notion_to_supabase", error_message=str(e))
    
    def _sync_supabase_to_notion(self, full_sync: bool, since_hours: int, metrics=None) -> SyncResult:
        """
        Override to handle content block creation.
        Supabase → Notion with page content
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
            records_to_sync = self.filter_records_needing_notion_sync(supabase_records, name_field='title')
            self.logger.info(f"Found {len(records_to_sync)} documents to sync to Notion")
            
            for record in records_to_sync:
                try:
                    notion_page_id = record.get('notion_page_id')
                    notion_props = self.convert_to_source(record)
                    blocks = self._build_content_blocks(record)
                    
                    if notion_page_id:
                        # Update existing page properties
                        self.notion.update_page(notion_page_id, notion_props)
                        metrics.notion_api_calls += 1
                        
                        # Update page content (clear and rebuild)
                        if blocks:
                            self.notion.clear_page_content(notion_page_id)
                            self.notion.append_blocks(notion_page_id, blocks)
                            metrics.notion_api_calls += 2
                        
                        stats.updated += 1
                    else:
                        # Create new page
                        new_page = self.notion.create_page(
                            database_id=self.notion_database_id,
                            properties=notion_props,
                            children=blocks
                        )
                        metrics.notion_api_calls += 1
                        
                        # Store the Notion page ID back
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page['id'],
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'supabase'
                        })
                        metrics.supabase_api_calls += 1
                        
                        stats.created += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing document to Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=__import__('time').time() - start_time
            )
            
        except Exception as e:
            self.logger.error(f"Supabase→Notion sync failed: {e}")
            return SyncResult(success=False, direction="supabase_to_notion", error_message=str(e))


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Run documents sync from command line."""
    parser = create_cli_parser("Documents Sync - Notion ↔ Supabase")
    args = parser.parse_args()
    
    service = DocumentsSyncService()
    result = service.sync(
        full_sync=args.full,
        since_hours=args.hours
    )
    
    # Print result - result is a SyncResult object
    print(f"\n{'='*60}")
    print(f"Documents Sync Complete")
    print(f"{'='*60}")
    
    if isinstance(result, SyncResult):
        print(f"Success: {result.success}")
        if result.stats:
            print(f"Created: {result.stats.created}")
            print(f"Updated: {result.stats.updated}")
            print(f"Skipped: {result.stats.skipped}")
            print(f"Errors: {result.stats.errors}")
        if result.error_message:
            print(f"Error: {result.error_message}")
        return 0 if result.success else 1
    else:
        # Dict format from base class
        for direction, sync_result in result.items():
            if isinstance(sync_result, SyncResult):
                print(f"\n{direction}:")
                print(f"  Success: {sync_result.success}")
                if sync_result.stats:
                    print(f"  Created: {sync_result.stats.created}")
                    print(f"  Updated: {sync_result.stats.updated}")
                    print(f"  Skipped: {sync_result.stats.skipped}")
                    print(f"  Errors: {sync_result.stats.errors}")
                if sync_result.error_message:
                    print(f"  Error: {sync_result.error_message}")
        return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
