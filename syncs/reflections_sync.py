"""
===================================================================================
REFLECTIONS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion Reflections DB and Supabase reflections table
- Content extraction and block creation
- Topic-based grouping (topic_key)
- Tag support
- Safety valves to prevent data loss

Notion Properties:
- Name (title): Reflection title
- Date (date): Reflection date
- Tags (multi_select): Tag labels
- Content in page body blocks

Usage:
    python -m syncs.reflections_sync --full    # Full sync
    python -m syncs.reflections_sync           # Incremental (last 24h)
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
    create_cli_parser,
    setup_logger,
    NOTION_API_TOKEN,
    SUPABASE_URL,
    SUPABASE_KEY
)

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_REFLECTIONS_DB_ID = os.environ.get('NOTION_REFLECTIONS_DB_ID', '2cacd3f1-eb28-80d9-903a-ee73d2f84b59')


# ============================================================================
# REFLECTIONS SYNC SERVICE
# ============================================================================

class ReflectionsSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Reflections between Notion and Supabase.
    
    Notion Properties:
    - Name (title): Reflection title
    - Date (date): Reflection date
    - Tags (multi_select): Tag labels
    - Content in page body blocks
    
    Supabase Fields:
    - title (text): Reflection title
    - date (date): Reflection date
    - topic_key (text): Topic identifier for grouping
    - content (text): Full text content
    - sections (jsonb): Structured sections
    - tags (text[]): Tags
    - people_mentioned (text[]): People mentioned
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """
    
    def __init__(self):
        super().__init__(
            service_name="ReflectionsSync",
            notion_database_id=NOTION_REFLECTIONS_DB_ID,
            supabase_table="reflections"
        )
        self.logger = setup_logger("ReflectionsSync")
    
    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion reflection to Supabase format.
        Notion → Supabase
        """
        props = notion_record.get('properties', {})
        
        # Extract title
        title = NotionPropertyExtractor.title(props, 'Name')
        
        # Extract date
        date = NotionPropertyExtractor.date(props, 'Date')
        
        # Extract tags
        tags = NotionPropertyExtractor.multi_select(props, 'Tags')
        
        return {
            'title': title,
            'date': date,
            'tags': tags if tags else None,
        }
    
    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase reflection to Notion properties format.
        Supabase → Notion
        """
        properties = {}
        
        # Title
        if supabase_record.get('title'):
            properties['Name'] = NotionPropertyBuilder.title(supabase_record['title'][:100])
        
        # Date
        if supabase_record.get('date'):
            properties['Date'] = NotionPropertyBuilder.date(supabase_record['date'])
        
        # Tags
        if supabase_record.get('tags'):
            properties['Tags'] = NotionPropertyBuilder.multi_select(supabase_record['tags'][:10])
        
        return properties
    
    def _build_content_blocks(self, reflection: Dict) -> List[Dict]:
        """
        Build Notion content blocks from reflection data.
        """
        blocks = []
        builder = ContentBlockBuilder()
        
        # Add sections if present
        sections = reflection.get('sections', [])
        if sections:
            for section in sections:
                heading = section.get('heading', '')
                content = section.get('content', '')
                
                if heading:
                    blocks.append(builder.heading_2(heading))
                
                if content:
                    blocks.extend(builder.chunked_paragraphs(content))
        
        # Add content if no sections
        elif reflection.get('content'):
            content = reflection['content']
            blocks.extend(builder.chunked_paragraphs(content))
        
        return blocks
    
    def _build_supabase_lookup(self) -> Dict[str, Dict]:
        """Build lookup dict for existing Supabase records by notion_page_id."""
        lookup = {}
        for r in self.supabase.get_all_active():
            if r.get('notion_page_id'):
                lookup[r['notion_page_id']] = r
        return lookup
    
    def _sync_notion_to_supabase(self, full_sync: bool, since_hours: int, metrics=None) -> SyncResult:
        """
        Override to include content extraction.
        Notion → Supabase with content
        """
        stats = SyncStats()
        start_time = __import__('time').time()
        
        try:
            # Get Notion records
            if full_sync:
                notion_records = self.notion.query_database(self.notion_database_id)
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                notion_records = self.notion.query_database(
                    self.notion_database_id,
                    filter={
                        "timestamp": "last_edited_time",
                        "last_edited_time": {"on_or_after": cutoff.isoformat()}
                    }
                )
            
            self.logger.info(f"Found {len(notion_records)} reflections in Notion")
            
            if metrics:
                metrics.notion_api_calls += 1
                metrics.source_total = len(notion_records)
                metrics.records_read += len(notion_records)
            
            # Build lookup for existing Supabase records
            supabase_lookup = self._build_supabase_lookup()
            
            if metrics:
                metrics.supabase_api_calls += 1
                metrics.destination_total = len(supabase_lookup)
            
            for notion_record in notion_records:
                try:
                    notion_id = notion_record['id']
                    data = self.convert_from_source(notion_record)
                    
                    if not data:
                        stats.skipped += 1
                        continue
                    
                    # Check if exists in Supabase
                    existing_record = supabase_lookup.get(notion_id)
                    
                    if existing_record:
                        # Skip if Supabase has local changes that need to sync TO Notion
                        if existing_record.get('last_sync_source') == 'supabase':
                            self.logger.info(f"Skipping '{existing_record.get('title', 'Untitled')}' - has local Supabase changes pending sync to Notion")
                            stats.skipped += 1
                            continue
                        
                        # Compare timestamps
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
                    
                    # Extract content from page blocks
                    try:
                        content = self.notion.extract_page_content(notion_id)
                        data['content'] = content
                    except Exception as e:
                        self.logger.warning(f"Failed to extract content: {e}")
                        data['content'] = ''
                    
                    # Add sync metadata
                    data['notion_page_id'] = notion_id
                    data['notion_updated_at'] = notion_record.get('last_edited_time')
                    data['last_sync_source'] = 'notion'
                    data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    
                    # Use upsert
                    if existing_record:
                        self.supabase.update(existing_record['id'], data)
                    else:
                        self.supabase.insert(data)
                    
                except Exception as e:
                    self.logger.error(f"Error syncing reflection from Notion: {e}")
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
        Override to include content block creation.
        Supabase → Notion with content blocks
        """
        stats = SyncStats()
        start_time = __import__('time').time()
        
        try:
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
            
            # Filter to records without notion_page_id (new) or updated locally
            records_to_sync = [
                r for r in supabase_records 
                if not r.get('notion_page_id') or r.get('last_sync_source') == 'supabase'
            ]
            
            self.logger.info(f"Found {len(records_to_sync)} reflections to sync to Notion")
            
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
                    
                    if notion_page_id:
                        # Update existing page
                        self.notion.update_page(notion_page_id, notion_props)
                        
                        # Update content blocks
                        blocks = self._build_content_blocks(record)
                        if blocks:
                            try:
                                existing_blocks = self.notion.get_all_blocks(notion_page_id)
                                for block in existing_blocks:
                                    try:
                                        self.notion.delete_block(block['id'])
                                    except:
                                        pass
                                self.notion.append_blocks(notion_page_id, blocks)
                            except Exception as e:
                                self.logger.warning(f"Failed to update content blocks: {e}")
                        
                        stats.updated += 1
                    else:
                        # Create new page
                        new_page = self.notion.create_page(
                            self.notion_database_id,
                            notion_props
                        )
                        new_page_id = new_page['id']
                        
                        # Add content blocks
                        blocks = self._build_content_blocks(record)
                        if blocks:
                            try:
                                self.notion.append_blocks(new_page_id, blocks)
                            except Exception as e:
                                self.logger.warning(f"Failed to add content blocks: {e}")
                        
                        # Update Supabase with notion_page_id
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page_id,
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'supabase'
                        })
                        
                        stats.created += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing reflection to Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=__import__('time').time() - start_time
            )
            
        except Exception as e:
            return SyncResult(success=False, direction="supabase_to_notion", error_message=str(e))


# ============================================================================
# CLI ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict[str, Any]:
    """
    Main entry point for reflections sync.
    
    Args:
        full_sync: If True, sync all records. If False, only recent changes.
        since_hours: For incremental sync, how many hours to look back.
    
    Returns:
        Dict with sync results
    """
    service = ReflectionsSyncService()
    result = service.sync(full_sync=full_sync, since_hours=since_hours)
    return result.to_dict()


if __name__ == "__main__":
    parser = create_cli_parser("Reflections bidirectional sync")
    args = parser.parse_args()
    
    result = run_sync(full_sync=args.full, since_hours=args.hours)
    print(f"\nResult: {result}")
