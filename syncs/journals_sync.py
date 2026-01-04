"""
===================================================================================
JOURNALS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion Journal DB and Supabase journals table
- Structured sections (key_events, accomplishments, challenges, gratitude, tomorrow_focus)
- Select properties (mood, effort, wakeup, nutrition)
- Multi-select properties (sports)
- Date-based unique constraint
- Safety valves to prevent data loss

Notion Properties:
- Name (title): "Journal Entry" 
- Date (date): Journal date (unique)
- Mood (select): Great, Good, Okay, Tired, etc.
- Effort (select): High, Medium, Low
- Wakeup (select): Time ranges
- Sport (multi_select): Running, Gym, Yoga, etc.
- Nutrition (select): Good, Okay, Poor
- Note (rich_text): Quick note/summary

Usage:
    python -m syncs.journals_sync --full    # Full sync
    python -m syncs.journals_sync           # Incremental (last 24h)
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

NOTION_JOURNAL_DB_ID = os.environ.get('NOTION_JOURNAL_DB_ID', '2cecd3f1-eb28-8098-bf5e-d49ae4a68f6b')


# ============================================================================
# JOURNALS SYNC SERVICE
# ============================================================================

class JournalsSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Journals between Notion and Supabase.
    
    Notion Properties:
    - Name (title): Journal title
    - Date (date): Journal date (unique identifier)
    - Mood (select): Emotional state
    - Effort (select): Effort level for the day
    - Wakeup (select): Wake up time
    - Nutrition (select): Nutrition quality
    - Sport (multi_select): Sports/activities
    - Note (rich_text): Quick summary
    - Content in page body blocks
    
    Supabase Fields:
    - title (text): Journal title
    - date (date): Journal date (unique)
    - mood, effort, wakeup_time, nutrition (text): Select values
    - sports (text[]): Multi-select values
    - note (text): Quick note
    - content (text): Full content
    - sections (jsonb): Structured sections
    - key_events, accomplishments, challenges, gratitude, tomorrow_focus (text[]): Structured lists
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """
    
    def __init__(self):
        super().__init__(
            service_name="JournalsSync",
            notion_database_id=NOTION_JOURNAL_DB_ID,
            supabase_table="journals"
        )
        self.logger = setup_logger("JournalsSync")
    
    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion journal to Supabase format.
        Notion → Supabase
        
        Note: Content is extracted separately via _sync_notion_to_supabase override.
        """
        props = notion_record.get('properties', {})
        
        # Extract title
        title = NotionPropertyExtractor.title(props, 'Name')
        if not title:
            title = 'Journal Entry'
        
        # Extract date (required - unique identifier)
        date = NotionPropertyExtractor.date(props, 'Date')
        if not date:
            self.logger.warning(f"Journal {notion_record.get('id')} has no date, skipping")
            return None
        
        # Extract select properties
        mood = NotionPropertyExtractor.select(props, 'Mood')
        effort = NotionPropertyExtractor.select(props, 'Effort')
        wakeup_time = NotionPropertyExtractor.select(props, 'Wakeup')
        nutrition = NotionPropertyExtractor.select(props, 'Nutrition')
        
        # Extract multi-select (sports)
        sports = NotionPropertyExtractor.multi_select(props, 'Sport')
        
        # Extract note (rich_text)
        note = NotionPropertyExtractor.rich_text(props, 'Note')
        
        return {
            'title': title,
            'date': date,
            'mood': mood,
            'effort': effort,
            'wakeup_time': wakeup_time,
            'nutrition': nutrition,
            'sports': sports if sports else None,
            'note': note,
            'source': 'notion',
        }
    
    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase journal to Notion properties format.
        Supabase → Notion
        
        Note: Content blocks are built separately via _build_content_blocks.
        """
        properties = {}
        
        # Title
        title = supabase_record.get('title', 'Journal Entry')
        properties['Name'] = NotionPropertyBuilder.title(title[:100])
        
        # Date (required)
        if supabase_record.get('date'):
            properties['Date'] = NotionPropertyBuilder.date(supabase_record['date'])
        
        # Select properties
        if supabase_record.get('mood'):
            properties['Mood'] = NotionPropertyBuilder.select(supabase_record['mood'])
        
        if supabase_record.get('effort'):
            properties['Effort'] = NotionPropertyBuilder.select(supabase_record['effort'])
        
        if supabase_record.get('wakeup_time'):
            properties['Wakeup'] = NotionPropertyBuilder.select(supabase_record['wakeup_time'])
        
        if supabase_record.get('nutrition'):
            properties['Nutrition'] = NotionPropertyBuilder.select(supabase_record['nutrition'])
        
        # Sports (multi_select)
        if supabase_record.get('sports'):
            properties['Sport'] = NotionPropertyBuilder.multi_select(supabase_record['sports'][:10])
        
        # Note - use summary if available, else note
        note = supabase_record.get('summary') or supabase_record.get('note') or ''
        if note:
            properties['Note'] = NotionPropertyBuilder.rich_text(note[:2000])
        
        return properties
    
    def _build_content_blocks(self, journal: Dict) -> List[Dict]:
        """
        Build Notion content blocks from journal data.
        Handles structured fields (key_events, accomplishments, etc.) and sections.
        """
        blocks = []
        builder = ContentBlockBuilder()
        
        # First, add structured sections if present
        sections = journal.get('sections', [])
        if sections:
            for section in sections:
                heading = section.get('heading', '')
                content = section.get('content', '')
                
                if heading:
                    blocks.append(builder.heading_2(heading[:100]))
                
                if content:
                    blocks.extend(builder.chunked_paragraphs(content))
        
        # Add key events
        if journal.get('key_events'):
            blocks.append(builder.heading_2('📌 Key Events'))
            for event in journal['key_events']:
                blocks.append(builder.bulleted_list_item(event[:2000]))
        
        # Add accomplishments
        if journal.get('accomplishments'):
            blocks.append(builder.heading_2('✅ Accomplishments'))
            for item in journal['accomplishments']:
                blocks.append(builder.bulleted_list_item(item[:2000]))
        
        # Add challenges
        if journal.get('challenges'):
            blocks.append(builder.heading_2('⚠️ Challenges'))
            for item in journal['challenges']:
                blocks.append(builder.bulleted_list_item(item[:2000]))
        
        # Add gratitude
        if journal.get('gratitude'):
            blocks.append(builder.heading_2('🙏 Gratitude'))
            for item in journal['gratitude']:
                blocks.append(builder.bulleted_list_item(item[:2000]))
        
        # Add tomorrow's focus
        if journal.get('tomorrow_focus'):
            blocks.append(builder.heading_2("🎯 Tomorrow's Focus"))
            for item in journal['tomorrow_focus']:
                blocks.append(builder.bulleted_list_item(item[:2000]))
        
        # Fallback: raw content if no structured data
        if not blocks and journal.get('content'):
            blocks.extend(builder.chunked_paragraphs(journal['content']))
        
        return blocks
    
    def _sync_notion_to_supabase(self, full_sync: bool, since_hours: int, metrics=None) -> SyncResult:
        """
        Override to include content extraction and date-based matching.
        Notion → Supabase with content
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
            self.logger.info(f"Found {len(notion_records)} journals in Notion")
            
            if metrics:
                metrics.notion_api_calls += 1
                metrics.source_total = len(notion_records)
                metrics.records_read += len(notion_records)
            
            # Get existing by both notion_page_id and date
            existing_by_notion_id = {}
            existing_by_date = {}
            for r in self.supabase.select_all():
                if r.get('notion_page_id'):
                    existing_by_notion_id[r['notion_page_id']] = r
                if r.get('date'):
                    existing_by_date[r['date']] = r
            
            if metrics:
                metrics.supabase_api_calls += 1
                metrics.destination_total = len(existing_by_notion_id)
            
            # Safety valve
            is_safe, msg = self.check_safety_valve(len(notion_records), len(existing_by_notion_id), "Notion → Supabase")
            if not is_safe and full_sync:
                self.logger.error(msg)
                return SyncResult(success=False, direction="notion_to_supabase", error_message=msg)
            
            # Process records
            for notion_record in notion_records:
                try:
                    notion_id = self.get_source_id(notion_record)
                    
                    # Convert properties
                    data = self.convert_from_source(notion_record)
                    if data is None:  # Skip if no date
                        stats.skipped += 1
                        continue
                    
                    # Find existing by notion_page_id OR by date
                    existing_record = existing_by_notion_id.get(notion_id)
                    if not existing_record and data.get('date'):
                        existing_record = existing_by_date.get(data['date'])
                    
                    # Skip if Supabase has local changes that need to sync TO Notion
                    if existing_record and existing_record.get('last_sync_source') == 'supabase':
                        self.logger.info(f"Skipping journal '{existing_record.get('title', 'Untitled')}' - has local Supabase changes pending sync to Notion")
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
                    
                    # Use upsert with date as conflict column (journals are unique by date)
                    if existing_record:
                        # Update existing
                        self.supabase.update(existing_record['id'], data)
                    else:
                        # Create new
                        self.supabase.insert(data)
                    
                except Exception as e:
                    self.logger.error(f"Error syncing journal from Notion: {e}")
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
        Supabase → Notion with structured content blocks
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
            
            self.logger.info(f"Found {len(records_to_sync)} journals to sync to Notion")
            
            # Safety valve
            notion_records = self.notion.query_database(self.notion_database_id)
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
                                # Delete existing blocks and append new ones
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
                        # Create new page with content
                        blocks = self._build_content_blocks(record)
                        new_page = self.notion.create_page(
                            self.notion_database_id, 
                            notion_props, 
                            children=blocks
                        )
                        
                        # Update Supabase with new Notion ID
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page['id'],
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        stats.created += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing journal to Notion: {e}")
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
# ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run the journals sync and return results."""
    service = JournalsSyncService()
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
    parser = create_cli_parser("Journals")
    args = parser.parse_args()
    
    result = run_sync(full_sync=args.full, since_hours=args.hours)
    print(f"\nResult: {result}")
