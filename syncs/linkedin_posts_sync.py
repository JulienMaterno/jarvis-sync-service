"""
===================================================================================
LINKEDIN POSTS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion LinkedIn Posts DB and Supabase linkedin_posts table
- Full page content extraction (post drafts and published content)
- Status and pillar tracking
- Post date and likes tracking

Notion Database: LinkedIn Posts (2d1068b5-e624-81f2-8be0-fd6783c4763f)

Notion Properties:
- Name (title): Post title/topic
- Date (date): Post date (planned or actual)
- Status (select): Idea, Posted
- Pillar (select): Personal, Longevity, Algenie
- Likes (rich_text): Number of likes/engagement
- [Page content]: Full post text

Usage:
    python -m syncs.linkedin_posts_sync --full    # Full sync
    python -m syncs.linkedin_posts_sync           # Incremental (last 24h)
    python -m syncs.linkedin_posts_sync --schema  # Show database schema
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
    SyncResult,
    SyncStats,
    create_cli_parser,
    setup_logger
)

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_LINKEDIN_POSTS_DB_ID = os.environ.get(
    'NOTION_LINKEDIN_POSTS_DB_ID', 
    '2d1068b5-e624-81f2-8be0-fd6783c4763f'
)

# Valid statuses
POST_STATUSES = ['Idea', 'Posted']

# Valid pillars
POST_PILLARS = ['Personal', 'Longevity', 'Algenie']


# ============================================================================
# LINKEDIN POSTS SYNC SERVICE
# ============================================================================

class LinkedInPostsSyncService(TwoWaySyncService):
    """
    Bidirectional sync for LinkedIn Posts between Notion and Supabase.
    
    Notion Properties:
    - Name (title): Post title/topic
    - Date (date): Post date
    - Status (select): Idea, Posted
    - Pillar (select): Content pillar
    - Likes (rich_text): Engagement count
    - Page content: Full post text
    
    Supabase Fields:
    - title (text): Post title
    - post_date (date): Date
    - status (text): Status
    - pillar (text): Content pillar
    - likes (text): Likes count
    - content (text): Full post text
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """
    
    def __init__(self):
        super().__init__(
            service_name="LinkedInPostsSync",
            notion_database_id=NOTION_LINKEDIN_POSTS_DB_ID,
            supabase_table="linkedin_posts"
        )
        self.logger = setup_logger("LinkedInPostsSync")
    
    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion post to Supabase format.
        Notion → Supabase
        """
        props = notion_record.get('properties', {})
        
        # Extract all properties
        title = NotionPropertyExtractor.title(props, 'Name')
        if not title:
            title = 'Untitled Post'
        
        result = {
            'title': title,
            'post_date': NotionPropertyExtractor.date(props, 'Date'),
            'status': NotionPropertyExtractor.select(props, 'Status') or 'Idea',
            'pillar': NotionPropertyExtractor.select(props, 'Pillar'),
            'likes': NotionPropertyExtractor.rich_text(props, 'Likes'),
        }
        
        return result
    
    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase post to Notion properties format.
        Supabase → Notion
        """
        title = supabase_record.get('title', 'Untitled Post')
        
        properties = {
            'Name': NotionPropertyBuilder.title(title[:100]),  # Notion title limit
        }
        
        # Date
        post_date = supabase_record.get('post_date')
        if post_date:
            properties['Date'] = NotionPropertyBuilder.date(post_date)
        
        # Status (select)
        status = supabase_record.get('status')
        if status and status in POST_STATUSES:
            properties['Status'] = NotionPropertyBuilder.select(status)
        
        # Pillar (select)
        pillar = supabase_record.get('pillar')
        if pillar and pillar in POST_PILLARS:
            properties['Pillar'] = NotionPropertyBuilder.select(pillar)
        
        # Likes (rich_text)
        likes = supabase_record.get('likes')
        if likes:
            properties['Likes'] = NotionPropertyBuilder.rich_text(str(likes))
        
        return properties
    
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
            self.logger.info(f"Found {len(notion_records)} LinkedIn posts in Notion")
            
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
                        self.logger.info(f"Skipping post '{data.get('title')}' - has local changes pending")
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
                    
                    # Extract page content (the actual post text)
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
                    
                    # Upsert
                    if existing_record:
                        self.supabase.update(existing_record['id'], data)
                    else:
                        self.supabase.insert(data)
                    
                except Exception as e:
                    self.logger.error(f"Error syncing post from Notion: {e}")
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
        Supabase → Notion
        """
        stats = SyncStats()
        start_time = __import__('time').time()
        
        try:
            # Get Supabase records that need syncing
            if full_sync:
                supabase_records = self.supabase.get_all_active()
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                all_records = self.supabase.select_updated_since(cutoff)
                supabase_records = [r for r in all_records if not r.get('deleted_at')]
            
            if metrics:
                metrics.supabase_api_calls += 1
            
            # Filter to records that need syncing to Notion
            records_to_sync = [
                r for r in supabase_records 
                if not r.get('notion_page_id') or r.get('last_sync_source') == 'supabase'
            ]
            
            self.logger.info(f"Found {len(records_to_sync)} posts to sync to Notion")
            
            for record in records_to_sync:
                try:
                    notion_page_id = record.get('notion_page_id')
                    notion_props = self.convert_to_source(record)
                    
                    if notion_page_id:
                        # Update existing page
                        updated_page = self.notion.update_page(notion_page_id, notion_props)
                        
                        # Update Supabase with new timestamp
                        self.supabase.update(record['id'], {
                            'notion_updated_at': updated_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        
                        stats.updated += 1
                    else:
                        # Create new page with content blocks
                        blocks = self._build_content_blocks(record)
                        new_page = self.notion.create_page(
                            self.notion_database_id,
                            notion_props,
                            children=blocks if blocks else None
                        )
                        
                        # Update Supabase with new Notion ID
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page['id'],
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        stats.created += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing post to Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=__import__('time').time() - start_time
            )
            
        except Exception as e:
            return SyncResult(success=False, direction="supabase_to_notion", error_message=str(e))
    
    def _build_content_blocks(self, record: Dict) -> List[Dict]:
        """Build Notion blocks from post content."""
        blocks = []
        content = record.get('content', '')
        
        if not content:
            return blocks
        
        # Split content into paragraphs
        paragraphs = content.split('\n\n')
        
        for para in paragraphs:
            para = para.strip()
            if not para:
                continue
            
            # Handle single line breaks as soft breaks within a paragraph
            lines = para.split('\n')
            
            for line in lines:
                line = line.strip()
                if line:
                    blocks.append({
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{
                                "type": "text",
                                "text": {"content": line[:2000]}  # Notion limit
                            }]
                        }
                    })
        
        return blocks


# ============================================================================
# ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run the LinkedIn posts sync."""
    service = LinkedInPostsSyncService()
    result = service.sync(full_sync=full_sync, since_hours=since_hours)
    return result.to_dict()


if __name__ == '__main__':
    parser = create_cli_parser("LinkedInPostsSync")
    args = parser.parse_args()
    
    if args.schema:
        service = LinkedInPostsSyncService()
        schema = service.notion.get_database_schema(NOTION_LINKEDIN_POSTS_DB_ID)
        print(f"\nNotion Database Schema:")
        print(f"ID: {NOTION_LINKEDIN_POSTS_DB_ID}")
        for name, prop in schema.items():
            print(f"  {name}: {prop.get('type')}")
    else:
        result = run_sync(full_sync=args.full, since_hours=args.hours)
        print(f"\nSync Result: {result}")
