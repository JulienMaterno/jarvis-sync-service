#!/usr/bin/env python3
"""
===================================================================================
HIGHLIGHTS SYNC SERVICE (Notion → Supabase)
===================================================================================

Syncs book highlights and annotations from Notion "Highlights" database to Supabase.
This is a ONE-WAY sync - Notion is the source of truth for highlights.

Data Flow:
    Notion Highlights DB → Supabase highlights table

Usage:
    python sync_highlights.py                    # Sync recent changes (24h)
    python sync_highlights.py --full             # Full sync of all highlights
    python sync_highlights.py --hours 168        # Sync last 7 days
    python sync_highlights.py --schema           # Show database schemas

Notion Database ID: 16a068b5-e624-81e9-a7ef-ecbf84c577ef (Highlights)
Supabase Table: highlights
Direction: ONE-WAY (Notion → Supabase)
"""

import os
import sys
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import httpx
from dotenv import load_dotenv

load_dotenv()

# Add lib to path
sys.path.insert(0, os.path.dirname(__file__))

from lib.utils import retry_on_error_sync
from lib.logging_service import log_sync_event_sync

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('HighlightsSync')

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
# Highlights database ID - discovered from Notion
NOTION_HIGHLIGHTS_DB_ID = os.environ.get(
    'NOTION_HIGHLIGHTS_DB_ID', 
    '16a068b5-e624-81e9-a7ef-ecbf84c577ef'
)

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# ============================================================================
# NOTION CLIENT
# ============================================================================

class NotionClient:
    """Notion API client with retry logic."""
    
    def __init__(self, token: str):
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Notion-Version': '2022-06-28',
            'Content-Type': 'application/json'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    @retry_on_error_sync()
    def query_database(
        self, 
        database_id: str, 
        filter: Optional[Dict] = None,
        page_size: int = 100
    ) -> List[Dict]:
        """Query all pages from a database with pagination."""
        results = []
        start_cursor = None
        
        while True:
            body = {"page_size": page_size}
            if filter:
                body["filter"] = filter
            if start_cursor:
                body["start_cursor"] = start_cursor
            
            response = self.client.post(
                f'https://api.notion.com/v1/databases/{database_id}/query',
                json=body
            )
            response.raise_for_status()
            data = response.json()
            
            results.extend(data.get('results', []))
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return results
    
    @retry_on_error_sync()
    def get_database_schema(self, database_id: str) -> Dict:
        """Get database schema to understand properties."""
        response = self.client.get(f'https://api.notion.com/v1/databases/{database_id}')
        response.raise_for_status()
        return response.json()


# ============================================================================
# SUPABASE CLIENT
# ============================================================================

class SupabaseClient:
    """Supabase client for highlights."""
    
    def __init__(self, url: str, key: str):
        self.base_url = f"{url}/rest/v1"
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    def upsert_highlight(self, data: Dict) -> Dict:
        """Upsert a highlight (insert or update based on notion_page_id)."""
        notion_page_id = data.get('notion_page_id')
        
        # Check if highlight exists
        existing = self.get_highlight_by_notion_id(notion_page_id)
        
        if existing:
            # Update existing record
            response = self.client.patch(
                f"{self.base_url}/highlights?notion_page_id=eq.{notion_page_id}",
                json=data
            )
            response.raise_for_status()
            result = response.json()
            return result[0] if result else {}
        else:
            # Insert new record
            response = self.client.post(
                f"{self.base_url}/highlights",
                json=data
            )
            response.raise_for_status()
            result = response.json()
            return result[0] if result else {}
    
    def get_highlight_by_notion_id(self, notion_page_id: str) -> Optional[Dict]:
        """Get a highlight by its Notion page ID."""
        response = self.client.get(
            f"{self.base_url}/highlights?notion_page_id=eq.{notion_page_id}&limit=1"
        )
        response.raise_for_status()
        result = response.json()
        return result[0] if result else None
    
    def get_all_highlights(self, limit: int = 1000) -> List[Dict]:
        """Get all highlights from Supabase."""
        response = self.client.get(
            f"{self.base_url}/highlights?select=*&order=highlighted_at.desc&limit={limit}"
        )
        response.raise_for_status()
        return response.json()
    
    def get_book_id_by_title(self, title: str) -> Optional[str]:
        """Look up book ID by title."""
        response = self.client.get(
            f"{self.base_url}/books?select=id&title=eq.{title}&limit=1"
        )
        if response.status_code == 200:
            data = response.json()
            if data:
                return data[0].get('id')
        return None


# ============================================================================
# PROPERTY EXTRACTION HELPERS
# ============================================================================

def extract_title(props: Dict, prop_name: str = 'Name') -> str:
    """Extract title from Notion title property."""
    title_prop = props.get(prop_name, {}).get('title', [])
    return title_prop[0].get('plain_text', '') if title_prop else ''


def extract_text(props: Dict, prop_name: str) -> Optional[str]:
    """Extract text from rich_text property."""
    text_prop = props.get(prop_name, {}).get('rich_text', [])
    if not text_prop:
        return None
    return ''.join(t.get('plain_text', '') for t in text_prop) or None


def extract_number(props: Dict, prop_name: str) -> Optional[int]:
    """Extract number from number property."""
    return props.get(prop_name, {}).get('number')


def extract_select(props: Dict, prop_name: str) -> Optional[str]:
    """Extract value from select property."""
    select = props.get(prop_name, {}).get('select')
    return select.get('name') if select else None


def extract_multi_select(props: Dict, prop_name: str) -> List[str]:
    """Extract values from multi_select property."""
    items = props.get(prop_name, {}).get('multi_select', [])
    return [item.get('name') for item in items if item.get('name')]


def extract_checkbox(props: Dict, prop_name: str) -> bool:
    """Extract value from checkbox property."""
    return props.get(prop_name, {}).get('checkbox', False)


def extract_date(props: Dict, prop_name: str) -> Optional[str]:
    """Extract date from date property."""
    date_prop = props.get(prop_name, {}).get('date')
    if not date_prop:
        return None
    return date_prop.get('start')


def extract_relation_titles(props: Dict, prop_name: str) -> List[str]:
    """Extract titles from relation property (requires rollup or manual fetch)."""
    # Note: This just returns IDs - need to fetch pages for titles
    relations = props.get(prop_name, {}).get('relation', [])
    return [r.get('id') for r in relations]


# ============================================================================
# CONVERSION FUNCTION
# ============================================================================

def convert_notion_to_supabase(page: Dict, supabase: SupabaseClient) -> Dict:
    """Convert Notion highlight page to Supabase format."""
    props = page.get('properties', {})
    
    # Based on actual Notion schema:
    # - Title: title (the actual highlight text)
    # - Content: relation (to books - NOT the highlight text!)
    # - Chapter Title: rich_text
    # - Chapter Index: number
    # - Location: number
    # - Added At: date
    # - Tags: multi_select
    # - Color: rich_text
    # - Authors: relation
    # - Location URL: url
    
    # The highlight text is in the "Title" property (title type)
    content = extract_title(props, 'Title')
    
    # Get book info - "Content" is a relation to books
    # We can't easily get the book title from a relation without additional API calls
    # For now, we'll leave book_title empty and book_id null
    book_title = None  # Would need to fetch from relation
    book_id = None
    
    # Location is a number in this schema
    location_num = extract_number(props, 'Location')
    
    # Highlight type from Color
    color = extract_text(props, 'Color')
    
    return {
        'notion_page_id': page.get('id'),
        'notion_updated_at': page.get('last_edited_time'),
        'last_sync_source': 'notion',
        'content': content,
        'note': extract_text(props, 'Note') or extract_text(props, 'Notes'),
        'book_id': book_id,
        'book_title': book_title,
        'page_number': None,  # Not in schema
        'chapter': extract_text(props, 'Chapter Title'),
        'location': str(location_num) if location_num else None,
        'highlight_type': color.lower() if color else 'highlight',
        'tags': extract_multi_select(props, 'Tags'),
        'is_favorite': False,  # Not in schema
        'highlighted_at': extract_date(props, 'Added At') or page.get('created_time'),
        'updated_at': datetime.now(timezone.utc).isoformat()
    }


# ============================================================================
# SYNC LOGIC
# ============================================================================

def run_sync(full_sync: bool = False, hours: int = 24) -> Dict:
    """
    Sync highlights from Notion to Supabase.
    
    Args:
        full_sync: If True, sync all highlights. If False, only sync recent changes.
        hours: For incremental sync, how many hours back to look.
    
    Returns:
        Dict with sync statistics
    """
    if not NOTION_HIGHLIGHTS_DB_ID:
        logger.error("NOTION_HIGHLIGHTS_DB_ID not configured")
        return {'success': False, 'error': 'Database ID not configured'}
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Supabase credentials not configured")
        return {'success': False, 'error': 'Supabase not configured'}
    
    start_time = time.time()
    stats = {
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'errors': 0
    }
    
    try:
        notion = NotionClient(NOTION_API_TOKEN)
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        # Build filter for incremental sync
        filter_obj = None
        if not full_sync:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
            filter_obj = {
                "timestamp": "last_edited_time",
                "last_edited_time": {
                    "after": cutoff.isoformat()
                }
            }
        
        # Query Notion
        logger.info(f"Querying Notion highlights (full={full_sync}, hours={hours})")
        pages = notion.query_database(NOTION_HIGHLIGHTS_DB_ID, filter=filter_obj)
        logger.info(f"Found {len(pages)} highlights to sync")
        
        # Sync each highlight
        for page in pages:
            try:
                data = convert_notion_to_supabase(page, supabase)
                
                if not data.get('content'):
                    logger.debug(f"Skipping highlight with no content: {page.get('id')}")
                    stats['skipped'] += 1
                    continue
                
                result = supabase.upsert_highlight(data)
                
                if result:
                    stats['created'] += 1
                    logger.debug(f"Synced highlight: {data.get('content', '')[:50]}...")
                else:
                    stats['skipped'] += 1
                    
            except Exception as e:
                logger.error(f"Failed to sync highlight {page.get('id')}: {e}")
                stats['errors'] += 1
        
        elapsed = time.time() - start_time
        
        # Log the sync event
        log_sync_event_sync(
            event_type='highlights_sync',
            status='success',
            message=f"Synced {stats['created']} highlights",
            details=stats
        )
        
        return {
            'success': True,
            'stats': stats,
            'elapsed_seconds': round(elapsed, 1)
        }
        
    except Exception as e:
        logger.exception("Highlights sync failed")
        log_sync_event_sync(
            event_type='highlights_sync',
            status='error',
            message=str(e)
        )
        return {
            'success': False,
            'error': str(e),
            'stats': stats
        }


# ============================================================================
# HELPER FUNCTIONS FOR JOURNAL
# ============================================================================

def get_recent_highlights(days: int = 1) -> List[Dict]:
    """Get highlights from the last N days."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    
    response = supabase.client.get(
        f"{supabase.base_url}/highlights"
        f"?select=content,note,book_title,page_number,is_favorite"
        f"&highlighted_at=gte.{cutoff}"
        f"&order=highlighted_at.desc"
        f"&limit=20"
    )
    
    if response.status_code == 200:
        return response.json()
    return []


def get_favorite_highlights(limit: int = 10) -> List[Dict]:
    """Get favorite/starred highlights."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []
    
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    
    response = supabase.client.get(
        f"{supabase.base_url}/highlights"
        f"?select=content,note,book_title"
        f"&is_favorite=eq.true"
        f"&order=highlighted_at.desc"
        f"&limit={limit}"
    )
    
    if response.status_code == 200:
        return response.json()
    return []


def format_highlights_for_journal() -> str:
    """Format today's highlights for journal prompt."""
    highlights = get_recent_highlights(days=1)
    
    if not highlights:
        return "No new highlights today."
    
    lines = [f"✨ **{len(highlights)} New Highlight(s) Today:**"]
    
    for h in highlights[:5]:  # Show max 5
        book = h.get('book_title', 'Unknown book')
        content = h.get('content', '')[:150]
        if len(h.get('content', '')) > 150:
            content += "..."
        
        lines.append(f"\n📖 *{book}*:")
        lines.append(f"> {content}")
        
        if h.get('note'):
            lines.append(f"💭 {h['note']}")
    
    if len(highlights) > 5:
        lines.append(f"\n...and {len(highlights) - 5} more")
    
    return "\n".join(lines)


# ============================================================================
# SCHEMA DISPLAY
# ============================================================================

def show_schema():
    """Display database schemas for reference."""
    print("\n" + "="*60)
    print("✨ HIGHLIGHTS SYNC - Database Schemas")
    print("="*60)
    
    # Notion schema
    print("\n🔵 NOTION HIGHLIGHTS DATABASE:")
    try:
        notion = NotionClient(NOTION_API_TOKEN)
        db_info = notion.get_database_schema(NOTION_HIGHLIGHTS_DB_ID)
        props = db_info.get('properties', {})
        title_arr = db_info.get('title', [])
        title = title_arr[0].get('plain_text', 'Unknown') if title_arr else 'Unknown'
        print(f"   Database ID: {NOTION_HIGHLIGHTS_DB_ID}")
        print(f"   Title: {title}")
        print(f"   Properties ({len(props)}):")
        for name, prop in sorted(props.items()):
            print(f"      - {name}: {prop.get('type')}")
    except Exception as e:
        print(f"   ❌ Failed to query Notion: {e}")
    
    # Supabase schema
    print("\n🔶 SUPABASE HIGHLIGHTS TABLE:")
    try:
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        records = supabase.get_all_highlights(limit=1)
        if records:
            print(f"   Records exist: Yes")
            print(f"   Columns: {', '.join(sorted(records[0].keys()))}")
        else:
            print("   (No records yet - run sync first)")
    except Exception as e:
        print(f"   ❌ Failed to query Supabase: {e}")
        print("   💡 Run the migration first: migrations/006_books_highlights.sql")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sync highlights from Notion to Supabase')
    parser.add_argument('--full', action='store_true', help='Full sync instead of incremental')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back for incremental sync')
    parser.add_argument('--schema', action='store_true', help='Show database schemas')
    args = parser.parse_args()
    
    if args.schema:
        show_schema()
    else:
        result = run_sync(full_sync=args.full, hours=args.hours)
        
        if result.get('success'):
            stats = result.get('stats', {})
            print(f"\n✅ Highlights sync complete!")
            print(f"   Created: {stats.get('created', 0)}")
            print(f"   Updated: {stats.get('updated', 0)}")
            print(f"   Skipped: {stats.get('skipped', 0)}")
            print(f"   Errors: {stats.get('errors', 0)}")
            print(f"   Time: {result.get('elapsed_seconds', 0):.1f}s")
            
            # Show recent highlights
            recent = get_recent_highlights(days=1)
            if recent:
                print(f"\n✨ Today's Highlights ({len(recent)}):")
                for h in recent[:3]:
                    content = h.get('content', '')[:80]
                    print(f"   • {content}...")
        else:
            print(f"\n❌ Sync failed: {result.get('error')}")
