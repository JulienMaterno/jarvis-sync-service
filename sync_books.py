"""
One-Way Notion → Supabase Sync for Books (Content)

Syncs books from Notion to Supabase (read-only from BookFusion via Notion).
This is a one-way sync since Notion is the source of truth (fed by BookFusion).

Architecture Pattern:
- Uses shared NotionClient from lib/sync_base.py
- Custom SupabaseClient for books-specific operations
- Conversion functions: Map Notion properties to Supabase columns
- Main sync logic: Fetch from Notion, upsert to Supabase

Usage:
    python sync_books.py              # Incremental sync
    python sync_books.py --full       # Full sync
"""

import os
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv
import httpx

# Import shared clients from lib
from lib.sync_base import NotionClient, retry_on_error
from lib.logging_service import log_sync_event_sync

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('BookSync')

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
# Content (Books) database ID - discovered from Notion
NOTION_BOOKS_DB_ID = os.environ.get('NOTION_BOOKS_DB_ID', '16a068b5-e624-8158-b858-dd72af14183f')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# ============================================================================
# SUPABASE CLIENT (Books-specific)
# ============================================================================

class BooksSupabaseClient:
    """Supabase client for books table with upsert support."""
    
    def __init__(self, url: str, key: str):
        self.base_url = f"{url}/rest/v1"
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def upsert_book(self, book_data: Dict) -> Dict:
        """Upsert a book using Supabase's native upsert."""
        # If we have an ID, update by ID (for linking orphan records)
        if 'id' in book_data:
            book_id = book_data.pop('id')
            response = self.client.patch(
                f"{self.base_url}/books?id=eq.{book_id}",
                json=book_data,
                headers={**self.headers, "Prefer": "return=representation"}
            )
        else:
            # Standard upsert by notion_page_id
            response = self.client.post(
                f"{self.base_url}/books?on_conflict=notion_page_id",
                json=book_data,
                headers={**self.headers, "Prefer": "resolution=merge-duplicates,return=representation"}
            )
        response.raise_for_status()
        result = response.json()
        return result[0] if result else {}

    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_all_books(self) -> List[Dict]:
        """Get all books from Supabase with fields needed for sync."""
        response = self.client.get(
            f"{self.base_url}/books?select=id,title,notion_page_id,summary,drive_url,drive_file_id,"
            f"original_drive_url,original_drive_file_id,bookfusion_id,processed_at&order=created_at.desc"
        )
        response.raise_for_status()
        return response.json()


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
    return text_prop[0].get('plain_text', '') if text_prop else None

def extract_number(props: Dict, prop_name: str) -> Optional[float]:
    """Extract number from number property."""
    return props.get(prop_name, {}).get('number')

def extract_select(props: Dict, prop_name: str) -> Optional[str]:
    """Extract select value from select property."""
    select = props.get(prop_name, {}).get('select')
    return select.get('name') if select else None

def extract_multi_select(props: Dict, prop_name: str) -> List[str]:
    """Extract multi-select values."""
    items = props.get(prop_name, {}).get('multi_select', [])
    return [item.get('name', '') for item in items]

def extract_date(props: Dict, prop_name: str) -> Optional[str]:
    """Extract date from date property."""
    date_prop = props.get(prop_name, {}).get('date')
    return date_prop.get('start') if date_prop else None

def extract_url(props: Dict, prop_name: str) -> Optional[str]:
    """Extract URL from url property."""
    return props.get(prop_name, {}).get('url')

def extract_checkbox(props: Dict, prop_name: str) -> bool:
    """Extract checkbox value."""
    return props.get(prop_name, {}).get('checkbox', False)


# ============================================================================
# CONVERSION FUNCTION (Update based on actual Notion schema)
# ============================================================================

def notion_book_to_supabase(notion_page: Dict) -> Dict:
    """
    Convert Notion book page to Supabase format.
    
    Update the property names below to match your actual Notion database schema.
    """
    props = notion_page.get('properties', {})
    
    # Supabase books table columns:
    # id, title, author, author_id, status, rating, current_page, total_pages, 
    # progress_percent, started_at, finished_at, summary, notes, tags, cover_url, 
    # goodreads_url, amazon_url, notion_page_id, notion_updated_at, last_sync_source, 
    # created_at, updated_at, deleted_at
    
    return {
        'notion_page_id': notion_page['id'],
        'title': extract_title(props, 'Name') or extract_title(props, 'Title'),
        'author': extract_text(props, 'Author'),
        'cover_url': extract_url(props, 'Cover') or extract_text(props, 'Cover URL'),
        'status': extract_select(props, 'Status'),
        'rating': extract_number(props, 'Rating'),
        'tags': extract_multi_select(props, 'Genres') or extract_multi_select(props, 'Tags'),
        'started_at': extract_date(props, 'Date Started'),
        'finished_at': extract_date(props, 'Date Finished'),
        'notes': extract_text(props, 'Notes'),
        'summary': extract_text(props, 'Summary'),
        'total_pages': extract_number(props, 'Pages') or extract_number(props, 'Total Pages'),
        'current_page': extract_number(props, 'Current Page'),
        'goodreads_url': extract_url(props, 'Goodreads URL') or extract_url(props, 'Goodreads'),
        'amazon_url': extract_url(props, 'Amazon URL') or extract_url(props, 'Amazon'),
        
        # Metadata
        'notion_updated_at': notion_page.get('last_edited_time'),
        'last_sync_source': 'notion',
        'updated_at': datetime.now(timezone.utc).isoformat(),
    }


# ============================================================================
# SYNC SERVICE
# ============================================================================

class BookSyncService:
    """One-way sync service: Notion → Supabase for books."""
    
    def __init__(self):
        if not NOTION_BOOKS_DB_ID:
            raise ValueError("NOTION_BOOKS_DB_ID not set. Please set the environment variable.")
        
        self.notion = NotionClient(NOTION_API_TOKEN)
        self.supabase = BooksSupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    
    def show_schema(self):
        """Display the Notion database schema to help with property mapping."""
        logger.info("Fetching database schema...")
        schema = self.notion.get_database_schema(NOTION_BOOKS_DB_ID)
        
        title_parts = schema.get('title', [])
        title = ''.join([t.get('plain_text', '') for t in title_parts])
        
        print(f"\n📚 Database: {title}")
        print(f"ID: {NOTION_BOOKS_DB_ID}")
        print("\nProperties:")
        print("-" * 60)
        
        for prop_name, prop_config in schema.get('properties', {}).items():
            prop_type = prop_config.get('type', 'unknown')
            print(f"  {prop_name}: {prop_type}")
        
        print()
    
    def sync(self, full_sync: bool = False, since_hours: int = 24) -> Dict:
        """
        Sync books from Notion to Supabase.
        
        Args:
            full_sync: If True, sync all books. If False, only recently updated.
            since_hours: For incremental sync, how far back to look.
        """
        start_time = time.time()
        stats = {'created': 0, 'updated': 0, 'errors': 0}
        
        logger.info(f"Starting book sync (full={full_sync})")
        
        # Build filter for incremental sync
        filter_query = None
        if not full_sync:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
            filter_query = {
                "timestamp": "last_edited_time",
                "last_edited_time": {"after": cutoff}
            }
        
        # Fetch from Notion
        try:
            notion_books = self.notion.query_database(NOTION_BOOKS_DB_ID, filter=filter_query)
            logger.info(f"Found {len(notion_books)} books in Notion")
        except Exception as e:
            logger.error(f"Failed to fetch from Notion: {e}")
            log_sync_event_sync('books_sync', 'error', str(e))
            raise
        
        # Get existing books from Supabase for comparison
        all_books = self.supabase.get_all_books()
        existing_by_notion_id = {b['notion_page_id']: b for b in all_books if b.get('notion_page_id')}

        # Also index orphan books (no notion_page_id) by title for linking
        orphan_books_by_title = {}
        for b in all_books:
            if not b.get('notion_page_id') and b.get('title'):
                # Normalize title for matching
                title_lower = b['title'].lower().strip()
                orphan_books_by_title[title_lower] = b

        if orphan_books_by_title:
            logger.info(f"Found {len(orphan_books_by_title)} orphan books that may be linked")

        # Sync each book
        for notion_book in notion_books:
            try:
                book_data = notion_book_to_supabase(notion_book)
                notion_id = book_data['notion_page_id']
                title = book_data.get('title', '').lower().strip()

                if notion_id in existing_by_notion_id:
                    # Update existing by notion_page_id
                    stats['updated'] += 1
                elif title in orphan_books_by_title:
                    # Found orphan book with matching title - link it!
                    orphan = orphan_books_by_title[title]
                    logger.info(f"Linking orphan book '{orphan['title']}' to Notion page {notion_id}")

                    # Preserve our data (summary, drive URLs, etc) but add notion_page_id
                    # Only update metadata from Notion, keep our enhanced data
                    book_data['id'] = orphan['id']  # Use existing ID

                    # Preserve our fields if Notion doesn't have them
                    for field in ['summary', 'drive_url', 'drive_file_id', 'original_drive_url',
                                  'original_drive_file_id', 'bookfusion_id', 'processed_at']:
                        if orphan.get(field) and not book_data.get(field):
                            book_data[field] = orphan[field]

                    stats['updated'] += 1
                    del orphan_books_by_title[title]  # Remove from orphan list
                else:
                    # Create new
                    stats['created'] += 1

                self.supabase.upsert_book(book_data)
                
            except Exception as e:
                logger.error(f"Error syncing book {notion_book.get('id')}: {e}")
                stats['errors'] += 1
        
        elapsed = time.time() - start_time
        logger.info(f"Sync complete: {stats['created']} created, {stats['updated']} updated, {stats['errors']} errors in {elapsed:.1f}s")
        
        log_sync_event_sync(
            'books_sync', 
            'success' if stats['errors'] == 0 else 'partial', 
            f"Synced {stats['created']} new, {stats['updated']} updated books"
        )
        
        return {
            'success': True,
            'stats': stats,
            'elapsed_seconds': elapsed
        }


# ============================================================================
# MAIN
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Entry point for sync."""
    service = BookSyncService()
    return service.sync(full_sync=full_sync, since_hours=since_hours)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Sync books from Notion to Supabase')
    parser.add_argument('--full', action='store_true', help='Full sync (default: incremental)')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back for incremental sync')
    parser.add_argument('--schema', action='store_true', help='Show Notion database schema')
    args = parser.parse_args()
    
    if args.schema:
        if not NOTION_BOOKS_DB_ID:
            print("Error: NOTION_BOOKS_DB_ID not set")
            exit(1)
        service = BookSyncService()
        service.show_schema()
    else:
        result = run_sync(full_sync=args.full, since_hours=args.hours)
        print(f"Result: {result}")
