#!/usr/bin/env python3
"""
===================================================================================
BOOKS SYNC SERVICE - One-Way Notion → Supabase
===================================================================================

Syncs books from Notion Content database to Supabase.
This is a READ-ONLY sync - all book data originates in Notion.

Usage:
    python sync_books_unified.py                    # Incremental sync (last 24h)
    python sync_books_unified.py --full             # Full sync
    python sync_books_unified.py --hours 48         # Last 48 hours
    python sync_books_unified.py --schema           # Show Notion schema

Database: books (Supabase) ← Content (Notion)
Direction: ONE-WAY (Notion is source of truth)
"""

import os
import sys

# Add lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

from lib.sync_base import (
    OneWaySyncService,
    NotionPropertyExtractor as Extract,
    NotionClient,
    create_cli_parser,
    setup_logger,
    NOTION_API_TOKEN
)
from typing import Dict

# ============================================================================
# CONFIGURATION
# ============================================================================

# TODO: Replace with actual database ID once user shares it with Jarvis
NOTION_BOOKS_DATABASE_ID = os.environ.get('NOTION_BOOKS_DATABASE_ID', 'YOUR_DATABASE_ID_HERE')
SUPABASE_TABLE = 'books'


# ============================================================================
# BOOKS SYNC SERVICE
# ============================================================================

class BooksSyncService(OneWaySyncService):
    """
    One-way sync for Books: Notion → Supabase
    
    Notion Property Mapping (adjust based on actual schema):
    - Name (title) → title
    - Author (rich_text) → author
    - Status (select) → status
    - Rating (number) → rating
    - Date Read (date) → date_read
    - Genre (multi_select) → genres
    - Notes (rich_text) → notes
    - URL (url) → url
    - Cover (files) → cover_url
    """
    
    def __init__(self):
        super().__init__(
            service_name='books_sync',
            notion_database_id=NOTION_BOOKS_DATABASE_ID,
            supabase_table=SUPABASE_TABLE
        )
    
    def convert_from_source(self, notion_record: Dict) -> Dict:
        """Convert Notion book page to Supabase format."""
        props = notion_record.get('properties', {})
        
        # Extract values using helpers
        return {
            'title': Extract.title(props, 'Name'),
            'author': Extract.rich_text(props, 'Author'),
            'status': Extract.select(props, 'Status'),  # "Reading", "Read", "To Read"
            'rating': Extract.number(props, 'Rating'),
            'date_read': Extract.date(props, 'Date Read'),
            'genres': Extract.multi_select(props, 'Genre'),
            'notes': Extract.rich_text(props, 'Notes'),
            'url': Extract.url(props, 'URL'),
            # Files need special handling
            'cover_url': self._extract_cover(props),
        }
    
    def _extract_cover(self, props: Dict) -> str:
        """Extract cover image URL from files property."""
        files = props.get('Cover', {}).get('files', [])
        if not files:
            return None
        
        file = files[0]
        if file.get('type') == 'external':
            return file.get('external', {}).get('url')
        elif file.get('type') == 'file':
            return file.get('file', {}).get('url')
        return None


# ============================================================================
# MAIN
# ============================================================================

def show_schema():
    """Display the Notion database schema."""
    if NOTION_BOOKS_DATABASE_ID == 'YOUR_DATABASE_ID_HERE':
        print("❌ Database ID not configured!")
        print("   Set NOTION_BOOKS_DATABASE_ID environment variable")
        print("   Or update the NOTION_BOOKS_DATABASE_ID constant in this file")
        return
    
    notion = NotionClient(NOTION_API_TOKEN)
    
    try:
        schema = notion.get_database_schema(NOTION_BOOKS_DATABASE_ID)
        
        print(f"\n📚 BOOKS DATABASE SCHEMA")
        print(f"{'='*60}")
        print(f"Title: {schema.get('title', [{}])[0].get('plain_text', 'Untitled')}")
        print(f"ID: {schema.get('id')}")
        print(f"\nProperties:")
        print(f"{'-'*60}")
        
        for name, prop in schema.get('properties', {}).items():
            prop_type = prop.get('type', 'unknown')
            
            # Get additional info based on type
            extra = ""
            if prop_type == 'select':
                options = [o.get('name') for o in prop.get('select', {}).get('options', [])]
                extra = f" → [{', '.join(options[:5])}{'...' if len(options) > 5 else ''}]"
            elif prop_type == 'multi_select':
                options = [o.get('name') for o in prop.get('multi_select', {}).get('options', [])]
                extra = f" → [{', '.join(options[:5])}{'...' if len(options) > 5 else ''}]"
            
            print(f"  • {name:25} ({prop_type}){extra}")
            
    except Exception as e:
        print(f"❌ Failed to get schema: {e}")


def run_sync(full: bool = False, hours: int = 24) -> Dict:
    """Run the books sync."""
    if NOTION_BOOKS_DATABASE_ID == 'YOUR_DATABASE_ID_HERE':
        return {
            'success': False,
            'error': 'Database ID not configured. Set NOTION_BOOKS_DATABASE_ID.'
        }
    
    service = BooksSyncService()
    result = service.sync(full_sync=full, since_hours=hours)
    return result.to_dict()


if __name__ == '__main__':
    parser = create_cli_parser('Books')
    args = parser.parse_args()
    
    if args.schema:
        show_schema()
    else:
        result = run_sync(full=args.full, hours=args.hours)
        
        if result.get('success'):
            stats = result.get('stats', {})
            print(f"\n✅ Books sync complete!")
            print(f"   Created: {stats.get('created', 0)}")
            print(f"   Updated: {stats.get('updated', 0)}")
            print(f"   Errors: {stats.get('errors', 0)}")
            print(f"   Time: {result.get('elapsed_seconds', 0):.1f}s")
        else:
            print(f"\n❌ Sync failed: {result.get('error_message')}")
