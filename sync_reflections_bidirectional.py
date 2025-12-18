"""
Bidirectional Notion ↔ Supabase Reflections Sync Service

Syncs reflections/thoughts between Notion and Supabase:
- Supabase → Notion: Reflections created from voice pipeline
- Notion → Supabase: Reflections created/updated manually in Notion

Based on sync_tasks_bidirectional.py structure.

Usage:
    python sync_reflections_bidirectional.py --full    # Full sync
    python sync_reflections_bidirectional.py           # Incremental (last 24h)
"""

import os
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
import httpx
from lib.utils import retry_on_error_sync

load_dotenv()

# Import logging service
try:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from lib.logging_service import log_sync_event_sync
    HAS_LOGGING_SERVICE = True
except ImportError:
    HAS_LOGGING_SERVICE = False
    def log_sync_event_sync(event_type, status, message, **kwargs):
        pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('ReflectionSync')

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_REFLECTIONS_DB_ID = os.environ.get('NOTION_REFLECTIONS_DB_ID', '2b3cd3f1-eb28-80a8-8999-e731bdaf433e')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# ============================================================================
# NOTION CLIENT
# ============================================================================

class NotionClient:
    """Notion API client for reflections."""
    
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
        sorts: Optional[List[Dict]] = None,
        page_size: int = 100,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """Query all pages from a database with pagination."""
        results = []
        start_cursor = None
        
        while True:
            body = {"page_size": min(page_size, limit) if limit else page_size}
            if filter:
                body["filter"] = filter
            if sorts:
                body["sorts"] = sorts
            if start_cursor:
                body["start_cursor"] = start_cursor
            
            response = self.client.post(
                f'https://api.notion.com/v1/databases/{database_id}/query',
                json=body
            )
            response.raise_for_status()
            data = response.json()
            
            results.extend(data.get('results', []))
            
            if limit and len(results) >= limit:
                return results[:limit]
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return results
    
    @retry_on_error_sync()
    def get_page(self, page_id: str) -> Dict:
        """Get a single page by ID."""
        response = self.client.get(f'https://api.notion.com/v1/pages/{page_id}')
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def get_page_content(self, page_id: str) -> str:
        """Get page content as text."""
        response = self.client.get(f'https://api.notion.com/v1/blocks/{page_id}/children')
        response.raise_for_status()
        blocks = response.json().get('results', [])
        
        content_parts = []
        for block in blocks:
            block_type = block.get('type')
            if block_type in ('paragraph', 'heading_1', 'heading_2', 'heading_3', 'bulleted_list_item', 'numbered_list_item'):
                rich_text = block.get(block_type, {}).get('rich_text', [])
                text = ''.join([t.get('plain_text', '') for t in rich_text])
                if text:
                    content_parts.append(text)
        
        return '\n\n'.join(content_parts)
    
    @retry_on_error_sync()
    def create_page(self, database_id: str, properties: Dict, blocks: List[Dict] = None) -> Dict:
        """Create a new page in a database."""
        body = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        if blocks:
            body["children"] = blocks
        
        response = self.client.post('https://api.notion.com/v1/pages', json=body)
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def update_page(self, page_id: str, properties: Dict) -> Dict:
        """Update an existing page."""
        response = self.client.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            json={"properties": properties}
        )
        response.raise_for_status()
        return response.json()


# ============================================================================
# SUPABASE CLIENT
# ============================================================================

class SupabaseClient:
    """Supabase client for reflections."""
    
    def __init__(self, url: str, key: str):
        self.base_url = f"{url}/rest/v1"
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    def get_all_reflections(self) -> List[Dict]:
        """Get all reflections from Supabase."""
        url = f"{self.base_url}/reflections?select=*&order=created_at.desc&deleted_at=is.null"
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_reflection_by_notion_id(self, notion_page_id: str) -> Optional[Dict]:
        """Find a reflection by its Notion page ID."""
        url = f"{self.base_url}/reflections?select=*&notion_page_id=eq.{notion_page_id}&limit=1"
        response = self.client.get(url)
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    
    def create_reflection(self, data: Dict) -> Dict:
        """Create a new reflection."""
        response = self.client.post(f"{self.base_url}/reflections", json=data)
        response.raise_for_status()
        return response.json()[0]
    
    def update_reflection(self, reflection_id: str, updates: Dict) -> Dict:
        """Update an existing reflection."""
        response = self.client.patch(
            f"{self.base_url}/reflections?id=eq.{reflection_id}",
            json=updates
        )
        response.raise_for_status()
        return response.json()[0] if response.json() else {}


# ============================================================================
# CONVERSION FUNCTIONS
# ============================================================================

def notion_reflection_to_supabase(notion_reflection: Dict, notion: NotionClient) -> Dict:
    """Convert Notion reflection properties to Supabase format."""
    props = notion_reflection.get('properties', {})
    page_id = notion_reflection.get('id')
    
    # Extract title
    title_prop = props.get('Name', {}).get('title', [])
    title = title_prop[0].get('plain_text', 'Untitled') if title_prop else 'Untitled'
    
    # Extract date
    date_prop = props.get('Date', {}).get('date')
    date = date_prop.get('start') if date_prop else None
    
    # Extract location/place
    place_prop = props.get('Place', {}).get('rich_text', [])
    location = place_prop[0].get('plain_text', '') if place_prop else None
    
    # Extract tags
    tags_prop = props.get('Tags', {}).get('multi_select', [])
    tags = [t.get('name') for t in tags_prop]
    
    # Get content from page blocks
    try:
        content = notion.get_page_content(page_id)
    except:
        content = ''
    
    return {
        'title': title,
        'date': date,
        'location': location if location else None,
        'tags': tags,
        'content': content,
    }


def supabase_reflection_to_notion(reflection: Dict) -> Tuple[Dict, List[Dict]]:
    """Convert Supabase reflection to Notion properties and blocks format."""
    title = reflection.get('title', 'Untitled')
    date = reflection.get('date')
    location = reflection.get('location')
    tags = reflection.get('tags', [])
    sections = reflection.get('sections', [])
    content = reflection.get('content', '')
    
    # Build properties
    properties = {
        'Name': {
            'title': [{'text': {'content': title[:100]}}]
        },
    }
    
    if date:
        properties['Date'] = {
            'date': {'start': date}
        }
    
    if location:
        properties['Place'] = {
            'rich_text': [{'text': {'content': location[:200]}}]
        }
    
    if tags:
        # Only include tags that exist in Notion (multi_select creates new ones if needed)
        properties['Tags'] = {
            'multi_select': [{'name': tag[:100]} for tag in tags[:10]]
        }
    
    # Build content blocks from sections
    blocks = []
    
    if sections:
        for section in sections:
            heading = section.get('heading', '')
            section_content = section.get('content', '')
            
            if heading:
                blocks.append({
                    'type': 'heading_2',
                    'heading_2': {
                        'rich_text': [{'text': {'content': heading[:100]}}]
                    }
                })
            
            if section_content:
                # Split content into chunks (Notion has 2000 char limit per block)
                for i in range(0, len(section_content), 1900):
                    chunk = section_content[i:i+1900]
                    blocks.append({
                        'type': 'paragraph',
                        'paragraph': {
                            'rich_text': [{'text': {'content': chunk}}]
                        }
                    })
    elif content:
        # Fallback to raw content if no sections
        for i in range(0, len(content), 1900):
            chunk = content[i:i+1900]
            blocks.append({
                'type': 'paragraph',
                'paragraph': {
                    'rich_text': [{'text': {'content': chunk}}]
                }
            })
    
    return properties, blocks


# ============================================================================
# SYNC FUNCTIONS
# ============================================================================

def sync_notion_to_supabase(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync reflections from Notion → Supabase."""
    created = 0
    updated = 0
    skipped = 0
    
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours) if not full_sync else None
    
    # Query Notion reflections
    filter_obj = None
    if not full_sync and since:
        filter_obj = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": since.isoformat()}
        }
    
    reflections = notion.query_database(
        NOTION_REFLECTIONS_DB_ID,
        filter=filter_obj,
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}]
    )
    
    logger.info(f"Found {len(reflections)} reflections in Notion to process")
    
    for notion_ref in reflections:
        notion_page_id = notion_ref['id']
        last_edited = notion_ref.get('last_edited_time', '')
        
        try:
            # Check if reflection exists in Supabase
            existing = supabase.get_reflection_by_notion_id(notion_page_id)
            
            if existing:
                # Check if Notion is newer
                supabase_updated = existing.get('notion_updated_at', '')
                last_sync_source = existing.get('last_sync_source', '')
                
                if last_sync_source == 'supabase':
                    skipped += 1
                    continue
                
                if supabase_updated and last_edited <= supabase_updated:
                    skipped += 1
                    continue
                
                # Update Supabase
                ref_data = notion_reflection_to_supabase(notion_ref, notion)
                ref_data['notion_updated_at'] = last_edited
                ref_data['last_sync_source'] = 'notion'
                
                supabase.update_reflection(existing['id'], ref_data)
                updated += 1
                logger.info(f"Updated Supabase reflection: {ref_data['title']}")
            else:
                # Create in Supabase
                ref_data = notion_reflection_to_supabase(notion_ref, notion)
                ref_data['notion_page_id'] = notion_page_id
                ref_data['notion_updated_at'] = last_edited
                ref_data['last_sync_source'] = 'notion'
                
                supabase.create_reflection(ref_data)
                created += 1
                logger.info(f"Created Supabase reflection: {ref_data['title']}")
                log_sync_event_sync("create_supabase_reflection", "success", f"Created reflection '{ref_data['title']}'")
                
        except Exception as e:
            logger.error(f"Error syncing Notion reflection {notion_page_id}: {e}")
    
    logger.info(f"Notion → Supabase: {created} created, {updated} updated, {skipped} skipped")
    return created, updated, skipped


def sync_supabase_to_notion(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync reflections from Supabase → Notion."""
    created = 0
    updated = 0
    skipped = 0
    
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours) if not full_sync else None
    
    # Get all reflections from Supabase
    all_reflections = supabase.get_all_reflections()
    
    if not full_sync:
        # Filter to reflections needing sync
        reflections = []
        for r in all_reflections:
            notion_page_id = r.get('notion_page_id')
            updated_at = r.get('updated_at', '')
            
            if not notion_page_id:
                reflections.append(r)
            elif since and updated_at:
                try:
                    updated_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                    if updated_dt > since:
                        reflections.append(r)
                except:
                    pass
        logger.info(f"Incremental mode: {len(reflections)} reflections need syncing (of {len(all_reflections)} total)")
    else:
        reflections = all_reflections
        logger.info(f"Full sync mode: processing all {len(reflections)} reflections")
    
    for reflection in reflections:
        reflection_id = reflection.get('id')
        notion_page_id = reflection.get('notion_page_id')
        
        try:
            if notion_page_id:
                # Already linked - check if we need to update Notion
                ref_updated = reflection.get('updated_at', '')
                notion_updated = reflection.get('notion_updated_at', '')
                last_sync_source = reflection.get('last_sync_source', '')
                
                if last_sync_source == 'notion':
                    skipped += 1
                    continue
                
                if notion_updated and ref_updated <= notion_updated:
                    skipped += 1
                    continue
                
                # Update Notion (properties only, not content blocks)
                try:
                    props, _ = supabase_reflection_to_notion(reflection)
                    notion.update_page(notion_page_id, props)
                    
                    supabase.update_reflection(reflection_id, {
                        'notion_updated_at': datetime.now(timezone.utc).isoformat(),
                        'last_sync_source': 'supabase'
                    })
                    updated += 1
                    logger.info(f"Updated Notion reflection: {reflection['title']}")
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        logger.warning(f"Notion page {notion_page_id} not found - unlinking")
                        supabase.update_reflection(reflection_id, {'notion_page_id': None, 'notion_updated_at': None})
                        skipped += 1
                    else:
                        raise
            else:
                # Not linked - create in Notion
                props, blocks = supabase_reflection_to_notion(reflection)
                logger.info(f"Creating Notion reflection: {reflection['title']}")
                
                try:
                    # Try with blocks first
                    new_page = notion.create_page(NOTION_REFLECTIONS_DB_ID, props, blocks[:100])
                except Exception as e:
                    logger.warning(f"Failed with blocks, trying without: {e}")
                    new_page = notion.create_page(NOTION_REFLECTIONS_DB_ID, props, [])
                
                new_page_id = new_page['id']
                
                supabase.update_reflection(reflection_id, {
                    'notion_page_id': new_page_id,
                    'notion_updated_at': new_page.get('last_edited_time'),
                    'last_sync_source': 'supabase'
                })
                created += 1
                log_sync_event_sync("create_notion_reflection", "success", f"Created Notion reflection '{reflection['title']}'")
                
        except Exception as e:
            logger.error(f"Error syncing Supabase reflection {reflection_id} ({reflection.get('title', 'Unknown')}): {e}")
    
    logger.info(f"Supabase → Notion: {created} created, {updated} updated, {skipped} skipped")
    return created, updated, skipped


def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run bidirectional reflection sync."""
    start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("BIDIRECTIONAL REFLECTION SYNC")
    logger.info(f"Mode: {'FULL' if full_sync else f'INCREMENTAL ({since_hours}h)'}")
    logger.info("=" * 60)
    
    log_sync_event_sync(
        "reflection_sync_start", "info",
        f"Starting {'full' if full_sync else 'incremental'} reflection sync"
    )
    
    # Initialize clients
    notion = NotionClient(NOTION_API_TOKEN)
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    
    # Run syncs
    logger.info("--- NOTION → SUPABASE ---")
    n2s_created, n2s_updated, n2s_skipped = sync_notion_to_supabase(
        notion, supabase, full_sync, since_hours
    )
    
    logger.info("--- SUPABASE → NOTION ---")
    s2n_created, s2n_updated, s2n_skipped = sync_supabase_to_notion(
        notion, supabase, full_sync, since_hours
    )
    
    elapsed = time.time() - start_time
    total_ops = n2s_created + n2s_updated + s2n_created + s2n_updated
    
    logger.info("=" * 60)
    logger.info("REFLECTION SYNC COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Notion → Supabase: {n2s_created} created, {n2s_updated} updated, {n2s_skipped} skipped")
    logger.info(f"Supabase → Notion: {s2n_created} created, {s2n_updated} updated, {s2n_skipped} skipped")
    logger.info(f"Total operations: {total_ops} in {elapsed:.1f}s")
    logger.info("=" * 60)
    
    log_sync_event_sync(
        "reflection_sync_complete", "success",
        f"Reflection sync complete: {total_ops} operations in {elapsed:.1f}s"
    )
    
    return {
        'notion_to_supabase': {'created': n2s_created, 'updated': n2s_updated, 'skipped': n2s_skipped},
        'supabase_to_notion': {'created': s2n_created, 'updated': s2n_updated, 'skipped': s2n_skipped},
        'total_operations': total_ops,
        'elapsed_seconds': elapsed
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Bidirectional Notion ↔ Supabase Reflection Sync')
    parser.add_argument('--full', action='store_true', help='Full sync (all reflections)')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back for incremental sync')
    
    args = parser.parse_args()
    
    result = run_sync(full_sync=args.full, since_hours=args.hours)
    print(f"\nResult: {result}")
