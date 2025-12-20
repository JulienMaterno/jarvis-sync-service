"""
Cleanup script: Archive Notion reflection pages that don't exist in Supabase.

This handles the case where reflections were hard-deleted from Supabase
but their Notion pages still exist.

Usage:
    python cleanup_orphan_notion_reflections.py --dry-run  # Preview only
    python cleanup_orphan_notion_reflections.py            # Actually archive
"""

import os
import argparse
import logging
import httpx
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('CleanupOrphanReflections')

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_REFLECTIONS_DB_ID = os.environ.get('NOTION_REFLECTIONS_DB_ID', '2b3cd3f1-eb28-80a8-8999-e731bdaf433e')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


def get_all_notion_reflections():
    """Get all reflection pages from Notion."""
    headers = {
        'Authorization': f'Bearer {NOTION_API_TOKEN}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }
    
    results = []
    start_cursor = None
    
    with httpx.Client(headers=headers, timeout=30.0) as client:
        while True:
            body = {"page_size": 100}
            if start_cursor:
                body["start_cursor"] = start_cursor
            
            response = client.post(
                f'https://api.notion.com/v1/databases/{NOTION_REFLECTIONS_DB_ID}/query',
                json=body
            )
            response.raise_for_status()
            data = response.json()
            
            results.extend(data.get('results', []))
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
    
    return results


def get_all_supabase_reflection_notion_ids():
    """Get all Notion page IDs from Supabase reflections (including soft-deleted)."""
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    }
    
    with httpx.Client(headers=headers, timeout=30.0) as client:
        # Get ALL reflections (including deleted) to see what's linked
        url = f"{SUPABASE_URL}/rest/v1/reflections?select=notion_page_id"
        response = client.get(url)
        response.raise_for_status()
        data = response.json()
    
    # Return set of notion_page_ids
    return {r.get('notion_page_id') for r in data if r.get('notion_page_id')}


def archive_notion_page(page_id: str):
    """Archive a Notion page."""
    headers = {
        'Authorization': f'Bearer {NOTION_API_TOKEN}',
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
    }
    
    with httpx.Client(headers=headers, timeout=30.0) as client:
        response = client.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            json={"archived": True}
        )
        response.raise_for_status()


def get_page_title(page: dict) -> str:
    """Extract title from Notion page."""
    props = page.get('properties', {})
    title_prop = props.get('Name', {}).get('title', [])
    return title_prop[0].get('plain_text', 'Untitled') if title_prop else 'Untitled'


def main(dry_run: bool = True):
    logger.info("=" * 60)
    logger.info("CLEANUP ORPHAN NOTION REFLECTIONS")
    logger.info(f"Mode: {'DRY RUN (preview only)' if dry_run else 'LIVE (will archive pages)'}")
    logger.info("=" * 60)
    
    # Get all Notion reflection pages
    logger.info("Fetching Notion reflections...")
    notion_pages = get_all_notion_reflections()
    logger.info(f"Found {len(notion_pages)} Notion reflection pages")
    
    # Get all Supabase reflection Notion IDs
    logger.info("Fetching Supabase reflection links...")
    supabase_notion_ids = get_all_supabase_reflection_notion_ids()
    logger.info(f"Found {len(supabase_notion_ids)} Supabase reflections with Notion links")
    
    # Find orphans (in Notion but not in Supabase)
    orphans = []
    for page in notion_pages:
        page_id = page.get('id')
        if page_id not in supabase_notion_ids:
            orphans.append(page)
    
    logger.info(f"Found {len(orphans)} orphan Notion pages (not linked to any Supabase reflection)")
    
    if not orphans:
        logger.info("No orphans to clean up!")
        return
    
    # Show orphans
    logger.info("\nOrphan pages:")
    for page in orphans:
        title = get_page_title(page)
        created = page.get('created_time', '')[:10]
        logger.info(f"  - {title} (created: {created})")
    
    if dry_run:
        logger.info(f"\n[DRY RUN] Would archive {len(orphans)} pages. Run without --dry-run to actually archive.")
        return
    
    # Archive orphans
    logger.info(f"\nArchiving {len(orphans)} orphan pages...")
    archived = 0
    for page in orphans:
        page_id = page.get('id')
        title = get_page_title(page)
        try:
            archive_notion_page(page_id)
            archived += 1
            logger.info(f"  ✓ Archived: {title}")
        except Exception as e:
            logger.error(f"  ✗ Failed to archive {title}: {e}")
    
    logger.info(f"\nDone! Archived {archived}/{len(orphans)} pages.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cleanup orphan Notion reflection pages')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, do not archive')
    
    args = parser.parse_args()
    main(dry_run=args.dry_run)
