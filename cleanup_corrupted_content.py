"""
Cleanup corrupted application content fields.

The bug: Before Jan 10, 2026, content extraction was storing the raw tuple
(text, has_unsupported) as JSON, resulting in '["",false]' or similar values.

This corrupted data then synced TO Notion, so Notion pages also have the bad content.

This script:
1. Finds applications with corrupted content  
2. Clears the Notion page content (deletes blocks)
3. Sets Supabase content to empty string
"""
import os
import sys
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

from lib.sync_base import NotionClient, SupabaseClient

NOTION_APPLICATIONS_DB_ID = os.environ.get(
    'NOTION_APPLICATIONS_DB_ID', 
    'bfb77dff-9721-47b6-9bab-0cd0b315a298'
)

def is_corrupted_content(content):
    """Check if content looks like a JSON-serialized tuple."""
    if not content:
        return False
    content_str = str(content)
    # Patterns that indicate tuple serialization
    return (
        content_str.startswith('["') and 
        ('false]' in content_str or 'true]' in content_str or ', false]' in content_str or ', true]' in content_str)
    )

def main(dry_run=True):
    print(f"{'DRY RUN - ' if dry_run else ''}Cleaning corrupted application content\n")
    
    notion = NotionClient(os.environ['NOTION_API_TOKEN'])
    supabase = SupabaseClient(
        os.environ['SUPABASE_URL'],
        os.environ['SUPABASE_KEY'],
        'applications'
    )
    
    # Get all applications
    apps = supabase.select_all()
    print(f"Total applications: {len(apps)}")
    
    # Find corrupted ones
    corrupted = [a for a in apps if is_corrupted_content(a.get('content'))]
    print(f"Corrupted applications: {len(corrupted)}")
    
    if not corrupted:
        print("No corrupted content found!")
        return
    
    fixed = 0
    errors = 0
    
    for app in corrupted:
        name = app.get('name', 'Unknown')
        notion_page_id = app.get('notion_page_id')
        
        print(f"\n  Processing: {name}")
        print(f"    Old content: {repr(app.get('content'))[:50]}")
        
        if not notion_page_id:
            print(f"    ⚠️ No Notion page ID - skipping")
            continue
        
        try:
            if not dry_run:
                # Step 1: Clear Notion page content (delete all blocks)
                existing_blocks = notion.get_all_blocks(notion_page_id)
                deleted_count = 0
                for block in existing_blocks:
                    try:
                        notion.delete_block(block['id'])
                        deleted_count += 1
                    except Exception:
                        pass
                print(f"    Deleted {deleted_count} blocks from Notion page")
                
                # Step 2: Update Supabase with empty content
                supabase.update(app['id'], {
                    'content': '',  # Clear the corrupted content
                    # Don't change last_sync_source - this is a data fix
                })
                print(f"    ✅ Fixed!")
            else:
                print(f"    Would clear Notion blocks and set content='' (dry run)")
            
            fixed += 1
            
        except Exception as e:
            print(f"    ❌ Error: {e}")
            errors += 1
    
    print(f"\n{'DRY RUN - ' if dry_run else ''}Summary:")
    print(f"  Fixed: {fixed}")
    print(f"  Errors: {errors}")
    
    if dry_run:
        print(f"\nRun with --apply to actually fix the data")

if __name__ == '__main__':
    dry_run = '--apply' not in sys.argv
    main(dry_run=dry_run)
