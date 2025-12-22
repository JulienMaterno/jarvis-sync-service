"""
Check if the Books databases are now accessible.
Sometimes Notion's API takes a moment to update permissions.
"""
import os
import httpx
from dotenv import load_dotenv
import time

load_dotenv()

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')

headers = {
    'Authorization': f'Bearer {NOTION_API_TOKEN}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}

def check_databases():
    """Check all accessible databases."""
    with httpx.Client(headers=headers, timeout=30.0) as client:
        response = client.post(
            'https://api.notion.com/v1/search',
            json={
                "filter": {"property": "object", "value": "database"},
                "page_size": 100
            }
        )
        response.raise_for_status()
        databases = response.json().get('results', [])
        
        print(f"\n{'='*60}")
        print(f"Found {len(databases)} databases:")
        print(f"{'='*60}\n")
        
        for db in databases:
            title_parts = db.get('title', [])
            title = ''.join([t.get('plain_text', '') for t in title_parts]) or "(Untitled)"
            db_id = db['id']
            
            # Check if this looks like books/highlights
            title_lower = title.lower()
            is_new = any(x in title_lower for x in ['content', 'book', 'highlight', 'reading'])
            marker = " 📚 NEW!" if is_new else ""
            
            print(f"  {title}{marker}")
            print(f"    ID: {db_id}")
            
            # Show properties for new ones
            if is_new or len(databases) <= 7:
                props = db.get('properties', {})
                prop_names = list(props.keys())
                print(f"    Properties: {', '.join(prop_names)}")
            print()
        
        return databases

if __name__ == '__main__':
    print("\nChecking Notion databases...")
    print("(If you just shared new databases, they may take a moment to appear)\n")
    
    dbs = check_databases()
    
    if len(dbs) == 5:
        print("\n⚠️  Still only seeing the original 5 databases.")
        print("\nTo share the Books/Highlights databases:")
        print("  1. Open the 'Content' or 'Highlights' database in Notion")
        print("  2. Click '•••' (three dots) in the top right")
        print("  3. Click 'Connections' or 'Add connections'")
        print("  4. Select your integration (e.g., 'Jarvis')")
        print("\nAlternatively, if the page name is 'Reading List' or 'BookFusion':")
        print("  - Share that parent page with the integration")
        print("  - Make sure 'Include sub-pages' is enabled")
    else:
        print(f"\n✅ Found {len(dbs)} databases - new ones detected!")
