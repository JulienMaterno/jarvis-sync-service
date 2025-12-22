"""List ALL databases accessible to the Notion integration."""
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')

headers = {
    'Authorization': f'Bearer {NOTION_API_TOKEN}',
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json'
}

def list_all_databases():
    """List all databases using the search endpoint with database filter."""
    print("\n📚 Querying ALL databases (no search filter)...\n")
    
    with httpx.Client(headers=headers, timeout=30.0) as client:
        # Use search with filter for databases only, no query
        response = client.post(
            'https://api.notion.com/v1/search',
            json={
                "filter": {"property": "object", "value": "database"},
                "page_size": 100
            }
        )
        response.raise_for_status()
        data = response.json()
        
        results = data.get('results', [])
        print(f"Found {len(results)} databases:\n")
        print("=" * 80)
        
        for db in results:
            db_id = db['id']
            title_parts = db.get('title', [])
            title = ''.join([t.get('plain_text', '') for t in title_parts]) or "(Untitled)"
            
            # Get parent info
            parent = db.get('parent', {})
            parent_type = parent.get('type', 'unknown')
            
            print(f"\n📖 {title}")
            print(f"   ID: {db_id}")
            print(f"   Parent: {parent_type}")
            
            # Show property names
            props = db.get('properties', {})
            prop_names = list(props.keys())[:10]  # First 10 properties
            print(f"   Properties: {', '.join(prop_names)}")
        
        print("\n" + "=" * 80)
        return results

if __name__ == '__main__':
    list_all_databases()
