"""Search for pages and databases containing BookFusion/Reading content."""
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

def search_all(query: str = ""):
    """Search for both pages AND databases."""
    print(f"\n🔍 Searching for everything{' matching: ' + query if query else ''}...\n")
    
    with httpx.Client(headers=headers, timeout=30.0) as client:
        body = {"page_size": 100}
        if query:
            body["query"] = query
        
        response = client.post('https://api.notion.com/v1/search', json=body)
        response.raise_for_status()
        data = response.json()
        
        results = data.get('results', [])
        print(f"Found {len(results)} results:\n")
        
        for item in results:
            obj_type = item.get('object', 'unknown')
            item_id = item['id']
            
            if obj_type == 'database':
                title_parts = item.get('title', [])
                title = ''.join([t.get('plain_text', '') for t in title_parts]) or "(Untitled DB)"
                print(f"📊 DATABASE: {title}")
                print(f"   ID: {item_id}")
                
            elif obj_type == 'page':
                props = item.get('properties', {})
                # Try to get title from various property types
                title = "(Untitled Page)"
                for prop_name, prop_value in props.items():
                    if prop_value.get('type') == 'title':
                        title_arr = prop_value.get('title', [])
                        if title_arr:
                            title = title_arr[0].get('plain_text', title)
                            break
                
                print(f"📄 PAGE: {title}")
                print(f"   ID: {item_id}")
                
                # Check if this page has child databases
                parent = item.get('parent', {})
                print(f"   Parent type: {parent.get('type', 'unknown')}")
            
            print()
        
        return results

def list_child_databases_of_page(page_id: str):
    """List child blocks of a page to find inline databases."""
    print(f"\n📂 Checking children of page {page_id}...\n")
    
    with httpx.Client(headers=headers, timeout=30.0) as client:
        response = client.get(
            f'https://api.notion.com/v1/blocks/{page_id}/children',
            params={'page_size': 100}
        )
        response.raise_for_status()
        data = response.json()
        
        for block in data.get('results', []):
            block_type = block.get('type', 'unknown')
            block_id = block['id']
            
            if block_type in ['child_database', 'linked_database']:
                print(f"🗄️ Found {block_type}: {block_id}")
            elif block_type == 'child_page':
                print(f"📄 Child page: {block_id}")

if __name__ == '__main__':
    # First, search for anything related to books/reading
    print("=" * 60)
    print("SEARCH 1: 'BookFusion'")
    print("=" * 60)
    search_all("BookFusion")
    
    print("=" * 60)
    print("SEARCH 2: 'Reading'")
    print("=" * 60)
    search_all("Reading")
    
    print("=" * 60)
    print("SEARCH 3: 'Content'")
    print("=" * 60)
    search_all("Content")
    
    print("=" * 60)
    print("SEARCH 4: 'Highlight'")
    print("=" * 60)
    search_all("Highlight")
    
    print("=" * 60)
    print("SEARCH 5: Empty (all accessible)")
    print("=" * 60)
    search_all("")
