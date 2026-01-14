"""Compare applications between Supabase and Notion to find mismatches."""
import os
from dotenv import load_dotenv
load_dotenv()

from supabase import create_client
from lib.notion_client import NotionClient

c = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))
n = NotionClient(os.getenv('NOTION_API_TOKEN'))

# Get apps
apps = c.table('applications').select('name, status, application_type, last_sync_source, notion_page_id').limit(20).execute()

print('=== COMPARING STATUS VALUES ===')
mismatches = []
for app in apps.data:
    notion_id = app.get('notion_page_id')
    if not notion_id:
        continue
    
    # Get from Notion
    try:
        notion_page = n.client.get(f'https://api.notion.com/v1/pages/{notion_id}').json()
        props = notion_page.get('properties', {})
        notion_status = props.get('Status', {}).get('select', {})
        notion_status_val = notion_status.get('name', 'None') if notion_status else 'None'
        notion_type = props.get('Type', {}).get('select', {})
        notion_type_val = notion_type.get('name', 'None') if notion_type else 'None'
        
        sb_status = app.get('status', 'None')
        sb_type = app.get('application_type', 'None')
        
        if sb_status != notion_status_val or sb_type != notion_type_val:
            name = app['name'][:40]
            print(f'{name}')
            print(f'  Status: SB="{sb_status}" vs Notion="{notion_status_val}"')
            print(f'  Type: SB="{sb_type}" vs Notion="{notion_type_val}"')
            print(f'  Last sync source: {app["last_sync_source"]}')
            mismatches.append(app['name'])
    except Exception as e:
        print(f'Error for {app["name"]}: {e}')

print(f'\nFound {len(mismatches)} mismatches out of {len(apps.data)} checked')
