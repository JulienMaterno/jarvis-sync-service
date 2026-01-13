"""Debug content extraction."""
from lib.sync_base import NotionClient
import os
from dotenv import load_dotenv
load_dotenv()

notion = NotionClient(os.environ['NOTION_API_TOKEN'])

# Get first application page ID
from lib.supabase_client import supabase
app = supabase.table('applications').select('notion_page_id,name').limit(1).execute().data[0]
print(f"App: {app['name']}")
print(f"Page ID: {app['notion_page_id']}")

# Extract content directly
result = notion.extract_page_content(app['notion_page_id'])
print(f"Result type: {type(result)}")
print(f"Result: {repr(result)}")
print(f"Result[0] (text): {repr(result[0])[:100]}")
print(f"Result[1] (has_unsupported): {result[1]}")
