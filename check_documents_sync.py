"""Check documents sync status."""
from supabase import create_client
import os
from dotenv import load_dotenv

load_dotenv()

db = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Check documents
docs = db.table('documents').select('id, title, type, notion_page_id, last_sync_source').limit(20).execute()
print('=== DOCUMENTS (first 20) ===')
synced = 0
not_synced = 0
for d in docs.data:
    has_notion = 'Yes' if d.get('notion_page_id') else 'No'
    sync_src = d.get('last_sync_source', 'N/A')
    doc_name = (d.get('title') or 'Untitled')[:35]
    doc_type = (d.get('type') or 'N/A')[:12]
    print(f"{doc_name:35} | Type: {doc_type:12} | Notion: {has_notion:3} | Sync: {sync_src}")
    if has_notion == 'Yes':
        synced += 1
    else:
        not_synced += 1

print(f"\nTotal: {len(docs.data)} | Synced to Notion: {synced} | Not synced: {not_synced}")
