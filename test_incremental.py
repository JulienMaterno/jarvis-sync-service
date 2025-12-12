import logging
from lib.notion_sync import sync_notion_to_supabase, sync_supabase_to_notion

# Configure logging
logging.basicConfig(level=logging.INFO)

def main():
    print("Testing Incremental Sync...")
    
    print("\n--- Notion -> Supabase (Should be incremental) ---")
    try:
        res1 = sync_notion_to_supabase()
        print("Result:", res1)
    except Exception as e:
        print("Error:", e)
        import traceback
        traceback.print_exc()

    print("\n--- Supabase -> Notion (Should handle deletions) ---")
    try:
        res2 = sync_supabase_to_notion()
        print("Result:", res2)
    except Exception as e:
        print("Error:", e)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
