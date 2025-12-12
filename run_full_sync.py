import asyncio
import logging
from lib.sync_service import sync_contacts
from lib.notion_sync import sync_notion_to_supabase, sync_supabase_to_notion

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_full_sync_cycle():
    logger.info("=== Starting Full Sync Cycle ===")
    
    # 1. Notion -> Supabase
    # Bring in any changes from Notion first (like the location change)
    logger.info("--- Step 1: Notion -> Supabase ---")
    try:
        sync_notion_to_supabase()
    except Exception as e:
        logger.error(f"Notion -> Supabase failed: {e}")

    # 2. Google <-> Supabase
    # Sync Supabase (now updated from Notion) with Google
    logger.info("--- Step 2: Google <-> Supabase ---")
    try:
        await sync_contacts()
    except Exception as e:
        logger.error(f"Google <-> Supabase failed: {e}")

    # 3. Supabase -> Notion
    # Push any Google updates (or new Google IDs) back to Notion
    logger.info("--- Step 3: Supabase -> Notion ---")
    try:
        sync_supabase_to_notion()
    except Exception as e:
        logger.error(f"Supabase -> Notion failed: {e}")
        
    logger.info("=== Full Sync Cycle Complete ===")

if __name__ == "__main__":
    asyncio.run(run_full_sync_cycle())
