from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from lib.sync_service import sync_contacts
from lib.notion_sync import sync_notion_to_supabase, sync_supabase_to_notion
from backup import backup_contacts
import logging

# Import meeting sync
from sync_meetings_bidirectional import run_sync as run_meeting_sync

# Import task sync
from sync_tasks_bidirectional import run_sync as run_task_sync

# Import reflection sync
from sync_reflections_bidirectional import run_sync as run_reflection_sync

# Import calendar and gmail sync
from sync_calendar import run_calendar_sync
from sync_gmail import run_gmail_sync

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Jarvis Backend")

@app.get("/")
async def root():
    return {"status": "Jarvis Backend is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/sync/contacts")
async def sync_all_contacts():
    """
    Bi-directional sync between Google Contacts and Supabase.
    """
    try:
        result = await sync_contacts()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/google-contacts")
async def sync_google_contacts_legacy():
    """
    Legacy endpoint. Redirects to /sync/contacts.
    """
    return await sync_all_contacts()

@app.post("/sync/notion-to-supabase")
async def endpoint_sync_notion_to_supabase():
    try:
        # Run synchronous function in threadpool
        return await run_in_threadpool(sync_notion_to_supabase)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/supabase-to-notion")
async def endpoint_sync_supabase_to_notion():
    try:
        # Run synchronous function in threadpool
        return await run_in_threadpool(sync_supabase_to_notion)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/sync/all")
async def sync_everything(background_tasks: BackgroundTasks):
    """
    Runs all syncs in the correct order:
    1. Contacts: Notion -> Supabase (Import new data)
    2. Contacts: Google <-> Supabase (Sync with Google)
    3. Contacts: Supabase -> Notion (Push updates back)
    4. Meetings: Bidirectional sync (Notion <-> Supabase)
    5. Tasks: Bidirectional sync (Notion <-> Supabase)
    """
    # We run this in the background to avoid timeout on Cloud Run (if it takes > 60s)
    # However, Cloud Run requires the request to stay open if we want CPU allocated, 
    # unless we use Cloud Tasks. For simple scheduled jobs, we can just await it 
    # and increase timeout config.
    
    results = {}
    try:
        logger.info("Starting full sync cycle via API")
        
        # === CONTACTS SYNC ===
        # 1. Notion -> Supabase (Sync, run in threadpool)
        results["notion_to_supabase"] = await run_in_threadpool(sync_notion_to_supabase)
        
        # 2. Google <-> Supabase (Async)
        results["google_sync"] = await sync_contacts()
        
        # 3. Supabase -> Notion (Sync, run in threadpool)
        results["supabase_to_notion"] = await run_in_threadpool(sync_supabase_to_notion)
        
        # === MEETINGS SYNC ===
        # 4. Bidirectional meeting sync (incremental, last 24h)
        logger.info("Starting meeting sync...")
        results["meetings_sync"] = await run_in_threadpool(run_meeting_sync, full_sync=False, since_hours=24)
        
        # === TASKS SYNC ===
        # 5. Bidirectional task sync (incremental, last 24h)
        logger.info("Starting task sync...")
        results["tasks_sync"] = await run_in_threadpool(run_task_sync, full_sync=False, since_hours=24)
        
        # === REFLECTIONS SYNC ===
        # 6. Bidirectional reflection sync (incremental, last 24h)
        logger.info("Starting reflection sync...")
        results["reflections_sync"] = await run_in_threadpool(run_reflection_sync, full_sync=False, since_hours=24)
        
        # === CALENDAR SYNC ===
        # 7. Google Calendar -> Supabase
        logger.info("Starting calendar sync...")
        results["calendar_sync"] = await run_calendar_sync()

        # === GMAIL SYNC ===
        # 8. Gmail -> Supabase
        logger.info("Starting gmail sync...")
        results["gmail_sync"] = await run_gmail_sync()
        
        return results
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backup")
async def trigger_backup():
    """
    Triggers a backup of all contacts to GCS (if configured) and local storage.
    """
    try:
        return await backup_contacts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Meeting Sync ---

@app.post("/sync/meetings")
async def sync_meetings(full: bool = False, hours: int = 24):
    """
    Bidirectional sync between Notion and Supabase for meetings.
    
    Args:
        full: If True, performs full sync. If False, incremental sync.
        hours: For incremental sync, how many hours to look back (default 24).
    """
    try:
        logger.info(f"Starting meeting sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_meeting_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Meeting sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Task Sync ---

@app.post("/sync/tasks")
async def sync_tasks(full: bool = False, hours: int = 24):
    """
    Bidirectional sync between Notion and Supabase for tasks.
    
    Args:
        full: If True, performs full sync. If False, incremental sync.
        hours: For incremental sync, how many hours to look back (default 24).
    """
    try:
        logger.info(f"Starting task sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_task_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Task sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Reflection Sync ---

@app.post("/sync/reflections")
async def sync_reflections(full: bool = False, hours: int = 24):
    """
    Bidirectional sync between Notion and Supabase for reflections/thoughts.
    
    Args:
        full: If True, performs full sync. If False, incremental sync.
        hours: For incremental sync, how many hours to look back (default 24).
    """
    try:
        logger.info(f"Starting reflection sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_reflection_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Reflection sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Calendar Sync ---

@app.post("/sync/calendar")
async def sync_calendar():
    """
    Sync Google Calendar events to Supabase.
    """
    try:
        logger.info("Starting calendar sync via API")
        return await run_calendar_sync()
    except Exception as e:
        logger.error(f"Calendar sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Gmail Sync ---

@app.post("/sync/gmail")
async def sync_gmail():
    """
    Sync Gmail messages to Supabase.
    """
    try:
        logger.info("Starting gmail sync via API")
        return await run_gmail_sync()
    except Exception as e:
        logger.error(f"Gmail sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    """
    return {"status": "not_implemented", "message": "Mail sync coming soon"}

