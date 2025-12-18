import asyncio
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from lib.sync_service import sync_contacts
from lib.notion_sync import sync_notion_to_supabase, sync_supabase_to_notion
from lib.telegram_client import notify_error
from lib.health_monitor import check_sync_health, get_sync_statistics
from reports import generate_daily_report
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
    """Basic health check endpoint."""
    return {"status": "healthy"}

@app.get("/health/sync")
async def sync_health_check():
    """
    Detailed health check for sync services.
    Returns statistics and checks for consecutive failures.
    """
    try:
        stats = await get_sync_statistics(hours=24)
        
        # Check each service for consecutive failures
        services = ["calendar_sync", "gmail_sync", "meetings_sync", "tasks_sync", "reflections_sync"]
        service_health = {}
        
        for service in services:
            health = await check_sync_health(service, failure_threshold=5)
            service_health[service] = health
        
        return {
            "status": "healthy" if all(h.get("healthy", True) for h in service_health.values()) else "degraded",
            "statistics": stats,
            "services": service_health
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

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
    Runs all syncs in the correct order.
    Failures in one module do not stop others.
    """
    results = {}
    logger.info("Starting full sync cycle via API")

    async def run_step(name, func, *args, **kwargs):
        try:
            logger.info(f"Starting {name}...")
            if asyncio.iscoroutinefunction(func):
                res = await func(*args, **kwargs)
            else:
                res = await run_in_threadpool(func, *args, **kwargs)
            results[name] = {"status": "success", "data": res}
        except Exception as e:
            logger.error(f"{name} failed: {e}")
            results[name] = {"status": "error", "error": str(e)}
            # Notify on error
            background_tasks.add_task(notify_error, name, str(e))

    # === CONTACTS SYNC ===
    await run_step("notion_to_supabase", sync_notion_to_supabase)
    await run_step("google_sync", sync_contacts)
    await run_step("supabase_to_notion", sync_supabase_to_notion)
    
    # === MEETINGS SYNC ===
    await run_step("meetings_sync", run_meeting_sync, full_sync=False, since_hours=24)
    
    # === TASKS SYNC ===
    await run_step("tasks_sync", run_task_sync, full_sync=False, since_hours=24)
    
    # === REFLECTIONS SYNC ===
    await run_step("reflections_sync", run_reflection_sync, full_sync=False, since_hours=24)
    
    # === CALENDAR SYNC ===
    await run_step("calendar_sync", run_calendar_sync)

    # === GMAIL SYNC ===
    await run_step("gmail_sync", run_gmail_sync)
    
    return results

@app.post("/report/daily")
async def daily_report(background_tasks: BackgroundTasks):
    """
    Generates and sends the daily sync report via Telegram.
    """
    try:
        # Run in background to avoid timeout
        background_tasks.add_task(generate_daily_report)
        return {"status": "queued", "message": "Daily report generation started"}
    except Exception as e:
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

