import asyncio
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from lib.sync_service import sync_contacts
from lib.notion_sync import sync_notion_to_supabase, sync_supabase_to_notion
from lib.telegram_client import notify_error, reset_failure_count
from lib.health_monitor import check_sync_health, get_sync_statistics, run_health_check, SystemHealthMonitor
from reports import generate_daily_report, generate_evening_journal_prompt
from backup import backup_contacts
import logging

# Import meeting sync (using new unified service)
from syncs.meetings_sync import run_sync as run_meeting_sync

# Import task sync (using new unified service)
from syncs.tasks_sync import run_sync as run_task_sync

# Import reflection sync (using new unified service)
from syncs.reflections_sync import run_sync as run_reflection_sync

# Import calendar and gmail sync
from sync_calendar import run_calendar_sync
from sync_gmail import run_gmail_sync

# Import journal sync (using new unified service)
from syncs.journals_sync import run_sync as run_journal_sync

# Import books and highlights sync (Notion → Supabase)
from sync_books import run_sync as run_books_sync
from sync_highlights import run_sync as run_highlights_sync

# Import ActivityWatch sync
from sync_activitywatch import run_activitywatch_sync, ActivityWatchSync, format_activity_summary_for_journal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Jarvis Backend")

# ============================================================================
# SYNC LOCKING - Prevent overlapping syncs
# ============================================================================
_sync_lock = asyncio.Lock()
_last_sync_start: datetime | None = None
_last_sync_end: datetime | None = None
_last_sync_results: dict | None = None

@app.get("/")
async def root():
    return {"status": "Jarvis Backend is running"}

@app.get("/health")
async def health_check():
    """
    Comprehensive health check endpoint.
    Returns sync lock status, last sync info, and component health.
    """
    global _last_sync_start, _last_sync_end, _last_sync_results
    
    # Check sync lock status
    sync_status = {
        "sync_in_progress": _sync_lock.locked(),
        "last_sync_start": _last_sync_start.isoformat() if _last_sync_start else None,
        "last_sync_end": _last_sync_end.isoformat() if _last_sync_end else None,
        "last_sync_duration_seconds": (
            (_last_sync_end - _last_sync_start).total_seconds() 
            if _last_sync_start and _last_sync_end else None
        )
    }
    
    # Get basic stats
    try:
        stats = await get_sync_statistics(hours=24)
    except Exception:
        stats = {"error": "could not fetch stats"}
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sync": sync_status,
        "statistics_24h": stats
    }

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


@app.get("/health/full")
async def full_health_check():
    """
    Comprehensive health check for the entire Jarvis ecosystem.
    Returns detailed status of all components, recent errors, and recommendations.
    This is what gets sent in the 8am Telegram report.
    """
    global _last_sync_results
    
    try:
        report = await run_health_check(send_telegram=False)
        result = report.to_dict()
        
        # Add last sync results if available
        if _last_sync_results:
            result["last_sync_results"] = _last_sync_results
        
        return result
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/health/report")
async def send_health_report(background_tasks: BackgroundTasks):
    """
    Generate and send a health report to Telegram.
    """
    async def run_report():
        await run_health_check(send_telegram=True)
    
    background_tasks.add_task(run_report)
    return {"status": "queued", "message": "Health report generation started"}

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
    
    Uses sync locking to prevent overlapping sync cycles.
    """
    global _last_sync_start, _last_sync_end, _last_sync_results
    
    # Check if a sync is already in progress
    if _sync_lock.locked():
        logger.warning("Sync already in progress, skipping this request")
        return {
            "status": "skipped",
            "reason": "sync_already_in_progress",
            "last_sync_start": _last_sync_start.isoformat() if _last_sync_start else None,
            "message": "A sync cycle is already running. Try again later."
        }
    
    async with _sync_lock:
        _last_sync_start = datetime.now(timezone.utc)
        results = {}
        logger.info("Starting full sync cycle via API (lock acquired)")

        async def run_step(name, func, *args, **kwargs):
            try:
                logger.info(f"Starting {name}...")
                if asyncio.iscoroutinefunction(func):
                    res = await func(*args, **kwargs)
                else:
                    res = await run_in_threadpool(func, *args, **kwargs)
                results[name] = {"status": "success", "data": res}
                # Reset failure counter on success
                reset_failure_count(name)
            except Exception as e:
                logger.error(f"{name} failed: {e}")
                results[name] = {"status": "error", "error": str(e)}
                # Error notifications are disabled - errors go to sync_logs table

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
        
        # === JOURNALS SYNC ===
        await run_step("journals_sync", run_journal_sync, full_sync=False, since_hours=24)
        
        # === CALENDAR SYNC ===
        await run_step("calendar_sync", run_calendar_sync)

        # === GMAIL SYNC ===
        await run_step("gmail_sync", run_gmail_sync)
        
        # === BOOKS SYNC (Notion → Supabase) ===
        await run_step("books_sync", run_books_sync, full_sync=False, since_hours=24)
        
        # === HIGHLIGHTS SYNC (Notion → Supabase) ===
        await run_step("highlights_sync", run_highlights_sync, full_sync=False, since_hours=24)
        
        # Track sync completion
        _last_sync_end = datetime.now(timezone.utc)
        _last_sync_results = results
        
        # Count successes and errors for summary
        success_count = sum(1 for r in results.values() if r.get("status") == "success")
        error_count = sum(1 for r in results.values() if r.get("status") == "error")
        
        logger.info(f"Full sync cycle complete: {success_count} success, {error_count} errors")
        
        return {
            "status": "completed",
            "summary": {
                "success_count": success_count,
                "error_count": error_count,
                "duration_seconds": (_last_sync_end - _last_sync_start).total_seconds()
            },
            "results": results
        }

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


@app.post("/report/evening-journal")
async def evening_journal_prompt(background_tasks: BackgroundTasks):
    """
    Generates and sends an evening journal prompt with topics based on
    today's activities (meetings, emails, calendar events, tasks).
    
    Schedule this at 7pm via Cloud Scheduler.
    """
    try:
        background_tasks.add_task(generate_evening_journal_prompt)
        return {"status": "queued", "message": "Evening journal prompt generation started"}
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


# --- Books Sync ---

@app.post("/sync/books")
async def sync_books(full: bool = False, hours: int = 24):
    """
    Sync books from Notion to Supabase (one-way).
    Source: Notion Content database (fed by BookFusion)
    
    Args:
        full: If True, sync all books. If False, only recently updated.
        hours: For incremental sync, how many hours to look back.
    """
    try:
        logger.info(f"Starting books sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_books_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Books sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Highlights Sync ---

@app.post("/sync/highlights")
async def sync_highlights(full: bool = False, hours: int = 24):
    """
    Sync book highlights from Notion to Supabase (one-way).
    Source: Notion Highlights database (fed by BookFusion)
    
    Args:
        full: If True, sync all highlights. If False, only recently updated.
        hours: For incremental sync, how many hours to look back.
    """
    try:
        logger.info(f"Starting highlights sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_highlights_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Highlights sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- ActivityWatch Sync ---

@app.post("/sync/activitywatch")
async def sync_activitywatch(hours: int = 24, full: bool = False):
    """
    Sync ActivityWatch data from local instance to Supabase.
    
    NOTE: This endpoint must be called from a machine where ActivityWatch is running
    (localhost:5600). It cannot be called from Cloud Run.
    
    For cloud integration, run the sync locally or use a tunnel.
    
    Args:
        hours: Number of hours of history to sync (default 24)
        full: If True, ignore last sync time and do full sync
    """
    try:
        logger.info(f"Starting ActivityWatch sync (hours={hours}, full={full})")
        result = await run_activitywatch_sync(hours=hours, full=full)
        return result
    except Exception as e:
        logger.error(f"ActivityWatch sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/activitywatch/summary/today")
async def get_today_activity_summary():
    """
    Get today's ActivityWatch summary.
    Returns screen time breakdown, top apps, top websites, and productivity metrics.
    """
    try:
        sync = ActivityWatchSync()
        summary = await sync.get_today_summary()
        
        if not summary:
            return {"status": "no_data", "message": "No activity data for today"}
        
        return {
            "status": "success",
            "summary": summary,
            "formatted": format_activity_summary_for_journal(summary)
        }
    except Exception as e:
        logger.error(f"Failed to get activity summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


