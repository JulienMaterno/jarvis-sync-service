import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse, Response
from pydantic import BaseModel
from lib.sync_service import sync_contacts
from lib.notion_sync import sync_notion_to_supabase, sync_supabase_to_notion
from lib.logging_service import log_sync_event
from lib.telegram_client import notify_error, reset_failure_count
from lib.health_monitor import check_sync_health, get_sync_statistics, run_health_check, SystemHealthMonitor
from reports import generate_daily_report, generate_evening_journal_prompt, generate_morning_task_digest, check_overdue_task_alerts, generate_email_digest
from backup import backup_contacts
import logging

# Import meeting sync (using new unified service)
from syncs.meetings_sync import run_sync as run_meeting_sync

# Import task sync (using new unified service)
from syncs.tasks_sync import run_sync as run_task_sync

# Import reflection sync (using new unified service)
from syncs.reflections_sync import run_sync as run_reflection_sync

# Import calendar, gmail, and follow-up sync
from sync_calendar import run_calendar_sync
from sync_gmail import run_gmail_sync
from sync_follow_ups import run_follow_up_sync

# Import journal sync (using new unified service)
from syncs.journals_sync import run_sync as run_journal_sync

# Import books and highlights sync (Notion → Supabase)
from sync_books import run_sync as run_books_sync
from sync_highlights import run_sync as run_highlights_sync

# Import applications and LinkedIn posts sync
from syncs.applications_sync import run_sync as run_applications_sync
from syncs.linkedin_posts_sync import run_sync as run_linkedin_posts_sync

# Import documents sync
from syncs.documents_sync import run_sync as run_documents_sync

# Import newsletters sync
from syncs.newsletters_sync import run_sync as run_newsletters_sync

# Import follow-ups Notion sync
from syncs.follow_ups_sync import run_sync as run_follow_ups_notion_sync

# Import ActivityWatch sync
from sync_activitywatch import run_activitywatch_sync, ActivityWatchSync, format_activity_summary_for_journal

# Import Beeper sync
from sync_beeper import run_beeper_sync, run_beeper_relink

# Import Beeper → Notion sync (one-way: appends messages to Notion pages)
from syncs.beeper_notion_sync import run_sync as run_beeper_notion_sync

# Import Meeting → AV HQ sync (one-way: creates meeting pages in AV HQ teamspace)
from syncs.meeting_av_hq_sync import run_sync as run_meeting_av_hq_sync

# Import Anki sync
from syncs.anki_sync import run_anki_sync

# Import Insight Timer sync
from syncs.insight_timer_sync import run_sync as run_insight_timer_sync

# Import Withings health sync
from sync_withings import run_sync as run_withings_sync
from lib.sleep_staging import run_post_withings_staging

# Import Supabase client and Gmail client
from lib.supabase_client import supabase
from lib.google_gmail import GmailClient

# Import sync audit for inventory and health checks
from lib.sync_audit import (
    get_database_inventory,
    check_sync_health as check_database_sync_health,
    SyncStats,
    record_sync_audit,
    generate_sync_report,
    generate_24h_summary,
    format_24h_summary_text
)

# Import lean sync cursor for change detection
from lib.sync_cursor import (
    check_for_changes,
    check_all_entities,
    update_cursor_after_sync,
    ChangeCheckResult
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load API key from environment (fail-closed: no key = reject all non-health requests)
_INTERNAL_API_KEY = os.getenv("INTERNAL_API_KEY")
if not _INTERNAL_API_KEY:
    logger.warning(
        "INTERNAL_API_KEY not set! All non-health requests will be rejected with 503. "
        "Set INTERNAL_API_KEY environment variable to enable API access."
    )


_INTELLIGENCE_SERVICE_URL = os.getenv(
    "INTELLIGENCE_SERVICE_URL",
    "https://jarvis-intelligence-service-776871804948.asia-southeast1.run.app",
)

# Map sync entity names → knowledge source_types for post-sync indexing
_SYNC_TO_INDEX_TYPES = {
    "contacts": "contact",
    "meetings": "meeting",
    "tasks": "task",
    "reflections": "reflection",
    "journals": "journal",
    "calendar_events": "calendar",
    "calendar": "calendar",
    "gmail": "email",
    "emails": "email",
    "beeper": "beeper_message",
    "beeper_messages": "beeper_message",
}


async def _trigger_knowledge_indexing(synced_entities: list) -> None:
    """Call Intelligence Service to index newly synced data.

    Runs as a background task after sync completes. Only indexes
    the source types that were actually synced. Fire-and-forget.
    """
    # Map synced entity names to knowledge source_types
    source_types = []
    for entity in synced_entities:
        mapped = _SYNC_TO_INDEX_TYPES.get(entity)
        if mapped and mapped not in source_types:
            source_types.append(mapped)

    if not source_types:
        return

    try:
        import httpx

        headers = {}
        if _INTERNAL_API_KEY:
            headers["X-API-Key"] = _INTERNAL_API_KEY

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{_INTELLIGENCE_SERVICE_URL}/api/v1/knowledge/index/incremental",
                json={"source_types": source_types, "batch_size": 30},
                headers=headers,
            )
            response.raise_for_status()
            result = response.json()
            total = result.get("total_indexed", 0)
            if total > 0:
                logger.info(
                    "Knowledge indexing: %d new chunks for %s",
                    total, source_types,
                )
            else:
                logger.debug("Knowledge indexing: nothing new for %s", source_types)
    except Exception as e:
        logger.warning(f"Knowledge indexing trigger failed (non-blocking): {e}")


class APIKeyAuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to enforce API key authentication on all endpoints.

    Security model (fail-closed):
    - If INTERNAL_API_KEY is set: validates X-API-Key header against it.
    - If INTERNAL_API_KEY is NOT set: rejects all non-health requests with 503.
    - Health and root endpoints are always public (for load balancer checks).
    """

    # Paths that never require authentication
    PUBLIC_PATHS = {"/", "/health", "/follow-ups/webhook/send", "/withings/authorize"}
    # Path prefixes that never require authentication (health sub-routes)
    PUBLIC_PREFIXES = ("/health/", "/dashboard/", "/webhooks/withings/", "/webhooks/hevy/")

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Allow public endpoints without authentication
        if path in self.PUBLIC_PATHS or path.startswith(self.PUBLIC_PREFIXES):
            return await call_next(request)

        # Fail-closed: if no API key is configured, reject with 503
        if not _INTERNAL_API_KEY:
            auth_logger = logging.getLogger("jarvis.auth")
            auth_logger.warning(
                f"Rejecting request to {path} - INTERNAL_API_KEY not configured"
            )
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "Service not configured: INTERNAL_API_KEY environment variable is not set. "
                    "All non-health endpoints are unavailable until authentication is configured."
                }
            )

        # Check for API key in request headers
        api_key = request.headers.get("X-API-Key")

        if not api_key:
            auth_logger = logging.getLogger("jarvis.auth")
            auth_logger.warning(
                f"Missing API key from {request.client.host} for {path}"
            )
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing API key. Include X-API-Key header."}
            )

        if api_key != _INTERNAL_API_KEY:
            auth_logger = logging.getLogger("jarvis.auth")
            auth_logger.warning(
                f"Invalid API key from {request.client.host} for {path}"
            )
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid API key"}
            )

        # Valid API key, proceed
        return await call_next(request)


app = FastAPI(title="Jarvis Backend")

# ============================================================================
# CORS MIDDLEWARE - Allow Chrome extension and other clients
# ============================================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "chrome-extension://*",
        "http://localhost:3000",
        "http://localhost:8080",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# ============================================================================
# API KEY AUTH MIDDLEWARE - Defense-in-depth (fail-closed)
# ============================================================================
app.add_middleware(APIKeyAuthMiddleware)

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
    Returns sync lock status, last sync info, and accurate statistics.
    
    Success rate calculation:
    - Only counts actionable operations (success vs error)
    - Excludes informational logs (sync_start, sync_complete, etc.)
    - 100% means zero errors in the time window
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
    
    # Get accurate stats
    try:
        stats = await get_sync_statistics(hours=24)
    except Exception:
        stats = {"error": "could not fetch stats"}
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sync": sync_status,
        "statistics_24h": stats,
        "note": "Success rate = success/(success+error), excluding info logs"
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


# ============================================================================
# DATABASE INVENTORY & SYNC AUDIT ENDPOINTS
# ============================================================================

@app.get("/inventory")
async def get_inventory():
    """
    Get current counts for all synced entities across Supabase and Notion.
    
    Returns:
        - Count of records in each database
        - Difference between databases
        - Whether databases are in sync
    """
    try:
        inventory = await run_in_threadpool(get_database_inventory)
        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "inventory": inventory
        }
    except Exception as e:
        logger.error(f"Error getting inventory: {e}")
        return {"status": "error", "error": str(e)}


@app.get("/inventory/health")
async def get_inventory_health():
    """
    Check sync health across all databases.
    
    Returns:
        - Overall health status (healthy/warning/critical)
        - Per-entity status and counts
        - List of any sync issues detected
    """
    try:
        health = await run_in_threadpool(check_database_sync_health)
        return health
    except Exception as e:
        logger.error(f"Error checking sync health: {e}")
        return {"status": "error", "error": str(e)}


@app.get("/inventory/table")
async def get_inventory_table():
    """
    Get a formatted table view of database inventory and sync status.
    
    Returns a human-readable table showing:
    - Entity type
    - Supabase count
    - Notion count
    - Google count (contacts only)
    - Difference
    - Sync status (✅/⚠️/❌)
    """
    try:
        inventory = await run_in_threadpool(get_database_inventory)
        
        # Build formatted table
        table_rows = []
        total_supabase = 0
        total_notion = 0
        
        for entity, counts in inventory.items():
            sb = counts.get('supabase', 0)
            n = counts.get('notion', 0)
            g = counts.get('google')  # Only for contacts
            diff = counts.get('difference', 0)
            
            if sb >= 0:
                total_supabase += sb
            if n >= 0:
                total_notion += n
            
            # Status indicator
            if diff is None:
                status = "❓"
            elif diff == 0:
                status = "✅"
            elif abs(diff) <= 3:
                status = "⚠️"
            else:
                status = "❌"
            
            row = {
                "entity": entity.title(),
                "supabase": sb if sb >= 0 else "Error",
                "notion": n if n >= 0 else "Error",
                "difference": diff if diff is not None else "N/A",
                "status": status
            }
            
            # Add Google count for contacts
            if g is not None:
                row["google"] = g
            
            table_rows.append(row)
        
        # Add totals row
        total_diff = total_notion - total_supabase
        table_rows.append({
            "entity": "TOTAL",
            "supabase": total_supabase,
            "notion": total_notion,
            "difference": total_diff,
            "status": "✅" if total_diff == 0 else ("⚠️" if abs(total_diff) <= 5 else "❌")
        })
        
        # Format as text table (include Google column)
        has_google = any(r.get('google') is not None for r in table_rows)
        if has_google:
            header = "| Entity      | Supabase | Notion | Google | Diff | Status |"
            separator = "|-------------|----------|--------|--------|------|--------|"
            rows = [
                f"| {r['entity']:<11} | {str(r['supabase']):>8} | {str(r['notion']):>6} | {str(r.get('google', '-')):>6} | {str(r['difference']):>4} | {r['status']:>6} |"
                for r in table_rows
            ]
        else:
            header = "| Entity      | Supabase | Notion | Diff | Status |"
            separator = "|-------------|----------|--------|------|--------|"
            rows = [
                f"| {r['entity']:<11} | {str(r['supabase']):>8} | {str(r['notion']):>6} | {str(r['difference']):>4} | {r['status']:>6} |"
                for r in table_rows
            ]
        
        table_text = "\n".join([header, separator] + rows)
        
        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "table": table_rows,
            "formatted": table_text,
            "summary": {
                "total_supabase": total_supabase,
                "total_notion": total_notion,
                "difference": total_diff,
                "all_synced": total_diff == 0
            }
        }
    except Exception as e:
        logger.error(f"Error getting inventory table: {e}")
        return {"status": "error", "error": str(e)}


@app.get("/sync/history")
async def get_sync_history_endpoint(entity: str = None, days: int = 7):
    """
    Get recent sync history from the audit table.

    Args:
        entity: Filter by entity type (contacts, meetings, tasks, etc.)
        days: Number of days to look back (default 7)
    """
    try:
        from lib.sync_audit import get_sync_history
        history = await run_in_threadpool(get_sync_history, entity, days)
        return {
            "status": "success",
            "count": len(history),
            "history": history
        }
    except Exception as e:
        logger.error(f"Error getting sync history: {e}")
        return {"status": "error", "error": str(e)}


@app.get("/sync/summary")
async def get_sync_summary():
    """
    Comprehensive 24-hour sync summary with:
    - All sync runs (not just last cycle)
    - Time breakdown per entity type (explains why sync took 200s)
    - Current inventory showing Supabase, Notion, AND Google counts
    - Active records only (excludes soft-deleted)
    - Recent errors with context

    This is the go-to endpoint for understanding sync health and performance.
    """
    try:
        summary = await run_in_threadpool(generate_24h_summary)
        summary['formatted'] = format_24h_summary_text(summary)
        return summary
    except Exception as e:
        logger.error(f"Error generating sync summary: {e}")
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
    LEAN SYNC - Only syncs entities that have actual changes.
    
    This endpoint is optimized for the 15-minute Cloud Scheduler calls:
    1. First, does a lightweight "has anything changed?" check per entity
    2. Only runs full sync for entities with detected changes
    3. Skips entirely if nothing has changed (saves API calls and compute)
    
    Uses sync locking to prevent overlapping sync cycles.
    Records audit trail of all sync operations.
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
        skipped_entities = []
        synced_entities = []
        
        # Generate a unique run_id for this sync cycle (for audit)
        import uuid
        run_id = str(uuid.uuid4())
        logger.info(f"Starting LEAN sync cycle (run_id: {run_id[:8]})")

        # =====================================================================
        # PHASE 1: Lightweight change detection for all entities
        # =====================================================================
        logger.info("Phase 1: Checking for changes across all entities...")
        change_checks = await run_in_threadpool(check_all_entities, supabase)
        
        for entity, check in change_checks.items():
            if check.has_changes:
                synced_entities.append(entity)
            else:
                skipped_entities.append(entity)
        
        logger.info(f"Change detection complete: {len(synced_entities)} need sync, {len(skipped_entities)} skipped")

        async def run_step(name, func, *args, entity_name=None, **kwargs):
            """Run a sync step and update cursor on success."""
            step_start = datetime.now(timezone.utc)
            try:
                logger.info(f"Starting {name}...")
                if asyncio.iscoroutinefunction(func):
                    res = await func(*args, **kwargs)
                else:
                    res = await run_in_threadpool(func, *args, **kwargs)
                results[name] = {"status": "success", "data": res}
                # Reset failure counter on success
                reset_failure_count(name)

                # Update sync cursor on success
                if entity_name:
                    await run_in_threadpool(update_cursor_after_sync, supabase, entity_name, datetime.now(timezone.utc))

            except Exception as e:
                logger.error(f"{name} failed: {e}")
                results[name] = {"status": "error", "error": str(e)}
            finally:
                # Log step duration for performance monitoring
                elapsed_ms = (datetime.now(timezone.utc) - step_start).total_seconds() * 1000
                logger.info(f"⏱️ {name} completed in {elapsed_ms:.0f}ms")

        # =====================================================================
        # PHASE 2: Run syncs in parallel where possible
        #
        # Dependency chains (must be sequential within chain):
        #   - Contacts: notion→sb → google → sb→notion
        #   - Gmail chain: gmail → follow_ups → follow_ups_notion
        #   - Beeper chain: beeper → beeper_notion
        #   - Withings chain: withings → sleep_staging
        #   - Meetings chain: meetings → meeting_av_hq
        #
        # Everything else is independent and runs concurrently.
        # =====================================================================

        # --- Define dependency chains as async functions ---

        async def contacts_chain():
            """Contacts: 3-step bidirectional sync (Notion ↔ Supabase ↔ Google)."""
            if 'contacts' in synced_entities:
                await run_step("notion_to_supabase", lambda: sync_notion_to_supabase(check_deletions=True), entity_name='contacts')
                await run_step("google_sync", sync_contacts)
                await run_step("supabase_to_notion", sync_supabase_to_notion)

                contacts_status = "success" if all(
                    results.get(k, {}).get("status") == "success"
                    for k in ["notion_to_supabase", "google_sync", "supabase_to_notion"]
                ) else "partial"
                google_stats = results.get("google_sync", {}).get("data", {})
                synced = google_stats.get("synced", 0) if isinstance(google_stats, dict) else 0
                errors = google_stats.get("errors", 0) if isinstance(google_stats, dict) else 0
                await log_sync_event(
                    "ContactsSync_complete", contacts_status,
                    f"Contacts sync: {synced} synced, {errors} errors (Notion+Google+Notion)"
                )
            else:
                results["contacts_sync"] = {"status": "skipped", "reason": "no_changes"}

        async def gmail_chain():
            """Gmail → follow-ups → follow-ups Notion (sequential dependency)."""
            await run_step("gmail_sync", run_gmail_sync)
            await run_step("follow_up_sync", run_follow_up_sync)
            await run_step("follow_ups_notion_sync", run_follow_ups_notion_sync, full_sync=False, since_hours=24)

        async def beeper_chain():
            """Beeper → Beeper Notion (messages must be in Supabase first)."""
            await run_step("beeper_sync", run_beeper_sync, supabase, full_sync=False)
            await run_step("beeper_notion_sync", run_beeper_notion_sync, supabase, full_sync=False)

        async def meetings_chain():
            """Meetings → AV HQ Notion pages (meetings must be in Supabase first)."""
            if 'meetings' in synced_entities:
                await run_step("meetings_sync", run_meeting_sync, full_sync=False, since_hours=24, entity_name='meetings')
            else:
                results["meetings_sync"] = {"status": "skipped", "reason": "no_changes"}
            await run_step("meeting_av_hq_sync", run_meeting_av_hq_sync, supabase, full_sync=False)

        async def withings_chain():
            """Withings → sleep staging (staging depends on fresh withings data)."""
            if os.getenv("WITHINGS_CLIENT_ID"):
                await run_step("withings_sync", run_withings_sync, supabase, days=7)
                await run_step("sleep_staging", run_post_withings_staging, supabase)
            else:
                results["withings_sync"] = {"status": "disabled", "reason": "WITHINGS_CLIENT_ID not set"}

        async def tasks_step():
            if 'tasks' in synced_entities:
                await run_step("tasks_sync", run_task_sync, full_sync=False, since_hours=24, entity_name='tasks')
            else:
                results["tasks_sync"] = {"status": "skipped", "reason": "no_changes"}

        async def reflections_step():
            if 'reflections' in synced_entities:
                await run_step("reflections_sync", run_reflection_sync, full_sync=False, since_hours=24, entity_name='reflections')
            else:
                results["reflections_sync"] = {"status": "skipped", "reason": "no_changes"}

        async def journals_step():
            if 'journals' in synced_entities:
                await run_step("journals_sync", run_journal_sync, full_sync=False, since_hours=24, entity_name='journals')
            else:
                results["journals_sync"] = {"status": "skipped", "reason": "no_changes"}

        async def anki_step():
            if os.getenv("ANKI_SYNC_ENABLED", "false").lower() == "true":
                current_hour = datetime.now(timezone.utc).hour
                daily_hour = int(os.getenv("ANKI_SYNC_DAILY_HOUR", "3"))
                if current_hour == daily_hour:
                    logger.info(f"Running daily Anki sync (hour={current_hour})")
                    await run_step("anki_sync", run_anki_sync, supabase)
                else:
                    results["anki_sync"] = {
                        "status": "skipped",
                        "reason": f"scheduled_for_hour_{daily_hour}_utc",
                        "current_hour": current_hour
                    }
            else:
                results["anki_sync"] = {"status": "disabled", "reason": "ANKI_SYNC_ENABLED not set"}

        # --- Run all chains and independent engines concurrently ---
        logger.info("Running sync engines in parallel...")
        await asyncio.gather(
            contacts_chain(),
            meetings_chain(),
            tasks_step(),
            reflections_step(),
            journals_step(),
            gmail_chain(),
            beeper_chain(),
            # Calendar (always, independent)
            run_step("calendar_sync", run_calendar_sync),
            # Books & highlights (one-way, independent)
            run_step("books_sync", run_books_sync, full_sync=False, since_hours=24),
            run_step("highlights_sync", run_highlights_sync, full_sync=False, hours=24),
            # Bidirectional content syncs (independent)
            run_step("applications_sync", run_applications_sync, full_sync=False, since_hours=24),
            run_step("linkedin_posts_sync", run_linkedin_posts_sync, full_sync=False, since_hours=24),
            run_step("documents_sync", run_documents_sync, full_sync=False, since_hours=24),
            # External data sources (independent)
            run_step("insight_timer_sync", run_insight_timer_sync, supabase),
            withings_chain(),
            anki_step(),
        )

        # Track sync completion
        _last_sync_end = datetime.now(timezone.utc)
        _last_sync_results = results

        # Count successes, skips, and errors for summary
        success_count = sum(1 for r in results.values() if r.get("status") == "success")
        skipped_count = sum(1 for r in results.values() if r.get("status") == "skipped")
        error_count = sum(1 for r in results.values() if r.get("status") == "error")

        # Log completion immediately (don't wait for audit)
        sync_duration = (_last_sync_end - _last_sync_start).total_seconds()
        logger.info(f"LEAN sync complete: {success_count} synced, {skipped_count} skipped (no changes), {error_count} errors in {sync_duration:.1f}s")

        # Define background audit task - runs after response is returned
        def record_audit_in_background():
            """Record comprehensive audit for sync run (runs in background to not block response)."""
            try:
                audit_start = datetime.now(timezone.utc)
                logger.info(f"Starting background audit for run {run_id[:8]}...")
                inventory = get_database_inventory()
                inventory_duration = (datetime.now(timezone.utc) - audit_start).total_seconds()
                logger.info(f"⏱️ Inventory collection completed in {inventory_duration:.1f}s")

                # Helper to extract counts from sync results - handles various formats
                def extract_counts(result_data):
                    if not isinstance(result_data, dict):
                        return 0, 0, 0
                    # Check for nested 'stats' object first (books, highlights, reflections)
                    stats = result_data.get('stats', result_data)
                    created = stats.get('created', 0) or 0
                    updated = stats.get('updated', 0) or 0
                    deleted = stats.get('deleted', 0) or 0
                    return created, updated, deleted

                # Core Notion↔Supabase entities with inventory counts
                for entity in ['contacts', 'meetings', 'tasks', 'reflections', 'journals']:
                    if entity in inventory:
                        # Extract operation counts from sync results
                        created, updated, deleted = 0, 0, 0
                        for sync_key in results:
                            if entity in sync_key:
                                c, u, d = extract_counts(results[sync_key].get('data'))
                                created += c
                                updated += u
                                deleted += d

                        audit_stats = SyncStats(
                            entity_type=entity,
                            supabase_count=inventory[entity].get('supabase', 0),
                            notion_count=inventory[entity].get('notion', 0),
                            google_count=inventory[entity].get('google'),
                            created_in_supabase=created,
                            updated_in_supabase=updated,
                            deleted_in_supabase=deleted
                        )
                        record_sync_audit(
                            run_id=run_id,
                            sync_type='scheduled',
                            entity_type=entity,
                            stats=audit_stats,
                            triggered_by='api',
                            started_at=_last_sync_start,
                            completed_at=_last_sync_end,
                            status='success' if results.get(f'{entity}_sync', {}).get('status') == 'success' else 'partial',
                            details={'sync_results': results.get(f'{entity}_sync')}
                        )

                # Calendar events (Google → Supabase only)
                if 'calendar_sync' in results:
                    cal_data = results['calendar_sync'].get('data', {})
                    events_created = cal_data.get('events_created', 0) or cal_data.get('created', 0) or 0
                    events_updated = cal_data.get('events_updated', 0) or cal_data.get('updated', 0) or 0
                    audit_stats = SyncStats(
                        entity_type='calendar_events',
                        supabase_count=inventory.get('calendar_events', {}).get('supabase', 0),
                        created_in_supabase=events_created,
                        updated_in_supabase=events_updated
                    )
                    record_sync_audit(
                        run_id=run_id,
                        sync_type='scheduled',
                        entity_type='calendar_events',
                        stats=audit_stats,
                        triggered_by='api',
                        started_at=_last_sync_start,
                        completed_at=_last_sync_end,
                        status=results['calendar_sync'].get('status', 'success'),
                        details={'sync_results': results['calendar_sync']}
                    )

                # Gmail (Google → Supabase only)
                if 'gmail_sync' in results:
                    gmail_data = results['gmail_sync'].get('data', {})
                    emails_created = gmail_data.get('emails_created', 0) or gmail_data.get('new_emails', 0) or gmail_data.get('created', 0) or 0
                    emails_updated = gmail_data.get('emails_updated', 0) or gmail_data.get('updated', 0) or 0
                    audit_stats = SyncStats(
                        entity_type='emails',
                        supabase_count=inventory.get('emails', {}).get('supabase', 0),
                        created_in_supabase=emails_created,
                        updated_in_supabase=emails_updated
                    )
                    record_sync_audit(
                        run_id=run_id,
                        sync_type='scheduled',
                        entity_type='emails',
                        stats=audit_stats,
                        triggered_by='api',
                        started_at=_last_sync_start,
                        completed_at=_last_sync_end,
                        status=results['gmail_sync'].get('status', 'success'),
                        details={'sync_results': results['gmail_sync']}
                    )

                # Beeper (Beeper Bridge → Supabase)
                if 'beeper_sync' in results:
                    beeper_data = results['beeper_sync'].get('data', {})
                    chats_created = beeper_data.get('chats_created', 0) or 0
                    chats_updated = beeper_data.get('chats_updated', 0) or 0
                    messages_new = beeper_data.get('messages_new', 0) or 0

                    # Record beeper_chats audit
                    stats_chats = SyncStats(
                        entity_type='beeper_chats',
                        supabase_count=inventory.get('beeper_chats', {}).get('supabase', 0),
                        created_in_supabase=chats_created,
                        updated_in_supabase=chats_updated
                    )
                    record_sync_audit(
                        run_id=run_id,
                        sync_type='scheduled',
                        entity_type='beeper_chats',
                        stats=stats_chats,
                        triggered_by='api',
                        started_at=_last_sync_start,
                        completed_at=_last_sync_end,
                        status=results['beeper_sync'].get('status', 'success'),
                        details={'sync_results': results['beeper_sync']}
                    )

                    # Record beeper_messages audit
                    stats_msgs = SyncStats(
                        entity_type='beeper_messages',
                        supabase_count=inventory.get('beeper_messages', {}).get('supabase', 0),
                        created_in_supabase=messages_new,
                        updated_in_supabase=0
                    )
                    record_sync_audit(
                        run_id=run_id,
                        sync_type='scheduled',
                        entity_type='beeper_messages',
                        stats=stats_msgs,
                        triggered_by='api',
                        started_at=_last_sync_start,
                        completed_at=_last_sync_end,
                        status=results['beeper_sync'].get('status', 'success'),
                        details={'sync_results': results['beeper_sync']}
                    )

                # Books (Notion → Supabase only)
                if 'books_sync' in results:
                    created, updated, _ = extract_counts(results['books_sync'].get('data', {}))
                    audit_stats = SyncStats(
                        entity_type='books',
                        supabase_count=inventory.get('books', {}).get('supabase', 0),
                        notion_count=inventory.get('books', {}).get('notion') or 0,
                        created_in_supabase=created,
                        updated_in_supabase=updated
                    )
                    record_sync_audit(
                        run_id=run_id,
                        sync_type='scheduled',
                        entity_type='books',
                        stats=audit_stats,
                        triggered_by='api',
                        started_at=_last_sync_start,
                        completed_at=_last_sync_end,
                        status=results['books_sync'].get('status', 'success'),
                        details={'sync_results': results['books_sync']}
                    )

                # Highlights (Notion → Supabase only)
                if 'highlights_sync' in results:
                    created, updated, _ = extract_counts(results['highlights_sync'].get('data', {}))
                    audit_stats = SyncStats(
                        entity_type='highlights',
                        supabase_count=inventory.get('highlights', {}).get('supabase', 0),
                        notion_count=inventory.get('highlights', {}).get('notion') or 0,
                        created_in_supabase=created,
                        updated_in_supabase=updated
                    )
                    record_sync_audit(
                        run_id=run_id,
                        sync_type='scheduled',
                        entity_type='highlights',
                        stats=audit_stats,
                        triggered_by='api',
                        started_at=_last_sync_start,
                        completed_at=_last_sync_end,
                        status=results['highlights_sync'].get('status', 'success'),
                        details={'sync_results': results['highlights_sync']}
                    )

                # Build consolidated summary of changes
                changes_summary = []
                for entity in ['contacts', 'meetings', 'tasks', 'reflections', 'journals', 'calendar_events', 'emails', 'books', 'highlights', 'beeper_chats', 'beeper_messages']:
                    for key in results:
                        if entity.replace('_', '') in key.replace('_', '').lower():
                            data = results[key].get('data', {})
                            if isinstance(data, dict):
                                s = data.get('stats', data)
                                c = s.get('created', 0) or 0
                                u = s.get('updated', 0) or 0
                                d = s.get('deleted', 0) or 0
                                if c or u or d:
                                    changes_summary.append(f"{entity}:{c}c/{u}u/{d}d")
                            break

                if changes_summary:
                    logger.info(f"📊 SYNC CHANGES: {', '.join(changes_summary)}")
                else:
                    logger.info("📊 SYNC CHANGES: No changes detected")

                audit_duration = (datetime.now(timezone.utc) - audit_start).total_seconds()
                logger.info(f"✅ Background audit completed for run {run_id[:8]} in {audit_duration:.1f}s")

            except Exception as e:
                logger.error(f"Failed to record sync audit: {e}", exc_info=True)

        # Schedule audit to run in background (doesn't block response)
        background_tasks.add_task(record_audit_in_background)

        # Trigger knowledge indexing for syncs that succeeded
        if success_count > 0:
            # Include both change-detected entities AND always-run syncs
            all_synced = list(synced_entities)
            for key in ("calendar_sync", "gmail_sync", "beeper_sync"):
                if results.get(key, {}).get("status") == "success":
                    all_synced.append(key.replace("_sync", ""))
            background_tasks.add_task(
                _trigger_knowledge_indexing,
                all_synced,
            )

        return {
            "status": "completed",
            "run_id": run_id,
            "mode": "lean",
            "summary": {
                "success_count": success_count,
                "skipped_count": skipped_count,
                "error_count": error_count,
                "duration_seconds": (_last_sync_end - _last_sync_start).total_seconds(),
                "entities_synced": synced_entities,
                "entities_skipped": skipped_entities,
            },
            "change_detection": {
                entity: {
                    "has_changes": check.has_changes,
                    "notion_changes": check.notion_changes,
                    "supabase_changes": check.supabase_changes,
                    "check_duration_ms": round(check.check_duration_ms, 1)
                }
                for entity, check in change_checks.items()
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


@app.post("/report/morning-tasks")
async def morning_tasks_digest(background_tasks: BackgroundTasks):
    """
    Generates and sends a morning task digest via Telegram.

    Shows overdue tasks, tasks due today, high-priority items,
    and tasks due this week.

    Schedule this at 8am SGT via Cloud Scheduler.
    """
    try:
        background_tasks.add_task(generate_morning_task_digest)
        return {"status": "queued", "message": "Morning task digest generation started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/report/email-digest")
@app.get("/report/email-digest")
async def email_follow_up_digest():
    """
    Scans Gmail and returns an email follow-up digest.

    Returns structured data about:
    - Sent emails awaiting reply (>3 days)
    - Received emails you haven't replied to (>2 days)
    - Due follow-ups from the follow_ups table

    Designed to be called by Claude Code to flag items conversationally.
    """
    try:
        result = await generate_email_digest()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/report/overdue-alerts")
async def overdue_task_alerts(background_tasks: BackgroundTasks):
    """
    Check for newly overdue tasks and send alerts via Telegram.

    Tracks which tasks have already been notified to avoid spam.

    Schedule this every 4 hours via Cloud Scheduler.
    """
    try:
        background_tasks.add_task(check_overdue_task_alerts)
        return {"status": "queued", "message": "Overdue task alert check started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/deliver-reminder/{task_id}")
async def deliver_reminder(task_id: str):
    """
    Deliver a scheduled reminder notification via Telegram.

    Called by Google Cloud Tasks at the exact remind_at time.
    Sends a Telegram message and marks the task as reminded.
    """
    from lib.supabase_client import supabase
    from lib.telegram_client import send_telegram_message
    from reports import _store_automated_message

    try:
        # Fetch the task
        result = supabase.table("tasks").select(
            "id, title, description, priority, status, remind_at, reminded_at"
        ).eq("id", task_id).is_("deleted_at", "null").execute()

        if not result.data:
            logger.warning(f"Reminder delivery: task {task_id} not found or deleted")
            return {"status": "skipped", "reason": "task not found"}

        task = result.data[0]

        # Skip if already reminded or completed
        if task.get("reminded_at"):
            logger.info(f"Reminder delivery: task {task_id} already reminded")
            return {"status": "skipped", "reason": "already reminded"}
        if task.get("status") == "completed":
            logger.info(f"Reminder delivery: task {task_id} already completed")
            return {"status": "skipped", "reason": "task completed"}

        # Build notification message
        priority_icon = {"high": "\U0001f534", "medium": "\U0001f7e1", "low": "\U0001f7e2"}.get(
            task.get("priority", ""), "\u26aa"
        )
        lines = [f"\u23f0 *Reminder*", ""]
        lines.append(f"{priority_icon} *{task['title']}*")
        if task.get("description"):
            lines.append(f"_{task['description']}_")
        lines.append("")
        lines.append("_Reply to complete, reschedule, or snooze._")

        message = "\n".join(lines)
        await send_telegram_message(message)

        # Store in chat history so AI has context
        _store_automated_message(message, {
            "notification_type": "scheduled_reminder",
            "task_id": task_id,
            "task_title": task["title"],
        })

        # Mark as reminded
        supabase.table("tasks").update({
            "reminded_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", task_id).execute()

        logger.info(f"Reminder delivered for task: {task['title']} (id={task_id})")
        return {"status": "delivered", "task_id": task_id, "title": task["title"]}

    except Exception as e:
        logger.error(f"Failed to deliver reminder for task {task_id}: {e}")
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


@app.post("/backup/full")
async def trigger_full_backup(background_tasks: BackgroundTasks):
    """
    Triggers a FULL backup of ALL Supabase tables.
    
    ⚠️ CRITICAL: Supabase FREE tier has NO automatic backups!
    This endpoint should be called daily via Cloud Scheduler.
    
    Tables backed up:
    - Critical: contacts, meetings, tasks, journals, reflections, transcripts
    - Important: calendar_events, emails, beeper_chats, beeper_messages
    - Optional: books, highlights, applications, linkedin_posts
    
    Backup stored in: Supabase Storage bucket "backups" + optional GCS
    """
    from backup_full import run_full_backup
    
    async def run_backup():
        try:
            result = await run_full_backup()
            logger.info(f"Full backup completed: {result}")
        except Exception as e:
            logger.error(f"Full backup failed: {e}", exc_info=True)
    
    background_tasks.add_task(run_backup)
    return {
        "status": "queued",
        "message": "Full backup started in background",
        "warning": "Supabase FREE tier has no automatic backups - this is your only backup!"
    }


# --- Database Cleanup (retention policy) ---

RETENTION_POLICY = [
    ("sync_logs", 30, "created_at"),
    ("sync_audit", 30, "created_at"),
    ("activity_events", 90, "timestamp"),
    ("mcp_activity_logs", 90, "created_at"),
    ("provider_traces", 30, "created_at"),
]


def _run_db_cleanup() -> dict:
    """Delete old rows from log/audit tables based on retention policy."""
    from lib.supabase_client import supabase as sb

    results = {}
    for table, days, date_col in RETENTION_POLICY:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        try:
            resp = sb.table(table).delete().lt(date_col, cutoff).execute()
            deleted = len(resp.data) if resp.data else 0
            results[table] = deleted
            if deleted > 0:
                logger.info(f"DB cleanup: deleted {deleted} rows from {table} older than {days} days")
        except Exception as e:
            logger.warning(f"DB cleanup: failed to clean {table}: {e}")
            results[table] = f"error: {e}"
    return results


# --- Weekly Maintenance (bundle of weekly jobs) ---

@app.post("/weekly-maintenance")
async def weekly_maintenance(background_tasks: BackgroundTasks):
    """
    Weekly maintenance endpoint that runs multiple weekly tasks in parallel.

    This is designed to be called by Cloud Scheduler once per week (e.g., Sunday 2am).

    Runs in parallel:
    1. Database cleanup (retention policy for logs/audit tables)
    2. Full backup (Supabase → GCS)
    3. Anki card generation (new highlights → cards, chapter/book summaries)

    Returns immediately, tasks run in background.
    """
    from backup_full import run_full_backup
    from generate_anki_cards import AnkiCardGenerator

    results = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "tasks": ["db_cleanup", "backup", "anki_cards"],
        "status": "queued"
    }

    async def run_all_weekly_tasks():
        task_results = {}

        # Run database cleanup (retention policy)
        try:
            logger.info("Weekly maintenance: Starting database cleanup...")
            cleanup_result = await run_in_threadpool(_run_db_cleanup)
            task_results["db_cleanup"] = {"status": "success", **cleanup_result}
            logger.info(f"Weekly maintenance: DB cleanup completed - {cleanup_result}")
        except Exception as e:
            logger.error(f"Weekly maintenance: DB cleanup failed: {e}", exc_info=True)
            task_results["db_cleanup"] = {"status": "error", "error": str(e)}

        # Run backup
        try:
            logger.info("Weekly maintenance: Starting full backup...")
            backup_result = await run_full_backup()
            task_results["backup"] = {"status": "success", "result": backup_result}
            logger.info("Weekly maintenance: Backup completed")
        except Exception as e:
            logger.error(f"Weekly maintenance: Backup failed: {e}", exc_info=True)
            task_results["backup"] = {"status": "error", "error": str(e)}

        # Run Anki card generation
        try:
            logger.info("Weekly maintenance: Starting Anki card generation...")
            generator = AnkiCardGenerator()

            # 1. Generate cards for new highlights
            new_highlights = generator.get_highlights_without_cards(limit=50)
            cards_created = 0
            if new_highlights:
                for highlight in new_highlights:
                    cards = generator.generate_cards(highlight, preview=False)
                    cards_created += len(cards)

            # 2. Process chapter/book completions
            completions = generator.process_completions(preview=False)

            task_results["anki_cards"] = {
                "status": "success",
                "new_highlight_cards": cards_created,
                "chapter_summaries": completions.get("chapters_processed", 0),
                "book_summaries": completions.get("books_processed", 0),
                "total_cards_created": completions.get("total_cards_created", 0) + cards_created
            }
            logger.info(f"Weekly maintenance: Anki cards completed - {cards_created} highlight cards, {completions.get('total_cards_created', 0)} summary cards")
        except Exception as e:
            logger.error(f"Weekly maintenance: Anki card generation failed: {e}", exc_info=True)
            task_results["anki_cards"] = {"status": "error", "error": str(e)}

        logger.info(f"Weekly maintenance completed: {task_results}")

    background_tasks.add_task(run_all_weekly_tasks)
    return results


@app.post("/generate/anki-cards")
async def generate_anki_cards_endpoint(
    background_tasks: BackgroundTasks,
    book_title: str = None,
    days: int = None,
    check_completions: bool = True,
    preview: bool = False
):
    """
    Generate Anki flashcards from book highlights.

    This endpoint creates varied card types from highlights:
    - Q&A cards from concepts
    - Cloze cards (fill-in-the-blank)
    - Reflection prompts
    - Chapter summaries (when a chapter is completed)
    - Book summaries (when a book is finished)

    Args:
        book_title: Optional - limit to specific book
        days: Optional - only process highlights from last N days
        check_completions: Also check for completed chapters/books (default True)
        preview: If True, returns what would be created without saving

    Returns:
        Summary of cards created (or preview of cards)
    """
    from generate_anki_cards import AnkiCardGenerator

    if preview:
        # Synchronous preview
        try:
            generator = AnkiCardGenerator()
            result = {
                "mode": "preview",
                "new_highlights": [],
                "completions": {}
            }

            # Get highlights without cards
            new_highlights = generator.get_highlights_without_cards(
                book_title=book_title,
                days=days,
                limit=20
            )
            result["new_highlights"] = [
                {"book": h["book_title"], "highlight": h["text"][:100] + "..."}
                for h in new_highlights
            ]

            if check_completions:
                result["completions"] = generator.process_completions(preview=True)

            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Background execution
    async def run_card_generation():
        try:
            generator = AnkiCardGenerator()

            # Get highlights
            new_highlights = generator.get_highlights_without_cards(
                book_title=book_title,
                days=days,
                limit=50
            )

            cards_created = 0
            for highlight in new_highlights:
                cards = generator.generate_cards(highlight, preview=False)
                cards_created += len(cards)

            # Check completions
            completions = {}
            if check_completions:
                completions = generator.process_completions(preview=False)

            logger.info(f"Anki card generation completed: {cards_created} cards from highlights, completions: {completions}")
        except Exception as e:
            logger.error(f"Anki card generation failed: {e}", exc_info=True)

    background_tasks.add_task(run_card_generation)
    return {
        "status": "queued",
        "message": "Anki card generation started in background",
        "params": {
            "book_title": book_title,
            "days": days,
            "check_completions": check_completions
        }
    }


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

# --- Newsletter Sync ---

@app.post("/sync/newsletters")
async def sync_newsletters(full: bool = False, hours: int = 24):
    """
    Bidirectional sync between Notion and Supabase for newsletters.

    Args:
        full: If True, performs full sync. If False, incremental sync.
        hours: For incremental sync, how many hours to look back (default 24).
    """
    try:
        logger.info(f"Starting newsletter sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_newsletters_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Newsletter sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- Journal Sync ---

@app.post("/sync/journals")
async def sync_journals(full: bool = False, hours: int = 24):
    """
    Bidirectional sync between Notion and Supabase for journals (daily entries).

    Args:
        full: If True, performs full sync. If False, incremental sync.
        hours: For incremental sync, how many hours to look back (default 24).
    """
    try:
        logger.info(f"Starting journal sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_journal_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Journal sync failed: {e}")
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


# --- Calendar Event Creation ---
from pydantic import BaseModel
from typing import Optional, List
from lib.google_calendar import GoogleCalendarClient

class CreateCalendarEventRequest(BaseModel):
    """Request body for creating a calendar event."""
    summary: str
    start_time: str  # ISO 8601 format
    end_time: str    # ISO 8601 format
    description: Optional[str] = None
    location: Optional[str] = None
    attendees: Optional[List[str]] = None  # List of email addresses
    timezone: Optional[str] = None  # e.g., 'Asia/Singapore'


@app.post("/calendar/create")
async def create_calendar_event(request: CreateCalendarEventRequest):
    """
    Create a new Google Calendar event.
    
    Args:
        summary: Event title
        start_time: ISO 8601 datetime string (e.g., '2025-01-15T14:00:00')
        end_time: ISO 8601 datetime string
        description: Optional event description
        location: Optional location
        attendees: Optional list of email addresses to invite
        timezone: Optional timezone (defaults to user's timezone or UTC)
        
    Returns:
        Created event details including Google event ID and link
    """
    try:
        from datetime import datetime
        
        # Parse datetime strings
        start_dt = datetime.fromisoformat(request.start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(request.end_time.replace('Z', '+00:00'))
        
        # Get user's timezone if not provided
        tz = request.timezone
        if not tz:
            try:
                from lib.supabase_client import supabase
                result = supabase.table("sync_state").select("value").eq("key", "user_timezone").execute()
                if result.data:
                    tz = result.data[0]["value"]
            except Exception:
                pass
        tz = tz or "UTC"
        
        logger.info(f"Creating calendar event: {request.summary} at {request.start_time}")
        
        calendar_client = GoogleCalendarClient()
        event = await calendar_client.create_event(
            summary=request.summary,
            start_time=start_dt,
            end_time=end_dt,
            description=request.description,
            location=request.location,
            attendees=request.attendees,
            timezone_str=tz
        )
        
        logger.info(f"Created calendar event: {event.get('id')}")
        
        return {
            "status": "success",
            "event_id": event.get("id"),
            "html_link": event.get("htmlLink"),
            "summary": event.get("summary"),
            "start": event.get("start"),
            "end": event.get("end"),
            "created": event.get("created")
        }
        
    except Exception as e:
        logger.error(f"Failed to create calendar event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class UpdateCalendarEventRequest(BaseModel):
    """Request body for updating a calendar event."""
    event_id: str
    summary: Optional[str] = None
    start_time: Optional[str] = None  # ISO 8601 format
    end_time: Optional[str] = None    # ISO 8601 format
    description: Optional[str] = None
    location: Optional[str] = None
    attendees: Optional[List[str]] = None
    timezone: Optional[str] = None
    send_updates: str = "all"  # 'all', 'externalOnly', or 'none'


@app.post("/calendar/update")
async def update_calendar_event(request: UpdateCalendarEventRequest):
    """
    Update an existing Google Calendar event (reschedule).
    
    Use cases:
    - Reschedule your own meeting: Update times and send notifications
    - Add a reason/note: Update description with "Rescheduled due to..."
    - Change location or attendees
    
    Args:
        event_id: Google Calendar event ID
        summary: Updated title (optional)
        start_time: New start time in ISO 8601 format (optional)
        end_time: New end time (optional)
        description: Updated description - use this to add reschedule reason (optional)
        location: Updated location (optional)
        attendees: Updated attendee list (optional)
        send_updates: 'all' (notify everyone), 'externalOnly', or 'none'
    """
    try:
        from datetime import datetime
        
        # Parse datetime strings if provided
        start_dt = None
        end_dt = None
        if request.start_time:
            start_dt = datetime.fromisoformat(request.start_time.replace('Z', '+00:00'))
        if request.end_time:
            end_dt = datetime.fromisoformat(request.end_time.replace('Z', '+00:00'))
        
        logger.info(f"Updating calendar event: {request.event_id}")
        
        calendar_client = GoogleCalendarClient()
        event = await calendar_client.update_event(
            event_id=request.event_id,
            summary=request.summary,
            start_time=start_dt,
            end_time=end_dt,
            description=request.description,
            location=request.location,
            attendees=request.attendees,
            timezone_str=request.timezone,
            send_updates=request.send_updates
        )
        
        logger.info(f"Updated calendar event: {event.get('id')}")
        
        return {
            "status": "success",
            "event_id": event.get("id"),
            "html_link": event.get("htmlLink"),
            "summary": event.get("summary"),
            "start": event.get("start"),
            "end": event.get("end"),
            "updated": event.get("updated")
        }
        
    except Exception as e:
        logger.error(f"Failed to update calendar event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class DeclineCalendarEventRequest(BaseModel):
    """Request body for declining a calendar invitation."""
    event_id: str
    comment: Optional[str] = None  # Message to send with the decline


@app.post("/calendar/decline")
async def decline_calendar_event(request: DeclineCalendarEventRequest):
    """
    Decline a calendar invitation.
    
    Use case: Someone invited you to a meeting, you want to decline and optionally
    suggest an alternative time via the comment.
    
    Args:
        event_id: Google Calendar event ID
        comment: Optional message (e.g., "Can we do 3pm instead?")
    """
    try:
        logger.info(f"Declining calendar event: {request.event_id}")
        
        calendar_client = GoogleCalendarClient()
        event = await calendar_client.decline_event(
            event_id=request.event_id,
            comment=request.comment
        )
        
        logger.info(f"Declined calendar event: {event.get('id')}")
        
        return {
            "status": "success",
            "event_id": event.get("id"),
            "summary": event.get("summary"),
            "response_status": "declined"
        }
        
    except Exception as e:
        logger.error(f"Failed to decline calendar event: {e}")
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


@app.get("/gmail/search")
async def search_gmail_live(q: str, max_results: int = 10):
    """
    Search Gmail in real-time using Gmail's search syntax.
    
    Args:
        q: Gmail search query (e.g., 'from:john subject:meeting after:2025/01/01')
        max_results: Maximum number of emails to return
        
    Returns:
        List of matching emails with full details
    """
    try:
        logger.info(f"Live Gmail search: {q}")
        gmail_client = GmailClient()
        
        # Search for messages
        messages = await gmail_client.search_messages(query=q, max_results=max_results)
        
        # Fetch full details for each message
        detailed_emails = []
        for msg_meta in messages:
            try:
                msg = await gmail_client.get_message(msg_meta["id"], format="full")
                payload = msg.get("payload", {})
                
                body_content = gmail_client.parse_message_body(payload) or {}
                subject = gmail_client.get_header(payload, "Subject")
                sender = gmail_client.get_header(payload, "From")
                recipient = gmail_client.get_header(payload, "To")
                date_str = gmail_client.get_header(payload, "Date")
                
                detailed_emails.append({
                    "id": msg["id"],
                    "thread_id": msg.get("threadId"),
                    "subject": subject or "(no subject)",
                    "from": sender or "Unknown",
                    "to": recipient or "Unknown",
                    "date": date_str,
                    "snippet": msg.get("snippet", ""),
                    "body": body_content.get("text", msg.get("snippet", "")),
                    "labels": msg.get("labelIds", [])
                })
            except Exception as e:
                logger.warning(f"Failed to fetch message {msg_meta['id']}: {e}")
                continue
        
        return {
            "emails": detailed_emails,
            "count": len(detailed_emails),
            "query": q
        }
        
    except Exception as e:
        logger.error(f"Gmail search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Gmail Send ---
from lib.google_gmail import GmailClient

class SendEmailRequest(BaseModel):
    """Request body for sending an email."""
    to: str  # Recipient email (can be comma-separated for multiple)
    subject: str
    body: str
    cc: Optional[str] = None
    bcc: Optional[str] = None
    reply_to_message_id: Optional[str] = None  # For replying to existing thread
    is_html: bool = False  # If True, body is treated as HTML


@app.post("/gmail/send")
async def send_email(request: SendEmailRequest):
    """
    Send an email via Gmail API.
    
    Args:
        to: Recipient email address (comma-separated for multiple)
        subject: Email subject line
        body: Email body (plain text or HTML)
        cc: Optional CC recipients
        bcc: Optional BCC recipients
        reply_to_message_id: If replying to an email thread
        is_html: If True, body is HTML formatted
        
    Returns:
        Sent message details including ID and thread ID
    """
    try:
        logger.info(f"Sending email to: {request.to}, subject: {request.subject[:50]}...")
        
        gmail_client = GmailClient()
        result = await gmail_client.send_email(
            to=request.to,
            subject=request.subject,
            body=request.body,
            cc=request.cc,
            bcc=request.bcc,
            reply_to_message_id=request.reply_to_message_id,
            is_html=request.is_html
        )
        
        logger.info(f"Email sent successfully: {result.get('id')}")
        
        return {
            "status": "success",
            "message_id": result.get("id"),
            "thread_id": result.get("threadId"),
            "label_ids": result.get("labelIds", [])
        }
        
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# GMAIL DRAFTS API - Full draft management for agents
# =============================================================================

class CreateDraftRequest(BaseModel):
    """Request body for creating an email draft."""
    to: str
    subject: str
    body: str
    cc: Optional[str] = None
    bcc: Optional[str] = None
    is_html: bool = False
    reply_to_message_id: Optional[str] = None


class UpdateDraftRequest(BaseModel):
    """Request body for updating an email draft."""
    to: str
    subject: str
    body: str
    cc: Optional[str] = None
    bcc: Optional[str] = None
    is_html: bool = False


@app.get("/gmail/drafts")
async def list_drafts(limit: int = 20):
    """
    List all email drafts from Gmail.
    
    Args:
        limit: Maximum number of drafts to return (default 20)
        
    Returns:
        List of drafts with id, to, subject, snippet
    """
    try:
        gmail_client = GmailClient()
        drafts = await gmail_client.list_drafts(max_results=limit)
        
        # Fetch details for each draft
        detailed_drafts = []
        for draft in drafts:
            try:
                draft_detail = await gmail_client.get_draft(draft["id"], format="metadata")
                message = draft_detail.get("message", {})
                payload = message.get("payload", {})
                
                # Extract headers
                to = gmail_client.get_header(payload, "To")
                subject = gmail_client.get_header(payload, "Subject")
                
                detailed_drafts.append({
                    "draft_id": draft["id"],
                    "message_id": message.get("id"),
                    "thread_id": message.get("threadId"),
                    "to": to,
                    "subject": subject,
                    "snippet": message.get("snippet", "")[:100]
                })
            except Exception as e:
                logger.warning(f"Failed to fetch draft {draft['id']}: {e}")
                continue
        
        return {
            "status": "success",
            "count": len(detailed_drafts),
            "drafts": detailed_drafts
        }
        
    except Exception as e:
        logger.error(f"Failed to list drafts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/gmail/drafts/{draft_id}")
async def get_draft(draft_id: str):
    """
    Get a specific draft by ID with full content.
    
    Args:
        draft_id: The Gmail draft ID
        
    Returns:
        Full draft details including body content
    """
    try:
        gmail_client = GmailClient()
        draft = await gmail_client.get_draft(draft_id, format="full")
        
        message = draft.get("message", {})
        payload = message.get("payload", {})
        
        # Extract headers
        to = gmail_client.get_header(payload, "To")
        cc = gmail_client.get_header(payload, "Cc")
        subject = gmail_client.get_header(payload, "Subject")
        
        # Extract body
        body_parts = gmail_client.parse_message_body(payload)
        
        return {
            "status": "success",
            "draft": {
                "draft_id": draft["id"],
                "message_id": message.get("id"),
                "thread_id": message.get("threadId"),
                "to": to,
                "cc": cc,
                "subject": subject,
                "body_text": body_parts.get("text", ""),
                "body_html": body_parts.get("html", ""),
                "snippet": message.get("snippet", "")
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get draft {draft_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/gmail/drafts")
async def create_draft(request: CreateDraftRequest):
    """
    Create a new email draft in Gmail.
    
    The draft will appear in the user's Gmail Drafts folder immediately.
    
    Args:
        to: Recipient email address
        subject: Email subject line
        body: Email body content
        cc: Optional CC recipients
        bcc: Optional BCC recipients
        is_html: If True, body is HTML formatted
        reply_to_message_id: If replying to an existing email thread
        
    Returns:
        Created draft details including draft_id
    """
    try:
        logger.info(f"Creating draft to: {request.to}, subject: {request.subject[:50]}...")
        
        gmail_client = GmailClient()
        result = await gmail_client.create_draft(
            to=request.to,
            subject=request.subject,
            body=request.body,
            cc=request.cc,
            bcc=request.bcc,
            is_html=request.is_html,
            reply_to_message_id=request.reply_to_message_id
        )
        
        logger.info(f"Draft created: {result.get('id')}")
        
        return {
            "status": "success",
            "draft_id": result.get("id"),
            "message_id": result.get("message", {}).get("id"),
            "thread_id": result.get("message", {}).get("threadId"),
            "message": f"Draft created and saved to Gmail Drafts folder"
        }
        
    except Exception as e:
        logger.error(f"Failed to create draft: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/gmail/drafts/{draft_id}")
async def update_draft(draft_id: str, request: UpdateDraftRequest):
    """
    Update an existing draft.
    
    Args:
        draft_id: The draft ID to update
        to: Recipient email address
        subject: Email subject line
        body: Email body content
        cc: Optional CC recipients
        bcc: Optional BCC recipients
        is_html: If True, body is HTML formatted
        
    Returns:
        Updated draft details
    """
    try:
        logger.info(f"Updating draft {draft_id}...")
        
        gmail_client = GmailClient()
        result = await gmail_client.update_draft(
            draft_id=draft_id,
            to=request.to,
            subject=request.subject,
            body=request.body,
            cc=request.cc,
            bcc=request.bcc,
            is_html=request.is_html
        )
        
        logger.info(f"Draft updated: {result.get('id')}")
        
        return {
            "status": "success",
            "draft_id": result.get("id"),
            "message_id": result.get("message", {}).get("id"),
            "message": "Draft updated successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to update draft {draft_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/gmail/drafts/{draft_id}")
async def delete_draft(draft_id: str):
    """
    Delete a draft permanently.
    
    Args:
        draft_id: The draft ID to delete
        
    Returns:
        Success confirmation
    """
    try:
        logger.info(f"Deleting draft {draft_id}...")
        
        gmail_client = GmailClient()
        await gmail_client.delete_draft(draft_id)
        
        logger.info(f"Draft deleted: {draft_id}")
        
        return {
            "status": "success",
            "message": f"Draft {draft_id} deleted"
        }
        
    except Exception as e:
        logger.error(f"Failed to delete draft {draft_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/gmail/drafts/{draft_id}/send")
async def send_draft(draft_id: str):
    """
    Send an existing draft.
    
    This removes the draft from Drafts folder and sends it as an email.
    Use this after user explicitly confirms they want to send.
    
    Args:
        draft_id: The draft ID to send
        
    Returns:
        Sent message details
    """
    try:
        logger.info(f"Sending draft {draft_id}...")
        
        gmail_client = GmailClient()
        result = await gmail_client.send_draft(draft_id)
        
        logger.info(f"Draft sent as message: {result.get('id')}")
        
        return {
            "status": "success",
            "message_id": result.get("id"),
            "thread_id": result.get("threadId"),
            "label_ids": result.get("labelIds", []),
            "message": "Email sent successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to send draft {draft_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Email Follow-Up Tracking ---

@app.post("/sync/follow-ups")
async def sync_follow_ups():
    """Manually trigger follow-up sync."""
    try:
        logger.info("Starting follow-up sync via API")
        result = await run_follow_up_sync()
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error(f"Follow-up sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/follow-ups")
async def list_follow_ups(status: Optional[str] = None):
    """
    List tracked follow-ups with optional status filter.

    Args:
        status: Filter by status (pending, draft_created, send_now, sent, replied, cancelled)
    """
    try:
        query = supabase.table("email_follow_ups").select(
            "id, subject, recipient_email, recipient_name, status, "
            "follow_up_count, max_follow_ups, interval_days, "
            "next_follow_up_date, original_date, last_sent_at, "
            "draft_body, gmail_draft_id, contact_id, created_at"
        ).is_("deleted_at", "null").order("created_at", desc=True)

        if status:
            query = query.eq("status", status)

        result = query.execute()
        return {"status": "success", "count": len(result.data), "data": result.data}
    except Exception as e:
        logger.error(f"Failed to list follow-ups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/follow-ups/webhook/send")
async def webhook_send_follow_up(request: Request, secret: str = ""):
    """
    Webhook endpoint for Notion 'Send' button.
    Looks up the follow-up by Notion page ID from the webhook payload.
    Requires ?secret= query parameter for basic auth.
    """
    # Simple secret check
    expected_secret = os.getenv("FOLLOW_UP_WEBHOOK_SECRET", "jarvis-followup-send-2026")
    if secret != expected_secret:
        raise HTTPException(status_code=403, detail="Invalid webhook secret")

    import re

    try:
        body = await request.json()
    except Exception:
        body = {}

    logger.info(f"Notion webhook payload: {body}")

    def _extract_notion_page_id(value: str) -> Optional[str]:
        """Extract a 32-char hex page ID from a Notion URL or raw ID."""
        # Notion URLs end with a 32-char hex ID (possibly after a title slug)
        match = re.search(r'([0-9a-f]{32})', value.replace("-", ""))
        if match:
            return match.group(1)
        return None

    # Try to extract Notion page ID from various payload locations
    notion_page_id = None

    # Notion webhook payloads can vary - try common structures
    if isinstance(body, dict):
        # Direct page_id field
        for key in ("page_id", "pageId", "id"):
            if body.get(key):
                notion_page_id = _extract_notion_page_id(str(body[key]))
                if notion_page_id:
                    break

        # Nested in data object
        if not notion_page_id and "data" in body:
            data = body["data"]
            if isinstance(data, dict):
                for key in ("page_id", "pageId", "id", "url"):
                    if data.get(key):
                        notion_page_id = _extract_notion_page_id(str(data[key]))
                        if notion_page_id:
                            break

        # Scan all string values for Notion URLs or page IDs
        if not notion_page_id:
            for key, val in body.items():
                if isinstance(val, str) and ("notion.so" in val or "notion.com" in val):
                    notion_page_id = _extract_notion_page_id(val)
                    if notion_page_id:
                        break

    if not notion_page_id:
        logger.warning(f"Notion webhook: could not extract page_id from payload: {body}")
        raise HTTPException(status_code=400, detail="Could not determine follow-up from webhook payload")

    # Format as UUID with dashes
    notion_page_id = f"{notion_page_id[:8]}-{notion_page_id[8:12]}-{notion_page_id[12:16]}-{notion_page_id[16:20]}-{notion_page_id[20:]}"

    # Look up follow-up by notion_page_id
    result = supabase.table("email_follow_ups").select(
        "id, gmail_draft_id, follow_up_count, max_follow_ups, interval_days, subject, recipient_email, status"
    ).eq("notion_page_id", notion_page_id).is_("deleted_at", "null").execute()

    if not result.data:
        logger.warning(f"Notion webhook: no follow-up found for page {notion_page_id}")
        raise HTTPException(status_code=404, detail=f"No follow-up found for Notion page {notion_page_id}")

    record = result.data[0]

    if not record.get("gmail_draft_id"):
        return {"status": "no_draft", "message": "No draft available yet - draft will be generated when timer expires"}

    # Send the draft
    gmail_client = GmailClient()
    await gmail_client.send_draft(record["gmail_draft_id"])

    now = datetime.now(timezone.utc)
    interval = record.get("interval_days", 7)

    if record["follow_up_count"] >= record["max_follow_ups"]:
        new_status = "sent"
        next_date = now.isoformat()
    else:
        new_status = "pending"
        next_date = (now + timedelta(days=interval)).isoformat()

    supabase.table("email_follow_ups").update({
        "status": new_status,
        "gmail_draft_id": None,
        "draft_body": None,
        "last_sent_at": now.isoformat(),
        "next_follow_up_date": next_date,
        "updated_at": now.isoformat(),
    }).eq("id", record["id"]).execute()

    # Update Notion page immediately so user sees feedback
    notion_token = os.getenv("NOTION_API_TOKEN")
    if notion_token:
        try:
            import httpx
            notion_status = "Sent" if new_status == "sent" else "Pending"
            last_sent_date = now.strftime("%Y-%m-%d")
            async with httpx.AsyncClient(timeout=10.0) as http_client:
                await http_client.patch(
                    f"https://api.notion.com/v1/pages/{notion_page_id}",
                    headers={
                        "Authorization": f"Bearer {notion_token}",
                        "Notion-Version": "2022-06-28",
                        "Content-Type": "application/json",
                    },
                    json={
                        "properties": {
                            "Status": {"select": {"name": notion_status}},
                            "Last Sent": {"date": {"start": last_sent_date}},
                        }
                    },
                )
            logger.info(f"Updated Notion page status to '{notion_status}'")
        except Exception as e:
            logger.warning(f"Failed to update Notion page status (non-blocking): {e}")

    logger.info(f"Notion webhook: sent follow-up to {record['recipient_email']} (page {notion_page_id})")
    return {"status": "success", "message": f"Follow-up sent to {record['recipient_email']}"}


@app.post("/follow-ups/track")
async def track_follow_up(email_id: str = "", thread_id: str = "", interval_days: int = 7, max_follow_ups: int = 3):
    """
    Start tracking an email for follow-up.
    Accepts either email_id (UUID from emails table) or thread_id.
    Adds Gmail Follow-Up label and creates tracking record.
    """
    from email.utils import parseaddr
    from sync_follow_ups import FOLLOW_UP_LABEL_NAME, FollowUpSync

    if not email_id and not thread_id:
        raise HTTPException(status_code=400, detail="Provide either email_id or thread_id")

    try:
        # Look up the email
        if email_id:
            email_result = supabase.table("emails").select(
                "id, google_message_id, thread_id, sender, recipient, subject, body_text, date"
            ).eq("id", email_id).execute()
        else:
            # Find the most recent outbound email in the thread
            email_result = supabase.table("emails").select(
                "id, google_message_id, thread_id, sender, recipient, subject, body_text, date"
            ).eq("thread_id", thread_id).order("date", desc=True).limit(10).execute()

        if not email_result.data:
            raise HTTPException(status_code=404, detail="Email not found")

        # Get user email for sender check
        syncer = FollowUpSync()
        user_email = await syncer._get_user_email()

        # Find the outbound email (sent by Aaron)
        email_record = None
        for e in email_result.data:
            sender_email = parseaddr(e.get("sender", ""))[1].lower()
            if sender_email == user_email:
                email_record = e
                break

        if not email_record:
            raise HTTPException(status_code=400, detail="No outbound email found (must be sent by you)")

        # Check if thread is already tracked
        existing = supabase.table("email_follow_ups").select("id, status").eq(
            "thread_id", email_record["thread_id"]
        ).is_("deleted_at", "null").execute()

        if existing.data:
            rec = existing.data[0]
            return {
                "status": "already_tracked",
                "follow_up_id": rec["id"],
                "current_status": rec["status"],
                "message": f"Thread already tracked (status: {rec['status']})",
            }

        # Add Gmail Follow-Up label
        label_id = await syncer._ensure_label()
        try:
            await syncer.gmail_client.modify_message_labels(
                email_record["google_message_id"],
                add_label_ids=[label_id]
            )
        except Exception as e:
            logger.warning(f"Failed to add Gmail label (continuing anyway): {e}")

        # Extract recipient info
        recipient_raw = email_record.get("recipient", "")
        recipient_email = parseaddr(recipient_raw)[1].lower() if recipient_raw else ""
        recipient_name = parseaddr(recipient_raw)[0] if recipient_raw else ""

        # Look up contact
        from lib.supabase_client import find_contact_by_email
        contact_id = find_contact_by_email(recipient_email) if recipient_email else None

        if not recipient_name and contact_id:
            try:
                contact_result = supabase.table("contacts").select("full_name").eq(
                    "id", contact_id
                ).limit(1).execute()
                if contact_result.data:
                    recipient_name = contact_result.data[0].get("full_name", "")
            except Exception:
                pass

        # Create tracking record
        now = datetime.now(timezone.utc)
        follow_up_record = {
            "email_id": email_record["id"],
            "google_message_id": email_record["google_message_id"],
            "thread_id": email_record["thread_id"],
            "subject": email_record.get("subject"),
            "recipient_email": recipient_email,
            "recipient_name": recipient_name or None,
            "original_body_text": (email_record.get("body_text") or "")[:5000] or None,
            "original_date": email_record.get("date"),
            "contact_id": contact_id,
            "status": "pending",
            "interval_days": interval_days,
            "next_follow_up_date": (now + timedelta(days=interval_days)).isoformat(),
            "follow_up_count": 0,
            "max_follow_ups": max_follow_ups,
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }

        insert_result = supabase.table("email_follow_ups").insert(follow_up_record).execute()
        new_record = insert_result.data[0] if insert_result.data else {}

        logger.info(
            f"Tracking follow-up via API: '{email_record.get('subject')}' "
            f"to {recipient_email} (interval: {interval_days}d, max: {max_follow_ups})"
        )

        return {
            "status": "success",
            "follow_up_id": new_record.get("id"),
            "subject": email_record.get("subject"),
            "recipient": recipient_email,
            "next_follow_up_date": follow_up_record["next_follow_up_date"],
            "message": f"Now tracking follow-up to {recipient_email}",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to track follow-up: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/follow-ups/{follow_up_id}/cancel")
async def cancel_follow_up(follow_up_id: str):
    """Cancel a follow-up and remove Gmail label."""
    try:
        # Get the follow-up record
        result = supabase.table("email_follow_ups").select(
            "google_message_id, gmail_draft_id"
        ).eq("id", follow_up_id).execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Follow-up not found")

        record = result.data[0]

        # Update status
        supabase.table("email_follow_ups").update({
            "status": "cancelled",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", follow_up_id).execute()

        # Remove Gmail label
        gmail_client = GmailClient()
        try:
            # Get cached label ID
            label_result = supabase.table("sync_state").select("value").eq(
                "key", "gmail_follow_up_label_id"
            ).execute()
            if label_result.data and label_result.data[0].get("value"):
                await gmail_client.modify_message_labels(
                    record["google_message_id"],
                    remove_label_ids=[label_result.data[0]["value"]]
                )
        except Exception as e:
            logger.warning(f"Failed to remove Gmail label: {e}")

        # Delete draft if exists
        if record.get("gmail_draft_id"):
            try:
                await gmail_client.delete_draft(record["gmail_draft_id"])
            except Exception:
                pass

        return {"status": "success", "message": "Follow-up cancelled"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel follow-up: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/follow-ups/{follow_up_id}/send")
async def send_follow_up(follow_up_id: str):
    """Send a follow-up draft immediately."""
    try:
        result = supabase.table("email_follow_ups").select(
            "gmail_draft_id, follow_up_count, max_follow_ups, interval_days, subject, recipient_email"
        ).eq("id", follow_up_id).execute()

        if not result.data:
            raise HTTPException(status_code=404, detail="Follow-up not found")

        record = result.data[0]
        if not record.get("gmail_draft_id"):
            raise HTTPException(status_code=400, detail="No draft available to send")

        # Send the draft
        gmail_client = GmailClient()
        await gmail_client.send_draft(record["gmail_draft_id"])

        now = datetime.now(timezone.utc)
        interval = record.get("interval_days", 7)

        if record["follow_up_count"] >= record["max_follow_ups"]:
            new_status = "sent"
            next_date = now.isoformat()
        else:
            new_status = "pending"
            next_date = (now + timedelta(days=interval)).isoformat()

        supabase.table("email_follow_ups").update({
            "status": new_status,
            "gmail_draft_id": None,
            "draft_body": None,
            "last_sent_at": now.isoformat(),
            "next_follow_up_date": next_date,
            "updated_at": now.isoformat(),
        }).eq("id", follow_up_id).execute()

        return {
            "status": "success",
            "message": f"Follow-up sent to {record['recipient_email']}",
            "next_status": new_status,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send follow-up: {e}")
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
        result = await run_in_threadpool(run_highlights_sync, full_sync=full, hours=hours)
        return result
    except Exception as e:
        logger.error(f"Highlights sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Articles Capture ---

from capture_article import ArticleCaptureService

class ArticleCaptureRequest(BaseModel):
    url: str
    tags: list[str] | None = None
    upload_to_bookfusion: bool = True


@app.post("/articles/capture")
async def capture_article(request: ArticleCaptureRequest):
    """
    Capture an online article and upload to Bookfusion.

    Workflow:
    1. Extract article content (title, text, images)
    2. Generate EPUB with e-ink optimized formatting
    3. Upload to Bookfusion (to "Articles" shelf)
    4. Store article record in Supabase

    The article will appear in Bookfusion app shortly and can be read on Boox.
    Highlights sync back via existing Bookfusion → Notion → sync_highlights pipeline.

    Args:
        url: Article URL to capture
        tags: Optional tags for categorization
        upload_to_bookfusion: Whether to upload to Bookfusion (default True)
    """
    try:
        logger.info(f"Capturing article: {request.url}")
        service = ArticleCaptureService()
        result = await service.capture(
            url=request.url,
            upload_to_bookfusion=request.upload_to_bookfusion,
            tags=request.tags,
            skip_existing=True,
            keep_epub=False
        )

        if result.success:
            return {
                "success": True,
                "article": {
                    "id": result.article_id,
                    "title": result.title,
                    "bookfusion_id": result.bookfusion_id,
                    "already_exists": result.already_exists
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)

    except Exception as e:
        logger.error(f"Article capture failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Articles Link Highlights ---

from link_highlights_to_articles import link_highlights_to_articles


@app.post("/articles/link-highlights")
async def link_article_highlights(dry_run: bool = False):
    """
    Link unlinked highlights to their corresponding articles.

    When highlights sync from Bookfusion → Notion → Supabase, they have a book_title
    but may not be linked to an article. This endpoint matches them by title.

    Args:
        dry_run: If True, preview without making changes
    """
    try:
        logger.info(f"Linking article highlights (dry_run={dry_run})")
        stats = await run_in_threadpool(link_highlights_to_articles, None, dry_run)
        return {
            "success": True,
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Article highlight linking failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Applications Sync ---

@app.post("/sync/applications")
async def sync_applications(full: bool = False, hours: int = 24):
    """
    Sync applications (grants, fellowships, accelerators) bidirectionally.
    Source: Notion Applications database
    
    Args:
        full: If True, sync all applications. If False, only recently updated.
        hours: For incremental sync, how many hours to look back.
    """
    try:
        logger.info(f"Starting applications sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_applications_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Applications sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- LinkedIn Posts Sync ---

@app.post("/sync/linkedin-posts")
async def sync_linkedin_posts(full: bool = False, hours: int = 24):
    """
    Sync LinkedIn posts bidirectionally.
    Source: Notion LinkedIn Posts database
    
    Args:
        full: If True, sync all posts. If False, only recently updated.
        hours: For incremental sync, how many hours to look back.
    """
    try:
        logger.info(f"Starting LinkedIn posts sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_linkedin_posts_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"LinkedIn posts sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Documents Sync ---

@app.post("/sync/documents")
async def sync_documents(full: bool = False, hours: int = 24):
    """
    Sync documents (CVs, applications, notes) bidirectionally.
    Source: Notion Documents database
    
    Args:
        full: If True, sync all documents. If False, only recently updated.
        hours: For incremental sync, how many hours to look back.
    """
    try:
        logger.info(f"Starting documents sync via API (full={full}, hours={hours})")
        result = await run_in_threadpool(run_documents_sync, full_sync=full, since_hours=hours)
        return result
    except Exception as e:
        logger.error(f"Documents sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Anki Sync ---

@app.post("/sync/anki/manual")
async def sync_anki_manual(test_mode: bool = False):
    """
    Manual trigger for Anki sync (Supabase ↔ Anki Desktop via AnkiConnect).

    Requires:
    - Anki Desktop running with AnkiConnect add-on installed
    - ANKI_SYNC_ENABLED=true in environment

    Performs:
    - Initial import of all existing Anki decks (first run only)
    - Bidirectional sync (Supabase → Anki → Supabase)
    - Review history sync

    Args:
        test_mode: If True, limits import to 2 decks with 10 cards each (for testing large DBs)

    Returns:
        Sync result with import/cards/reviews statistics
    """
    if os.getenv("ANKI_SYNC_ENABLED", "false").lower() != "true":
        return {
            "status": "disabled",
            "message": "Anki sync not enabled. Set ANKI_SYNC_ENABLED=true in .env"
        }

    try:
        if test_mode:
            logger.info("Starting manual Anki sync in TEST MODE (limited import)...")
        else:
            logger.info("Starting manual Anki sync...")
        result = await run_anki_sync(supabase, test_mode=test_mode)
        logger.info(f"Anki sync completed: {result.get('status')}")
        return result
    except Exception as e:
        logger.error(f"Anki sync failed: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "hint": "Make sure Anki Desktop is running with AnkiConnect add-on installed"
        }


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


# ============================================================================
# BEEPER MESSAGING INTEGRATION
# ============================================================================
# NOTE: Beeper sync requires the beeper-bridge service running locally with
# Tailscale access. The BEEPER_BRIDGE_URL environment variable must be set.

from sync_beeper import BeeperSyncService, run_beeper_sync
from lib.supabase_client import supabase
import httpx

BEEPER_BRIDGE_URL = os.getenv("BEEPER_BRIDGE_URL", "http://localhost:8377")


@app.post("/sync/beeper")
async def sync_beeper(full: bool = False):
    """
    Sync Beeper messages from the bridge to Supabase.
    
    This syncs chats and messages from WhatsApp, Telegram, LinkedIn, etc.
    Requires the beeper-bridge service to be running and accessible.
    
    Args:
        full: If True, resync all messages (up to 30 days). 
              If False, only sync since last sync time per chat.
    
    Returns:
        Sync statistics including chats synced, new messages, contacts linked.
    """
    try:
        logger.info(f"Starting Beeper sync via API (full={full})")
        result = await run_beeper_sync(supabase, full_sync=full)
        return {"status": "success", **result}
    except Exception as e:
        logger.error(f"Beeper sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/beeper-notion")
async def sync_beeper_notion(full: bool = False):
    """
    Sync Beeper messages to Notion pages.

    Appends new messages from beeper_messages in Supabase to configured
    Notion pages. Requires BEEPER_NOTION_SYNC_MAPPINGS env var.

    Args:
        full: If True, resync all messages (ignore cursor).
              If False, only sync since last sync time.
    """
    try:
        logger.info(f"Starting Beeper→Notion sync via API (full={full})")
        result = await run_in_threadpool(run_beeper_notion_sync, supabase, full_sync=full)
        return {"status": "success", **result}
    except Exception as e:
        logger.error(f"Beeper→Notion sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/meeting-av-hq")
async def sync_meeting_av_hq(full: bool = False):
    """
    Sync meetings with AV HQ contacts to the AV HQ Meeting DB.

    Creates pages in the AV HQ teamspace Meeting database for meetings
    involving configured contacts (e.g. Victor), with summary + transcript.

    Args:
        full: If True, re-sync all meetings (ignore existing sync map).
              If False, only sync meetings not yet in AV HQ.
    """
    try:
        logger.info(f"Starting Meeting→AV HQ sync via API (full={full})")
        result = await run_in_threadpool(run_meeting_av_hq_sync, supabase, full_sync=full)
        return {"status": "success", **result}
    except Exception as e:
        logger.error(f"Meeting→AV HQ sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/beeper/relink")
async def relink_beeper_chats():
    """
    Re-process all unlinked Beeper chats to try contact linking again.
    
    Use this after:
    - Adding new contacts to the database
    - Improving the matching algorithm
    - Fixing contact data (phone numbers, names)
    
    This uses improved fuzzy matching that:
    - Checks both chat_name AND remote_user_name
    - Looks for cross-platform matches (same person on WhatsApp + LinkedIn)
    - Uses more aggressive name matching
    
    Returns:
        Statistics on newly linked chats
    """
    try:
        logger.info("Starting Beeper relink of unlinked chats")
        result = await run_beeper_relink(supabase)
        return {"status": "success", **result}
    except Exception as e:
        logger.error(f"Beeper relink failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/beeper/status")
async def beeper_status():
    """
    Check Beeper bridge connectivity and account status.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{BEEPER_BRIDGE_URL}/health")
            if response.status_code != 200:
                return {
                    "status": "error",
                    "bridge_reachable": True,
                    "error": f"Bridge returned status {response.status_code}"
                }
            
            health = response.json()
            
            # Get accounts
            accounts_resp = await client.get(f"{BEEPER_BRIDGE_URL}/accounts")
            accounts = accounts_resp.json() if accounts_resp.status_code == 200 else []
            
            return {
                "status": "healthy",
                "bridge_url": BEEPER_BRIDGE_URL,
                "beeper_connected": health.get("beeper_connected", False),
                "accounts": len(accounts),
                "account_details": [
                    {"id": a.get("id") or a.get("accountID"), "platform": a.get("platform") or a.get("network")}
                    for a in accounts
                ]
            }
    except httpx.ConnectError:
        return {
            "status": "error",
            "bridge_reachable": False,
            "error": f"Cannot connect to bridge at {BEEPER_BRIDGE_URL}",
            "hint": "Ensure beeper-bridge is running and Tailscale is connected"
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.get("/beeper/chats")
async def list_beeper_chats(
    platform: Optional[str] = None,
    unread_only: bool = False,
    unlinked_only: bool = False,
    limit: int = 50
):
    """
    List synced Beeper chats from the database.
    
    Args:
        platform: Filter by platform (whatsapp, telegram, linkedin, etc.)
        unread_only: Only return chats with unread messages
        unlinked_only: Only return chats not linked to a contact
        limit: Maximum number of chats to return
    
    Returns:
        List of chats with contact info where linked
    """
    try:
        query = supabase.table("beeper_chats").select(
            "*, contacts(id, first_name, last_name, company)"
        ).order("last_message_at", desc=True).limit(limit)
        
        if platform:
            query = query.eq("platform", platform)
        if unread_only:
            query = query.gt("unread_count", 0)
        if unlinked_only:
            query = query.is_("contact_id", "null")
        
        result = query.execute()
        
        # Transform for cleaner output
        chats = []
        for chat in result.data or []:
            contact = chat.pop("contacts", None)
            chat["contact"] = {
                "id": contact["id"],
                "name": f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip(),
                "company": contact.get("company")
            } if contact else None
            chats.append(chat)
        
        return {"chats": chats, "count": len(chats)}
    except Exception as e:
        logger.error(f"Failed to list Beeper chats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/beeper/chats/{beeper_chat_id}/messages")
async def get_chat_messages(
    beeper_chat_id: str,
    limit: int = 50,
    before: Optional[str] = None  # ISO timestamp for pagination
):
    """
    Get messages for a specific chat from the database.
    
    Args:
        beeper_chat_id: The Beeper chat ID
        limit: Maximum number of messages to return
        before: Only return messages before this timestamp (for pagination)
    
    Returns:
        List of messages, newest first
    """
    try:
        import urllib.parse
        decoded_id = urllib.parse.unquote(beeper_chat_id)
        
        query = supabase.table("beeper_messages").select("*").eq(
            "beeper_chat_id", decoded_id
        ).order("timestamp", desc=True).limit(limit)
        
        if before:
            query = query.lt("timestamp", before)
        
        result = query.execute()
        
        return {"messages": result.data or [], "count": len(result.data or [])}
    except Exception as e:
        logger.error(f"Failed to get messages for chat {beeper_chat_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/beeper/messages/search")
async def search_beeper_messages(
    q: str,
    platform: Optional[str] = None,
    contact_id: Optional[str] = None,
    limit: int = 50
):
    """
    Full-text search across synced Beeper messages.
    
    Args:
        q: Search query
        platform: Filter by platform (optional)
        contact_id: Filter by linked contact (optional)
        limit: Maximum results to return
    
    Returns:
        Matching messages with chat context
    """
    try:
        # Use Postgres full-text search
        query = supabase.table("beeper_messages").select(
            "*, beeper_chats(chat_name, platform, contact_id, contacts(first_name, last_name))"
        ).text_search("content", q, type_="websearch").order(
            "timestamp", desc=True
        ).limit(limit)
        
        if platform:
            query = query.eq("platform", platform)
        if contact_id:
            query = query.eq("contact_id", contact_id)
        
        result = query.execute()
        
        return {"messages": result.data or [], "count": len(result.data or []), "query": q}
    except Exception as e:
        logger.error(f"Failed to search Beeper messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/beeper/messages/unread")
async def get_unread_messages(limit: int = 100):
    """
    Get all unread messages across all platforms.
    
    Returns:
        Messages grouped by chat with contact info
    """
    try:
        # Get chats with unread messages
        chats_result = supabase.table("beeper_chats").select(
            "beeper_chat_id, chat_name, platform, unread_count, contact_id, contacts(first_name, last_name, company)"
        ).gt("unread_count", 0).order("last_message_at", desc=True).execute()
        
        unread_chats = []
        for chat in chats_result.data or []:
            # Get recent unread messages for this chat
            msgs_result = supabase.table("beeper_messages").select("*").eq(
                "beeper_chat_id", chat["beeper_chat_id"]
            ).eq("is_read", False).eq("is_outgoing", False).order(
                "timestamp", desc=True
            ).limit(5).execute()
            
            contact = chat.pop("contacts", None)
            unread_chats.append({
                **chat,
                "contact": {
                    "id": chat["contact_id"],
                    "name": f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip() if contact else None,
                    "company": contact.get("company") if contact else None
                } if contact else None,
                "recent_messages": msgs_result.data or []
            })
        
        total_unread = sum(c.get("unread_count", 0) for c in unread_chats)
        
        return {
            "total_unread": total_unread,
            "chats_with_unread": len(unread_chats),
            "chats": unread_chats
        }
    except Exception as e:
        logger.error(f"Failed to get unread messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class LinkChatToContactRequest(BaseModel):
    """Request to link a Beeper chat to a contact."""
    contact_id: str


@app.patch("/beeper/chats/{beeper_chat_id}/link-contact")
async def link_chat_to_contact(beeper_chat_id: str, request: LinkChatToContactRequest):
    """
    Manually link a Beeper chat to a contact.
    
    Use this when automatic matching failed or was incorrect.
    """
    try:
        import urllib.parse
        decoded_id = urllib.parse.unquote(beeper_chat_id)
        
        result = supabase.table("beeper_chats").update({
            "contact_id": request.contact_id,
            "contact_link_method": "manual",
            "contact_link_confidence": 1.0,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("beeper_chat_id", decoded_id).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        # Also update all messages in this chat
        supabase.table("beeper_messages").update({
            "contact_id": request.contact_id
        }).eq("beeper_chat_id", decoded_id).execute()
        
        return {"status": "linked", "chat_id": decoded_id, "contact_id": request.contact_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to link chat to contact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/beeper/chats/{beeper_chat_id}/link-contact")
async def unlink_chat_from_contact(beeper_chat_id: str):
    """
    Remove the contact link from a Beeper chat.
    
    Use this when automatic matching was incorrect.
    The chat will become available for manual re-linking.
    """
    try:
        import urllib.parse
        decoded_id = urllib.parse.unquote(beeper_chat_id)
        
        result = supabase.table("beeper_chats").update({
            "contact_id": None,
            "contact_link_method": None,
            "contact_link_confidence": None,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("beeper_chat_id", decoded_id).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        # Also remove from messages
        supabase.table("beeper_messages").update({
            "contact_id": None
        }).eq("beeper_chat_id", decoded_id).execute()
        
        return {"status": "unlinked", "chat_id": decoded_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to unlink chat from contact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class SendMessageRequest(BaseModel):
    """Request to send a message via Beeper."""
    content: str
    reply_to_event_id: Optional[str] = None


@app.post("/beeper/chats/{beeper_chat_id}/send")
async def send_beeper_message(beeper_chat_id: str, request: SendMessageRequest):
    """
    Send a message via Beeper bridge.
    
    ⚠️ IMPORTANT: This endpoint should only be called AFTER explicit user confirmation.
    The Intelligence Service should present the message to the user and get approval
    before calling this endpoint.
    
    Args:
        beeper_chat_id: The chat to send to (URL encoded)
        content: Message text to send
        reply_to_event_id: Optional event ID to reply to
    
    Returns:
        Sent message details
    """
    try:
        import urllib.parse
        decoded_id = urllib.parse.unquote(beeper_chat_id)
        
        logger.info(f"Sending Beeper message to chat {decoded_id}")
        
        # Get chat info for context
        chat_result = supabase.table("beeper_chats").select("platform, chat_name").eq(
            "beeper_chat_id", decoded_id
        ).execute()
        
        if not chat_result.data:
            raise HTTPException(status_code=404, detail="Chat not found in database")
        
        chat_info = chat_result.data[0]
        
        # Send via bridge
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{BEEPER_BRIDGE_URL}/chats/{urllib.parse.quote(decoded_id, safe='')}/send",
                json={
                    "content": request.content,
                    "reply_to_event_id": request.reply_to_event_id
                }
            )
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Bridge error: {response.text}"
                )
            
            result = response.json()
        
        return {
            "status": "sent",
            "platform": chat_info.get("platform"),
            "chat_name": chat_info.get("chat_name"),
            "message_preview": request.content[:100] + ("..." if len(request.content) > 100 else ""),
            "beeper_event_id": result.get("event_id")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to send Beeper message: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Beeper Archive/Unarchive (Inbox-Zero Workflow) ---

@app.post("/beeper/chats/{beeper_chat_id}/archive")
async def archive_beeper_chat(beeper_chat_id: str):
    """
    Archive a Beeper chat (marks it as "answered" in inbox-zero workflow).
    
    In the inbox-zero model:
    - Archived = you've handled this conversation
    - Unarchived = might still need attention
    - needs_response is set to FALSE when archived
    
    This also triggers archive on the Beeper bridge if available.
    """
    try:
        import urllib.parse
        decoded_id = urllib.parse.unquote(beeper_chat_id)
        
        # Update in database
        result = supabase.table("beeper_chats").update({
            "is_archived": True,
            "needs_response": False,  # Archived = handled
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("beeper_chat_id", decoded_id).execute()
        
        if not result.data:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        # Try to archive on Beeper side too (best effort)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f"{BEEPER_BRIDGE_URL}/chats/{urllib.parse.quote(decoded_id, safe='')}/archive"
                )
        except Exception as e:
            logger.warning(f"Could not archive on Beeper side: {e}")
        
        chat = result.data[0]
        return {
            "status": "archived",
            "chat_id": decoded_id,
            "chat_name": chat.get("chat_name"),
            "platform": chat.get("platform"),
            "message": f"Chat archived. It's now considered 'handled' in your inbox."
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to archive chat: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/beeper/chats/{beeper_chat_id}/unarchive")
async def unarchive_beeper_chat(beeper_chat_id: str):
    """
    Unarchive a Beeper chat (brings it back to active inbox).
    
    This recalculates needs_response based on the last message.
    """
    try:
        import urllib.parse
        decoded_id = urllib.parse.unquote(beeper_chat_id)
        
        # Get current chat data to recalculate needs_response
        existing = supabase.table("beeper_chats").select("*").eq(
            "beeper_chat_id", decoded_id
        ).execute()
        
        if not existing.data:
            raise HTTPException(status_code=404, detail="Chat not found")
        
        chat = existing.data[0]
        
        # Recalculate needs_response: TRUE if DM and last message was incoming
        is_dm = chat.get("chat_type") == "dm"
        last_was_incoming = not chat.get("last_message_is_outgoing", True)
        needs_response = is_dm and last_was_incoming
        
        # Update in database
        result = supabase.table("beeper_chats").update({
            "is_archived": False,
            "needs_response": needs_response,
            "archived_at": None,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("beeper_chat_id", decoded_id).execute()
        
        return {
            "status": "unarchived",
            "chat_id": decoded_id,
            "chat_name": chat.get("chat_name"),
            "platform": chat.get("platform"),
            "needs_response": needs_response,
            "message": f"Chat unarchived. needs_response={needs_response}"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to unarchive chat: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/beeper/inbox")
async def get_beeper_inbox(
    include_groups: bool = False,
    limit: int = 50
):
    """
    Get the Beeper inbox (chats that need attention).
    
    This is the main view for inbox-zero workflow:
    - Only DMs by default (groups are usually less urgent)
    - Only non-archived chats
    - Sorted by: needs_response first, then by last_message_at
    - Filters out Slack and Matrix (ignored platforms)
    
    Args:
        include_groups: If True, also include group chats (separately)
        limit: Max chats to return
    
    Returns:
        Inbox with needs_response chats highlighted
    """
    try:
        # Platforms to ignore (high noise, no personal value)
        ignored_platforms = ["slack", "hungryserv", "matrix"]
        
        # Get DMs that need response
        needs_response_query = supabase.table("beeper_chats").select(
            "*, contacts(id, first_name, last_name, company)"
        ).eq("chat_type", "dm").eq("is_archived", False).eq(
            "needs_response", True
        ).not_.in_("platform", ignored_platforms).order(
            "last_message_at", desc=True
        ).limit(limit)
        
        needs_response_result = needs_response_query.execute()
        
        # Get other active DMs
        other_dms_query = supabase.table("beeper_chats").select(
            "*, contacts(id, first_name, last_name, company)"
        ).eq("chat_type", "dm").eq("is_archived", False).eq(
            "needs_response", False
        ).not_.in_("platform", ignored_platforms).order(
            "last_message_at", desc=True
        ).limit(limit)
        
        other_dms_result = other_dms_query.execute()
        
        def format_chat(chat):
            contact = chat.pop("contacts", None)
            return {
                **chat,
                "contact": {
                    "id": contact["id"],
                    "name": f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip(),
                    "company": contact.get("company")
                } if contact else None
            }
        
        inbox = {
            "needs_response": [format_chat(c) for c in (needs_response_result.data or [])],
            "needs_response_count": len(needs_response_result.data or []),
            "other_active": [format_chat(c) for c in (other_dms_result.data or [])],
            "other_active_count": len(other_dms_result.data or []),
        }
        
        # Optionally include groups
        if include_groups:
            groups_query = supabase.table("beeper_chats").select("*").in_(
                "chat_type", ["group", "channel"]
            ).eq("is_archived", False).order("last_message_at", desc=True).limit(20)
            
            groups_result = groups_query.execute()
            inbox["groups"] = groups_result.data or []
            inbox["groups_count"] = len(groups_result.data or [])
        
        return inbox
    except Exception as e:
        logger.error(f"Failed to get Beeper inbox: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/beeper/chats/groups")
async def list_beeper_groups(limit: int = 30):
    """
    List group chats (lower priority, for reference).
    
    Groups are generally less urgent than DMs in the inbox-zero model.
    Filters out Slack and Matrix (ignored platforms).
    """
    try:
        ignored_platforms = ["slack", "hungryserv", "matrix"]
        
        result = supabase.table("beeper_chats").select("*").in_(
            "chat_type", ["group", "channel"]
        ).eq("is_archived", False).not_.in_(
            "platform", ignored_platforms
        ).order("last_message_at", desc=True).limit(limit).execute()
        
        return {
            "groups": result.data or [],
            "count": len(result.data or []),
            "note": "Group chats are lower priority in inbox-zero workflow"
        }
    except Exception as e:
        logger.error(f"Failed to list groups: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/beeper/chats/{beeper_chat_id}/mark-read")
async def mark_chat_read(beeper_chat_id: str):
    """
    Mark all messages in a chat as read.
    """
    try:
        import urllib.parse
        decoded_id = urllib.parse.unquote(beeper_chat_id)
        
        # Update chat unread count
        supabase.table("beeper_chats").update({
            "unread_count": 0,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }).eq("beeper_chat_id", decoded_id).execute()
        
        # Update messages
        supabase.table("beeper_messages").update({
            "is_read": True
        }).eq("beeper_chat_id", decoded_id).eq("is_read", False).execute()
        
        # Try to mark read on Beeper side
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(
                    f"{BEEPER_BRIDGE_URL}/chats/{urllib.parse.quote(decoded_id, safe='')}/read"
                )
        except Exception as e:
            logger.warning(f"Could not mark read on Beeper side: {e}")
        
        return {"status": "marked_read", "chat_id": decoded_id}
    except Exception as e:
        logger.error(f"Failed to mark chat as read: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- Book Enhancement Pipeline ---

from process_new_book import BookProcessingPipeline
from sync_drive_books import DriveBookSync
import tempfile
import re
from pathlib import Path


class EnhanceBookRequest(BaseModel):
    """Request to enhance a book EPUB."""
    drive_file_id: str | None = None
    drive_url: str | None = None
    skip_bookfusion: bool = False
    skip_drive: bool = False
    preview: bool = False


def extract_drive_file_id(url_or_id: str) -> str | None:
    """Extract Google Drive file ID from URL or return as-is if already an ID."""
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',  # /file/d/ID/view
        r'id=([a-zA-Z0-9_-]+)',        # ?id=ID
        r'^([a-zA-Z0-9_-]{25,})$',     # Just the ID (25+ chars)
    ]
    for pattern in patterns:
        match = re.search(pattern, url_or_id)
        if match:
            return match.group(1)
    return None


@app.post("/books/enhance")
async def enhance_book(request: EnhanceBookRequest):
    """
    Enhance an EPUB book with learning aids.

    Downloads the EPUB from Google Drive, processes it through the enhancement
    pipeline (adding chapter previews and retrieval questions), then uploads
    the enhanced version back to Drive and Bookfusion.

    The enhancement pipeline:
    1. Parse EPUB → extract metadata and chapters
    2. Build personalized reader context from Jarvis profile
    3. Generate AI chapter previews (what's coming)
    4. Generate AI retrieval questions (for learning)
    5. Inject enhancements into EPUB
    6. Upload original to Drive/Jarvis/books/originals/
    7. Upload enhanced to Drive/Jarvis/books/
    8. Upload enhanced to Bookfusion for Boox sync
    9. Store all metadata in Supabase

    Args:
        drive_file_id: Google Drive file ID of the EPUB
        drive_url: Alternative: Google Drive URL (extracts file ID)
        skip_bookfusion: Don't upload to Bookfusion
        skip_drive: Don't upload to Drive (only process and store metadata)
        preview: Preview mode - show what would be generated without saving

    Returns:
        Processing results including book_id, URLs, and enhancement stats
    """
    # Validate input
    if not request.drive_file_id and not request.drive_url:
        raise HTTPException(
            status_code=400,
            detail="Either drive_file_id or drive_url must be provided"
        )

    # Extract file ID from URL if needed
    file_id = request.drive_file_id
    if not file_id and request.drive_url:
        file_id = extract_drive_file_id(request.drive_url)
        if not file_id:
            raise HTTPException(
                status_code=400,
                detail=f"Could not extract file ID from URL: {request.drive_url}"
            )

    logger.info(f"Starting book enhancement for Drive file: {file_id}")

    try:
        # Initialize Drive service (uses DriveBookSync for downloading)
        drive_sync = DriveBookSync()

        # Get file metadata first
        try:
            file_metadata = drive_sync.drive.files().get(
                fileId=file_id,
                fields='id, name, mimeType'
            ).execute()
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail=f"File not found in Drive: {file_id}. Error: {e}"
            )

        filename = file_metadata.get('name', 'book.epub')
        if not filename.lower().endswith('.epub'):
            raise HTTPException(
                status_code=400,
                detail=f"File is not an EPUB: {filename}"
            )

        logger.info(f"Found EPUB: {filename}")

        # Download EPUB to temp file using DriveBookSync.download_epub
        logger.info(f"Downloading EPUB...")
        temp_path = drive_sync.download_epub(file_id, filename)

        try:
            # Initialize and run pipeline
            pipeline = BookProcessingPipeline(
                supabase_url=os.getenv("SUPABASE_URL"),
                supabase_key=os.getenv("SUPABASE_KEY"),
                anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
                use_drive=not request.skip_drive,
                use_bookfusion=not request.skip_bookfusion
            )

            # Run synchronously in threadpool (process() is sync)
            result = await run_in_threadpool(
                pipeline.process,
                temp_path,
                None,  # output_path - let pipeline decide
                request.preview
            )

            logger.info(f"Book enhancement completed: {result.get('book_title')}")

            return {
                "status": "success" if result.get('success') else "failed",
                "book_id": result.get('book_id'),
                "book_title": result.get('book_title'),
                "chapters_processed": result.get('chapters_processed', 0),
                "enhancements_generated": result.get('enhancements_generated', 0),
                "original_drive_url": result.get('original_drive_url'),
                "enhanced_drive_url": result.get('enhanced_drive_url'),
                "bookfusion_id": result.get('bookfusion_id'),
                "preview_mode": request.preview
            }

        finally:
            # Clean up temp file
            if temp_path.exists():
                temp_path.unlink()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Book enhancement failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# Folder name for book input
BOOK_INPUT_FOLDER_NAME = "Book input"


@app.post("/books/process-inbox")
async def process_book_inbox(preview: bool = False):
    """
    Process all EPUBs in the 'Book input' folder.

    Workflow:
    1. Find all EPUB files in Jarvis/Book input/
    2. For each EPUB:
       - Download and process through enhancement pipeline
       - Upload enhanced version to Bookfusion
       - Move original to Jarvis/books/ folder
       - Delete from Book input folder
    3. Return results for all processed books

    This is the recommended way to add new books:
    1. Drop EPUB into "Jarvis/Book input" folder in Google Drive
    2. Tell Jarvis: "Process my book inbox" or call this endpoint
    3. Enhanced book appears in Bookfusion and syncs to Boox

    Args:
        preview: If True, show what would be processed without actually doing it

    Returns:
        List of processing results for each book found
    """
    logger.info(f"Processing book inbox (preview={preview})")

    try:
        # Initialize Drive service
        drive_sync = DriveBookSync()

        # Find Jarvis folder first
        results = drive_sync.drive.files().list(
            q="name='Jarvis' and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        jarvis_folders = results.get('files', [])
        if not jarvis_folders:
            raise HTTPException(status_code=404, detail="Jarvis folder not found in Drive")

        jarvis_id = jarvis_folders[0]['id']

        # Find Book input folder
        results = drive_sync.drive.files().list(
            q=f"name='{BOOK_INPUT_FOLDER_NAME}' and '{jarvis_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        input_folders = results.get('files', [])
        if not input_folders:
            raise HTTPException(
                status_code=404,
                detail=f"'{BOOK_INPUT_FOLDER_NAME}' folder not found in Jarvis. Please create it in Google Drive."
            )

        input_folder_id = input_folders[0]['id']
        logger.info(f"Found Book input folder: {input_folder_id}")

        # Find books folder for moving processed files
        results = drive_sync.drive.files().list(
            q=f"name='books' and '{jarvis_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        books_folders = results.get('files', [])
        books_folder_id = books_folders[0]['id'] if books_folders else None

        # List all EPUBs in input folder
        results = drive_sync.drive.files().list(
            q=f"'{input_folder_id}' in parents and name contains '.epub' and trashed=false",
            spaces='drive',
            fields='files(id, name, webViewLink)'
        ).execute()

        epub_files = results.get('files', [])

        if not epub_files:
            return {
                "status": "empty",
                "message": "No EPUB files found in Book input folder",
                "books_processed": 0,
                "results": []
            }

        logger.info(f"Found {len(epub_files)} EPUB(s) to process")

        processing_results = []

        for epub_file in epub_files:
            file_id = epub_file['id']
            filename = epub_file['name']

            logger.info(f"Processing: {filename}")

            if preview:
                processing_results.append({
                    "filename": filename,
                    "file_id": file_id,
                    "status": "would_process",
                    "preview_mode": True
                })
                continue

            try:
                # Download EPUB
                temp_path = drive_sync.download_epub(file_id, filename)

                try:
                    # Initialize and run pipeline
                    pipeline = BookProcessingPipeline(
                        supabase_url=os.getenv("SUPABASE_URL"),
                        supabase_key=os.getenv("SUPABASE_KEY"),
                        anthropic_api_key=os.getenv("ANTHROPIC_API_KEY"),
                        use_drive=True,
                        use_bookfusion=True
                    )

                    # Run processing
                    result = await run_in_threadpool(
                        pipeline.process,
                        temp_path,
                        None,
                        False  # not preview
                    )

                    # Delete from input folder (pipeline already uploaded to books/)
                    if result.get('success'):
                        try:
                            drive_sync.drive.files().delete(fileId=file_id).execute()
                            logger.info(f"Deleted {filename} from input folder (already uploaded by pipeline)")
                        except Exception as delete_error:
                            logger.warning(f"Could not delete {filename} from input: {delete_error}")

                    processing_results.append({
                        "filename": filename,
                        "file_id": file_id,
                        "status": "success" if result.get('success') else "failed",
                        "book_id": result.get('book_id'),
                        "book_title": result.get('book_title'),
                        "chapters_processed": result.get('chapters_processed', 0),
                        "enhancements_generated": result.get('enhancements_generated', 0),
                        "enhanced_drive_url": result.get('enhanced_drive_url'),
                        "bookfusion_id": result.get('bookfusion_id'),
                    })

                finally:
                    # Clean up temp file
                    if temp_path.exists():
                        temp_path.unlink()

            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")
                processing_results.append({
                    "filename": filename,
                    "file_id": file_id,
                    "status": "error",
                    "error": str(e)
                })

        successful = sum(1 for r in processing_results if r.get('status') == 'success')

        return {
            "status": "success" if successful > 0 else "failed",
            "message": f"Processed {successful}/{len(epub_files)} books",
            "books_processed": successful,
            "preview_mode": preview,
            "results": processing_results
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Book inbox processing failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# SUMMARY BOOK PIPELINE
# =============================================================================

class CreateSummaryBookRequest(BaseModel):
    """Request body for creating a summary book."""
    topic: Optional[str] = None
    title: Optional[str] = None
    book_list: Optional[List[dict]] = None
    max_books: int = 30
    context: str = ""
    project_id: Optional[str] = None
    resume_from: Optional[str] = None
    compile_only: bool = False
    skip_upload: bool = False
    workers: int = 8


# Track running summary book jobs
_summary_book_jobs: dict = {}


@app.post("/books/create-summary-book")
async def create_summary_book(request: CreateSummaryBookRequest, background_tasks: BackgroundTasks):
    """
    Create a summary book from a topic or book list.

    This is a long-running operation that:
    1. Generates/uses a book list
    2. Downloads EPUBs from LibGen
    3. Converts PDFs to EPUB
    4. Processes all books with Haiku (parallel AI summaries)
    5. Compiles into a single summary EPUB
    6. Uploads to Bookfusion

    The job runs in the background. Use GET /books/summary-book-status/{project_id}
    to check progress.

    Args:
        topic: Topic for AI-generated book list (e.g., "synthetic biology")
        title: Custom title for the summary book
        book_list: Pre-defined list of books [{"title": "...", "author": "..."}]
        max_books: Max books when generating from topic (default 30)
        context: Additional context for AI book selection
        project_id: Resume an existing project
        resume_from: Step to resume from ('download', 'convert', 'process', 'compile', 'upload')
        compile_only: Only compile + upload (skip download/process)
        skip_upload: Don't upload to Bookfusion
        workers: Number of parallel workers (default 8)

    Returns:
        Project ID and status for tracking
    """
    # Validate: need either topic, book_list, or project_id
    if not request.topic and not request.book_list and not request.project_id:
        raise HTTPException(
            status_code=400,
            detail="Must provide 'topic', 'book_list', or 'project_id'"
        )

    import uuid

    project_id = request.project_id or str(uuid.uuid4())[:8]
    title = request.title or (f"Summary: {request.topic.title()}" if request.topic else "Summary Book")

    logger.info(f"Creating summary book: project={project_id}, title={title}")

    # Track job status
    _summary_book_jobs[project_id] = {
        "project_id": project_id,
        "title": title,
        "status": "queued",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "current_step": None,
        "error": None,
    }

    async def _run_pipeline():
        """Run the summary book pipeline in background."""
        try:
            _summary_book_jobs[project_id]["status"] = "running"

            from scripts.create_summary_book import (
                generate_book_list, run_pipeline
            )

            # Step 1: Get book list
            book_list = request.book_list
            if not book_list and request.topic and not request.project_id:
                _summary_book_jobs[project_id]["current_step"] = "generating_book_list"
                logger.info(f"[{project_id}] Generating book list for topic: {request.topic}")
                book_list = await run_in_threadpool(
                    generate_book_list,
                    request.topic,
                    request.max_books,
                    request.context
                )
                logger.info(f"[{project_id}] Generated {len(book_list)} books")
            elif not book_list:
                book_list = []

            _summary_book_jobs[project_id]["book_count"] = len(book_list)
            _summary_book_jobs[project_id]["current_step"] = "running_pipeline"

            # Step 2: Run full pipeline
            import scripts.create_summary_book as pipeline_module
            pipeline_module.NUM_WORKERS = request.workers

            result = await run_in_threadpool(
                run_pipeline,
                title,
                book_list,
                project_id,
                request.resume_from,
                request.compile_only,
                request.skip_upload
            )

            _summary_book_jobs[project_id].update({
                "status": "completed",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "result": result,
                "current_step": None,
            })
            logger.info(f"[{project_id}] Summary book pipeline completed")

        except Exception as e:
            logger.error(f"[{project_id}] Summary book pipeline failed: {e}", exc_info=True)
            _summary_book_jobs[project_id].update({
                "status": "failed",
                "error": str(e),
                "current_step": None,
            })

    background_tasks.add_task(_run_pipeline)

    return {
        "status": "queued",
        "project_id": project_id,
        "title": title,
        "message": f"Summary book pipeline started. Track progress at /books/summary-book-status/{project_id}"
    }


@app.get("/books/summary-book-status/{project_id}")
async def get_summary_book_status(project_id: str):
    """
    Get the status of a summary book pipeline job.

    Returns current step, progress, and result when complete.
    """
    # Check in-memory job status first
    if project_id in _summary_book_jobs:
        return _summary_book_jobs[project_id]

    # Check on-disk project file
    try:
        from scripts.create_summary_book import load_project
        project = await run_in_threadpool(load_project, project_id)
        return {
            "project_id": project_id,
            "title": project.get("title"),
            "status": project.get("status", "unknown"),
            "results": project.get("results"),
            "created_at": project.get("created_at"),
        }
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Project not found: {project_id}")
    except Exception as e:
        logger.error(f"Error getting summary book status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/books/summary-book-projects")
async def list_summary_book_projects():
    """List all summary book projects."""
    try:
        from pathlib import Path
        import json

        projects_dir = Path(__file__).parent / "data" / "summary_projects"
        if not projects_dir.exists():
            return {"projects": []}

        projects = []
        for f in sorted(projects_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
            if f.stem.endswith("_books"):
                continue  # Skip book list files
            try:
                with open(f, 'r', encoding='utf-8') as fh:
                    project = json.load(fh)
                    projects.append({
                        "project_id": project.get("project_id"),
                        "title": project.get("title"),
                        "status": project.get("status"),
                        "book_count": len(project.get("book_list", [])),
                        "created_at": project.get("created_at"),
                        "updated_at": project.get("updated_at"),
                    })
            except Exception:
                continue

        # Also include in-memory jobs not yet saved to disk
        for pid, job in _summary_book_jobs.items():
            if not any(p.get("project_id") == pid for p in projects):
                projects.insert(0, {
                    "project_id": pid,
                    "title": job.get("title"),
                    "status": job.get("status"),
                    "book_count": job.get("book_count", 0),
                    "started_at": job.get("started_at"),
                })

        return {"projects": projects, "count": len(projects)}
    except Exception as e:
        logger.error(f"Error listing summary book projects: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# DASHBOARD - Visual widgets (public, no auth required)
# ============================================================================

@app.get("/dashboard/activity-heatmap", response_class=HTMLResponse)
async def activity_heatmap():
    """
    GitHub-style contribution heatmaps for activity, meditation, sleep, and Anki.
    Embeddable in Notion via /embed block.
    Green = screen time, Blue = meditation, Purple = sleep, Amber = workouts.
    """
    import json as _json

    # --- Fetch activity data (heatmap: lightweight, no hourly_breakdown) ---
    # NOTE: Multiple hostnames can exist for the same date (e.g. "Laptop" + "unknown"),
    # so we SUM across hostnames rather than overwriting.
    try:
        result = supabase.table("activity_summaries").select(
            "date, total_active_time, productive_time, distracting_time"
        ).order("date", desc=False).limit(365).execute()
        activity_data = {}
        for row in result.data:
            active_seconds = row.get("total_active_time") or 0
            date_key = row["date"]
            activity_data[date_key] = round(activity_data.get(date_key, 0) + active_seconds / 3600, 2)
        activity_json = _json.dumps(activity_data)
    except Exception as e:
        logger.error(f"Failed to fetch activity data for heatmap: {e}")
        activity_json = "{}"

    # --- Fetch hourly breakdown (only last 30 days for pace chart) ---
    # Merge hourly breakdowns across hostnames for the same date.
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        hourly_result = supabase.table("activity_summaries").select(
            "date, hourly_breakdown"
        ).gte("date", cutoff).order("date", desc=False).execute()
        hourly_by_date = {}
        for row in hourly_result.data:
            hb = row.get("hourly_breakdown")
            if not hb:
                continue
            date_key = row["date"]
            if date_key not in hourly_by_date:
                hourly_by_date[date_key] = hb
            else:
                # Merge: sum active/afk per hour across hostnames
                existing = {h["hour"]: h for h in hourly_by_date[date_key]}
                for entry in hb:
                    hr = entry["hour"]
                    if hr in existing:
                        existing[hr]["active"] = existing[hr].get("active", 0) + entry.get("active", 0)
                        existing[hr]["afk"] = existing[hr].get("afk", 0) + entry.get("afk", 0)
                    else:
                        existing[hr] = entry
                hourly_by_date[date_key] = sorted(existing.values(), key=lambda x: x["hour"])
        hourly_json = _json.dumps(hourly_by_date)
    except Exception as e:
        logger.error(f"Failed to fetch hourly data for heatmap: {e}")
        hourly_json = "{}"

    # --- Fetch meditation data ---
    try:
        med_result = supabase.table("meditation_sessions").select(
            "date, duration_seconds, practice_type"
        ).order("date", desc=False).execute()
        med_data = {}
        for row in med_result.data:
            d = row["date"]
            mins = (row.get("duration_seconds") or 0) / 60
            med_data[d] = round(med_data.get(d, 0) + mins, 1)
        meditation_json = _json.dumps(med_data)
    except Exception as e:
        logger.error(f"Failed to fetch meditation data for heatmap: {e}")
        meditation_json = "{}"

    # --- Fetch sleep data (hours per night from health_sleep) ---
    try:
        sleep_result = supabase.table("health_sleep").select(
            "date, duration_total_s, end_at, sleep_score"
        ).order("date", desc=False).limit(365).execute()
        sleep_data = {}
        for row in sleep_result.data:
            d = row["date"]
            dur_s = row.get("duration_total_s") or 0
            sleep_data[d] = round(dur_s / 3600, 2)
        sleep_json = _json.dumps(sleep_data)
    except Exception as e:
        logger.error(f"Failed to fetch sleep data for heatmap: {e}")
        sleep_json = "{}"

    # --- Fetch sleep custom data (staging detail from health_sleep_custom) ---
    try:
        sleep_custom_result = supabase.table("health_sleep_custom").select(
            "sleep_date, duration_deep_s, duration_light_s, duration_rem_s, "
            "duration_awake_s, duration_total_s, custom_sleep_score"
        ).order("sleep_date", desc=False).limit(365).execute()
        sleep_custom_data = {}
        for row in sleep_custom_result.data:
            d = row["sleep_date"]
            sleep_custom_data[d] = {
                "deep": row.get("duration_deep_s") or 0,
                "light": row.get("duration_light_s") or 0,
                "rem": row.get("duration_rem_s") or 0,
                "awake": row.get("duration_awake_s") or 0,
                "total": row.get("duration_total_s") or 0,
                "score": row.get("custom_sleep_score"),
            }
        sleep_custom_json = _json.dumps(sleep_custom_data)
    except Exception as e:
        logger.error(f"Failed to fetch sleep custom data for heatmap: {e}")
        sleep_custom_json = "{}"

    # --- Fetch workout data (from Hevy via health_workout_sessions) ---
    try:
        workout_result = supabase.table("health_workout_sessions").select(
            "started_at, duration_seconds, title, session_type"
        ).order("started_at", desc=False).limit(365).execute()
        workout_data = {}
        for row in workout_result.data:
            ts = row.get("started_at", "")
            if not ts:
                continue
            # Convert to SGT (UTC+8) for correct date bucketing
            d = ts[:10]  # YYYY-MM-DD from ISO timestamp
            workout_data[d] = workout_data.get(d, 0) + 1
        workout_json = _json.dumps(workout_data)
    except Exception as e:
        logger.error(f"Failed to fetch workout data for heatmap: {e}")
        workout_json = "{}"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Activity Dashboard</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    background: #191919;
    color: #c9d1d9;
    padding: 12px 16px;
  }}
  .container {{ max-width: 960px; margin: 0 auto; }}
  .section {{ margin-bottom: 18px; }}
  h1 {{
    font-size: 13px;
    font-weight: 600;
    margin-bottom: 2px;
    color: #e6edf3;
  }}
  .subtitle {{
    font-size: 10px;
    color: #7d8590;
    margin-bottom: 6px;
  }}
  .stats {{
    display: flex;
    gap: 20px;
    margin-bottom: 8px;
    flex-wrap: wrap;
  }}
  .stat {{ text-align: center; }}
  .stat-value {{
    font-size: 18px;
    font-weight: 700;
    color: #e6edf3;
  }}
  .stat-label {{
    font-size: 9px;
    color: #7d8590;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }}
  .heatmap-scroll {{
    overflow-x: auto;
    padding-bottom: 4px;
  }}
  .heatmap {{
    display: inline-grid;
    grid-template-rows: repeat(7, 13px);
    grid-auto-flow: column;
    grid-auto-columns: 13px;
    gap: 3px;
  }}
  .day {{
    width: 13px;
    height: 13px;
    border-radius: 2px;
    outline: 1px solid rgba(27, 31, 35, 0.06);
    outline-offset: -1px;
  }}
  .day:hover {{
    outline: 2px solid #58a6ff;
    outline-offset: -1px;
    cursor: pointer;
  }}
  /* Green scale (activity) */
  .green .lvl-0 {{ background-color: #161b22; }}
  .green .lvl-1 {{ background-color: #0e4429; }}
  .green .lvl-2 {{ background-color: #006d32; }}
  .green .lvl-3 {{ background-color: #26a641; }}
  .green .lvl-4 {{ background-color: #39d353; }}
  /* Blue scale (meditation) */
  .blue .lvl-0 {{ background-color: #161b22; }}
  .blue .lvl-1 {{ background-color: #0c2d6b; }}
  .blue .lvl-2 {{ background-color: #0550ae; }}
  .blue .lvl-3 {{ background-color: #1a7af8; }}
  .blue .lvl-4 {{ background-color: #58a6ff; }}
  /* Purple scale (sleep) — 7 levels for granularity */
  .purple .lvl-0 {{ background-color: #161b22; }}
  .purple .lvl-1 {{ background-color: #2d1754; }}
  .purple .lvl-2 {{ background-color: #4c1d95; }}
  .purple .lvl-3 {{ background-color: #6b21a8; }}
  .purple .lvl-4 {{ background-color: #9333ea; }}
  .purple .lvl-5 {{ background-color: #a855f7; }}
  .purple .lvl-6 {{ background-color: #c084fc; }}
  /* Amber scale (workouts) */
  .amber .lvl-0 {{ background-color: #161b22; }}
  .amber .lvl-1 {{ background-color: #5c3d0e; }}
  .amber .lvl-2 {{ background-color: #92600a; }}
  .amber .lvl-3 {{ background-color: #d97706; }}
  .amber .lvl-4 {{ background-color: #f59e0b; }}
  .month-labels {{
    display: flex;
    font-size: 9px;
    color: #7d8590;
    margin-bottom: 2px;
    padding-left: 0;
  }}
  .month-label {{ text-align: left; }}
  .day-labels {{
    display: inline-grid;
    grid-template-rows: repeat(7, 13px);
    gap: 3px;
    margin-right: 6px;
    font-size: 9px;
    color: #7d8590;
    vertical-align: top;
  }}
  .day-label {{
    height: 13px;
    line-height: 13px;
  }}
  .legend {{
    display: flex;
    align-items: center;
    gap: 3px;
    margin-top: 6px;
    font-size: 9px;
    color: #7d8590;
  }}
  .legend .day {{ outline: none; width: 13px; height: 13px; }}
  .tooltip {{
    position: fixed;
    background: #1c2128;
    border: 1px solid #30363d;
    border-radius: 6px;
    padding: 8px 10px;
    font-size: 12px;
    color: #c9d1d9;
    pointer-events: none;
    z-index: 1000;
    display: none;
    white-space: nowrap;
  }}
  .tooltip strong {{ color: #e6edf3; }}
  .heatmap-wrapper {{
    display: flex;
    align-items: flex-start;
  }}
  /* Day detail modal */
  .modal-overlay {{
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.6);
    z-index: 2000;
    display: none;
    align-items: center;
    justify-content: center;
  }}
  .modal-overlay.active {{ display: flex; }}
  .modal {{
    background: #1c2128;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 20px 24px;
    width: 90%;
    max-width: 580px;
    max-height: 85vh;
    overflow-y: auto;
    color: #c9d1d9;
  }}
  .modal-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
  }}
  .modal-header h2 {{
    font-size: 16px;
    color: #e6edf3;
    font-weight: 600;
  }}
  .modal-close {{
    background: none;
    border: none;
    color: #7d8590;
    font-size: 20px;
    cursor: pointer;
    padding: 4px 8px;
    border-radius: 4px;
  }}
  .modal-close:hover {{ background: #30363d; color: #e6edf3; }}
  .modal-stats {{
    display: flex;
    gap: 16px;
    margin-bottom: 16px;
    flex-wrap: wrap;
  }}
  .modal-stat {{
    background: #161b22;
    border-radius: 6px;
    padding: 8px 12px;
    flex: 1;
    min-width: 80px;
    text-align: center;
  }}
  .modal-stat-value {{
    font-size: 18px;
    font-weight: 700;
    color: #e6edf3;
  }}
  .modal-stat-label {{
    font-size: 9px;
    color: #7d8590;
    text-transform: uppercase;
    letter-spacing: 0.3px;
  }}
  .modal h3 {{
    font-size: 12px;
    color: #7d8590;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin: 14px 0 8px;
  }}
  .hourly-chart {{
    display: flex;
    align-items: flex-end;
    gap: 2px;
    height: 80px;
    padding: 0 2px;
  }}
  .hourly-bar-wrap {{
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    height: 100%;
    justify-content: flex-end;
  }}
  .hourly-bar {{
    width: 100%;
    min-width: 8px;
    border-radius: 2px 2px 0 0;
    background: #238636;
    transition: background 0.15s;
    position: relative;
  }}
  .hourly-bar[data-label]:hover {{ background: #39d353; }}
  .hourly-bar[data-label]:hover::before {{
    content: attr(data-label);
    position: absolute;
    bottom: calc(100% + 5px);
    left: 50%;
    transform: translateX(-50%);
    background: #161b22;
    color: #e6edf3;
    padding: 3px 7px;
    border-radius: 5px;
    font-size: 10px;
    white-space: nowrap;
    z-index: 200;
    border: 1px solid #30363d;
    pointer-events: none;
  }}
  .hourly-label {{
    font-size: 8px;
    color: #7d8590;
    margin-top: 2px;
  }}
  .app-row {{
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 6px;
  }}
  .app-name {{
    font-size: 12px;
    color: #c9d1d9;
    width: 100px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex-shrink: 0;
  }}
  .app-bar-bg {{
    flex: 1;
    height: 14px;
    background: #161b22;
    border-radius: 3px;
    overflow: hidden;
  }}
  .app-bar-fill {{
    height: 100%;
    border-radius: 3px;
    background: #238636;
  }}
  .app-pct {{
    font-size: 11px;
    color: #7d8590;
    width: 36px;
    text-align: right;
    flex-shrink: 0;
  }}
  .app-dur {{
    font-size: 11px;
    color: #7d8590;
    width: 48px;
    text-align: right;
    flex-shrink: 0;
  }}
  .site-row {{ display: flex; align-items: center; gap: 8px; margin-bottom: 5px; }}
  .site-name {{
    font-size: 12px;
    color: #c9d1d9;
    width: 140px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex-shrink: 0;
  }}
  .site-bar-bg {{
    flex: 1;
    height: 12px;
    background: #161b22;
    border-radius: 3px;
    overflow: hidden;
  }}
  .site-bar-fill {{
    height: 100%;
    border-radius: 3px;
    background: #1a7af8;
  }}
  .site-dur {{
    font-size: 11px;
    color: #7d8590;
    width: 48px;
    text-align: right;
    flex-shrink: 0;
  }}
  .no-data {{ color: #7d8590; font-size: 13px; text-align: center; padding: 20px; }}
  .viz-option {{ background: #161b22; border-radius: 8px; padding: 12px 14px; }}
  .viz-label {{
    font-size: 11px;
    font-weight: 600;
    color: #7d8590;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
  }}
  /* Sleep detail modal extras */
  .sleep-canvas-wrap {{
    background: #161b22;
    border-radius: 6px;
    padding: 8px 4px;
    margin-bottom: 10px;
  }}
  .sleep-canvas-wrap canvas {{
    width: 100%;
    display: block;
  }}
  .sleep-vitals {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(90px, 1fr));
    gap: 8px;
    margin-top: 8px;
  }}
  .sleep-vital {{
    background: #161b22;
    border-radius: 6px;
    padding: 6px 8px;
    text-align: center;
  }}
  .sleep-vital-value {{
    font-size: 15px;
    font-weight: 700;
    color: #e6edf3;
  }}
  .sleep-vital-label {{
    font-size: 8px;
    color: #7d8590;
    text-transform: uppercase;
    letter-spacing: 0.3px;
  }}
  /* Sleep Quality collapsible section */
  .sleep-quality-section {{
    margin-top: 12px;
  }}
  .sleep-quality-canvas-wrap {{
    background: #161b22;
    border-radius: 8px;
    padding: 12px 8px;
    margin-bottom: 10px;
  }}
</style>
</head>
<body>
<div class="container">

  <!-- Activity Heatmap (green) -->
  <div class="section green" id="activity-section">
    <h1>Working Time</h1>
    <div class="subtitle" id="activity-subtitle"></div>
    <div class="stats" id="activity-stats"></div>
    <div class="month-labels" id="activity-months"></div>
    <div class="heatmap-wrapper">
      <div class="day-labels" id="activity-daylabels"></div>
      <div class="heatmap-scroll">
        <div class="heatmap" id="activity-heatmap"></div>
      </div>
    </div>
    <div class="legend">
      <span>Less</span>
      <div class="day lvl-0"></div>
      <div class="day lvl-1"></div>
      <div class="day lvl-2"></div>
      <div class="day lvl-3"></div>
      <div class="day lvl-4"></div>
      <span>More</span>
    </div>
  </div>

  <!-- Meditation Heatmap (blue) -->
  <div class="section blue" id="meditation-section">
    <h1>Meditation</h1>
    <div class="subtitle" id="meditation-subtitle"></div>
    <div class="stats" id="meditation-stats"></div>
    <div class="month-labels" id="meditation-months"></div>
    <div class="heatmap-wrapper">
      <div class="day-labels" id="meditation-daylabels"></div>
      <div class="heatmap-scroll">
        <div class="heatmap" id="meditation-heatmap"></div>
      </div>
    </div>
    <div class="legend">
      <span>Less</span>
      <div class="day lvl-0"></div>
      <div class="day lvl-1"></div>
      <div class="day lvl-2"></div>
      <div class="day lvl-3"></div>
      <div class="day lvl-4"></div>
      <span>More</span>
    </div>
  </div>


  <!-- Sleep Heatmap (purple) -->
  <div class="section purple" id="sleep-section">
    <h1>Sleep</h1>
    <div class="subtitle" id="sleep-subtitle"></div>
    <div class="stats" id="sleep-stats"></div>
    <div class="month-labels" id="sleep-months"></div>
    <div class="heatmap-wrapper">
      <div class="day-labels" id="sleep-daylabels"></div>
      <div class="heatmap-scroll">
        <div class="heatmap" id="sleep-heatmap"></div>
      </div>
    </div>
    <div class="legend">
      <span>Less</span>
      <div class="day lvl-0"></div>
      <div class="day lvl-1"></div>
      <div class="day lvl-2"></div>
      <div class="day lvl-3"></div>
      <div class="day lvl-4"></div>
      <div class="day lvl-5"></div>
      <div class="day lvl-6"></div>
      <span>More</span>
    </div>
  </div>

  <!-- Workout Heatmap (amber) -->
  <div class="section amber" id="workout-section">
    <h1>Workouts</h1>
    <div class="subtitle" id="workout-subtitle"></div>
    <div class="stats" id="workout-stats"></div>
    <div class="month-labels" id="workout-months"></div>
    <div class="heatmap-wrapper">
      <div class="day-labels" id="workout-daylabels"></div>
      <div class="heatmap-scroll">
        <div class="heatmap" id="workout-heatmap"></div>
      </div>
    </div>
    <div class="legend">
      <span>Less</span>
      <div class="day lvl-0"></div>
      <div class="day lvl-1"></div>
      <div class="day lvl-2"></div>
      <div class="day lvl-3"></div>
      <div class="day lvl-4"></div>
      <span>More</span>
    </div>
  </div>

  <!-- Sleep Quality — 30-day trends (collapsed by default) -->
  <div class="section sleep-quality-section" id="sleep-quality-section">
    <h1 id="sleep-quality-toggle" style="cursor:pointer;user-select:none;">
      <span id="sleep-quality-arrow" style="display:inline-block;transition:transform 0.2s;font-size:10px;margin-right:4px;">&#9654;</span>Sleep Quality (30 days)
    </h1>
    <div id="sleep-quality-content" style="display:none;">
      <div class="subtitle">Sleep stages breakdown and score trend</div>
      <div class="sleep-quality-canvas-wrap" style="margin-top:10px;">
        <div class="viz-label">Stages per Night</div>
        <canvas id="sleep-stages-canvas" height="120" style="width:100%;"></canvas>
      </div>
      <div class="sleep-quality-canvas-wrap">
        <div class="viz-label">Sleep Score Trend</div>
        <canvas id="sleep-score-canvas" height="80" style="width:100%;"></canvas>
      </div>
    </div>
  </div>

  <!-- Today's Work Curve — Pace Tracker (collapsed by default) -->
  <div class="section" id="curve-section" style="margin-top:24px;">
    <h1 id="curve-toggle" style="cursor:pointer;user-select:none;">
      <span id="curve-arrow" style="display:inline-block;transition:transform 0.2s;font-size:10px;margin-right:4px;">&#9654;</span>Today's Work Curve
    </h1>
    <div id="curve-content" style="display:none;">
      <div class="subtitle">Comparing today to your recent pattern (past 4 weeks)</div>
      <div class="viz-option" style="margin-top:14px;">
        <canvas id="viz-pace" height="110" style="width:100%;"></canvas>
      </div>
    </div>
  </div>

</div>
<div class="tooltip" id="tooltip"></div>
<div class="modal-overlay" id="modal-overlay">
  <div class="modal" id="modal">
    <div class="modal-header">
      <h2 id="modal-title">Loading...</h2>
      <button class="modal-close" id="modal-close">&times;</button>
    </div>
    <div id="modal-body"></div>
  </div>
</div>

<script>
const ACTIVITY_DATA = {activity_json};
const HOURLY_DATA = {hourly_json};
const MEDITATION_DATA = {meditation_json};
const SLEEP_DATA = {sleep_json};
const SLEEP_CUSTOM_DATA = {sleep_custom_json};
const WORKOUT_DATA = {workout_json};
const DAYS = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

function formatDate(d) {{
  return d.toLocaleDateString('en-US', {{ weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' }});
}}

function localDateStr(d) {{
  return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
}}

function formatHours(h) {{
  if (!h || h < 0.01) return 'No activity';
  const hrs = Math.floor(h);
  const mins = Math.round((h - hrs) * 60);
  if (hrs === 0) return mins + ' min';
  if (mins === 0) return hrs + 'h';
  return hrs + 'h ' + mins + 'm';
}}

function formatMins(m) {{
  if (!m || m < 0.5) return 'No sessions';
  if (m < 60) return Math.round(m) + ' min';
  const hrs = Math.floor(m / 60);
  const mins = Math.round(m % 60);
  if (mins === 0) return hrs + 'h';
  return hrs + 'h ' + mins + 'm';
}}

function renderHeatmap(config) {{
  const {{ data, heatmapId, monthsId, dayLabelsId, statsId, subtitleId, getLevel, formatValue, unitLabel }} = config;
  const heatmap = document.getElementById(heatmapId);
  const monthLabels = document.getElementById(monthsId);
  const dayLabels = document.getElementById(dayLabelsId);
  const statsEl = document.getElementById(statsId);
  const subtitleEl = document.getElementById(subtitleId);

  const today = new Date();
  today.setHours(0,0,0,0);
  const todayDay = (today.getDay() + 6) % 7;
  const start = new Date(today);
  start.setDate(start.getDate() - (52 * 7) - todayDay);

  DAYS.forEach((d, i) => {{
    const el = document.createElement('div');
    el.className = 'day-label';
    el.textContent = (i % 2 === 0) ? d : '';
    dayLabels.appendChild(el);
  }});

  let totalValue = 0;
  let activeDays = 0;
  let todayValue = 0;
  let thisWeekValue = 0;
  let prevWeekValue = 0;
  let last28Value = 0;
  let last28Days = 0;
  const weekStartMonth = [];

  // Monday of current week (Mon-Sun)
  const thisMonday = new Date(today);
  thisMonday.setDate(today.getDate() - todayDay);
  const prevMonday = new Date(thisMonday);
  prevMonday.setDate(prevMonday.getDate() - 7);
  const prevSunday = new Date(thisMonday);
  prevSunday.setDate(prevSunday.getDate() - 1);
  const fourWeeksAgo = new Date(today);
  fourWeeksAgo.setDate(fourWeeksAgo.getDate() - 28);

  // Days elapsed this week (Mon=1 day, Tue=2, ..., Sun=7)
  const daysThisWeek = todayDay + 1;

  const cursor = new Date(start);
  while (cursor <= today) {{
    const dayOfWeek = (cursor.getDay() + 6) % 7;
    const dateStr = localDateStr(cursor);
    const val = data[dateStr] || 0;

    totalValue += val;
    if (val > 0) activeDays++;

    if (localDateStr(cursor) === localDateStr(today)) todayValue = val;
    if (cursor >= thisMonday && cursor <= today) thisWeekValue += val;
    if (cursor >= prevMonday && cursor <= prevSunday) prevWeekValue += val;
    if (cursor >= fourWeeksAgo && cursor <= today) {{ last28Value += val; last28Days++; }}

    if (dayOfWeek === 0) {{
      weekStartMonth.push({{ month: cursor.getMonth() }});
    }}

    const el = document.createElement('div');
    el.className = 'day lvl-' + getLevel(val);
    el.dataset.date = dateStr;
    el.dataset.value = val;
    heatmap.appendChild(el);

    cursor.setDate(cursor.getDate() + 1);
  }}

  const lastDayOfWeek = (today.getDay() + 6) % 7;
  for (let i = lastDayOfWeek + 1; i < 7; i++) {{
    const el = document.createElement('div');
    el.className = 'day lvl-0';
    el.style.visibility = 'hidden';
    heatmap.appendChild(el);
  }}

  let monthHTML = '';
  let prevMonth = -1;
  const weekWidth = 16;
  weekStartMonth.forEach((w, i) => {{
    if (w.month !== prevMonth) {{
      const left = i * weekWidth;
      monthHTML += '<span class="month-label" style="position:absolute;left:' + left + 'px">' + MONTHS[w.month] + '</span>';
      prevMonth = w.month;
    }}
  }});
  monthLabels.style.position = 'relative';
  monthLabels.style.height = '16px';
  monthLabels.style.marginLeft = (dayLabels.offsetWidth + 6) + 'px';
  monthLabels.innerHTML = monthHTML;

  let streak = 0;
  for (let i = 0; i <= 365; i++) {{
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    const ds = localDateStr(d);
    if ((data[ds] || 0) > 0) streak++;
    else break;
  }}

  const dateKeys = Object.keys(data).sort();
  const totalDays = dateKeys.length;
  subtitleEl.textContent = totalDays + ' days of tracking';

  // Daily average and median from past 4 weeks
  const avgValue = last28Days > 0 ? last28Value / last28Days : 0;
  const last28Values = [];
  for (let i = 0; i < 28; i++) {{
    const d2 = new Date(today);
    d2.setDate(d2.getDate() - i);
    const v = data[localDateStr(d2)] || 0;
    if (v > 0) last28Values.push(v);
  }}
  last28Values.sort((a, b) => a - b);
  const medianValue = last28Values.length > 0
    ? (last28Values.length % 2 === 0
      ? (last28Values[last28Values.length / 2 - 1] + last28Values[last28Values.length / 2]) / 2
      : last28Values[Math.floor(last28Values.length / 2)])
    : 0;

  // Week trend: compare daily avg this week vs daily avg prev week
  let weekDelta = '';
  if (prevWeekValue > 0) {{
    const thisAvgPerDay = thisWeekValue / daysThisWeek;
    const prevAvgPerDay = prevWeekValue / 7;
    const pct = Math.round(((thisAvgPerDay - prevAvgPerDay) / prevAvgPerDay) * 100);
    if (pct > 0) weekDelta = ' <span style="color:#3fb950;font-size:10px">&#9650; ' + pct + '%</span>';
    else if (pct < 0) weekDelta = ' <span style="color:#f85149;font-size:10px">&#9660; ' + Math.abs(pct) + '%</span>';
    else weekDelta = ' <span style="color:#7d8590;font-size:10px">=</span>';
  }}

  statsEl.innerHTML = `
    <div class="stat"><div class="stat-value">${{formatValue(todayValue)}}</div><div class="stat-label">Today</div></div>
    <div class="stat"><div class="stat-value">${{formatValue(thisWeekValue)}}${{weekDelta}}</div><div class="stat-label">This Week (trend)</div></div>
    <div class="stat"><div class="stat-value">${{formatValue(avgValue)}}</div><div class="stat-label">Avg (4w)</div></div>
    <div class="stat"><div class="stat-value">${{formatValue(medianValue)}}</div><div class="stat-label">Median (4w)</div></div>
    <div class="stat"><div class="stat-value">${{streak}}d</div><div class="stat-label">Streak</div></div>
    <div class="stat"><div class="stat-value">${{activeDays}}</div><div class="stat-label">Active Days</div></div>
  `;

  // Tooltips
  const tooltip = document.getElementById('tooltip');
  heatmap.addEventListener('mouseover', (e) => {{
    if (e.target.classList.contains('day') && e.target.dataset.date) {{
      const d = new Date(e.target.dataset.date + 'T00:00:00');
      const v = parseFloat(e.target.dataset.value) || 0;
      tooltip.innerHTML = '<strong>' + formatValue(v) + '</strong> on ' + formatDate(d);
      tooltip.style.display = 'block';
    }}
  }});
  heatmap.addEventListener('mousemove', (e) => {{
    const tw = tooltip.offsetWidth || 120;
    tooltip.style.left = (e.clientX - tw - 12) + 'px';
    tooltip.style.top = (e.clientY - 30) + 'px';
  }});
  heatmap.addEventListener('mouseout', () => {{
    tooltip.style.display = 'none';
  }});
}}

// Render Activity heatmap (green, hours)
renderHeatmap({{
  data: ACTIVITY_DATA,
  heatmapId: 'activity-heatmap',
  monthsId: 'activity-months',
  dayLabelsId: 'activity-daylabels',
  statsId: 'activity-stats',
  subtitleId: 'activity-subtitle',
  getLevel: (h) => {{
    if (!h || h < 0.1) return 0;
    if (h < 2) return 1;
    if (h < 4) return 2;
    if (h < 6) return 3;
    return 4;
  }},
  formatValue: formatHours,
}});

// Render Meditation heatmap (blue, minutes)
renderHeatmap({{
  data: MEDITATION_DATA,
  heatmapId: 'meditation-heatmap',
  monthsId: 'meditation-months',
  dayLabelsId: 'meditation-daylabels',
  statsId: 'meditation-stats',
  subtitleId: 'meditation-subtitle',
  getLevel: (m) => {{
    if (!m || m < 1) return 0;
    if (m < 10) return 1;
    if (m < 15) return 2;
    if (m < 20) return 3;
    return 4;
  }},
  formatValue: formatMins,
}});

// Render Sleep heatmap (purple, hours — 7 levels for granularity)
renderHeatmap({{
  data: SLEEP_DATA,
  heatmapId: 'sleep-heatmap',
  monthsId: 'sleep-months',
  dayLabelsId: 'sleep-daylabels',
  statsId: 'sleep-stats',
  subtitleId: 'sleep-subtitle',
  getLevel: (h) => {{
    if (!h || h < 0.1) return 0;
    if (h < 4) return 1;
    if (h < 5.5) return 2;
    if (h < 6.5) return 3;
    if (h < 7.5) return 4;
    if (h < 9) return 5;
    return 6;
  }},
  formatValue: formatHours,
}});

// Render Workout heatmap (amber, session count)
renderHeatmap({{
  data: WORKOUT_DATA,
  heatmapId: 'workout-heatmap',
  monthsId: 'workout-months',
  dayLabelsId: 'workout-daylabels',
  statsId: 'workout-stats',
  subtitleId: 'workout-subtitle',
  getLevel: (c) => {{
    if (!c) return 0;
    if (c === 1) return 1;
    if (c === 2) return 2;
    if (c === 3) return 3;
    return 4;
  }},
  formatValue: (c) => {{
    if (!c) return 'Rest day';
    return c + ' session' + (c > 1 ? 's' : '');
  }},
}});

// Workout heatmap click handler
document.getElementById('workout-heatmap').addEventListener('click', (e) => {{
  if (e.target.classList.contains('day') && e.target.dataset.date) {{
    const dateStr = e.target.dataset.date;
    const d = new Date(dateStr + 'T00:00:00');
    modalTitle.textContent = 'Workout: ' + formatDate(d);
    modalBody.innerHTML = '<div class="no-data">Loading...</div>';
    overlay.classList.add('active');
    const baseUrl = window.location.origin;
    fetch(baseUrl + '/dashboard/workout-detail?date=' + dateStr)
      .then(r => r.json())
      .then(data => {{
        if (!data.sessions || data.sessions.length === 0) {{
          modalBody.innerHTML = '<div class="no-data">No workout data for this day.</div>';
          return;
        }}
        let html = '';
        data.sessions.forEach(s => {{
          const durMin = Math.round((s.duration_seconds || 0) / 60);
          html += '<h3 style="margin-top:12px;color:#f59e0b">' + (s.title || 'Workout') + '</h3>';
          html += '<div style="display:flex;gap:16px;margin:8px 0">';
          html += '<div class="stat"><div class="stat-value">' + durMin + 'm</div><div class="stat-label">Duration</div></div>';
          html += '<div class="stat"><div class="stat-value">' + s.exercise_count + '</div><div class="stat-label">Exercises</div></div>';
          html += '<div class="stat"><div class="stat-value">' + s.set_count + '</div><div class="stat-label">Sets</div></div>';
          html += '</div>';
          if (s.exercises) {{
            s.exercises.forEach(ex => {{
              const rehab = ex.is_rehab ? ' <span style="color:#f59e0b;font-size:9px">REHAB</span>' : '';
              const sets = ex.sets || [];
              let setInfo = '';
              if (sets.length > 0) {{
                const s0 = sets[0];
                if (s0.weight_kg && s0.reps) setInfo = sets.length + 'x' + s0.reps + ' @ ' + s0.weight_kg + 'kg';
                else if (s0.reps) setInfo = sets.length + 'x' + s0.reps;
                else if (s0.duration_seconds) setInfo = sets.length + 'x' + s0.duration_seconds + 's';
              }}
              html += '<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid #21262d">';
              html += '<span style="font-size:11px">' + ex.exercise_name + rehab + '</span>';
              html += '<span style="font-size:11px;color:#7d8590">' + setInfo + '</span>';
              html += '</div>';
            }});
          }}
        }});
        modalBody.innerHTML = html;
      }})
      .catch(() => {{
        modalBody.innerHTML = '<div class="no-data">Failed to load workout details.</div>';
      }});
  }}
}});

// ---- Day detail modal ----
const overlay = document.getElementById('modal-overlay');
const modalTitle = document.getElementById('modal-title');
const modalBody = document.getElementById('modal-body');

document.getElementById('modal-close').addEventListener('click', () => {{
  overlay.classList.remove('active');
}});
overlay.addEventListener('click', (e) => {{
  if (e.target === overlay) overlay.classList.remove('active');
}});

function fmtSec(s) {{
  if (!s || s < 60) return '<1m';
  const h = Math.floor(s / 3600);
  const m = Math.round((s % 3600) / 60);
  if (h === 0) return m + 'm';
  if (m === 0) return h + 'h';
  return h + 'h ' + m + 'm';
}}

function cleanApp(name) {{
  return name.replace('.exe', '').replace('.Root', '');
}}

function showDayDetail(dateStr) {{
  const d = new Date(dateStr + 'T00:00:00');
  modalTitle.textContent = formatDate(d);
  modalBody.innerHTML = '<div class="no-data">Loading...</div>';
  overlay.classList.add('active');

  const baseUrl = window.location.origin;
  fetch(baseUrl + '/dashboard/day-detail?date=' + dateStr)
    .then(r => r.json())
    .then(data => {{
      if (!data.found) {{
        modalBody.innerHTML = '<div class="no-data">No activity data for this day.</div>';
        return;
      }}

      let html = '';

      // Summary stats
      html += '<div class="modal-stats">';
      html += '<div class="modal-stat"><div class="modal-stat-value">' + fmtSec(data.total_active_time) + '</div><div class="modal-stat-label">Working Time</div></div>';
      html += '<div class="modal-stat"><div class="modal-stat-value">' + fmtSec(data.productive_time) + '</div><div class="modal-stat-label">Productive</div></div>';
      html += '<div class="modal-stat"><div class="modal-stat-value">' + fmtSec(data.distracting_time) + '</div><div class="modal-stat-label">Distracting</div></div>';
      html += '</div>';

      // Hourly breakdown chart
      const hourly = data.hourly_breakdown || [];
      if (hourly.length > 0) {{
        html += '<h3>Hours of the Day</h3>';
        const maxActive = Math.max(...hourly.map(h => h.active || 0), 1);
        // Fill all 24 hours
        const byHour = {{}};
        hourly.forEach(h => {{ byHour[h.hour] = h; }});
        // Find range of active hours
        const activeHours = hourly.filter(h => (h.active || 0) > 0).map(h => h.hour);
        const minH = Math.max(0, Math.min(...activeHours) - 1);
        const maxH = Math.min(23, Math.max(...activeHours) + 1);

        html += '<div class="hourly-chart">';
        for (let i = minH; i <= maxH; i++) {{
          const h = byHour[i] || {{ active: 0, afk: 0 }};
          const pct = Math.max(4, (h.active / maxActive) * 100);
          const hasActivity = h.active > 0;
          const barStyle = hasActivity
            ? 'height:' + pct + '%;background:#238636'
            : 'height:0';
          const mins = Math.floor((h.active || 0) / 60);
          const secs = Math.round((h.active || 0) % 60);
          const label = hasActivity
            ? (mins > 0 ? mins + 'm' : '') + (secs > 0 && mins < 10 ? ' ' + secs + 's' : '')
            : '';
          const dataLabel = hasActivity ? 'data-label="' + i + ':00 — ' + label.trim() + '"' : '';
          html += '<div class="hourly-bar-wrap">';
          html += '<div class="hourly-bar" style="' + barStyle + '" ' + dataLabel + '></div>';
          html += '<div class="hourly-label">' + i + '</div>';
          html += '</div>';
        }}
        html += '</div>';
      }}

      // Top apps
      const apps = (data.top_apps || []).filter(a => a.app !== 'LockApp.exe').slice(0, 8);
      if (apps.length > 0) {{
        html += '<h3>Applications</h3>';
        const maxApp = apps[0].duration || 1;
        apps.forEach(a => {{
          const pct = Math.round((a.duration / maxApp) * 100);
          html += '<div class="app-row">';
          html += '<div class="app-name" title="' + a.app + '">' + cleanApp(a.app) + '</div>';
          html += '<div class="app-bar-bg"><div class="app-bar-fill" style="width:' + pct + '%"></div></div>';
          html += '<div class="app-dur">' + fmtSec(a.duration) + '</div>';
          html += '</div>';
        }});
      }}

      // Top sites
      const sites = (data.top_sites || []).filter(s => s.domain !== 'newtab').slice(0, 8);
      if (sites.length > 0) {{
        html += '<h3>Websites</h3>';
        const maxSite = sites[0].duration || 1;
        sites.forEach(s => {{
          const pct = Math.round((s.duration / maxSite) * 100);
          html += '<div class="site-row">';
          html += '<div class="site-name" title="' + s.domain + '">' + s.domain + '</div>';
          html += '<div class="site-bar-bg"><div class="site-bar-fill" style="width:' + pct + '%"></div></div>';
          html += '<div class="site-dur">' + fmtSec(s.duration) + '</div>';
          html += '</div>';
        }});
      }}

      modalBody.innerHTML = html;
    }})
    .catch(() => {{
      modalBody.innerHTML = '<div class="no-data">Failed to load details.</div>';
    }});
}}

// Click handler on activity heatmap cells only
document.getElementById('activity-heatmap').addEventListener('click', (e) => {{
  if (e.target.classList.contains('day') && e.target.dataset.date) {{
    showDayDetail(e.target.dataset.date);
  }}
}});

// Click handler on sleep heatmap cells
document.getElementById('sleep-heatmap').addEventListener('click', (e) => {{
  if (e.target.classList.contains('day') && e.target.dataset.date) {{
    showSleepDetail(e.target.dataset.date);
  }}
}});

function showSleepDetail(dateStr) {{
  const d = new Date(dateStr + 'T00:00:00');
  modalTitle.textContent = 'Sleep: ' + formatDate(d);
  modalBody.innerHTML = '<div class="no-data">Loading...</div>';
  overlay.classList.add('active');

  const baseUrl = window.location.origin;
  fetch(baseUrl + '/dashboard/sleep-detail?date=' + dateStr)
    .then(r => r.json())
    .then(data => {{
      if (!data.found) {{
        modalBody.innerHTML = '<div class="no-data">No sleep data for this night.</div>';
        return;
      }}

      let html = '';

      // Summary stats row
      const score = data.custom_sleep_score != null ? Math.round(data.custom_sleep_score) : '--';
      const totalH = data.duration_total_s ? fmtSec(data.duration_total_s) : '--';
      const deepH = data.duration_deep_s ? fmtSec(data.duration_deep_s) : '--';
      const remH = data.duration_rem_s ? fmtSec(data.duration_rem_s) : '--';
      const lightH = data.duration_light_s ? fmtSec(data.duration_light_s) : '--';
      const awakeH = data.duration_awake_s ? fmtSec(data.duration_awake_s) : '--';

      html += '<div class="modal-stats">';
      html += '<div class="modal-stat"><div class="modal-stat-value" style="color:#a855f7">' + score + '</div><div class="modal-stat-label">Score</div></div>';
      html += '<div class="modal-stat"><div class="modal-stat-value">' + totalH + '</div><div class="modal-stat-label">Total</div></div>';
      html += '<div class="modal-stat"><div class="modal-stat-value" style="color:#312e81">' + deepH + '</div><div class="modal-stat-label">Deep</div></div>';
      html += '<div class="modal-stat"><div class="modal-stat-value" style="color:#818cf8">' + remH + '</div><div class="modal-stat-label">REM</div></div>';
      html += '<div class="modal-stat"><div class="modal-stat-value" style="color:#a5b4fc">' + lightH + '</div><div class="modal-stat-label">Light</div></div>';
      html += '<div class="modal-stat"><div class="modal-stat-value" style="color:#fbbf24">' + awakeH + '</div><div class="modal-stat-label">Awake</div></div>';
      html += '</div>';

      // Hypnogram canvas
      if (data.epochs && data.epochs.length > 0) {{
        html += '<h3>Hypnogram</h3>';
        html += '<div class="sleep-canvas-wrap"><canvas id="hypnogram-canvas" height="150"></canvas></div>';

        // HR + RMSSD canvas
        const hasHr = data.epochs.some(e => e.hr != null);
        const hasRmssd = data.epochs.some(e => e.rmssd != null);
        if (hasHr || hasRmssd) {{
          html += '<h3>Heart Rate &amp; HRV</h3>';
          html += '<div class="sleep-canvas-wrap"><canvas id="hr-rmssd-canvas" height="120"></canvas></div>';
        }}
      }}

      // Withings vitals row
      if (data.withings) {{
        const w = data.withings;
        html += '<h3>Vitals (Withings)</h3>';
        html += '<div class="sleep-vitals">';
        if (w.hr_average != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + Math.round(w.hr_average) + '</div><div class="sleep-vital-label">HR Avg</div></div>';
        if (w.hr_min != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + Math.round(w.hr_min) + '</div><div class="sleep-vital-label">HR Min</div></div>';
        if (w.hr_max != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + Math.round(w.hr_max) + '</div><div class="sleep-vital-label">HR Max</div></div>';
        if (w.rr_average != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + w.rr_average.toFixed(1) + '</div><div class="sleep-vital-label">RR Avg</div></div>';
        if (w.sleep_score != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + Math.round(w.sleep_score) + '</div><div class="sleep-vital-label">W. Score</div></div>';
        if (w.sleep_efficiency_pct != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + Math.round(w.sleep_efficiency_pct * 100) + '%</div><div class="sleep-vital-label">Efficiency</div></div>';
        if (w.sleep_latency_s != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + Math.round(w.sleep_latency_s / 60) + 'm</div><div class="sleep-vital-label">Latency</div></div>';
        if (w.waso_s != null) html += '<div class="sleep-vital"><div class="sleep-vital-value">' + Math.round(w.waso_s / 60) + 'm</div><div class="sleep-vital-label">WASO</div></div>';
        html += '</div>';
      }}

      modalBody.innerHTML = html;

      // Draw hypnogram after DOM insert
      if (data.epochs && data.epochs.length > 0) {{
        drawHypnogram(data.epochs);
        const hasHr2 = data.epochs.some(e => e.hr != null);
        const hasRmssd2 = data.epochs.some(e => e.rmssd != null);
        if (hasHr2 || hasRmssd2) drawHrRmssd(data.epochs);
      }}
    }})
    .catch((err) => {{
      modalBody.innerHTML = '<div class="no-data">Failed to load sleep details.<br><span style="font-size:10px;color:#7d8590">' + (err.message || err) + '</span></div>';
    }});
}}

function drawHypnogram(epochs) {{
  const canvas = document.getElementById('hypnogram-canvas');
  if (!canvas) return;
  const W = canvas.parentElement.offsetWidth - 8;
  const H = 150;
  canvas.width = W * 2; canvas.height = H * 2;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(2, 2);

  const pad = {{ l: 42, r: 10, t: 10, b: 24 }};
  const cw = W - pad.l - pad.r;
  const ch = H - pad.t - pad.b;

  // Stage mapping: numeric (0=Wake, 1=Light, 2=Deep, 3=REM) -> display order
  // Display order: Wake=0 (top), Light=1, REM=2, Deep=3 (bottom)
  const stageToDisplay = {{ 0: 0, 1: 1, 2: 3, 3: 2 }};  // 2(Deep)->3(bottom), 3(REM)->2
  const stageColors = {{ 0: '#fbbf24', 1: '#a5b4fc', 2: '#312e81', 3: '#818cf8' }};
  const displayLabels = ['Wake', 'Light', 'REM', 'Deep'];
  const displayColors = ['#fbbf24', '#a5b4fc', '#818cf8', '#312e81'];
  const stageY = (displayIdx) => pad.t + (displayIdx / 3) * ch;

  const n = epochs.length;
  const barW = Math.max(1, cw / n);

  // Parse first/last timestamps for x-axis labels
  let firstTs = null, lastTs = null;
  if (epochs[0].ts) firstTs = new Date(epochs[0].ts);
  if (epochs[n - 1].ts) lastTs = new Date(epochs[n - 1].ts);

  // Draw stage bars
  for (let i = 0; i < n; i++) {{
    const e = epochs[i];
    const stage = typeof e.stage === 'number' ? e.stage : 0;
    const displayIdx = stageToDisplay[stage] ?? 0;
    const x = pad.l + (i / n) * cw;
    const y = stageY(displayIdx);
    const barH = ch - (displayIdx / 3) * ch;
    ctx.fillStyle = stageColors[stage] || '#a5b4fc';
    ctx.fillRect(x, y, Math.ceil(barW) + 0.5, barH);
  }}

  // Y-axis labels
  ctx.fillStyle = '#7d8590'; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
  for (let s = 0; s <= 3; s++) {{
    ctx.fillText(displayLabels[s], pad.l - 4, stageY(s) + 4);
    ctx.strokeStyle = '#21262d'; ctx.lineWidth = 0.5;
    ctx.beginPath(); ctx.moveTo(pad.l, stageY(s)); ctx.lineTo(W - pad.r, stageY(s)); ctx.stroke();
  }}

  // X-axis time labels
  if (firstTs && lastTs) {{
    ctx.fillStyle = '#7d8590'; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
    const totalMs = lastTs - firstTs;
    const step = Math.max(1, Math.floor(n / 6));
    for (let i = 0; i < n; i += step) {{
      if (epochs[i].ts) {{
        const t = new Date(epochs[i].ts);
        const lbl = String(t.getHours()).padStart(2, '0') + ':' + String(t.getMinutes()).padStart(2, '0');
        ctx.fillText(lbl, pad.l + (i / n) * cw, H - 6);
      }}
    }}
  }}
}}

function drawHrRmssd(epochs) {{
  const canvas = document.getElementById('hr-rmssd-canvas');
  if (!canvas) return;
  const W = canvas.parentElement.offsetWidth - 8;
  const H = 120;
  canvas.width = W * 2; canvas.height = H * 2;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  const ctx = canvas.getContext('2d');
  ctx.scale(2, 2);

  const pad = {{ l: 32, r: 36, t: 10, b: 24 }};
  const cw = W - pad.l - pad.r;
  const ch = H - pad.t - pad.b;
  const n = epochs.length;

  // Collect HR and RMSSD values
  const hrs = epochs.map(e => e.hr);
  const rmssds = epochs.map(e => e.rmssd);
  const hrVals = hrs.filter(v => v != null);
  const rmssdVals = rmssds.filter(v => v != null);

  const hrMin = hrVals.length > 0 ? Math.min(...hrVals) - 5 : 40;
  const hrMax = hrVals.length > 0 ? Math.max(...hrVals) + 5 : 100;
  const rmssdMin = rmssdVals.length > 0 ? Math.max(0, Math.min(...rmssdVals) - 5) : 0;
  const rmssdMax = rmssdVals.length > 0 ? Math.max(...rmssdVals) + 10 : 100;

  const xOf = (i) => pad.l + (i / (n - 1)) * cw;
  const yHr = (v) => pad.t + ch - ((v - hrMin) / (hrMax - hrMin)) * ch;
  const yRmssd = (v) => pad.t + ch - ((v - rmssdMin) / (rmssdMax - rmssdMin)) * ch;

  // Draw HR line (rose)
  if (hrVals.length > 0) {{
    ctx.strokeStyle = '#fb7185'; ctx.lineWidth = 1.5;
    ctx.beginPath();
    let started = false;
    for (let i = 0; i < n; i++) {{
      if (hrs[i] != null) {{
        const x = xOf(i), y = yHr(hrs[i]);
        if (!started) {{ ctx.moveTo(x, y); started = true; }}
        else ctx.lineTo(x, y);
      }}
    }}
    ctx.stroke();
  }}

  // Draw RMSSD line (purple)
  if (rmssdVals.length > 0) {{
    ctx.strokeStyle = '#a78bfa'; ctx.lineWidth = 1.5;
    ctx.beginPath();
    let started = false;
    for (let i = 0; i < n; i++) {{
      if (rmssds[i] != null) {{
        const x = xOf(i), y = yRmssd(rmssds[i]);
        if (!started) {{ ctx.moveTo(x, y); started = true; }}
        else ctx.lineTo(x, y);
      }}
    }}
    ctx.stroke();
  }}

  // Left axis (HR)
  ctx.fillStyle = '#fb7185'; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
  const hrStep = Math.ceil((hrMax - hrMin) / 4);
  for (let v = Math.ceil(hrMin); v <= hrMax; v += hrStep) {{
    ctx.fillText(v + '', pad.l - 4, yHr(v) + 3);
    ctx.strokeStyle = '#21262d'; ctx.lineWidth = 0.5;
    ctx.beginPath(); ctx.moveTo(pad.l, yHr(v)); ctx.lineTo(W - pad.r, yHr(v)); ctx.stroke();
  }}

  // Right axis (RMSSD)
  if (rmssdVals.length > 0) {{
    ctx.fillStyle = '#a78bfa'; ctx.textAlign = 'left';
    const rStep = Math.ceil((rmssdMax - rmssdMin) / 4);
    for (let v = Math.ceil(rmssdMin); v <= rmssdMax; v += rStep) {{
      ctx.fillText(v + '', W - pad.r + 4, yRmssd(v) + 3);
    }}
  }}

  // X-axis time labels
  ctx.fillStyle = '#7d8590'; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
  const step = Math.max(1, Math.floor(n / 6));
  for (let i = 0; i < n; i += step) {{
    if (epochs[i].ts) {{
      const t = new Date(epochs[i].ts);
      const lbl = String(t.getHours()).padStart(2, '0') + ':' + String(t.getMinutes()).padStart(2, '0');
      ctx.fillText(lbl, xOf(i), H - 6);
    }}
  }}

  // Legend
  ctx.font = '9px sans-serif'; ctx.textAlign = 'left';
  ctx.fillStyle = '#fb7185'; ctx.fillText('HR (bpm)', pad.l + 4, pad.t + 10);
  ctx.fillStyle = '#a78bfa'; ctx.fillText('RMSSD (ms)', pad.l + 70, pad.t + 10);
}}

// ============================================================
// Sleep Quality — 30 day trends (collapsed section)
// ============================================================
(function() {{
  let sqOpen = false;
  document.getElementById('sleep-quality-toggle').addEventListener('click', () => {{
    sqOpen = !sqOpen;
    document.getElementById('sleep-quality-content').style.display = sqOpen ? 'block' : 'none';
    document.getElementById('sleep-quality-arrow').style.transform = sqOpen ? 'rotate(90deg)' : '';
    if (sqOpen && !window._sleepQualityRendered) {{
      window._sleepQualityRendered = true;
      renderSleepQuality();
    }}
  }});

  function renderSleepQuality() {{
    const today = new Date();
    today.setHours(0,0,0,0);
    const dates = [];
    const stagesData = [];
    const scores = [];

    for (let i = 29; i >= 0; i--) {{
      const d = new Date(today);
      d.setDate(d.getDate() - i);
      const ds = localDateStr(d);
      dates.push(ds);
      const sc = SLEEP_CUSTOM_DATA[ds];
      if (sc) {{
        stagesData.push({{ date: ds, deep: sc.deep / 3600, light: sc.light / 3600, rem: sc.rem / 3600, awake: sc.awake / 3600 }});
        scores.push({{ date: ds, score: sc.score }});
      }} else {{
        stagesData.push({{ date: ds, deep: 0, light: 0, rem: 0, awake: 0 }});
        scores.push({{ date: ds, score: null }});
      }}
    }}

    // --- Stacked bar chart ---
    const c1 = document.getElementById('sleep-stages-canvas');
    if (c1) {{
      const W = c1.parentElement.offsetWidth - 16;
      const H = 120;
      c1.width = W * 2; c1.height = H * 2;
      c1.style.width = W + 'px'; c1.style.height = H + 'px';
      const ctx = c1.getContext('2d');
      ctx.scale(2, 2);

      const pad = {{ l: 28, r: 6, t: 6, b: 22 }};
      const cw = W - pad.l - pad.r;
      const ch = H - pad.t - pad.b;
      const barW = Math.max(3, (cw / 30) - 2);
      const gap = (cw - barW * 30) / 29;

      // Find max total for scaling
      const maxTotal = Math.max(...stagesData.map(s => s.deep + s.light + s.rem + s.awake), 1);
      const yScale = ch / Math.max(maxTotal, 10);

      const colors = {{ deep: '#312e81', rem: '#818cf8', light: '#a5b4fc', awake: '#fbbf24' }};

      for (let i = 0; i < 30; i++) {{
        const s = stagesData[i];
        const x = pad.l + i * (barW + gap);
        let y = pad.t + ch;

        // Stack: deep -> rem -> light -> awake (bottom to top)
        const segments = [
          {{ val: s.deep, color: colors.deep }},
          {{ val: s.rem, color: colors.rem }},
          {{ val: s.light, color: colors.light }},
          {{ val: s.awake, color: colors.awake }},
        ];
        segments.forEach(seg => {{
          const h = seg.val * yScale;
          if (h > 0) {{
            ctx.fillStyle = seg.color;
            ctx.fillRect(x, y - h, barW, h);
            y -= h;
          }}
        }});
      }}

      // Y-axis
      ctx.fillStyle = '#7d8590'; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
      for (let v = 0; v <= Math.ceil(maxTotal); v += 2) {{
        const y = pad.t + ch - v * yScale;
        if (y >= pad.t) {{
          ctx.fillText(v + 'h', pad.l - 4, y + 3);
          ctx.strokeStyle = '#21262d'; ctx.lineWidth = 0.5;
          ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
        }}
      }}

      // X-axis (show every 5th date)
      ctx.fillStyle = '#7d8590'; ctx.font = '8px sans-serif'; ctx.textAlign = 'center';
      for (let i = 0; i < 30; i += 5) {{
        const parts = dates[i].split('-');
        const lbl = parseInt(parts[1]) + '/' + parseInt(parts[2]);
        ctx.fillText(lbl, pad.l + i * (barW + gap) + barW / 2, H - 6);
      }}

      // Legend
      ctx.font = '8px sans-serif'; ctx.textAlign = 'left';
      let lx = pad.l + 4;
      [['Deep','#312e81'],['REM','#818cf8'],['Light','#a5b4fc'],['Awake','#fbbf24']].forEach(([label, color]) => {{
        ctx.fillStyle = color;
        ctx.fillRect(lx, pad.t, 8, 8);
        ctx.fillStyle = '#7d8590';
        ctx.fillText(label, lx + 10, pad.t + 7);
        lx += ctx.measureText(label).width + 18;
      }});

      // Tooltip on hover
      const stagesToolip = document.getElementById('tooltip');
      c1.addEventListener('mousemove', (e) => {{
        const rect = c1.getBoundingClientRect();
        const scaleX = c1.width / rect.width / 2;
        const mx = (e.clientX - rect.left) * scaleX;
        const idx = Math.floor((mx - pad.l) / (barW + gap));
        if (idx >= 0 && idx < 30) {{
          const s = stagesData[idx];
          const total = s.deep + s.light + s.rem + s.awake;
          if (total > 0) {{
            const pct = (v) => Math.round(v / total * 100);
            const parts = dates[idx].split('-');
            const lbl = parseInt(parts[1]) + '/' + parseInt(parts[2]);
            stagesToolip.innerHTML = '<strong>' + lbl + '</strong><br>' +
              'Deep: ' + fmtSec(s.deep * 3600) + ' (' + pct(s.deep) + '%)<br>' +
              'REM: ' + fmtSec(s.rem * 3600) + ' (' + pct(s.rem) + '%)<br>' +
              'Light: ' + fmtSec(s.light * 3600) + ' (' + pct(s.light) + '%)<br>' +
              'Awake: ' + fmtSec(s.awake * 3600) + ' (' + pct(s.awake) + '%)';
            stagesToolip.style.display = 'block';
            stagesToolip.style.left = (e.clientX - stagesToolip.offsetWidth - 12) + 'px';
            stagesToolip.style.top = (e.clientY - 30) + 'px';
          }} else {{
            stagesToolip.style.display = 'none';
          }}
        }} else {{
          stagesToolip.style.display = 'none';
        }}
      }});
      c1.addEventListener('mouseleave', () => {{ stagesToolip.style.display = 'none'; }});
    }}

    // --- Score trend line ---
    const c2 = document.getElementById('sleep-score-canvas');
    if (c2) {{
      const W = c2.parentElement.offsetWidth - 16;
      const H = 80;
      c2.width = W * 2; c2.height = H * 2;
      c2.style.width = W + 'px'; c2.style.height = H + 'px';
      const ctx = c2.getContext('2d');
      ctx.scale(2, 2);

      const pad = {{ l: 28, r: 6, t: 10, b: 22 }};
      const cw = W - pad.l - pad.r;
      const ch = H - pad.t - pad.b;

      const validScores = scores.filter(s => s.score != null);
      if (validScores.length === 0) {{
        ctx.fillStyle = '#7d8590'; ctx.font = '12px sans-serif'; ctx.textAlign = 'center';
        ctx.fillText('No score data', W / 2, H / 2);
        return;
      }}

      const minS = Math.max(0, Math.min(...validScores.map(s => s.score)) - 5);
      const maxS = Math.min(100, Math.max(...validScores.map(s => s.score)) + 5);

      const xOf = (i) => pad.l + (i / 29) * cw;
      const yOf = (v) => pad.t + ch - ((v - minS) / (maxS - minS)) * ch;

      // Fill area
      ctx.fillStyle = 'rgba(168,85,247,0.15)';
      ctx.beginPath();
      let firstIdx = -1;
      let lastIdx = -1;
      for (let i = 0; i < 30; i++) {{
        if (scores[i].score != null) {{
          if (firstIdx < 0) {{ firstIdx = i; ctx.moveTo(xOf(i), yOf(scores[i].score)); }}
          else ctx.lineTo(xOf(i), yOf(scores[i].score));
          lastIdx = i;
        }}
      }}
      if (firstIdx >= 0) {{
        ctx.lineTo(xOf(lastIdx), yOf(minS));
        ctx.lineTo(xOf(firstIdx), yOf(minS));
        ctx.closePath(); ctx.fill();
      }}

      // Line
      ctx.strokeStyle = '#a855f7'; ctx.lineWidth = 2;
      ctx.beginPath();
      let started = false;
      for (let i = 0; i < 30; i++) {{
        if (scores[i].score != null) {{
          const x = xOf(i), y = yOf(scores[i].score);
          if (!started) {{ ctx.moveTo(x, y); started = true; }}
          else ctx.lineTo(x, y);
        }}
      }}
      ctx.stroke();

      // Dots
      ctx.fillStyle = '#a855f7';
      for (let i = 0; i < 30; i++) {{
        if (scores[i].score != null) {{
          ctx.beginPath();
          ctx.arc(xOf(i), yOf(scores[i].score), 2.5, 0, Math.PI * 2);
          ctx.fill();
        }}
      }}

      // Y-axis
      ctx.fillStyle = '#7d8590'; ctx.font = '9px sans-serif'; ctx.textAlign = 'right';
      const sStep = Math.ceil((maxS - minS) / 4);
      for (let v = Math.ceil(minS); v <= maxS; v += sStep) {{
        ctx.fillText(v + '', pad.l - 4, yOf(v) + 3);
        ctx.strokeStyle = '#21262d'; ctx.lineWidth = 0.5;
        ctx.beginPath(); ctx.moveTo(pad.l, yOf(v)); ctx.lineTo(W - pad.r, yOf(v)); ctx.stroke();
      }}

      // X-axis
      ctx.fillStyle = '#7d8590'; ctx.font = '8px sans-serif'; ctx.textAlign = 'center';
      for (let i = 0; i < 30; i += 5) {{
        const parts = dates[i].split('-');
        const lbl = parseInt(parts[1]) + '/' + parseInt(parts[2]);
        ctx.fillText(lbl, xOf(i), H - 6);
      }}
    }}
  }}
}})();

// ============================================================
// Today's Work Curve — 4 Visualization Options
// ============================================================
(function() {{
  const today = new Date();
  today.setHours(0,0,0,0);
  const todayStr = localDateStr(today);
  const START_H = 5, END_H = 22;

  // Build hourly arrays: hour -> active seconds
  function getHourlyMap(dateStr) {{
    const hb = HOURLY_DATA[dateStr] || [];
    const m = {{}};
    hb.forEach(h => {{ m[h.hour] = (h.active || 0); }});
    return m;
  }}

  // Collect past 28 days of hourly data
  const pastDays = [];
  for (let i = 1; i <= 28; i++) {{
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    const ds = localDateStr(d);
    if (HOURLY_DATA[ds]) pastDays.push(getHourlyMap(ds));
  }}
  const todayHours = getHourlyMap(todayStr);
  const nowHour = new Date().getHours();

  // Average hourly for past days
  function avgAtHour(h) {{
    if (pastDays.length === 0) return 0;
    return pastDays.reduce((s, d) => s + (d[h] || 0), 0) / pastDays.length;
  }}

  const hours = END_H - START_H;

  // ---- Work Curve toggle ----
  let curveOpen = false;
  document.getElementById('curve-toggle').addEventListener('click', () => {{
    curveOpen = !curveOpen;
    document.getElementById('curve-content').style.display = curveOpen ? 'block' : 'none';
    document.getElementById('curve-arrow').style.transform = curveOpen ? 'rotate(90deg)' : '';
    if (curveOpen && !window._paceRendered) {{ window._paceRendered = true; renderPace(); }}
  }});
  function renderPace() {{

  // ---- Pace Tracker (canvas) ----
  const canvasC = document.getElementById('viz-pace');
  if (canvasC) {{
    const ctx = canvasC.getContext('2d');
    const W = canvasC.offsetWidth;
    const H = 110;
    canvasC.width = W * 2; canvasC.height = H * 2;
    const pad = {{ l: 30, r: 40, t: 10, b: 20 }};
    const cw = W - pad.l - pad.r;
    const ch = H - pad.t - pad.b;

    // Build pace curves
    let avgTotal = 0;
    for (let i = 0; i <= hours; i++) avgTotal += avgAtHour(START_H + i) / 3600;
    let paceToday = [], paceAvg = [];
    let tRun = 0, aRun = 0;
    const expectedTotal = avgTotal || 6; // fallback 6h
    for (let i = 0; i <= hours; i++) {{
      const h = START_H + i;
      tRun += (todayHours[h] || 0) / 3600;
      aRun += avgAtHour(h) / 3600;
      paceToday.push(tRun);
      paceAvg.push(aRun);
    }}

    const xOf = (i) => pad.l + (i / hours) * cw;

    // Today pace (green solid) + projection (green dotted)
    const todayEnd = Math.min(nowHour - START_H, hours);

    // Smart projection: find most similar past days and use their trajectories
    let projected = [];
    if (todayEnd >= 0 && pastDays.length > 0) {{
      const todayCum = paceToday[todayEnd] || 0;

      // Build cumulative curves for each past day
      const pastCurves = pastDays.map(pd => {{
        let cum = [];
        let s = 0;
        for (let i = 0; i <= hours; i++) {{
          s += (pd[START_H + i] || 0) / 3600;
          cum.push(s);
        }}
        return cum;
      }});

      // Score each past day by similarity to today up to todayEnd
      // Uses cumulative value at current hour + shape similarity
      const scored = pastCurves.map((curve, idx) => {{
        const pastCum = curve[todayEnd] || 0;
        // Primary: how close is their cumulative total at this hour?
        const cumDiff = Math.abs(pastCum - todayCum);
        // Secondary: shape similarity (sum of squared diffs for hours so far)
        let shapeDiff = 0;
        for (let i = 0; i <= todayEnd; i++) {{
          const d = (paceToday[i] || 0) - (curve[i] || 0);
          shapeDiff += d * d;
        }}
        return {{ idx, score: cumDiff + Math.sqrt(shapeDiff) * 0.3, curve }};
      }});

      scored.sort((a, b) => a.score - b.score);

      // Take top K most similar days (or all if fewer)
      const K = Math.min(5, scored.length);
      const topDays = scored.slice(0, K);

      // Build projection: up to todayEnd use actual, then blend similar days
      // Anchor the projection to today's actual value at todayEnd
      for (let i = 0; i <= hours; i++) {{
        if (i <= todayEnd) {{
          projected.push(paceToday[i]);
        }} else {{
          // Average the incremental gain from similar days (from todayEnd onward)
          let sum = 0;
          topDays.forEach(d => {{
            const gain = (d.curve[i] || 0) - (d.curve[todayEnd] || 0);
            sum += gain;
          }});
          const avgGain = sum / K;
          projected.push(todayCum + avgGain);
        }}
      }}
    }} else if (todayEnd >= 0) {{
      // Fallback: no past data, use average increments
      for (let i = 0; i <= hours; i++) {{
        if (i <= todayEnd) {{
          projected.push(paceToday[i]);
        }} else {{
          const avgInc = avgAtHour(START_H + i) / 3600;
          projected.push((projected[i - 1] || 0) + avgInc);
        }}
      }}
    }}

    // Update maxP to include projection
    const projMax = projected.length > 0 ? Math.max(...projected) : 0;
    const maxP2 = Math.max(expectedTotal, ...paceToday, projMax, 1);
    // Redefine yOf with updated max
    const yOf2 = (v) => pad.t + ch - (v / maxP2) * ch;

    // Redraw avg with corrected scale (clear and redraw everything)
    ctx.clearRect(0, 0, canvasC.width, canvasC.height);
    ctx.save();
    ctx.scale(2, 2);

    // Expected end line
    ctx.strokeStyle = '#30363d'; ctx.lineWidth = 1;
    ctx.setLineDash([2, 2]);
    ctx.beginPath(); ctx.moveTo(pad.l, yOf2(expectedTotal)); ctx.lineTo(W - pad.r, yOf2(expectedTotal)); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = '#484f58'; ctx.font = '9px sans-serif'; ctx.textAlign = 'left';
    ctx.fillText('avg day total: ' + expectedTotal.toFixed(1) + 'h', pad.l + 4, yOf2(expectedTotal) - 4);

    // Average pace (grey fill + line)
    ctx.fillStyle = 'rgba(72,79,88,0.2)';
    ctx.beginPath(); ctx.moveTo(xOf(0), yOf2(0));
    paceAvg.forEach((v, i) => ctx.lineTo(xOf(i), yOf2(v)));
    ctx.lineTo(xOf(hours), yOf2(0)); ctx.closePath(); ctx.fill();
    ctx.strokeStyle = '#484f58'; ctx.lineWidth = 1.5;
    ctx.beginPath();
    paceAvg.forEach((v, i) => {{ i === 0 ? ctx.moveTo(xOf(i), yOf2(v)) : ctx.lineTo(xOf(i), yOf2(v)); }});
    ctx.stroke();

    // Today actual (solid green fill + line)
    if (todayEnd >= 0) {{
      ctx.fillStyle = 'rgba(57,211,83,0.15)';
      ctx.beginPath(); ctx.moveTo(xOf(0), yOf2(0));
      for (let i = 0; i <= todayEnd; i++) ctx.lineTo(xOf(i), yOf2(paceToday[i]));
      ctx.lineTo(xOf(todayEnd), yOf2(0)); ctx.closePath(); ctx.fill();

      ctx.strokeStyle = '#39d353'; ctx.lineWidth = 2;
      ctx.beginPath();
      for (let i = 0; i <= todayEnd; i++) {{
        i === 0 ? ctx.moveTo(xOf(i), yOf2(paceToday[i])) : ctx.lineTo(xOf(i), yOf2(paceToday[i]));
      }}
      ctx.stroke();

      // Projection (dotted green line from current position)
      if (todayEnd < hours) {{
        ctx.strokeStyle = '#39d353'; ctx.lineWidth = 1.5;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(xOf(todayEnd), yOf2(projected[todayEnd]));
        for (let i = todayEnd + 1; i <= hours; i++) {{
          ctx.lineTo(xOf(i), yOf2(projected[i]));
        }}
        ctx.stroke();
        ctx.setLineDash([]);

        // Projected end-of-day label (placed above the endpoint, left-aligned to stay in view)
        const projEnd = projected[hours];
        ctx.fillStyle = 'rgba(57,211,83,0.7)'; ctx.font = 'bold 9px sans-serif'; ctx.textAlign = 'right';
        const projLabelX = xOf(hours) - 2;
        const projLabelY = yOf2(projEnd) - 6;
        ctx.fillText('~' + projEnd.toFixed(1) + 'h', projLabelX, Math.max(projLabelY, pad.t + 8));
      }}

      // Dot at current position
      ctx.fillStyle = '#39d353';
      ctx.beginPath();
      ctx.arc(xOf(todayEnd), yOf2(paceToday[todayEnd]), 3, 0, Math.PI * 2);
      ctx.fill();
    }}

    // Axes
    ctx.fillStyle = '#7d8590'; ctx.font = '9px sans-serif'; ctx.textAlign = 'center';
    for (let i = 0; i <= hours; i += 2) ctx.fillText((START_H + i) + '', xOf(i), H - 4);
    ctx.textAlign = 'right';
    for (let v = 0; v <= maxP2; v += 2) {{
      ctx.fillText(v + 'h', pad.l - 4, yOf2(v) + 3);
      ctx.strokeStyle = '#21262d'; ctx.lineWidth = 0.5;
      ctx.beginPath(); ctx.moveTo(pad.l, yOf2(v)); ctx.lineTo(W - pad.r, yOf2(v)); ctx.stroke();
    }}
    ctx.restore();

    // Save base image for hover redraw
    const baseImage = ctx.getImageData(0, 0, canvasC.width, canvasC.height);

    // Hover tooltip
    const fmtH = (v) => {{ const hrs = Math.floor(v); const m = Math.round((v - hrs) * 60); return hrs > 0 ? hrs + 'h ' + m + 'm' : m + 'm'; }};
    canvasC.style.cursor = 'crosshair';
    canvasC.addEventListener('mousemove', function(e) {{
      const rect = canvasC.getBoundingClientRect();
      const scaleX = canvasC.width / rect.width;
      const mx = (e.clientX - rect.left) * scaleX / 2;
      const i = Math.round((mx - pad.l) / cw * hours);
      if (i < 0 || i > hours) {{ ctx.putImageData(baseImage, 0, 0); return; }}

      ctx.putImageData(baseImage, 0, 0);
      const h = START_H + i;
      const isPast = h <= nowHour;
      const aVal = paceAvg[i] || 0;
      const tVal = isPast ? (paceToday[i] || 0) : (projected[i] || 0);
      const x = xOf(i);

      ctx.save();
      ctx.scale(2, 2);

      // Vertical crosshair line
      ctx.strokeStyle = 'rgba(255,255,255,0.15)';
      ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(x, pad.t); ctx.lineTo(x, H - pad.b); ctx.stroke();

      // Dots
      ctx.fillStyle = isPast ? '#39d353' : 'rgba(57,211,83,0.5)';
      ctx.beginPath(); ctx.arc(x, yOf2(tVal), 4, 0, Math.PI * 2); ctx.fill();
      ctx.fillStyle = '#484f58';
      ctx.beginPath(); ctx.arc(x, yOf2(aVal), 4, 0, Math.PI * 2); ctx.fill();

      // Tooltip content
      const diff = tVal - aVal;
      const line1 = h + ':00';
      const line2 = (isPast ? 'Today: ' : 'Projected: ') + fmtH(tVal);
      const line3 = 'Avg: ' + fmtH(aVal);
      const line4 = isPast ? ((diff >= 0 ? '▲ ' : '▼ ') + (diff >= 0 ? '+' : '') + fmtH(Math.abs(diff))) : '';

      ctx.font = 'bold 10px sans-serif';
      const tw = Math.max(ctx.measureText(line2).width, ctx.measureText(line3).width, line4 ? ctx.measureText(line4).width : 0) + 16;
      const th = line4 ? 54 : 42;
      let tx = x + 8;
      if (tx + tw > W - pad.r) tx = x - tw - 8;
      let ty = pad.t + 4;

      ctx.fillStyle = 'rgba(22,27,34,0.92)';
      ctx.strokeStyle = '#30363d'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.roundRect(tx, ty, tw, th, 4); ctx.fill(); ctx.stroke();

      ctx.fillStyle = '#e6edf3'; ctx.font = 'bold 10px sans-serif'; ctx.textAlign = 'left';
      ctx.fillText(line1, tx + 6, ty + 13);
      ctx.fillStyle = isPast ? '#39d353' : 'rgba(57,211,83,0.6)'; ctx.font = '10px sans-serif';
      ctx.fillText(line2, tx + 6, ty + 26);
      ctx.fillStyle = '#7d8590';
      ctx.fillText(line3, tx + 6, ty + 38);
      if (line4) {{
        ctx.fillStyle = diff >= 0 ? '#39d353' : '#f85149';
        ctx.font = 'bold 10px sans-serif';
        ctx.fillText(line4, tx + 6, ty + 50);
      }}
      ctx.restore();
    }});

    canvasC.addEventListener('mouseleave', function() {{
      ctx.putImageData(baseImage, 0, 0);
    }});
  }}

  }} // end renderPace

}})();
</script>
</body>
</html>"""

    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "public, max-age=300"},
    )


@app.get("/dashboard/day-detail")
async def day_detail(date: str):
    """Return activity summary detail for a single date (YYYY-MM-DD).

    Dates in activity_summaries are stored as timestamptz (SGT midnight = UTC-8h),
    so we query with a range instead of exact match.
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz

    try:
        sgt = _tz(_td(hours=8))
        day = _dt.strptime(date, "%Y-%m-%d").replace(tzinfo=sgt)
        day_start = day.isoformat()
        day_end = (day + _td(days=1)).isoformat()

        result = supabase.table("activity_summaries").select("*").gte(
            "date", day_start
        ).lt(
            "date", day_end
        ).limit(1).execute()

        if not result.data:
            return {"date": date, "found": False}

        row = result.data[0]
        return {
            "date": date,
            "found": True,
            "total_active_time": row.get("total_active_time", 0),
            "total_afk_time": row.get("total_afk_time", 0),
            "productive_time": row.get("productive_time", 0),
            "distracting_time": row.get("distracting_time", 0),
            "top_apps": row.get("top_apps", []),
            "top_sites": row.get("top_sites", []),
            "hourly_breakdown": row.get("hourly_breakdown", []),
        }
    except Exception as e:
        logger.error(f"Day detail error: {e}")
        return {"date": date, "found": False, "error": str(e)}


@app.get("/dashboard/sleep-detail")
async def dashboard_sleep_detail(date: str):
    """Return sleep staging details for the modal (combines health_sleep_custom + health_sleep)."""
    try:
        # Query health_sleep_custom for staging data
        # Try exact date first, then date-1 (timezone offset: health_sleep stores dates
        # shifted by UTC+8, so the heatmap date may be +1 vs sleep_date in custom table)
        custom_result = supabase.table("health_sleep_custom").select(
            "sleep_date, duration_deep_s, duration_light_s, duration_rem_s, "
            "duration_awake_s, duration_total_s, custom_sleep_score, "
            "epoch_count, algorithm_version, epochs"
        ).eq("sleep_date", date).limit(1).execute()

        if not custom_result.data:
            # Try date - 1 day (timezone offset fallback)
            from datetime import datetime as _dt, timedelta as _td
            prev_date = (_dt.strptime(date, "%Y-%m-%d") - _td(days=1)).strftime("%Y-%m-%d")
            custom_result = supabase.table("health_sleep_custom").select(
                "sleep_date, duration_deep_s, duration_light_s, duration_rem_s, "
                "duration_awake_s, duration_total_s, custom_sleep_score, "
                "epoch_count, algorithm_version, epochs"
            ).eq("sleep_date", prev_date).limit(1).execute()

        if not custom_result.data:
            return {"date": date, "found": False}

        row = custom_result.data[0]

        # Query health_sleep for Withings vitals
        withings_data = None
        try:
            sleep_result = supabase.table("health_sleep").select(
                "hr_average, hr_min, hr_max, rr_average, sleep_score, "
                "sleep_efficiency_pct, sleep_latency_s, waso_s"
            ).eq("date", date).limit(1).execute()
            if sleep_result.data:
                withings_data = sleep_result.data[0]
        except Exception as e:
            logger.warning(f"Failed to fetch Withings sleep data for {date}: {e}")

        return {
            "date": date,
            "found": True,
            "sleep_date": row.get("sleep_date"),
            "duration_deep_s": row.get("duration_deep_s") or 0,
            "duration_light_s": row.get("duration_light_s") or 0,
            "duration_rem_s": row.get("duration_rem_s") or 0,
            "duration_awake_s": row.get("duration_awake_s") or 0,
            "duration_total_s": row.get("duration_total_s") or 0,
            "custom_sleep_score": row.get("custom_sleep_score"),
            "epoch_count": row.get("epoch_count"),
            "algorithm_version": row.get("algorithm_version"),
            "epochs": row.get("epochs") or [],
            "withings": withings_data,
        }
    except Exception as e:
        logger.error(f"Sleep detail error for {date}: {e}")
        return {"date": date, "found": False, "error": str(e)}


@app.get("/dashboard/workout-detail")
async def dashboard_workout_detail(date: str):
    """Return workout details for a given date for the heatmap modal."""
    try:
        date_start = f"{date}T00:00:00"
        date_end = f"{date}T23:59:59"
        sessions_result = supabase.table("health_workout_sessions").select(
            "id, title, session_type, started_at, ended_at, duration_seconds, notes"
        ).gte("started_at", date_start).lte("started_at", date_end).execute()

        if not sessions_result.data:
            return {"date": date, "sessions": []}

        sessions = []
        for session in sessions_result.data:
            exercises_result = supabase.table("health_workout_exercises").select(
                "id, exercise_name, is_rehab, muscle_group, exercise_order"
            ).eq("session_id", session["id"]).order("exercise_order").execute()

            exercises = []
            for ex in exercises_result.data:
                sets_result = supabase.table("health_workout_sets").select(
                    "set_order, set_type, weight_kg, reps, duration_seconds, rpe"
                ).eq("exercise_id", ex["id"]).order("set_order").execute()

                exercises.append({
                    "exercise_name": ex["exercise_name"],
                    "is_rehab": ex.get("is_rehab", False),
                    "muscle_group": ex.get("muscle_group"),
                    "sets": sets_result.data,
                })

            sessions.append({
                "title": session.get("title", "Workout"),
                "duration_seconds": session.get("duration_seconds", 0),
                "exercise_count": len(exercises),
                "set_count": sum(len(e["sets"]) for e in exercises),
                "exercises": exercises,
            })

        return {"date": date, "sessions": sessions}
    except Exception as e:
        logger.error(f"Workout detail error: {e}")
        return {"date": date, "sessions": [], "error": str(e)}



# ===========================================================================
# WITHINGS HEALTH INTEGRATION
# ===========================================================================

@app.get("/withings/authorize")
async def withings_authorize():
    """Start Withings OAuth2 flow. Visit this URL in a browser."""
    from lib.withings_client import WithingsClient
    client = WithingsClient()
    auth_url = client.get_authorize_url()
    return HTMLResponse(f"""
    <html><body style="font-family: sans-serif; max-width: 600px; margin: 80px auto; text-align: center;">
        <h1>Jarvis Health - Withings Authorization</h1>
        <p>Click the button below to authorize Jarvis to access your Withings health data.</p>
        <a href="{auth_url}" style="display: inline-block; padding: 16px 32px; background: #3b82f6;
           color: white; text-decoration: none; border-radius: 8px; font-size: 18px; margin-top: 20px;">
            Connect Withings
        </a>
    </body></html>
    """)


@app.get("/webhooks/withings/callback")
async def withings_oauth_callback(code: str = None, state: str = None, error: str = None):
    """Handle Withings OAuth2 callback. Exchanges code for tokens."""
    if error:
        return HTMLResponse(f"""
        <html><body style="font-family: sans-serif; max-width: 600px; margin: 80px auto; text-align: center;">
            <h1>Authorization Failed</h1>
            <p style="color: red;">Error: {error}</p>
        </body></html>
        """, status_code=400)

    if not code:
        return HTMLResponse("""
        <html><body style="font-family: sans-serif; max-width: 600px; margin: 80px auto; text-align: center;">
            <h1>Missing Code</h1>
            <p>No authorization code received from Withings.</p>
        </body></html>
        """, status_code=400)

    try:
        from lib.withings_client import WithingsClient
        client = WithingsClient()
        token_data = client.exchange_code(code)
        user_id = token_data.get("userid", "unknown")
        logger.info(f"Withings OAuth complete for user {user_id}")
        return HTMLResponse(f"""
        <html><body style="font-family: sans-serif; max-width: 600px; margin: 80px auto; text-align: center;">
            <h1 style="color: #22c55e;">Connected!</h1>
            <p>Withings account linked successfully (user: {user_id}).</p>
            <p>Health data will sync automatically every 15 minutes.</p>
            <p style="margin-top: 30px; color: #94a3b8;">You can close this window.</p>
        </body></html>
        """)
    except Exception as e:
        logger.error(f"Withings OAuth callback error: {e}")
        return HTMLResponse(f"""
        <html><body style="font-family: sans-serif; max-width: 600px; margin: 80px auto; text-align: center;">
            <h1 style="color: red;">Error</h1>
            <p>{str(e)}</p>
        </body></html>
        """, status_code=500)


@app.post("/sync/withings")
async def sync_withings(days: int = 7, full: bool = False, data_type: str | None = None):
    """Manually trigger Withings health data sync.

    Args:
        days: Lookback window in days (default 7).
        full: If True, sync last 365 days.
        data_type: Optional specific type to sync (measurements, activity, sleep,
                   heart_rate, ecg, workouts, sleep_details). If None, syncs all.
    """
    try:
        result = await run_in_threadpool(
            run_withings_sync, supabase, days=days, full_sync=full, data_type=data_type
        )
        return result
    except Exception as e:
        logger.error(f"Withings sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/sleep-staging")
async def sync_sleep_staging(days: int = 3):
    """Manually trigger custom sleep staging for recent nights.

    Args:
        days: How many days back to check for unprocessed nights (default 3).
    """
    try:
        result = await run_in_threadpool(run_post_withings_staging, supabase, days_back=days)
        return result
    except Exception as e:
        logger.error(f"Sleep staging sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.head("/webhooks/withings/notify")
async def withings_webhook_verify():
    """Withings verifies webhook URL with HEAD request."""
    return Response(status_code=200)


@app.post("/webhooks/withings/notify")
async def withings_webhook_notify(request: Request, background_tasks: BackgroundTasks):
    """Receive Withings push notification when new data is available.

    Withings sends: userid, startdate, enddate, appli (data type).
    Triggers a targeted sync for the notified data type.
    """
    try:
        form = await request.form()
        appli = int(form.get("appli", 0))
        startdate = int(form.get("startdate", 0))
        enddate = int(form.get("enddate", 0))
        userid = form.get("userid", "unknown")

        logger.info(f"Withings webhook: appli={appli}, user={userid}, range={startdate}-{enddate}")

        # Map appli to data type for targeted sync
        appli_map = {
            1: "measurements",   # Weight/body composition
            4: "measurements",   # Blood pressure, SpO2
            16: "activity",      # Activity
            44: "sleep",         # Sleep
            54: "ecg",           # ECG
            62: "heart_rate",    # HRV
        }

        data_type = appli_map.get(appli)
        if data_type:
            background_tasks.add_task(run_withings_sync, supabase, days=3, data_type=data_type)
            # Run custom sleep staging after sleep data sync
            if data_type == "sleep":
                background_tasks.add_task(run_post_withings_staging, supabase)
            log_sync_event("withings_webhook", "info",
                           f"Webhook received: appli={appli} ({data_type}), triggering sync")
        else:
            log_sync_event("withings_webhook", "warning",
                           f"Unhandled webhook appli={appli}")

        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Withings webhook error: {e}")
        return {"status": "error", "detail": str(e)}


@app.post("/sync/hevy/manual")
async def sync_hevy_manual():
    """Manually trigger a full Hevy workout backfill.

    Syncs all workouts from Hevy that are not yet in Supabase.
    """
    from syncs.hevy_sync import sync_all_hevy_workouts
    try:
        result = await run_in_threadpool(sync_all_hevy_workouts, supabase)
        return result
    except Exception as e:
        logger.error(f"Hevy manual sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/webhooks/hevy/notify")
async def hevy_webhook_notify(request: Request, background_tasks: BackgroundTasks):
    """Receive Hevy webhook when a workout is completed.

    Hevy sends: {"workoutId": "uuid"}
    Must respond 200 within 5 seconds.
    """
    # Validate authorization header
    auth_header = request.headers.get("Authorization", "")
    expected_secret = os.getenv("HEVY_WEBHOOK_SECRET", "")
    if expected_secret and auth_header != f"Bearer {expected_secret}":
        logger.warning("Hevy webhook: invalid authorization header")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        body = await request.json()
        workout_id = body.get("workoutId")
        if not workout_id:
            logger.warning("Hevy webhook: missing workoutId in payload")
            return {"status": "error", "detail": "missing workoutId"}

        logger.info(f"Hevy webhook: received workout {workout_id}")

        # Import and queue background sync
        from syncs.hevy_sync import sync_hevy_workout
        background_tasks.add_task(sync_hevy_workout, supabase, workout_id)

        log_sync_event("hevy_webhook", "info",
                       f"Webhook received: workoutId={workout_id}, triggering sync")

        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Hevy webhook error: {e}")
        return {"status": "error", "detail": str(e)}


# --- Health Data Read Endpoints (for dashboard) ---

@app.get("/health-data/summary")
async def get_health_summary(days: int = 7):
    """Get aggregated health summary for dashboard."""
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        since_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

        latest_meas = supabase.table("health_measurements").select("*").order(
            "measured_at", desc=True).limit(5).execute()
        latest_sleep = supabase.table("health_sleep").select("*").gte(
            "date", since_date).order("date", desc=True).execute()
        latest_activity = supabase.table("health_activity").select("*").gte(
            "date", since_date).order("date", desc=True).execute()
        latest_hr = supabase.table("health_heart_rate").select("*").gte(
            "timestamp", since).order("timestamp", desc=True).limit(1).execute()

        return {
            "measurements": latest_meas.data,
            "sleep": latest_sleep.data,
            "activity": latest_activity.data,
            "latest_heart_rate": latest_hr.data[0] if latest_hr.data else None,
        }
    except Exception as e:
        logger.error(f"Health summary error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/measurements")
async def get_health_measurements(days: int = 30):
    """Get measurement time series for dashboard."""
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        result = supabase.table("health_measurements").select("*").gte(
            "measured_at", since).order("measured_at", desc=True).execute()
        return {"data": result.data, "count": len(result.data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/sleep")
async def get_health_sleep(days: int = 30):
    """Get sleep data time series for dashboard."""
    try:
        since_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        result = supabase.table("health_sleep").select("*").gte(
            "date", since_date).order("date", desc=True).execute()
        return {"data": result.data, "count": len(result.data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/activity")
async def get_health_activity(days: int = 30):
    """Get activity data time series for dashboard."""
    try:
        since_date = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
        result = supabase.table("health_activity").select("*").gte(
            "date", since_date).order("date", desc=True).execute()
        return {"data": result.data, "count": len(result.data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/heart-rate")
async def get_health_heart_rate(days: int = 7):
    """Get intraday heart rate for dashboard."""
    try:
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        result = supabase.table("health_heart_rate").select("*").gte(
            "timestamp", since).order("timestamp", desc=True).execute()
        return {"data": result.data, "count": len(result.data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/workouts")
async def get_health_workouts(days: int = 30):
    """Get workout data for dashboard."""
    try:
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        result = supabase.table("health_workouts") \
            .select("*") \
            .gte("date", start_date[:10]) \
            .order("start_at", desc=True) \
            .execute()
        return {"data": result.data, "count": len(result.data)}
    except Exception as e:
        logger.error(f"Failed to fetch workouts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/sleep-details")
async def get_health_sleep_details(days: int = 7):
    """Get high-frequency sleep data for dashboard."""
    try:
        start_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        result = supabase.table("health_sleep_details") \
            .select("*") \
            .gte("sleep_date", start_date[:10]) \
            .order("sleep_date", desc=True) \
            .execute()
        return {"data": result.data, "count": len(result.data)}
    except Exception as e:
        logger.error(f"Failed to fetch sleep details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- AI Health Insights ---

@app.post("/health-data/insights/generate")
async def generate_insights(days: int = 7, force: bool = False):
    """Generate AI health insights using Claude + NeuroKit2.

    Aggregates all health data, computes category scores, runs advanced
    HRV analysis, and generates evidence-based insights and recommendations.

    Args:
        days: Number of days to analyze (default 7)
        force: If True, regenerate even if recent insights exist (default False)
    """
    from lib.health_insights import generate_health_insights
    try:
        result = await run_in_threadpool(generate_health_insights, supabase, days=days, force=force)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Health insights generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/insights")
async def get_latest_insights(days: int = 7):
    """Get the most recent health insights (cached, no regeneration)."""
    try:
        period_label = f"{days}d"
        result = supabase.table("health_insights").select("*").eq(
            "period_label", period_label
        ).order("generated_at", desc=True).limit(1).execute()

        if result.data:
            return result.data[0]
        return {"status": "no_insights", "message": f"No insights generated yet for {days}-day period. Call POST /health-data/insights/generate first."}
    except Exception as e:
        logger.error(f"Failed to fetch health insights: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health-data/insights/history")
async def get_insights_history(limit: int = 10):
    """Get history of generated health insights."""
    try:
        result = supabase.table("health_insights").select(
            "id,period_start,period_end,period_label,overall_score,"
            "recovery_score,sleep_score,cardiovascular_score,fitness_score,"
            "body_composition_score,stress_score,summary,generated_at"
        ).order("generated_at", desc=True).limit(limit).execute()
        return {"data": result.data, "count": len(result.data)}
    except Exception as e:
        logger.error(f"Failed to fetch insights history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/report/health-insights")
async def scheduled_health_insights():
    """
    Generate daily health insights and deliver via Telegram.

    Runs synchronously to prevent Cloud Run from scaling down mid-generation.
    Cloud Scheduler attempt-deadline is 300s which covers the ~90s Claude call.

    Schedule this daily at 08:30 SGT via Cloud Scheduler.
    """
    from lib.health_insights import generate_health_insights, format_telegram_briefing
    from lib.telegram_client import send_telegram_message

    try:
        logger.info("Scheduled health insights generation starting")
        result = await run_in_threadpool(generate_health_insights, supabase, 7, True)

        # Send Telegram briefing
        briefing = format_telegram_briefing(result)
        await send_telegram_message(briefing)

        overall = result.get("overall_score")
        logger.info(f"Health insights delivered: overall={overall}")
        return {
            "status": "success",
            "overall_score": overall,
            "summary": result.get("summary", "")[:200],
        }
    except Exception as e:
        logger.error(f"Scheduled health insights failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/report/daily-health")
async def scheduled_daily_health():
    """
    Generate daily morning health micro-briefing and deliver via Telegram.

    Uses Haiku for cost efficiency. Compares last night vs baselines.
    Schedule this daily at 07:00 SGT via cron on Hetzner.
    """
    from lib.health_insights import generate_daily_briefing, format_daily_telegram
    from lib.telegram_client import send_telegram_message

    try:
        logger.info("Daily health briefing generation starting")
        result = await run_in_threadpool(generate_daily_briefing, supabase)

        # Send Telegram briefing
        msg = format_daily_telegram(result)
        await send_telegram_message(msg)

        logger.info(f"Daily health briefing delivered: {len(result.get('briefing_text', ''))} chars")
        return {
            "status": "success",
            "briefing": result.get("briefing_text", "")[:200],
        }
    except Exception as e:
        logger.error(f"Daily health briefing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
