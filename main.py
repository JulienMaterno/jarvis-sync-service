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

# Import applications and LinkedIn posts sync
from syncs.applications_sync import run_sync as run_applications_sync
from syncs.linkedin_posts_sync import run_sync as run_linkedin_posts_sync

# Import ActivityWatch sync
from sync_activitywatch import run_activitywatch_sync, ActivityWatchSync, format_activity_summary_for_journal

# Import Beeper sync
from sync_beeper import run_beeper_sync, run_beeper_relink

# Import Supabase client for Beeper sync
from lib.supabase_client import supabase

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

        # =====================================================================
        # PHASE 2: Run syncs only for entities with changes
        # =====================================================================
        
        # === CONTACTS SYNC (always run - has Google component) ===
        await run_step("notion_to_supabase", sync_notion_to_supabase)
        await run_step("google_sync", sync_contacts)
        await run_step("supabase_to_notion", sync_supabase_to_notion)
        
        # === MEETINGS SYNC (conditional) ===
        if 'meetings' in synced_entities:
            await run_step("meetings_sync", run_meeting_sync, full_sync=False, since_hours=24, entity_name='meetings')
        else:
            results["meetings_sync"] = {"status": "skipped", "reason": "no_changes"}
        
        # === TASKS SYNC (conditional) ===
        if 'tasks' in synced_entities:
            await run_step("tasks_sync", run_task_sync, full_sync=False, since_hours=24, entity_name='tasks')
        else:
            results["tasks_sync"] = {"status": "skipped", "reason": "no_changes"}
        
        # === REFLECTIONS SYNC (conditional) ===
        if 'reflections' in synced_entities:
            await run_step("reflections_sync", run_reflection_sync, full_sync=False, since_hours=24, entity_name='reflections')
        else:
            results["reflections_sync"] = {"status": "skipped", "reason": "no_changes"}
        
        # === JOURNALS SYNC (conditional) ===
        if 'journals' in synced_entities:
            await run_step("journals_sync", run_journal_sync, full_sync=False, since_hours=24, entity_name='journals')
        else:
            results["journals_sync"] = {"status": "skipped", "reason": "no_changes"}
        
        # === CALENDAR SYNC (always run - external source) ===
        await run_step("calendar_sync", run_calendar_sync)

        # === GMAIL SYNC (always run - external source) ===
        await run_step("gmail_sync", run_gmail_sync)
        
        # === BOOKS SYNC (Notion → Supabase, one-way) ===
        await run_step("books_sync", run_books_sync, full_sync=False, since_hours=24)
        
        # === HIGHLIGHTS SYNC (Notion → Supabase, one-way) ===
        await run_step("highlights_sync", run_highlights_sync, full_sync=False, hours=24)
        
        # === APPLICATIONS SYNC (bidirectional) ===
        await run_step("applications_sync", run_applications_sync, full_sync=False, since_hours=24)
        
        # === LINKEDIN POSTS SYNC (bidirectional) ===
        await run_step("linkedin_posts_sync", run_linkedin_posts_sync, full_sync=False, since_hours=24)
        
        # === BEEPER SYNC (WhatsApp/Telegram/LinkedIn messages) ===
        # This gracefully handles offline laptop - just skips and catches up next run
        await run_step("beeper_sync", run_beeper_sync, supabase, full_sync=False)
        
        # Track sync completion
        _last_sync_end = datetime.now(timezone.utc)
        _last_sync_results = results
        
        # Count successes, skips, and errors for summary
        success_count = sum(1 for r in results.values() if r.get("status") == "success")
        skipped_count = sum(1 for r in results.values() if r.get("status") == "skipped")
        error_count = sum(1 for r in results.values() if r.get("status") == "error")
        
        # Record audit for this sync cycle - ALL entities
        try:
            inventory = get_database_inventory()
            
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
                    
                    stats = SyncStats(
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
                        stats=stats,
                        triggered_by='api',
                        started_at=_last_sync_start,
                        completed_at=_last_sync_end,
                        status='success' if results.get(f'{entity}_sync', {}).get('status') == 'success' else 'partial',
                        details={'sync_results': results.get(f'{entity}_sync')}
                    )
            
            # Calendar events (Google → Supabase only)
            # Note: Calendar does full upsert each time, so we only track actual DB count
            # The "count" returned by sync_calendar is "events processed", not "events changed"
            if 'calendar_sync' in results:
                cal_data = results['calendar_sync'].get('data', {})
                # Get actual created/updated from sync result if available
                events_created = cal_data.get('events_created', 0) or cal_data.get('created', 0) or 0
                events_updated = cal_data.get('events_updated', 0) or cal_data.get('updated', 0) or 0
                stats = SyncStats(
                    entity_type='calendar_events',
                    supabase_count=inventory.get('calendar_events', {}).get('supabase', 0),
                    created_in_supabase=events_created,
                    updated_in_supabase=events_updated
                )
                record_sync_audit(
                    run_id=run_id,
                    sync_type='scheduled',
                    entity_type='calendar_events',
                    stats=stats,
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
                stats = SyncStats(
                    entity_type='emails',
                    supabase_count=inventory.get('emails', {}).get('supabase', 0),
                    created_in_supabase=emails_created,
                    updated_in_supabase=emails_updated
                )
                record_sync_audit(
                    run_id=run_id,
                    sync_type='scheduled',
                    entity_type='emails',
                    stats=stats,
                    triggered_by='api',
                    started_at=_last_sync_start,
                    completed_at=_last_sync_end,
                    status=results['gmail_sync'].get('status', 'success'),
                    details={'sync_results': results['gmail_sync']}
                )
            
            # Beeper (Beeper Bridge → Supabase) - Cloud tracks this separately
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
                stats = SyncStats(
                    entity_type='beeper_messages',
                    supabase_count=inventory.get('beeper_messages', {}).get('supabase', 0),
                    created_in_supabase=messages_new,
                    updated_in_supabase=0
                )
                record_sync_audit(
                    run_id=run_id,
                    sync_type='scheduled',
                    entity_type='beeper_messages',
                    stats=stats,
                    triggered_by='api',
                    started_at=_last_sync_start,
                    completed_at=_last_sync_end,
                    status=results['beeper_sync'].get('status', 'success'),
                    details={'sync_results': results['beeper_sync']}
                )
            
            # Books (Notion → Supabase only)
            if 'books_sync' in results:
                created, updated, _ = extract_counts(results['books_sync'].get('data', {}))
                stats = SyncStats(
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
                    stats=stats,
                    triggered_by='api',
                    started_at=_last_sync_start,
                    completed_at=_last_sync_end,
                    status=results['books_sync'].get('status', 'success'),
                    details={'sync_results': results['books_sync']}
                )
            
            # Highlights (Notion → Supabase only)
            if 'highlights_sync' in results:
                created, updated, _ = extract_counts(results['highlights_sync'].get('data', {}))
                stats = SyncStats(
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
                    stats=stats,
                    triggered_by='api',
                    started_at=_last_sync_start,
                    completed_at=_last_sync_end,
                    status=results['highlights_sync'].get('status', 'success'),
                    details={'sync_results': results['highlights_sync']}
                )
            
            # Build consolidated summary of changes
            changes_summary = []
            for entity in ['contacts', 'meetings', 'tasks', 'reflections', 'journals', 'calendar_events', 'emails', 'books', 'highlights', 'beeper_chats', 'beeper_messages']:
                sync_key = f'{entity}_sync' if entity not in ['contacts', 'calendar_events', 'emails', 'beeper_chats', 'beeper_messages'] else entity.replace('_', ' ')
                # Find the right key in results
                for key in results:
                    if entity.replace('_', '') in key.replace('_', '').lower():
                        data = results[key].get('data', {})
                        if isinstance(data, dict):
                            stats = data.get('stats', data)
                            c = stats.get('created', 0) or 0
                            u = stats.get('updated', 0) or 0
                            d = stats.get('deleted', 0) or 0
                            if c or u or d:
                                changes_summary.append(f"{entity}:{c}c/{u}u/{d}d")
                        break
            
            if changes_summary:
                logger.info(f"📊 SYNC CHANGES: {', '.join(changes_summary)}")
            else:
                logger.info("📊 SYNC CHANGES: No changes detected")
            
            logger.info(f"Recorded comprehensive audit for sync run {run_id[:8]}")
        except Exception as e:
            logger.error(f"Failed to record sync audit: {e}", exc_info=True)
        
        logger.info(f"LEAN sync complete: {success_count} synced, {skipped_count} skipped (no changes), {error_count} errors")
        
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
import os

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


