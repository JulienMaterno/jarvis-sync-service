"""
===================================================================================
SYNC CURSOR - Lightweight Change Detection for Lean Syncing
===================================================================================

This module provides fast "has anything changed?" checks before running full syncs.
If nothing changed since the last sync, we skip entirely - saving API calls and compute.

Architecture:
1. Store last_sync_timestamp for each entity (meetings, tasks, reflections, journals)
2. Before sync: count records modified since that timestamp in BOTH systems
3. If zero changes on both sides → skip sync entirely
4. If changes detected → run the actual sync, then update cursor

Storage: sync_state table in Supabase (already exists)
Key format: "{entity}_sync_cursor" → ISO timestamp
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
import httpx
import os

logger = logging.getLogger(__name__)

# Notion API
NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')

# Database IDs (same as in sync modules)
NOTION_DB_IDS = {
    'meetings': os.environ.get('NOTION_MEETING_DB_ID', '297cd3f1-eb28-810f-86f0-f142f7e3a5ca'),
    'tasks': os.environ.get('NOTION_TASK_DB_ID', '1f7cd3f1-eb28-80ec-85b9-dc9f5e3e50ac'),
    'reflections': os.environ.get('NOTION_REFLECTIONS_DB_ID', '1f7cd3f1-eb28-8062-8bf6-de95f57f39ef'),
    'journals': os.environ.get('NOTION_JOURNALS_DB_ID', '1f7cd3f1-eb28-8032-9a5c-f91afab45c6b'),
}


@dataclass
class ChangeCheckResult:
    """Result of checking for changes."""
    entity: str
    has_changes: bool
    notion_changes: int
    supabase_changes: int
    last_cursor: Optional[datetime]
    check_duration_ms: float
    skipped_reason: Optional[str] = None


def get_sync_cursor(supabase_client, entity: str) -> Optional[datetime]:
    """Get the last sync timestamp for an entity."""
    try:
        result = supabase_client.table('sync_state').select('value').eq('key', f'{entity}_sync_cursor').execute()
        if result.data and result.data[0].get('value'):
            return datetime.fromisoformat(result.data[0]['value'].replace('Z', '+00:00'))
    except Exception as e:
        logger.warning(f"Could not get sync cursor for {entity}: {e}")
    return None


def set_sync_cursor(supabase_client, entity: str, timestamp: datetime):
    """Update the sync cursor after a successful sync."""
    try:
        supabase_client.table('sync_state').upsert({
            'key': f'{entity}_sync_cursor',
            'value': timestamp.isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat()
        }).execute()
        logger.debug(f"Updated sync cursor for {entity} to {timestamp}")
    except Exception as e:
        logger.warning(f"Could not update sync cursor for {entity}: {e}")


def count_notion_changes_since(database_id: str, since: datetime) -> int:
    """
    Count records in Notion modified since the given timestamp.
    Uses a lightweight query with just a filter - no full data fetch.
    """
    if not NOTION_API_TOKEN:
        logger.warning("No Notion API token - cannot check for changes")
        return -1  # Unknown
    
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(
                f'https://api.notion.com/v1/databases/{database_id}/query',
                headers={
                    'Authorization': f'Bearer {NOTION_API_TOKEN}',
                    'Notion-Version': '2022-06-28',
                    'Content-Type': 'application/json'
                },
                json={
                    'filter': {
                        'timestamp': 'last_edited_time',
                        'last_edited_time': {'after': since.isoformat()}
                    },
                    'page_size': 1  # We only need count, not data
                }
            )
            response.raise_for_status()
            data = response.json()
            
            # If has_more is True, there are definitely changes
            # Otherwise, count the results (0 or 1)
            if data.get('has_more'):
                # More than 1 change - we know we need to sync
                # Could paginate to get exact count, but >1 is enough to trigger sync
                return 100  # Signal "multiple changes"
            return len(data.get('results', []))
            
    except Exception as e:
        logger.warning(f"Error checking Notion changes: {e}")
        return -1  # Unknown - should sync to be safe


def count_supabase_changes_since(supabase_client, table: str, since: datetime) -> int:
    """
    Count records in Supabase modified since the given timestamp.
    Uses a lightweight count query.
    """
    try:
        # Query records with last_sync_source='supabase' (locally modified)
        # OR records updated since the cursor that weren't synced from notion
        result = supabase_client.table(table)\
            .select('id', count='exact')\
            .gte('updated_at', since.isoformat())\
            .eq('last_sync_source', 'supabase')\
            .execute()
        
        return result.count if result.count is not None else len(result.data)
    except Exception as e:
        logger.warning(f"Error checking Supabase changes for {table}: {e}")
        return -1  # Unknown


def check_for_changes(supabase_client, entity: str) -> ChangeCheckResult:
    """
    Check if there are any changes to sync for an entity.
    Returns quickly if no changes detected.
    
    Args:
        supabase_client: Supabase client instance
        entity: One of 'meetings', 'tasks', 'reflections', 'journals'
    
    Returns:
        ChangeCheckResult with has_changes=True/False
    """
    import time
    start_time = time.time()
    
    # Get database ID
    database_id = NOTION_DB_IDS.get(entity)
    if not database_id:
        return ChangeCheckResult(
            entity=entity,
            has_changes=True,  # Unknown entity - sync to be safe
            notion_changes=-1,
            supabase_changes=-1,
            last_cursor=None,
            check_duration_ms=(time.time() - start_time) * 1000,
            skipped_reason="Unknown entity"
        )
    
    # Get last sync cursor
    cursor = get_sync_cursor(supabase_client, entity)
    
    # If no cursor, this is first sync - must run
    if not cursor:
        return ChangeCheckResult(
            entity=entity,
            has_changes=True,
            notion_changes=-1,
            supabase_changes=-1,
            last_cursor=None,
            check_duration_ms=(time.time() - start_time) * 1000,
            skipped_reason="No cursor - first sync"
        )
    
    # Check Notion for changes
    notion_changes = count_notion_changes_since(database_id, cursor)
    
    # Check Supabase for changes
    supabase_changes = count_supabase_changes_since(supabase_client, entity, cursor)
    
    # Determine if sync needed
    # -1 means error/unknown - sync to be safe
    has_changes = (
        notion_changes != 0 or 
        supabase_changes != 0 or 
        notion_changes == -1 or 
        supabase_changes == -1
    )
    
    duration_ms = (time.time() - start_time) * 1000
    
    if not has_changes:
        logger.info(f"[{entity}] No changes detected since {cursor.isoformat()} - skipping sync (checked in {duration_ms:.0f}ms)")
    else:
        logger.info(f"[{entity}] Changes detected: Notion={notion_changes}, Supabase={supabase_changes} (checked in {duration_ms:.0f}ms)")
    
    return ChangeCheckResult(
        entity=entity,
        has_changes=has_changes,
        notion_changes=notion_changes,
        supabase_changes=supabase_changes,
        last_cursor=cursor,
        check_duration_ms=duration_ms
    )


def check_all_entities(supabase_client) -> Dict[str, ChangeCheckResult]:
    """
    Check all entities for changes.
    Returns a dict of entity -> ChangeCheckResult
    """
    results = {}
    for entity in NOTION_DB_IDS.keys():
        results[entity] = check_for_changes(supabase_client, entity)
    return results


def update_cursor_after_sync(supabase_client, entity: str, sync_completed_at: datetime = None):
    """
    Update the sync cursor after a successful sync.
    Call this at the END of each successful sync operation.
    """
    timestamp = sync_completed_at or datetime.now(timezone.utc)
    set_sync_cursor(supabase_client, entity, timestamp)
