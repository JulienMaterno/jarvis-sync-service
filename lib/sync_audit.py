"""
Sync Audit Service

Provides functionality for:
1. Recording sync operations with before/after counts
2. Comparing database inventories across Supabase, Notion, and Google
3. Health checking to detect sync discrepancies
"""

import os
import uuid
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field

from lib.supabase_client import supabase
from lib.notion_client import notion

logger = logging.getLogger("SyncAudit")

# Notion Database IDs (loaded from environment)
NOTION_DBS = {
    'contacts': os.environ.get('NOTION_CRM_DATABASE_ID', ''),
    'meetings': os.environ.get('NOTION_MEETING_DB_ID', ''),
    'tasks': os.environ.get('NOTION_TASKS_DB_ID', ''),
    'reflections': os.environ.get('NOTION_REFLECTIONS_DB_ID', ''),
    'journals': os.environ.get('NOTION_JOURNAL_DB_ID', '')
}


def get_google_contacts_count() -> int:
    """
    Get count of contacts in Google People API.
    Uses stored OAuth token from Supabase.
    """
    try:
        from lib.google_contacts import get_all_contacts
        from lib.oauth_handler import get_valid_google_token
        
        # Get token
        loop = asyncio.new_event_loop()
        try:
            token = loop.run_until_complete(get_valid_google_token())
            if not token:
                logger.warning("No valid Google token available")
                return -1
            
            # Get contacts count
            contacts = loop.run_until_complete(get_all_contacts(token))
            return len(contacts)
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"Error counting Google contacts: {e}")
        return -1


@dataclass
class SyncStats:
    """Statistics for a single entity sync operation"""
    entity_type: str
    supabase_count: int = 0
    notion_count: int = 0
    google_count: Optional[int] = None
    created_in_notion: int = 0
    created_in_supabase: int = 0
    updated_in_notion: int = 0
    updated_in_supabase: int = 0
    deleted_in_notion: int = 0
    deleted_in_supabase: int = 0
    errors: List[str] = field(default_factory=list)
    
    @property
    def count_difference(self) -> int:
        # Handle None values gracefully
        n = self.notion_count or 0
        s = self.supabase_count or 0
        return abs(n - s)
    
    @property
    def is_in_sync(self) -> bool:
        # Considered in sync if both are 0 or equal
        return self.count_difference == 0
    
    @property
    def sync_health(self) -> str:
        diff = self.count_difference
        if diff == 0:
            return 'healthy'
        elif diff <= 5:
            return 'warning'
        else:
            return 'critical'
    
    @property
    def total_operations(self) -> int:
        return (self.created_in_notion + self.created_in_supabase +
                self.updated_in_notion + self.updated_in_supabase +
                self.deleted_in_notion + self.deleted_in_supabase)


def get_supabase_count(table: str) -> int:
    """Get count of active (non-deleted) records in Supabase table"""
    try:
        # Try with deleted_at filter first
        result = supabase.table(table).select('id', count='exact').is_('deleted_at', 'null').execute()
        return result.count
    except:
        # Fall back to total count if no deleted_at column
        try:
            result = supabase.table(table).select('id', count='exact').execute()
            return result.count
        except Exception as e:
            logger.error(f"Error counting {table} in Supabase: {e}")
            return -1


def get_notion_count(entity_type: str) -> int:
    """Get count of records in Notion database"""
    db_id = NOTION_DBS.get(entity_type)
    if not db_id:
        logger.warning(f"No Notion DB ID configured for {entity_type}")
        return -1
    
    try:
        pages = list(notion.query_database_all(db_id))
        return len(pages)
    except Exception as e:
        logger.error(f"Error counting {entity_type} in Notion: {e}")
        return -1


def get_database_inventory() -> Dict[str, Dict[str, int]]:
    """
    Get current counts for all synced entities across all databases.
    
    Returns:
        {
            'contacts': {'supabase': 126, 'notion': 126, 'google': 150},
            'meetings': {'supabase': 120, 'notion': 120},
            'calendar_events': {'supabase': 200},  # Google → Supabase only
            'emails': {'supabase': 150},  # Gmail → Supabase only
            'beeper_chats': {'supabase': 208},  # Beeper → Supabase only
            'books': {'supabase': 50, 'notion': 50},
            'highlights': {'supabase': 300, 'notion': 300},
            ...
        }
    """
    inventory = {}
    
    # Core bidirectional entities (Notion ↔ Supabase)
    bidirectional_entities = ['contacts', 'meetings', 'tasks', 'reflections', 'journals']
    
    for entity in bidirectional_entities:
        inventory[entity] = {
            'supabase': get_supabase_count(entity),
            'notion': get_notion_count(entity)
        }
        
        # Add Google contacts count for contacts entity
        if entity == 'contacts':
            google_count = get_google_contacts_count()
            if google_count >= 0:
                inventory[entity]['google'] = google_count
        
        # Calculate difference and health (Supabase vs Notion)
        sb = inventory[entity]['supabase']
        n = inventory[entity]['notion']
        if sb >= 0 and n >= 0:
            inventory[entity]['difference'] = n - sb
            inventory[entity]['is_in_sync'] = (n == sb)
        else:
            inventory[entity]['difference'] = None
            inventory[entity]['is_in_sync'] = None
    
    # Supabase-only entities (Google/Beeper → Supabase, no Notion sync)
    supabase_only_entities = ['calendar_events', 'emails', 'beeper_chats', 'beeper_messages']
    
    for entity in supabase_only_entities:
        count = get_supabase_count(entity)
        inventory[entity] = {
            'supabase': count,
            'source': 'google' if entity in ['calendar_events', 'emails'] else 'beeper'
        }
    
    # Notion → Supabase only entities (read-only from Notion)
    notion_to_supabase = {
        'books': os.environ.get('NOTION_BOOKS_DB_ID', ''),
        'highlights': os.environ.get('NOTION_HIGHLIGHTS_DB_ID', '')
    }
    
    for entity, db_id in notion_to_supabase.items():
        sb_count = get_supabase_count(entity)
        n_count = -1
        if db_id:
            try:
                pages = list(notion.query_database_all(db_id))
                n_count = len(pages)
            except Exception as e:
                logger.warning(f"Error counting {entity} in Notion: {e}")
        
        inventory[entity] = {
            'supabase': sb_count,
            'notion': n_count if n_count >= 0 else None
        }
        
        # Calculate sync health if both counts available
        if sb_count >= 0 and n_count >= 0:
            inventory[entity]['difference'] = n_count - sb_count
            inventory[entity]['is_in_sync'] = (n_count == sb_count)
    
    # Additional bidirectional entities (Notion ↔ Supabase)
    extra_bidirectional = {
        'applications': os.environ.get('NOTION_APPLICATIONS_DB_ID', ''),
        'documents': os.environ.get('NOTION_DOCUMENTS_DB_ID', ''),
        'linkedin_posts': os.environ.get('NOTION_LINKEDIN_POSTS_DB_ID', '')
    }
    
    for entity, db_id in extra_bidirectional.items():
        sb_count = get_supabase_count(entity)
        n_count = -1
        if db_id:
            try:
                pages = list(notion.query_database_all(db_id))
                n_count = len(pages)
            except Exception as e:
                logger.warning(f"Error counting {entity} in Notion: {e}")
        
        inventory[entity] = {
            'supabase': sb_count,
            'notion': n_count if n_count >= 0 else None
        }
        
        # Calculate sync health if both counts available
        if sb_count >= 0 and n_count >= 0:
            inventory[entity]['difference'] = n_count - sb_count
            inventory[entity]['is_in_sync'] = (n_count == sb_count)
    
    return inventory


def check_sync_health() -> Dict[str, Any]:
    """
    Perform a health check on all sync services.
    
    Returns:
        {
            'status': 'healthy' | 'warning' | 'critical',
            'timestamp': '2026-01-04T12:00:00Z',
            'entities': {
                'contacts': {'status': 'healthy', 'supabase': 126, 'notion': 126},
                ...
            },
            'issues': ['meetings: 2 records missing in Notion']
        }
    """
    inventory = get_database_inventory()
    issues = []
    worst_status = 'healthy'
    
    status_priority = {'healthy': 0, 'warning': 1, 'critical': 2}
    
    entities_status = {}
    for entity, counts in inventory.items():
        diff = counts.get('difference')
        
        if diff is None:
            entity_status = 'unknown'
            issues.append(f"{entity}: Unable to compare databases")
        elif diff == 0:
            entity_status = 'healthy'
        elif abs(diff) <= 5:
            entity_status = 'warning'
            if diff > 0:
                issues.append(f"{entity}: {abs(diff)} extra records in Notion")
            else:
                issues.append(f"{entity}: {abs(diff)} records missing in Notion")
        else:
            entity_status = 'critical'
            if diff > 0:
                issues.append(f"{entity}: {abs(diff)} extra records in Notion (CRITICAL)")
            else:
                issues.append(f"{entity}: {abs(diff)} records missing in Notion (CRITICAL)")
        
        entities_status[entity] = {
            'status': entity_status,
            'supabase': counts['supabase'],
            'notion': counts['notion'],
            'difference': diff
        }
        
        if entity_status in status_priority:
            if status_priority.get(entity_status, 0) > status_priority.get(worst_status, 0):
                worst_status = entity_status
    
    return {
        'status': worst_status,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'entities': entities_status,
        'issues': issues
    }


def record_sync_audit(
    run_id: str,
    sync_type: str,
    entity_type: str,
    stats: SyncStats,
    triggered_by: str = 'api',
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
    status: str = 'success',
    error_message: Optional[str] = None,
    details: Optional[Dict] = None
) -> Optional[Dict]:
    """
    Record a sync operation in the audit table.
    
    Args:
        run_id: UUID grouping all syncs from one run
        sync_type: 'full', 'incremental', 'triggered'
        entity_type: 'contacts', 'meetings', etc.
        stats: SyncStats object with counts and operations
        triggered_by: 'scheduler', 'api', 'webhook'
        started_at: When sync started
        completed_at: When sync finished
        status: 'success', 'partial', 'failed'
        error_message: Error details if failed
        details: Additional JSON metadata
    
    Returns:
        The created audit record, or None if recording fails
    """
    try:
        now = datetime.now(timezone.utc)
        started = started_at or now
        completed = completed_at or now
        
        duration_ms = int((completed - started).total_seconds() * 1000) if completed else None
        
        record = {
            'run_id': run_id,
            'sync_type': sync_type,
            'entity_type': entity_type,
            'supabase_count': stats.supabase_count,
            'notion_count': stats.notion_count,
            'google_count': stats.google_count,
            'created_in_notion': stats.created_in_notion,
            'created_in_supabase': stats.created_in_supabase,
            'updated_in_notion': stats.updated_in_notion,
            'updated_in_supabase': stats.updated_in_supabase,
            'deleted_in_notion': stats.deleted_in_notion,
            'deleted_in_supabase': stats.deleted_in_supabase,
            'is_in_sync': stats.is_in_sync,
            'count_difference': stats.count_difference,
            'sync_health': stats.sync_health,
            'started_at': started.isoformat(),
            'completed_at': completed.isoformat() if completed else None,
            'duration_ms': duration_ms,
            'status': status,
            'error_message': error_message,
            'triggered_by': triggered_by,
            'details': details
        }
        
        result = supabase.table('sync_audit').insert(record).execute()
        return result.data[0] if result.data else None
        
    except Exception as e:
        # Table might not exist yet - don't fail the sync
        error_str = str(e)
        if 'sync_audit' in error_str and ('does not exist' in error_str or 'PGRST' in error_str):
            logger.warning("sync_audit table not found - run migration 011_sync_audit.sql")
        else:
            logger.error(f"Failed to record sync audit: {e}")
        return None


def get_sync_history(entity_type: Optional[str] = None, days: int = 7) -> List[Dict]:
    """Get recent sync history, optionally filtered by entity type"""
    try:
        query = supabase.table('sync_audit').select('*')
        
        if entity_type:
            query = query.eq('entity_type', entity_type)
        
        # Get last N days
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        query = query.gte('created_at', cutoff)
        
        result = query.order('created_at', desc=True).limit(100).execute()
        return result.data
        
    except Exception as e:
        logger.error(f"Failed to get sync history: {e}")
        return []


def generate_sync_report(run_id: str) -> Dict[str, Any]:
    """Generate a summary report for a sync run"""
    try:
        result = supabase.table('sync_audit').select('*').eq('run_id', run_id).execute()
        
        if not result.data:
            return {'error': 'No sync found with this run_id'}
        
        records = result.data
        
        # Calculate totals
        total_created_notion = sum(r.get('created_in_notion', 0) for r in records)
        total_created_supabase = sum(r.get('created_in_supabase', 0) for r in records)
        total_updated_notion = sum(r.get('updated_in_notion', 0) for r in records)
        total_updated_supabase = sum(r.get('updated_in_supabase', 0) for r in records)
        
        # Determine overall status
        statuses = [r.get('status') for r in records]
        if 'failed' in statuses:
            overall_status = 'failed'
        elif 'partial' in statuses:
            overall_status = 'partial'
        else:
            overall_status = 'success'
        
        # Determine overall health
        healths = [r.get('sync_health') for r in records]
        if 'critical' in healths:
            overall_health = 'critical'
        elif 'warning' in healths:
            overall_health = 'warning'
        else:
            overall_health = 'healthy'
        
        return {
            'run_id': run_id,
            'started_at': min(r.get('started_at') for r in records),
            'completed_at': max(r.get('completed_at') for r in records if r.get('completed_at')),
            'status': overall_status,
            'health': overall_health,
            'entities_synced': len(records),
            'totals': {
                'created_in_notion': total_created_notion,
                'created_in_supabase': total_created_supabase,
                'updated_in_notion': total_updated_notion,
                'updated_in_supabase': total_updated_supabase
            },
            'entities': {
                r['entity_type']: {
                    'supabase': r.get('supabase_count'),
                    'notion': r.get('notion_count'),
                    'status': r.get('status'),
                    'health': r.get('sync_health'),
                    'operations': {
                        'created_notion': r.get('created_in_notion', 0),
                        'created_supabase': r.get('created_in_supabase', 0),
                        'updated_notion': r.get('updated_in_notion', 0),
                        'updated_supabase': r.get('updated_in_supabase', 0)
                    }
                }
                for r in records
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to generate sync report: {e}")
        return {'error': str(e)}


def generate_24h_summary() -> Dict[str, Any]:
    """
    Generate a comprehensive 24-hour sync summary including:
    - Sync runs with time breakdown per entity type
    - Notion and Google counts alongside Supabase (excluding soft-deleted)
    - Total operations across all runs
    - Error summary
    """
    try:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

        # Get all sync audit records from last 24h
        result = supabase.table('sync_audit').select('*').gte('created_at', cutoff).order('created_at', desc=True).execute()
        records = result.data or []

        if not records:
            return {
                'status': 'no_data',
                'message': 'No sync runs in the last 24 hours',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }

        # Group by run_id to get sync runs
        runs = {}
        for r in records:
            run_id = r.get('run_id')
            if run_id not in runs:
                runs[run_id] = {
                    'run_id': run_id,
                    'started_at': r.get('started_at'),
                    'completed_at': r.get('completed_at'),
                    'entities': [],
                    'total_duration_ms': 0,
                    'errors': 0
                }
            runs[run_id]['entities'].append(r)
            runs[run_id]['total_duration_ms'] += r.get('duration_ms', 0) or 0
            if r.get('status') == 'failed':
                runs[run_id]['errors'] += 1

        # Calculate time breakdown per entity type across all runs
        time_by_entity = {}
        ops_by_entity = {}
        for r in records:
            entity = r.get('entity_type')
            if entity not in time_by_entity:
                time_by_entity[entity] = 0
                ops_by_entity[entity] = {'created': 0, 'updated': 0, 'deleted': 0}
            time_by_entity[entity] += r.get('duration_ms', 0) or 0
            ops_by_entity[entity]['created'] += (r.get('created_in_supabase', 0) or 0) + (r.get('created_in_notion', 0) or 0)
            ops_by_entity[entity]['updated'] += (r.get('updated_in_supabase', 0) or 0) + (r.get('updated_in_notion', 0) or 0)
            ops_by_entity[entity]['deleted'] += (r.get('deleted_in_supabase', 0) or 0) + (r.get('deleted_in_notion', 0) or 0)

        # Sort entities by time spent (descending)
        time_breakdown = sorted(
            [{'entity': k, 'duration_ms': v, 'duration_sec': round(v / 1000, 1)} for k, v in time_by_entity.items()],
            key=lambda x: x['duration_ms'],
            reverse=True
        )

        # Get current inventory with active counts only (excluding soft-deleted)
        inventory = get_database_inventory()

        # Calculate totals across all runs
        total_ops = {
            'created_supabase': sum(r.get('created_in_supabase', 0) or 0 for r in records),
            'created_notion': sum(r.get('created_in_notion', 0) or 0 for r in records),
            'updated_supabase': sum(r.get('updated_in_supabase', 0) or 0 for r in records),
            'updated_notion': sum(r.get('updated_in_notion', 0) or 0 for r in records),
            'deleted_supabase': sum(r.get('deleted_in_supabase', 0) or 0 for r in records),
            'deleted_notion': sum(r.get('deleted_in_notion', 0) or 0 for r in records),
        }
        total_ops['total'] = sum(total_ops.values())

        # Get errors from sync_logs for richer error context
        error_result = supabase.table('sync_logs').select('event_type, message, created_at').eq('status', 'error').gte('created_at', cutoff).order('created_at', desc=True).limit(10).execute()
        recent_errors = error_result.data or []

        return {
            'status': 'success',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'period': '24h',
            'summary': {
                'sync_runs': len(runs),
                'total_operations': total_ops['total'],
                'total_errors': sum(run['errors'] for run in runs.values()),
                'total_duration_sec': round(sum(run['total_duration_ms'] for run in runs.values()) / 1000, 1)
            },
            'operations_24h': total_ops,
            'time_breakdown': time_breakdown,
            'operations_by_entity': ops_by_entity,
            'current_inventory': {
                entity: {
                    'supabase': counts.get('supabase', 0),
                    'notion': counts.get('notion'),
                    'google': counts.get('google'),
                    'in_sync': counts.get('is_in_sync'),
                    'difference': counts.get('difference')
                }
                for entity, counts in inventory.items()
            },
            'recent_errors': [
                {'type': e.get('event_type'), 'message': e.get('message', '')[:100], 'time': e.get('created_at')}
                for e in recent_errors
            ],
            'runs': [
                {
                    'run_id': run['run_id'][:8],
                    'started_at': run['started_at'],
                    'duration_sec': round(run['total_duration_ms'] / 1000, 1),
                    'entities': len(run['entities']),
                    'errors': run['errors']
                }
                for run in sorted(runs.values(), key=lambda x: x['started_at'] or '', reverse=True)[:10]
            ]
        }

    except Exception as e:
        logger.error(f"Failed to generate 24h summary: {e}")
        return {'status': 'error', 'error': str(e)}


def format_24h_summary_text(summary: Dict[str, Any]) -> str:
    """Format the 24h summary as a human-readable text report."""
    if summary.get('status') == 'no_data':
        return "📊 No sync runs in the last 24 hours"

    if summary.get('status') == 'error':
        return f"❌ Error generating report: {summary.get('error')}"

    lines = []
    lines.append("📊 **Sync Summary (Last 24h)**")
    lines.append("")

    # Summary stats
    s = summary.get('summary', {})
    lines.append(f"**Runs:** {s.get('sync_runs', 0)} | **Operations:** {s.get('total_operations', 0)} | **Errors:** {s.get('total_errors', 0)} | **Duration:** {s.get('total_duration_sec', 0)}s")
    lines.append("")

    # Time breakdown by entity
    lines.append("**Time Breakdown:**")
    for tb in summary.get('time_breakdown', [])[:8]:
        ops = summary.get('operations_by_entity', {}).get(tb['entity'], {})
        ops_str = f"+{ops.get('created', 0)}/-{ops.get('deleted', 0)}/~{ops.get('updated', 0)}"
        lines.append(f"  {tb['entity']}: {tb['duration_sec']}s ({ops_str})")
    lines.append("")

    # Current inventory
    lines.append("**Current Counts (active only):**")
    inv = summary.get('current_inventory', {})
    
    # Core bidirectional entities
    for entity in ['contacts', 'meetings', 'tasks', 'reflections', 'journals']:
        if entity in inv:
            e = inv[entity]
            sb = e.get('supabase', 0)
            n = e.get('notion', '-')
            g = e.get('google')
            sync_icon = "✅" if e.get('in_sync') else "⚠️"
            counts = f"SB:{sb} N:{n}"
            if g is not None:
                counts += f" G:{g}"
            lines.append(f"  {sync_icon} {entity}: {counts}")
    
    # Additional bidirectional entities (applications, documents, linkedin_posts)
    for entity in ['applications', 'documents', 'linkedin_posts']:
        if entity in inv:
            e = inv[entity]
            sb = e.get('supabase', 0)
            n = e.get('notion', '-')
            sync_icon = "✅" if e.get('in_sync') else "⚠️"
            lines.append(f"  {sync_icon} {entity}: SB:{sb} N:{n}")

    # Recent errors
    errors = summary.get('recent_errors', [])
    if errors:
        lines.append("")
        lines.append("**Recent Errors:**")
        for err in errors[:3]:
            lines.append(f"  - {err.get('type')}: {err.get('message', '')[:60]}...")

    return "\n".join(lines)


# Export for easy importing
__all__ = [
    'SyncStats',
    'get_database_inventory',
    'check_sync_health',
    'record_sync_audit',
    'get_sync_history',
    'generate_sync_report',
    'generate_24h_summary',
    'format_24h_summary_text',
    'get_supabase_count',
    'get_notion_count'
]
