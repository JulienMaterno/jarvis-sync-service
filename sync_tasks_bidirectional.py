"""
Bidirectional Notion ↔ Supabase Task Sync Service

Syncs tasks between Notion and Supabase:
- Supabase → Notion: Tasks created from voice pipeline
- Notion → Supabase: Tasks created/updated manually in Notion

Based on sync_meetings_bidirectional.py structure.

Usage:
    python sync_tasks_bidirectional.py --full    # Full sync
    python sync_tasks_bidirectional.py           # Incremental (last 24h)
"""

import os
import logging
import argparse
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
import httpx
from lib.utils import retry_on_error_sync

load_dotenv()

# Import logging service
try:
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from lib.logging_service import log_sync_event_sync
    HAS_LOGGING_SERVICE = True
except ImportError:
    HAS_LOGGING_SERVICE = False
    def log_sync_event_sync(event_type, status, message, **kwargs):
        pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('TaskSync')

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
NOTION_TASKS_DB_ID = os.environ.get('NOTION_TASKS_DB_ID', '2b3cd3f1-eb28-8004-a33a-d26b8bb3fa58')

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Status mapping between Supabase and Notion
SUPABASE_TO_NOTION_STATUS = {
    'pending': 'Not started',
    'in_progress': 'Not started',  # Notion only has Not started/Done
    'completed': 'Done',
    'cancelled': 'Done',
}

NOTION_TO_SUPABASE_STATUS = {
    'Not started': 'pending',
    'Done': 'completed',
}


# ============================================================================
# NOTION CLIENT (simplified from meetings sync)
# ============================================================================

class NotionClient:
    """Notion API client for tasks."""
    
    def __init__(self, token: str):
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Notion-Version': '2022-06-28',
            'Content-Type': 'application/json'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    @retry_on_error_sync()
    def query_database(
        self, 
        database_id: str, 
        filter: Optional[Dict] = None,
        sorts: Optional[List[Dict]] = None,
        page_size: int = 100,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """Query all pages from a database with pagination."""
        results = []
        start_cursor = None
        
        while True:
            body = {"page_size": min(page_size, limit) if limit else page_size}
            if filter:
                body["filter"] = filter
            if sorts:
                body["sorts"] = sorts
            if start_cursor:
                body["start_cursor"] = start_cursor
            
            response = self.client.post(
                f'https://api.notion.com/v1/databases/{database_id}/query',
                json=body
            )
            response.raise_for_status()
            data = response.json()
            
            results.extend(data.get('results', []))
            
            if limit and len(results) >= limit:
                return results[:limit]
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return results
    
    @retry_on_error_sync()
    def get_page(self, page_id: str) -> Dict:
        """Get a single page by ID."""
        response = self.client.get(f'https://api.notion.com/v1/pages/{page_id}')
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def create_page(self, database_id: str, properties: Dict) -> Dict:
        """Create a new page in a database."""
        body = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        response = self.client.post('https://api.notion.com/v1/pages', json=body)
        response.raise_for_status()
        return response.json()
    
    @retry_on_error_sync()
    def update_page(self, page_id: str, properties: Dict) -> Dict:
        """Update an existing page."""
        response = self.client.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            json={"properties": properties}
        )
        response.raise_for_status()
        return response.json()


# ============================================================================
# SUPABASE CLIENT (simplified)
# ============================================================================

class SupabaseClient:
    """Supabase client for tasks."""
    
    def __init__(self, url: str, key: str):
        self.base_url = f"{url}/rest/v1"
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
    
    def get_all_tasks(self, include_completed: bool = False) -> List[Dict]:
        """Get all tasks from Supabase."""
        url = f"{self.base_url}/tasks?select=*&order=created_at.desc"
        if not include_completed:
            url += "&or=(status.eq.pending,status.eq.in_progress)"
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_task_by_notion_id(self, notion_page_id: str) -> Optional[Dict]:
        """Find a task by its Notion page ID."""
        url = f"{self.base_url}/tasks?select=*&notion_page_id=eq.{notion_page_id}&limit=1"
        response = self.client.get(url)
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    
    def create_task(self, task_data: Dict) -> Dict:
        """Create a new task."""
        response = self.client.post(f"{self.base_url}/tasks", json=task_data)
        response.raise_for_status()
        return response.json()[0]
    
    def update_task(self, task_id: str, updates: Dict) -> Dict:
        """Update an existing task."""
        response = self.client.patch(
            f"{self.base_url}/tasks?id=eq.{task_id}",
            json=updates
        )
        response.raise_for_status()
        return response.json()[0] if response.json() else {}


# ============================================================================
# CONVERSION FUNCTIONS
# ============================================================================

def notion_task_to_supabase(notion_task: Dict) -> Dict:
    """Convert Notion task properties to Supabase format."""
    props = notion_task.get('properties', {})
    
    # Extract title
    title_prop = props.get('Name', {}).get('title', [])
    title = title_prop[0].get('plain_text', 'Untitled') if title_prop else 'Untitled'
    
    # Extract due date
    due_prop = props.get('Due', {}).get('date')
    due_date = due_prop.get('start') if due_prop else None
    
    # Extract status
    status_prop = props.get('Status', {}).get('status')
    notion_status = status_prop.get('name') if status_prop else 'Not started'
    status = NOTION_TO_SUPABASE_STATUS.get(notion_status, 'pending')
    
    return {
        'title': title,
        'due_date': due_date,
        'status': status,
    }


def supabase_task_to_notion(task: Dict) -> Dict:
    """Convert Supabase task to Notion properties format."""
    title = task.get('title', 'Untitled')
    due_date = task.get('due_date')
    status = task.get('status', 'pending')
    
    notion_status = SUPABASE_TO_NOTION_STATUS.get(status, 'Not started')
    
    properties = {
        'Name': {
            'title': [{'text': {'content': title[:100]}}]  # Notion title limit
        },
        'Status': {
            'status': {'name': notion_status}
        }
    }
    
    if due_date:
        properties['Due'] = {
            'date': {'start': due_date}
        }
    
    return properties


# ============================================================================
# SYNC FUNCTIONS
# ============================================================================

def sync_notion_to_supabase(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync tasks from Notion → Supabase."""
    created = 0
    updated = 0
    skipped = 0
    errors = 0
    
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours) if not full_sync else None
    
    # Query Notion tasks
    filter_obj = None
    if not full_sync and since:
        filter_obj = {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": since.isoformat()}
        }
    
    tasks = notion.query_database(
        NOTION_TASKS_DB_ID,
        filter=filter_obj,
        sorts=[{"timestamp": "last_edited_time", "direction": "descending"}]
    )
    
    logger.info(f"Found {len(tasks)} tasks in Notion to process")
    
    # Get existing Supabase tasks for safety valve
    existing_supabase = supabase.get_all_tasks(include_completed=True)
    logger.info(f"Supabase has {len(existing_supabase)} total tasks")
    
    # SAFETY VALVE: If Notion returns empty/few but Supabase has many, abort
    if full_sync and len(existing_supabase) > 10 and len(tasks) < (len(existing_supabase) * 0.1):
        msg = f"Safety Valve: Notion returned {len(tasks)} tasks, but Supabase has {len(existing_supabase)}. Aborting to prevent data loss."
        logger.error(msg)
        raise Exception(msg)
    
    for notion_task in tasks:
        notion_page_id = notion_task['id']
        last_edited = notion_task.get('last_edited_time', '')
        
        try:
            # Check if task exists in Supabase
            existing = supabase.get_task_by_notion_id(notion_page_id)
            
            if existing:
                # Check if Notion is newer
                supabase_updated = existing.get('notion_updated_at', '')
                last_sync_source = existing.get('last_sync_source', '')
                
                # Parse timestamps for buffer comparison
                try:
                    notion_dt = datetime.fromisoformat(last_edited.replace('Z', '+00:00'))
                    existing_dt = datetime.fromisoformat(supabase_updated.replace('Z', '+00:00')) if supabase_updated else None
                except:
                    notion_dt = None
                    existing_dt = None
                
                # Skip if Supabase already has this version
                # Use 5-second buffer if last update came from Notion to avoid ping-pong
                if supabase_updated and notion_dt and existing_dt:
                    if last_sync_source == 'notion':
                        if notion_dt <= existing_dt + timedelta(seconds=5):
                            skipped += 1
                            continue
                    else:
                        if last_edited <= supabase_updated:
                            skipped += 1
                            continue
                elif supabase_updated and last_edited <= supabase_updated:
                    skipped += 1
                    continue
                
                # Parse task data
                task_data = notion_task_to_supabase(notion_task)
                
                # Content equality check - avoid unnecessary updates
                fields_to_check = ['title', 'due_date', 'status']
                needs_update = False
                for field in fields_to_check:
                    new_val = task_data.get(field)
                    existing_val = existing.get(field)
                    if (new_val is None and existing_val == "") or (new_val == "" and existing_val is None):
                        continue
                    if new_val != existing_val:
                        needs_update = True
                        logger.debug(f"Field '{field}' changed: {existing_val!r} → {new_val!r}")
                        break
                
                if needs_update:
                    task_data['notion_updated_at'] = last_edited
                    task_data['last_sync_source'] = 'notion'
                    supabase.update_task(existing['id'], task_data)
                    updated += 1
                    logger.info(f"Updated Supabase task: {task_data['title']}")
                else:
                    skipped += 1
                    logger.debug(f"Skipped (content unchanged): {task_data['title']}")
            else:
                # Create in Supabase
                task_data = notion_task_to_supabase(notion_task)
                task_data['notion_page_id'] = notion_page_id
                task_data['notion_updated_at'] = last_edited
                task_data['last_sync_source'] = 'notion'
                
                supabase.create_task(task_data)
                created += 1
                logger.info(f"Created Supabase task: {task_data['title']}")
                log_sync_event_sync("create_supabase_task", "success", f"Created task '{task_data['title']}'")
                
        except Exception as e:
            logger.error(f"Error syncing Notion task {notion_page_id}: {e}")
    
    logger.info(f"Notion → Supabase: {created} created, {updated} updated, {skipped} skipped")
    return created, updated, skipped


def sync_supabase_to_notion(
    notion: NotionClient, 
    supabase: SupabaseClient,
    full_sync: bool = False,
    since_hours: int = 24
) -> Tuple[int, int, int]:
    """Sync tasks from Supabase → Notion."""
    created = 0
    updated = 0
    skipped = 0
    
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours) if not full_sync else None
    
    # Get all tasks from Supabase
    all_tasks = supabase.get_all_tasks(include_completed=full_sync)
    
    # Get Notion tasks count for safety valve
    if full_sync:
        notion_tasks = notion.query_database(
            NOTION_TASKS_DB_ID,
            sorts=[{"timestamp": "last_edited_time", "direction": "descending"}]
        )
        logger.info(f"Notion has {len(notion_tasks)} total tasks")
        
        # SAFETY VALVE: If Supabase has many but Notion is empty, abort
        if len(all_tasks) > 10 and len(notion_tasks) < (len(all_tasks) * 0.1):
            msg = f"Safety Valve: Supabase has {len(all_tasks)} tasks, but Notion has {len(notion_tasks)}. Aborting to prevent data loss."
            logger.error(msg)
            raise Exception(msg)
    
    if not full_sync:
        # Filter to tasks needing sync
        tasks = []
        for t in all_tasks:
            notion_page_id = t.get('notion_page_id')
            updated_at = t.get('updated_at', '')
            
            if not notion_page_id:
                # Not linked to Notion - needs creation
                tasks.append(t)
            elif since and updated_at:
                try:
                    updated_dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                    if updated_dt > since:
                        tasks.append(t)
                except:
                    pass
        logger.info(f"Incremental mode: {len(tasks)} tasks need syncing (of {len(all_tasks)} total)")
    else:
        tasks = all_tasks
        logger.info(f"Full sync mode: processing all {len(tasks)} tasks")
    
    for task in tasks:
        task_id = task.get('id')
        notion_page_id = task.get('notion_page_id')
        
        try:
            if notion_page_id:
                # Already linked - check if we need to update Notion
                task_updated = task.get('updated_at', '')
                notion_updated = task.get('notion_updated_at', '')
                last_sync_source = task.get('last_sync_source', '')
                
                # Parse timestamps for buffer comparison
                try:
                    task_dt = datetime.fromisoformat(task_updated.replace('Z', '+00:00'))
                    notion_dt = datetime.fromisoformat(notion_updated.replace('Z', '+00:00')) if notion_updated else None
                except:
                    task_dt = None
                    notion_dt = None
                
                # Skip if Notion already has this version
                # Use 5-second buffer if last update came from Supabase to avoid ping-pong
                if notion_updated and task_dt and notion_dt:
                    if last_sync_source == 'supabase':
                        if task_dt <= notion_dt + timedelta(seconds=5):
                            skipped += 1
                            continue
                    else:
                        if task_updated <= notion_updated:
                            skipped += 1
                            continue
                elif notion_updated and task_updated <= notion_updated:
                    skipped += 1
                    continue
                
                # Update Notion
                try:
                    props = supabase_task_to_notion(task)
                    notion.update_page(notion_page_id, props)
                    
                    # Update Supabase with sync info
                    supabase.update_task(task_id, {
                        'notion_updated_at': datetime.now(timezone.utc).isoformat(),
                        'last_sync_source': 'supabase'
                    })
                    updated += 1
                    logger.info(f"Updated Notion task: {task['title']}")
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        logger.warning(f"Notion page {notion_page_id} not found - unlinking")
                        supabase.update_task(task_id, {'notion_page_id': None, 'notion_updated_at': None})
                        skipped += 1
                    else:
                        raise
            else:
                # Not linked - create in Notion
                props = supabase_task_to_notion(task)
                logger.info(f"Creating Notion task: {task['title']}")
                
                new_page = notion.create_page(NOTION_TASKS_DB_ID, props)
                new_page_id = new_page['id']
                
                # Link back to Supabase
                supabase.update_task(task_id, {
                    'notion_page_id': new_page_id,
                    'notion_updated_at': new_page.get('last_edited_time'),
                    'last_sync_source': 'supabase'
                })
                created += 1
                log_sync_event_sync("create_notion_task", "success", f"Created Notion task '{task['title']}'")
                
        except Exception as e:
            logger.error(f"Error syncing Supabase task {task_id} ({task.get('title', 'Unknown')}): {e}")
    
    logger.info(f"Supabase → Notion: {created} created, {updated} updated, {skipped} skipped")
    return created, updated, skipped


def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run bidirectional task sync."""
    start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("BIDIRECTIONAL TASK SYNC")
    logger.info(f"Mode: {'FULL' if full_sync else f'INCREMENTAL ({since_hours}h)'}")
    logger.info("=" * 60)
    
    log_sync_event_sync(
        "task_sync_start", "info",
        f"Starting {'full' if full_sync else 'incremental'} task sync"
    )
    
    # Initialize clients
    notion = NotionClient(NOTION_API_TOKEN)
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    
    # Run syncs
    logger.info("--- NOTION → SUPABASE ---")
    n2s_created, n2s_updated, n2s_skipped = sync_notion_to_supabase(
        notion, supabase, full_sync, since_hours
    )
    
    logger.info("--- SUPABASE → NOTION ---")
    s2n_created, s2n_updated, s2n_skipped = sync_supabase_to_notion(
        notion, supabase, full_sync, since_hours
    )
    
    elapsed = time.time() - start_time
    total_ops = n2s_created + n2s_updated + s2n_created + s2n_updated
    
    logger.info("=" * 60)
    logger.info("TASK SYNC COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Notion → Supabase: {n2s_created} created, {n2s_updated} updated, {n2s_skipped} skipped")
    logger.info(f"Supabase → Notion: {s2n_created} created, {s2n_updated} updated, {s2n_skipped} skipped")
    logger.info(f"Total operations: {total_ops} in {elapsed:.1f}s")
    logger.info("=" * 60)
    
    log_sync_event_sync(
        "task_sync_complete", "success",
        f"Task sync complete: {total_ops} operations in {elapsed:.1f}s"
    )
    
    return {
        'notion_to_supabase': {'created': n2s_created, 'updated': n2s_updated, 'skipped': n2s_skipped},
        'supabase_to_notion': {'created': s2n_created, 'updated': s2n_updated, 'skipped': s2n_skipped},
        'total_operations': total_ops,
        'elapsed_seconds': elapsed
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Bidirectional Notion ↔ Supabase Task Sync')
    parser.add_argument('--full', action='store_true', help='Full sync (all tasks)')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back for incremental sync')
    
    args = parser.parse_args()
    
    result = run_sync(full_sync=args.full, since_hours=args.hours)
    print(f"\nResult: {result}")
