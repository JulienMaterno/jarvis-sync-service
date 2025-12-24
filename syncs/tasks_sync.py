"""
===================================================================================
TASKS SYNC SERVICE - Bidirectional Notion ↔ Supabase
===================================================================================

Uses the unified sync architecture from lib/sync_base.py.

Features:
- Bidirectional sync between Notion Tasks DB and Supabase tasks table
- Automatic deletion sync (both directions)
- Status mapping between Supabase and Notion formats
- Safety valves to prevent data loss

Usage:
    python -m syncs.tasks_sync --full    # Full sync
    python -m syncs.tasks_sync           # Incremental (last 24h)
"""

import os
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment
load_dotenv()

from lib.sync_base import (
    TwoWaySyncService,
    NotionPropertyExtractor,
    NotionPropertyBuilder,
    SyncResult,
    create_cli_parser,
    setup_logger
)

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_TASKS_DB_ID = os.environ.get('NOTION_TASKS_DB_ID', '2b3cd3f1-eb28-8004-a33a-d26b8bb3fa58')

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
# TASKS SYNC SERVICE
# ============================================================================

class TasksSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Tasks between Notion and Supabase.
    
    Notion Properties:
    - Name (title): Task title
    - Status (status): Not started / Done
    - Due (date): Due date
    
    Supabase Fields:
    - title (text): Task title
    - status (text): pending / in_progress / completed / cancelled
    - due_date (date): Due date
    - notion_page_id, notion_updated_at, last_sync_source (sync tracking)
    """
    
    def __init__(self):
        super().__init__(
            service_name="TasksSync",
            notion_database_id=NOTION_TASKS_DB_ID,
            supabase_table="tasks"
        )
        self.logger = setup_logger("TasksSync")
    
    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Convert Notion task to Supabase format.
        Notion → Supabase
        """
        props = notion_record.get('properties', {})
        
        # Extract title safely
        title = NotionPropertyExtractor.title(props, 'Name')
        if not title:
            title = 'Untitled'
        
        # Extract status with mapping
        notion_status = NotionPropertyExtractor.select(props, 'Status')
        status = NOTION_TO_SUPABASE_STATUS.get(notion_status, 'pending')
        
        # Extract due date
        due_date = NotionPropertyExtractor.date(props, 'Due')
        
        return {
            'title': title,
            'status': status,
            'due_date': due_date,
        }
    
    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Convert Supabase task to Notion properties format.
        Supabase → Notion
        """
        title = supabase_record.get('title', 'Untitled')
        status = supabase_record.get('status', 'pending')
        due_date = supabase_record.get('due_date')
        
        # Map status to Notion format
        notion_status = SUPABASE_TO_NOTION_STATUS.get(status, 'Not started')
        
        properties = {
            'Name': NotionPropertyBuilder.title(title[:100]),  # Notion title limit
            'Status': {"status": {"name": notion_status}},  # Status property uses special format
        }
        
        if due_date:
            properties['Due'] = NotionPropertyBuilder.date(due_date)
        
        return properties


# ============================================================================
# ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run the tasks sync and return results."""
    service = TasksSyncService()
    result = service.sync(full_sync=full_sync, since_hours=since_hours)
    
    return {
        'success': result.success,
        'direction': result.direction,
        'created': result.stats.created,
        'updated': result.stats.updated,
        'deleted': result.stats.deleted,
        'skipped': result.stats.skipped,
        'errors': result.stats.errors,
        'elapsed_seconds': result.elapsed_seconds
    }


if __name__ == "__main__":
    parser = create_cli_parser("Tasks")
    args = parser.parse_args()
    
    result = run_sync(full_sync=args.full, since_hours=args.hours)
    print(f"\nResult: {result}")
