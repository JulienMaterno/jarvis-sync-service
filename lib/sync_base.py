"""
===================================================================================
UNIFIED SYNC ARCHITECTURE - Base Classes and Templates (v2)
===================================================================================

This module provides a standardized, reusable architecture for all sync services.
All sync modules should inherit from these base classes for consistency.

Architecture Patterns:
1. ONE-WAY SYNC: Source → Destination (e.g., Books from Notion, LinkedIn posts)
2. TWO-WAY SYNC: Bidirectional with conflict resolution (e.g., Meetings, Tasks)
3. MULTI-SOURCE: Multiple sources to one destination (e.g., Contacts: Notion + Google → Supabase)

Key Features:
- Consistent error handling and logging
- Automatic retry with exponential backoff  
- Safety valves to prevent data loss
- Unified Notion and Supabase clients
- Page content extraction and creation
- Bidirectional deletion sync
- Standardized entry points and CLI interface

v2 Changes:
- Added content block extraction (extract_page_content)
- Added content block creation (build_content_blocks, chunked_paragraphs)
- Added bidirectional deletion sync (S→N archives Notion pages)
- Added CRM contact linking support
- Enhanced NotionClient with block management
"""

import os
import logging
import argparse
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import httpx
from functools import wraps

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_API_TOKEN = os.environ.get('NOTION_API_TOKEN')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Safety valve threshold - abort if source has <10% of destination
SAFETY_VALVE_THRESHOLD = 0.1
SAFETY_VALVE_MIN_RECORDS = 10  # Only apply safety valve if destination has > this many

# Notion API limits
MAX_BLOCKS_PER_REQUEST = 100  # Notion allows max 100 blocks per append request


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Create a consistently formatted logger."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        ))
        logger.addHandler(handler)

    return logger


def format_exception(e: Exception) -> str:
    """Format exception with type name when str(e) is empty (e.g., timeouts)."""
    msg = str(e)
    if not msg:
        msg = f"{type(e).__name__} (no message)"
    return msg


# ============================================================================
# RETRY DECORATOR
# ============================================================================

def retry_on_error(max_retries: int = 3, base_delay: float = 1.0, exceptions: tuple = (Exception,)):
    """Decorator for automatic retry with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


def retry_on_error_async(max_retries: int = 3, base_delay: float = 1.0, exceptions: tuple = (Exception,)):
    """Async decorator for automatic retry with exponential backoff."""
    import asyncio
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        await asyncio.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


# ============================================================================
# SYNC RESULT DATA CLASSES
# ============================================================================

class SyncDirection(Enum):
    ONE_WAY = "one_way"
    TWO_WAY = "two_way"
    MULTI_SOURCE = "multi_source"


@dataclass
class SyncStats:
    """Statistics from a sync operation."""
    created: int = 0
    updated: int = 0
    deleted: int = 0
    skipped: int = 0
    errors: int = 0
    
    @property
    def total_processed(self) -> int:
        return self.created + self.updated + self.deleted + self.skipped
    
    def to_dict(self) -> Dict:
        return {
            'created': self.created,
            'updated': self.updated,
            'deleted': self.deleted,
            'skipped': self.skipped,
            'errors': self.errors,
            'total_processed': self.total_processed
        }


@dataclass
class SyncMetrics:
    """
    Enhanced observability metrics for sync operations.
    Tracks performance, rate limits, staleness, and data flow.
    """
    # Timing metrics
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    
    # Phase durations (seconds)
    notion_deletions_duration: float = 0.0
    supabase_deletions_duration: float = 0.0
    notion_to_supabase_duration: float = 0.0
    supabase_to_notion_duration: float = 0.0
    
    # Record counts
    source_total: int = 0
    destination_total: int = 0
    records_read: int = 0
    records_written: int = 0
    
    # API call tracking
    notion_api_calls: int = 0
    supabase_api_calls: int = 0
    rate_limit_events: int = 0
    retries: int = 0
    
    # Staleness tracking
    newest_source_change: Optional[datetime] = None
    newest_dest_change: Optional[datetime] = None
    staleness_seconds: float = 0.0  # How old is newest remote change not in dest
    
    # Data integrity
    orphaned_records: int = 0
    duplicate_records: int = 0
    conflict_resolutions: int = 0
    
    @property
    def total_duration_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now(timezone.utc) - self.start_time).total_seconds()
    
    @property
    def records_per_second(self) -> float:
        duration = self.total_duration_seconds
        if duration > 0:
            return self.records_read / duration
        return 0.0
    
    def finish(self):
        """Mark the sync as complete."""
        self.end_time = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict:
        return {
            'timing': {
                'total_seconds': round(self.total_duration_seconds, 2),
                'notion_deletions_seconds': round(self.notion_deletions_duration, 2),
                'supabase_deletions_seconds': round(self.supabase_deletions_duration, 2),
                'notion_to_supabase_seconds': round(self.notion_to_supabase_duration, 2),
                'supabase_to_notion_seconds': round(self.supabase_to_notion_duration, 2),
            },
            'counts': {
                'source_total': self.source_total,
                'destination_total': self.destination_total,
                'records_read': self.records_read,
                'records_written': self.records_written,
            },
            'api': {
                'notion_calls': self.notion_api_calls,
                'supabase_calls': self.supabase_api_calls,
                'rate_limit_events': self.rate_limit_events,
                'retries': self.retries,
            },
            'staleness': {
                'staleness_seconds': round(self.staleness_seconds, 2),
                'newest_source_change': self.newest_source_change.isoformat() if self.newest_source_change else None,
                'newest_dest_change': self.newest_dest_change.isoformat() if self.newest_dest_change else None,
            },
            'data_quality': {
                'orphaned_records': self.orphaned_records,
                'duplicate_records': self.duplicate_records,
                'conflict_resolutions': self.conflict_resolutions,
            },
            'throughput': {
                'records_per_second': round(self.records_per_second, 2),
            }
        }


@dataclass
class SyncResult:
    """Result of a complete sync operation."""
    success: bool
    direction: str
    stats: SyncStats = field(default_factory=SyncStats)
    metrics: SyncMetrics = field(default_factory=SyncMetrics)
    source_count: int = 0
    destination_count: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'success': self.success,
            'direction': self.direction,
            'stats': self.stats.to_dict(),
            'metrics': self.metrics.to_dict(),
            'source_count': self.source_count,
            'destination_count': self.destination_count,
            'elapsed_seconds': round(self.elapsed_seconds, 2),
            'error_message': self.error_message
        }


# ============================================================================
# UNIFIED NOTION CLIENT
# ============================================================================

class NotionClient:
    """
    Unified Notion API client with retry logic.
    
    Usage:
        notion = NotionClient(NOTION_API_TOKEN)
        pages = notion.query_database(database_id)
        page = notion.get_page(page_id)
        notion.create_page(database_id, properties)
        notion.update_page(page_id, properties)
    """
    
    def __init__(self, token: str):
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Notion-Version': '2022-06-28',
            'Content-Type': 'application/json'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
        self.logger = setup_logger('NotionClient')
    
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def query_database(
        self, 
        database_id: str, 
        filter: Optional[Dict] = None,
        sorts: Optional[List[Dict]] = None,
        page_size: int = 100
    ) -> List[Dict]:
        """Query all pages from a Notion database with automatic pagination."""
        results = []
        start_cursor = None
        
        while True:
            body = {"page_size": page_size}
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
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return results
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_page(self, page_id: str) -> Dict:
        """Get a single Notion page by ID."""
        response = self.client.get(f'https://api.notion.com/v1/pages/{page_id}')
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def create_page(self, database_id: str, properties: Dict, children: Optional[List] = None) -> Dict:
        """Create a new page in a Notion database."""
        body = {
            "parent": {"database_id": database_id},
            "properties": properties
        }
        if children:
            body["children"] = children

        response = self.client.post(
            'https://api.notion.com/v1/pages',
            json=body
        )

        # Log full error response for debugging
        if not response.is_success:
            try:
                error_data = response.json()
                self.logger.error(f"Notion API error creating page: {error_data}")
            except:
                self.logger.error(f"Notion API error creating page (no JSON): {response.text}")

        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def update_page(self, page_id: str, properties: Dict) -> Dict:
        """Update an existing Notion page."""
        response = self.client.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            json={"properties": properties}
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def archive_page(self, page_id: str) -> Dict:
        """Archive (soft-delete) a Notion page. Safe to call if already archived."""
        # First check if already archived
        try:
            get_resp = self.client.get(f'https://api.notion.com/v1/pages/{page_id}')
            if get_resp.status_code == 200:
                page_data = get_resp.json()
                if page_data.get('archived') or page_data.get('in_trash'):
                    return page_data  # Already archived, no action needed
        except Exception:
            pass  # Continue to try archiving
        
        response = self.client.patch(
            f'https://api.notion.com/v1/pages/{page_id}',
            json={"archived": True}
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_page_content(self, page_id: str, max_blocks: int = 100) -> List[Dict]:
        """Get block content of a page."""
        response = self.client.get(
            f'https://api.notion.com/v1/blocks/{page_id}/children',
            params={'page_size': max_blocks}
        )
        response.raise_for_status()
        return response.json().get('results', [])
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_all_blocks(self, page_id: str) -> List[Dict]:
        """Get ALL blocks from a page with pagination."""
        blocks = []
        start_cursor = None
        
        while True:
            url = f'https://api.notion.com/v1/blocks/{page_id}/children'
            params = {'page_size': 100}
            if start_cursor:
                params['start_cursor'] = start_cursor
            
            response = self.client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            blocks.extend(data.get('results', []))
            
            if not data.get('has_more'):
                break
            start_cursor = data.get('next_cursor')
        
        return blocks
    
    def get_block_children(self, block_id: str) -> List[Dict]:
        """Get children of a specific block (for nested content)."""
        try:
            response = self.client.get(
                f'https://api.notion.com/v1/blocks/{block_id}/children',
                params={'page_size': 100}
            )
            # Handle blocks that don't support children
            if response.status_code in [400, 404]:
                return []
            response.raise_for_status()
            return response.json().get('results', [])
        except Exception:
            return []
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def append_blocks(self, page_id: str, blocks: List[Dict]) -> List[Dict]:
        """Append content blocks to a page."""
        if not blocks:
            return []
        response = self.client.patch(
            f'https://api.notion.com/v1/blocks/{page_id}/children',
            json={"children": blocks}
        )
        response.raise_for_status()
        return response.json().get('results', [])
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def delete_block(self, block_id: str) -> bool:
        """Delete a specific block."""
        response = self.client.delete(f'https://api.notion.com/v1/blocks/{block_id}')
        response.raise_for_status()
        return True
    
    def extract_page_content(self, page_id: str, max_depth: int = 3) -> Tuple[str, bool]:
        """
        Extract readable text content from a page.
        
        Args:
            page_id: The Notion page ID
            max_depth: Maximum nesting depth to traverse
            
        Returns:
            Tuple of (content_text, has_unsupported_blocks)
        """
        blocks = self.get_all_blocks(page_id)
        return self._extract_blocks_text(blocks, max_depth=max_depth)
    
    def _extract_blocks_text(self, blocks: List[Dict], depth: int = 0, max_depth: int = 3) -> Tuple[str, bool]:
        """Recursively extract text from blocks."""
        text_parts = []
        has_unsupported = False
        indent = "  " * depth
        
        for block in blocks:
            block_type = block.get('type')
            block_id = block.get('id')
            has_children = block.get('has_children', False)
            
            # Handle unsupported blocks (AI meeting notes, etc.)
            if block_type == 'unsupported':
                has_unsupported = True
                # Try to get children anyway
                if has_children and depth < max_depth:
                    children = self.get_block_children(block_id)
                    child_text, _ = self._extract_blocks_text(children, depth + 1, max_depth)
                    if child_text:
                        text_parts.append(child_text)
                continue
            
            # Extract text based on block type
            text = self._get_block_text(block)
            if text:
                prefix = self._get_block_prefix(block_type)
                text_parts.append(f"{indent}{prefix}{text}")
            
            # Recursively get children (for toggles, etc.)
            if has_children and depth < max_depth:
                children = self.get_block_children(block_id)
                child_text, child_unsupported = self._extract_blocks_text(children, depth + 1, max_depth)
                if child_text:
                    text_parts.append(child_text)
                if child_unsupported:
                    has_unsupported = True
        
        return '\n'.join(filter(None, text_parts)), has_unsupported
    
    def _get_block_text(self, block: Dict) -> Optional[str]:
        """Extract plain text from a single block."""
        block_type = block.get('type')
        
        text_block_types = [
            'paragraph', 'heading_1', 'heading_2', 'heading_3',
            'bulleted_list_item', 'numbered_list_item', 'to_do',
            'toggle', 'quote', 'callout'
        ]
        
        if block_type in text_block_types:
            rich_text = block.get(block_type, {}).get('rich_text', [])
            return ''.join([t.get('plain_text', '') for t in rich_text])
        
        return None
    
    def _get_block_prefix(self, block_type: str) -> str:
        """Get display prefix for block type."""
        prefixes = {
            'heading_1': '# ',
            'heading_2': '## ',
            'heading_3': '### ',
            'bulleted_list_item': '• ',
            'numbered_list_item': '- ',
            'to_do': '☐ ',
            'quote': '> ',
            'callout': '💡 ',
            'toggle': '▶ ',
        }
        return prefixes.get(block_type, '')

    def extract_page_sections(self, page_id: str) -> List[Dict[str, str]]:
        """
        Extract structured sections from a Notion page.

        A section is defined as:
        - A heading_2 block followed by its content (paragraphs, lists, etc.)
        - Content continues until the next heading_2 or end of page

        Returns:
            List of dicts with 'heading' and 'content' keys
            Example: [{'heading': 'Overview', 'content': 'This is the overview text...'}]
        """
        blocks = self.get_all_blocks(page_id)
        sections = []
        current_heading = None
        current_content = []

        for block in blocks:
            block_type = block.get('type')

            # New section starts with heading_2
            if block_type == 'heading_2':
                # Save previous section if exists
                if current_heading:
                    sections.append({
                        'heading': current_heading,
                        'content': '\n'.join(current_content).strip()
                    })

                # Start new section
                rich_text = block.get('heading_2', {}).get('rich_text', [])
                current_heading = ''.join([t.get('plain_text', '') for t in rich_text])
                current_content = []

            # Accumulate content for current section
            elif current_heading:
                text = self._get_block_text(block)
                if text:
                    prefix = self._get_block_prefix(block_type)
                    current_content.append(f"{prefix}{text}")

        # Save last section
        if current_heading:
            sections.append({
                'heading': current_heading,
                'content': '\n'.join(current_content).strip()
            })

        return sections

    def get_database_schema(self, database_id: str) -> Dict:
        """Get database schema to understand available properties."""
        response = self.client.get(f'https://api.notion.com/v1/databases/{database_id}')
        response.raise_for_status()
        return response.json()


# ============================================================================
# UNIFIED SUPABASE CLIENT
# ============================================================================

class SupabaseClient:
    """
    Unified Supabase REST API client with retry logic.
    
    Usage:
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY, 'meetings')
        records = supabase.select_all()
        record = supabase.get_by_id(record_id)
        supabase.upsert(data, conflict_column='notion_page_id')
        supabase.update(record_id, data)
        supabase.delete(record_id)
    """
    
    def __init__(self, url: str, key: str, table_name: str):
        self.base_url = f"{url}/rest/v1"
        self.table_name = table_name
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)
        self.logger = setup_logger(f'Supabase.{table_name}')
    
    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def select_all(self, columns: str = "*", order_by: str = "created_at.desc") -> List[Dict]:
        """Select all records from the table."""
        response = self.client.get(
            f"{self.base_url}/{self.table_name}",
            params={"select": columns, "order": order_by}
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def select_where(self, column: str, value: Any, columns: str = "*") -> List[Dict]:
        """Select records where column equals value."""
        response = self.client.get(
            f"{self.base_url}/{self.table_name}",
            params={"select": columns, column: f"eq.{value}"}
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def select_updated_since(self, since: datetime, columns: str = "*") -> List[Dict]:
        """Select records updated since a given timestamp."""
        response = self.client.get(
            f"{self.base_url}/{self.table_name}",
            params={
                "select": columns,
                "updated_at": f"gte.{since.isoformat()}"
            }
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_by_id(self, record_id: str) -> Optional[Dict]:
        """Get a single record by ID."""
        response = self.client.get(
            f"{self.base_url}/{self.table_name}",
            params={"select": "*", "id": f"eq.{record_id}"}
        )
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_by_notion_id(self, notion_page_id: str) -> Optional[Dict]:
        """Get a record by its Notion page ID."""
        response = self.client.get(
            f"{self.base_url}/{self.table_name}",
            params={"select": "*", "notion_page_id": f"eq.{notion_page_id}"}
        )
        response.raise_for_status()
        data = response.json()
        return data[0] if data else None
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def upsert(self, data: Dict, conflict_column: str = "notion_page_id") -> Dict:
        """Insert or update a record based on conflict column."""
        # Upsert requires resolution=merge-duplicates to update on conflict
        response = self.client.post(
            f"{self.base_url}/{self.table_name}?on_conflict={conflict_column}",
            json=data,
            headers={"Prefer": "resolution=merge-duplicates,return=representation"}
        )
        response.raise_for_status()
        result = response.json()
        return result[0] if result else {}
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def insert(self, data: Dict) -> Dict:
        """Insert a new record."""
        response = self.client.post(
            f"{self.base_url}/{self.table_name}",
            json=data
        )
        response.raise_for_status()
        result = response.json()
        return result[0] if result else {}
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def update(self, record_id: str, data: Dict) -> Dict:
        """Update a record by ID."""
        response = self.client.patch(
            f"{self.base_url}/{self.table_name}?id=eq.{record_id}",
            json=data
        )
        response.raise_for_status()
        result = response.json()
        return result[0] if result else {}
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def delete(self, record_id: str) -> bool:
        """Delete a record by ID."""
        response = self.client.delete(
            f"{self.base_url}/{self.table_name}?id=eq.{record_id}"
        )
        response.raise_for_status()
        return True
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def soft_delete(self, record_id: str) -> Dict:
        """Soft-delete a record by setting deleted_at."""
        return self.update(record_id, {"deleted_at": datetime.now(timezone.utc).isoformat()})
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_deleted_with_notion_id(self) -> List[Dict]:
        """Get soft-deleted records that still have a notion_page_id (need archiving in Notion)."""
        response = self.client.get(
            f"{self.base_url}/{self.table_name}",
            params={
                "select": "*",
                "deleted_at": "not.is.null",
                "notion_page_id": "not.is.null"
            }
        )
        response.raise_for_status()
        return response.json()
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def clear_notion_page_id(self, record_id: str) -> Dict:
        """Clear notion_page_id after archiving (prevents re-archiving)."""
        return self.update(record_id, {
            "notion_page_id": None,
            "notion_updated_at": None
        })
    
    @retry_on_error(max_retries=3, base_delay=1.0)
    def get_all_active(self) -> List[Dict]:
        """Get all non-deleted records."""
        response = self.client.get(
            f"{self.base_url}/{self.table_name}",
            params={
                "select": "*",
                "deleted_at": "is.null",
                "order": "created_at.desc"
            }
        )
        response.raise_for_status()
        return response.json()


# ============================================================================
# CONTENT BLOCK BUILDERS
# ============================================================================

class ContentBlockBuilder:
    """Helper class to build Notion content blocks from text."""
    
    NOTION_BLOCK_LIMIT = 2000  # Notion's character limit per block
    
    @staticmethod
    def paragraph(text: str) -> Dict:
        """Create a paragraph block."""
        return {
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
    
    @staticmethod
    def heading_1(text: str) -> Dict:
        """Create a heading 1 block."""
        return {
            "object": "block",
            "type": "heading_1",
            "heading_1": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
    
    @staticmethod
    def heading_2(text: str) -> Dict:
        """Create a heading 2 block."""
        return {
            "object": "block",
            "type": "heading_2",
            "heading_2": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
    
    @staticmethod
    def heading_3(text: str) -> Dict:
        """Create a heading 3 block."""
        return {
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
    
    @staticmethod
    def bulleted_list_item(text: str) -> Dict:
        """Create a bulleted list item block."""
        return {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
    
    @staticmethod
    def numbered_list_item(text: str, children: List[Dict] = None) -> Dict:
        """Create a numbered list item block with optional nested children."""
        block = {
            "object": "block",
            "type": "numbered_list_item",
            "numbered_list_item": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
        if children:
            block["numbered_list_item"]["children"] = children
        return block
    
    @staticmethod
    def bulleted_list_item_with_children(text: str, children: List[Dict] = None) -> Dict:
        """Create a bulleted list item block with optional nested children."""
        block = {
            "object": "block",
            "type": "bulleted_list_item",
            "bulleted_list_item": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
        if children:
            block["bulleted_list_item"]["children"] = children
        return block
    
    @staticmethod
    def to_do(text: str, checked: bool = False) -> Dict:
        """Create a to-do block."""
        return {
            "object": "block",
            "type": "to_do",
            "to_do": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}],
                "checked": checked
            }
        }
    
    @staticmethod
    def toggle(text: str, children: List[Dict] = None) -> Dict:
        """Create a toggle block."""
        block = {
            "object": "block",
            "type": "toggle",
            "toggle": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
        if children:
            block["toggle"]["children"] = children
        return block
    
    @staticmethod
    def quote(text: str) -> Dict:
        """Create a quote block."""
        return {
            "object": "block",
            "type": "quote",
            "quote": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}]
            }
        }
    
    @staticmethod
    def callout(text: str, icon: str = "💡") -> Dict:
        """Create a callout block."""
        return {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": text[:2000]}}],
                "icon": {"type": "emoji", "emoji": icon}
            }
        }
    
    @staticmethod
    def divider() -> Dict:
        """Create a divider block."""
        return {"object": "block", "type": "divider", "divider": {}}
    
    @classmethod
    def chunked_paragraphs(cls, text: str, chunk_size: int = None) -> List[Dict]:
        """
        Split long text into multiple paragraph blocks.
        
        Args:
            text: The text to split
            chunk_size: Max characters per block (default: NOTION_BLOCK_LIMIT)
        """
        if not text:
            return []
        
        chunk_size = chunk_size or cls.NOTION_BLOCK_LIMIT
        blocks = []
        
        # Try to split on paragraph boundaries first
        paragraphs = text.split('\n\n')
        current_chunk = ""
        
        for para in paragraphs:
            if len(current_chunk) + len(para) + 2 <= chunk_size:
                if current_chunk:
                    current_chunk += "\n\n"
                current_chunk += para
            else:
                if current_chunk:
                    blocks.append(cls.paragraph(current_chunk))
                # Handle very long paragraphs
                while len(para) > chunk_size:
                    blocks.append(cls.paragraph(para[:chunk_size]))
                    para = para[chunk_size:]
                current_chunk = para
        
        if current_chunk:
            blocks.append(cls.paragraph(current_chunk))
        
        return blocks
    
    @classmethod
    def from_structured_content(cls, content: Dict) -> List[Dict]:
        """
        Build blocks from a structured content dictionary.
        
        Expected format:
        {
            "summary": "Summary text",
            "sections": [
                {"heading": "Section Title", "content": "Section content", "items": ["item1", "item2"]}
            ],
            "action_items": ["task1", "task2"]
        }
        """
        blocks = []
        
        # Summary
        if content.get("summary"):
            blocks.append(cls.heading_2("Summary"))
            blocks.extend(cls.chunked_paragraphs(content["summary"]))
        
        # Sections
        for section in content.get("sections", []):
            if section.get("heading"):
                blocks.append(cls.heading_3(section["heading"]))
            if section.get("content"):
                blocks.extend(cls.chunked_paragraphs(section["content"]))
            for item in section.get("items", []):
                blocks.append(cls.bulleted_list_item(item))
        
        # Action items
        if content.get("action_items"):
            blocks.append(cls.heading_2("Action Items"))
            for item in content["action_items"]:
                blocks.append(cls.to_do(item))
        
        return blocks


# ============================================================================
# SYNC LOGGING SERVICE WITH METRICS
# ============================================================================

class SyncLogger:
    """Unified logging to sync_logs table with metrics support."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY, 'sync_logs')
        self.logger = setup_logger(f'SyncLogger.{service_name}')
    
    def log(self, event_type: str, status: str, message: str, details: Optional[Dict] = None):
        """Log a sync event to the database.
        
        Note: The 'details' column may not exist in all environments.
        We try with details first, then fall back to without.
        """
        log_data = {
            'event_type': f"{self.service_name}_{event_type}",
            'status': status,
            'message': message[:500] if message else '',
            'created_at': datetime.now(timezone.utc).isoformat()
        }
        
        try:
            # Try without details first (safer)
            self.supabase.insert(log_data)
        except Exception as e:
            self.logger.warning(f"Failed to log sync event: {e}")
    
    def log_success(self, event_type: str, message: str, details: Optional[Dict] = None):
        self.log(event_type, 'success', message, details)
    
    def log_error(self, event_type: str, message: str, details: Optional[Dict] = None):
        self.log(event_type, 'error', message, details)
    
    def log_start(self, sync_type: str = "sync"):
        self.log('start', 'info', f"Starting {sync_type}")
    
    def log_complete(self, result: SyncResult):
        status = 'success' if result.success else 'error'
        
        # Build comprehensive metrics summary
        m = result.metrics
        metrics_summary = (
            f"Completed: {result.stats.created}c/{result.stats.updated}u/{result.stats.deleted}d/{result.stats.errors}err "
            f"| {m.total_duration_seconds:.1f}s | "
            f"API: {m.notion_api_calls}N/{m.supabase_api_calls}S | "
            f"Rate limits: {m.rate_limit_events} | Retries: {m.retries}"
        )
        
        if m.staleness_seconds > 0:
            metrics_summary += f" | Staleness: {m.staleness_seconds:.0f}s"
        
        self.log('complete', status, metrics_summary, result.to_dict())
        
        # Also log to console for Cloud Run visibility
        self.logger.info(f"📊 SYNC METRICS: {metrics_summary}")
    
    def log_metrics(self, metrics: 'SyncMetrics', phase: str = "sync"):
        """Log intermediate metrics during sync phases."""
        msg = (
            f"{phase}: read={metrics.records_read} written={metrics.records_written} "
            f"api_calls={metrics.notion_api_calls + metrics.supabase_api_calls}"
        )
        self.logger.info(msg)


# ============================================================================
# NOTION PROPERTY HELPERS
# ============================================================================

class NotionPropertyExtractor:
    """Helper class to extract values from Notion property objects."""
    
    @staticmethod
    def title(props: Dict, prop_name: str = 'Name') -> str:
        """Extract title/name from title property."""
        title_prop = props.get(prop_name, {}).get('title', [])
        return title_prop[0].get('plain_text', '') if title_prop else ''
    
    @staticmethod
    def rich_text(props: Dict, prop_name: str) -> Optional[str]:
        """Extract text from rich_text property."""
        text_prop = props.get(prop_name, {}).get('rich_text', [])
        return text_prop[0].get('plain_text', '') if text_prop else None
    
    @staticmethod
    def number(props: Dict, prop_name: str) -> Optional[float]:
        """Extract number from number property."""
        return props.get(prop_name, {}).get('number')
    
    @staticmethod
    def select(props: Dict, prop_name: str) -> Optional[str]:
        """Extract select value from select property."""
        select = props.get(prop_name, {}).get('select')
        return select.get('name') if select else None
    
    @staticmethod
    def multi_select(props: Dict, prop_name: str) -> List[str]:
        """Extract multi-select values."""
        items = props.get(prop_name, {}).get('multi_select', [])
        return [item.get('name', '') for item in items]
    
    @staticmethod
    def date(props: Dict, prop_name: str) -> Optional[str]:
        """Extract date from date property."""
        date_prop = props.get(prop_name, {}).get('date')
        return date_prop.get('start') if date_prop else None
    
    @staticmethod
    def url(props: Dict, prop_name: str) -> Optional[str]:
        """Extract URL from url property."""
        return props.get(prop_name, {}).get('url')
    
    @staticmethod
    def checkbox(props: Dict, prop_name: str) -> bool:
        """Extract checkbox value."""
        return props.get(prop_name, {}).get('checkbox', False)
    
    @staticmethod
    def email(props: Dict, prop_name: str) -> Optional[str]:
        """Extract email from email property."""
        return props.get(prop_name, {}).get('email')
    
    @staticmethod
    def phone(props: Dict, prop_name: str) -> Optional[str]:
        """Extract phone from phone_number property."""
        return props.get(prop_name, {}).get('phone_number')
    
    @staticmethod
    def relation(props: Dict, prop_name: str) -> List[str]:
        """Extract related page IDs from relation property."""
        relations = props.get(prop_name, {}).get('relation', [])
        return [r.get('id', '') for r in relations if r.get('id')]


class NotionPropertyBuilder:
    """Helper class to build Notion property objects for creating/updating pages."""
    
    @staticmethod
    def title(value: str) -> Dict:
        return {"title": [{"text": {"content": value or ""}}]}
    
    @staticmethod
    def rich_text(value: Optional[str]) -> Dict:
        if not value:
            return {"rich_text": []}
        # Notion limit is 2000 chars, but Unicode chars (emojis) may count differently
        # Use 1990 to be safe with multi-byte characters
        return {"rich_text": [{"text": {"content": value[:1990]}}]}
    
    @staticmethod
    def number(value: Optional[float]) -> Dict:
        return {"number": value}
    
    @staticmethod
    def select(value: Optional[str]) -> Dict:
        if not value:
            return {"select": None}
        return {"select": {"name": value}}
    
    @staticmethod
    def multi_select(values: List[str]) -> Dict:
        return {"multi_select": [{"name": v} for v in values if v]}
    
    @staticmethod
    def date(value: Optional[str]) -> Dict:
        if not value:
            return {"date": None}
        return {"date": {"start": value}}
    
    @staticmethod
    def url(value: Optional[str]) -> Dict:
        return {"url": value}
    
    @staticmethod
    def checkbox(value: bool) -> Dict:
        return {"checkbox": value}
    
    @staticmethod
    def email(value: Optional[str]) -> Dict:
        return {"email": value}
    
    @staticmethod
    def phone(value: Optional[str]) -> Dict:
        return {"phone_number": value}
    
    @staticmethod
    def relation(page_ids: List[str]) -> Dict:
        return {"relation": [{"id": pid} for pid in page_ids if pid]}


# ============================================================================
# ABSTRACT BASE SYNC SERVICE
# ============================================================================

T = TypeVar('T')


class BaseSyncService(ABC, Generic[T]):
    """
    Abstract base class for all sync services.
    
    Subclasses must implement:
    - convert_from_source(source_record) -> Dict
    - convert_to_source(destination_record) -> Dict (for two-way sync)
    - get_source_id(source_record) -> str
    - get_destination_id(dest_record) -> str
    """
    
    def __init__(
        self,
        service_name: str,
        direction: SyncDirection = SyncDirection.TWO_WAY
    ):
        self.service_name = service_name
        self.direction = direction
        self.logger = setup_logger(service_name)
        self.sync_logger = SyncLogger(service_name)
    
    @abstractmethod
    def convert_from_source(self, source_record: Dict) -> Dict:
        """Convert source record to destination format."""
        pass
    
    @abstractmethod
    def get_source_id(self, source_record: Dict) -> str:
        """Get unique identifier from source record."""
        pass
    
    def convert_to_source(self, dest_record: Dict) -> Dict:
        """Convert destination record to source format (for two-way sync)."""
        raise NotImplementedError("Two-way sync requires convert_to_source implementation")
    
    def get_destination_id(self, dest_record: Dict) -> str:
        """Get unique identifier from destination record."""
        return dest_record.get('id', '')
    
    def check_safety_valve(
        self, 
        source_count: int, 
        dest_count: int, 
        direction: str
    ) -> Tuple[bool, str]:
        """
        Check if sync should proceed based on record counts.
        Returns (is_safe, message).
        """
        if dest_count <= SAFETY_VALVE_MIN_RECORDS:
            return True, "Safety valve bypassed (destination below threshold)"
        
        if source_count < dest_count * SAFETY_VALVE_THRESHOLD:
            msg = f"Safety Valve Triggered: Source has {source_count}, destination has {dest_count}. Aborting {direction}."
            return False, msg
        
        return True, ""
    
    def compare_timestamps(
        self,
        source_updated: Optional[str],
        dest_updated: Optional[str],
        buffer_seconds: int = 5
    ) -> int:
        """
        Compare timestamps to determine which record is newer.
        Returns: 1 if source is newer, -1 if dest is newer, 0 if equal/unknown
        """
        if not source_updated or not dest_updated:
            return 0

        try:
            # Parse timestamps
            def parse_ts(ts: str) -> datetime:
                if ts.endswith('Z'):
                    ts = ts[:-1] + '+00:00'
                return datetime.fromisoformat(ts)

            source_dt = parse_ts(source_updated)
            dest_dt = parse_ts(dest_updated)

            diff = (source_dt - dest_dt).total_seconds()

            if abs(diff) <= buffer_seconds:
                return 0
            return 1 if diff > 0 else -1
            
        except Exception:
            return 0


# ============================================================================
# ONE-WAY SYNC SERVICE (Notion → Supabase)
# ============================================================================

class OneWaySyncService(BaseSyncService):
    """
    One-way sync from Notion to Supabase.
    
    Use for: Books, Highlights, LinkedIn posts, etc.
    """
    
    def __init__(
        self,
        service_name: str,
        notion_database_id: str,
        supabase_table: str
    ):
        super().__init__(service_name, SyncDirection.ONE_WAY)
        
        self.notion = NotionClient(NOTION_API_TOKEN)
        self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY, supabase_table)
        self.notion_database_id = notion_database_id
    
    def get_source_id(self, source_record: Dict) -> str:
        return source_record.get('id', '')
    
    def sync(self, full_sync: bool = False, since_hours: int = 24) -> SyncResult:
        """
        Sync from Notion to Supabase.
        
        Args:
            full_sync: If True, sync all records. If False, only recently updated.
            since_hours: For incremental sync, how far back to look.
        """
        start_time = time.time()
        stats = SyncStats()
        
        self.logger.info(f"Starting {self.service_name} sync (full={full_sync})")
        self.sync_logger.log_start()
        
        try:
            # Build filter for incremental sync
            filter_query = None
            if not full_sync:
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
                filter_query = {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"after": cutoff}
                }
            
            # Fetch from Notion
            notion_records = self.notion.query_database(self.notion_database_id, filter=filter_query)
            self.logger.info(f"Found {len(notion_records)} records in Notion")
            
            # Get existing Supabase records for comparison
            existing = {r['notion_page_id']: r for r in self.supabase.select_all() if r.get('notion_page_id')}
            
            # Safety valve
            is_safe, msg = self.check_safety_valve(len(notion_records), len(existing), "Notion → Supabase")
            if not is_safe and full_sync:
                self.logger.error(msg)
                self.sync_logger.log_error('safety_valve', msg)
                return SyncResult(
                    success=False,
                    direction="notion_to_supabase",
                    error_message=msg,
                    elapsed_seconds=time.time() - start_time
                )
            
            # Process each Notion record
            for notion_record in notion_records:
                try:
                    notion_id = self.get_source_id(notion_record)
                    
                    # Skip if Supabase has local changes that need to sync TO Notion
                    existing_record = existing.get(notion_id)
                    if existing_record and existing_record.get('last_sync_source') == 'supabase':
                        self.logger.info(f"Skipping '{existing_record.get('title', 'Untitled')}' - has local Supabase changes pending sync to Notion")
                        stats.skipped += 1
                        continue
                    
                    data = self.convert_from_source(notion_record)
                    data['notion_page_id'] = notion_id
                    data['notion_updated_at'] = notion_record.get('last_edited_time')
                    data['last_sync_source'] = 'notion'
                    data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    
                    if notion_id in existing:
                        stats.updated += 1
                    else:
                        stats.created += 1
                    
                    self.supabase.upsert(data, conflict_column='notion_page_id')
                    
                except Exception as e:
                    self.logger.error(f"Error processing record {notion_record.get('id')}: {e}")
                    stats.errors += 1
            
            elapsed = time.time() - start_time
            result = SyncResult(
                success=stats.errors == 0,
                direction="notion_to_supabase",
                stats=stats,
                source_count=len(notion_records),
                destination_count=len(existing) + stats.created,
                elapsed_seconds=elapsed
            )
            
            self.logger.info(f"Sync complete: {stats.created} created, {stats.updated} updated, {stats.skipped} skipped, {stats.errors} errors in {elapsed:.1f}s")
            self.sync_logger.log_complete(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Sync failed: {e}")
            self.sync_logger.log_error('sync_failed', str(e))
            return SyncResult(
                success=False,
                direction="notion_to_supabase",
                error_message=str(e),
                elapsed_seconds=time.time() - start_time
            )


# ============================================================================
# TWO-WAY SYNC SERVICE (Notion ↔ Supabase)
# ============================================================================

class TwoWaySyncService(BaseSyncService):
    """
    Bidirectional sync between Notion and Supabase.

    Use for: Meetings, Tasks, Reflections, Journals, etc.
    """

    def filter_records_needing_notion_sync(self, records: List[Dict], name_field: str = 'title') -> List[Dict]:
        """
        Filter Supabase records that need syncing to Notion.

        A record needs syncing if:
        1. It has no notion_page_id (new record), OR
        2. last_sync_source is 'supabase' (explicitly marked for sync), OR
        3. updated_at > notion_updated_at (local changes since last sync)

        Args:
            records: List of Supabase records
            name_field: Field name to use for logging (default: 'title')

        Returns:
            List of records that need syncing to Notion
        """
        result = []
        for r in records:
            # Skip soft-deleted records
            if r.get('deleted_at'):
                continue

            if not r.get('notion_page_id'):
                # New record - needs to be created in Notion
                result.append(r)
            elif r.get('last_sync_source') == 'supabase':
                # Explicitly marked for sync
                result.append(r)
            else:
                # Check if updated_at > notion_updated_at (local changes)
                comparison = self.compare_timestamps(
                    r.get('updated_at'),
                    r.get('notion_updated_at')
                )
                if comparison > 0:
                    name = r.get(name_field, r.get('name', 'Unknown'))
                    self.logger.debug(f"Record '{name}' has local changes")
                    result.append(r)
        return result
    
    def __init__(
        self,
        service_name: str,
        notion_database_id: str,
        supabase_table: str
    ):
        super().__init__(service_name, SyncDirection.TWO_WAY)
        
        self.notion = NotionClient(NOTION_API_TOKEN)
        self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY, supabase_table)
        self.notion_database_id = notion_database_id
    
    def get_source_id(self, source_record: Dict) -> str:
        return source_record.get('id', '')
    
    def _sync_notion_deletions(self) -> int:
        """
        Detect and sync deletions: Soft-delete Supabase records whose Notion pages were deleted.
        
        Returns: Number of records soft-deleted in Supabase
        """
        deleted_count = 0
        
        # Get all Supabase records that have a notion_page_id and are not already deleted
        all_records = self.supabase.select_all()
        linked_records = [r for r in all_records if r.get('notion_page_id') and not r.get('deleted_at')]
        
        if not linked_records:
            self.logger.info("No linked records to check for Notion deletions")
            return 0
        
        self.logger.info(f"Checking {len(linked_records)} linked records for Notion deletions...")
        
        # Get all current Notion page IDs
        try:
            all_notion_pages = self.notion.query_database(self.notion_database_id)
            notion_page_ids = {p['id'] for p in all_notion_pages}
            self.logger.info(f"Found {len(notion_page_ids)} pages in Notion database")
        except Exception as e:
            self.logger.error(f"Failed to query Notion database: {e}")
            return 0
        
        # Find orphaned records (Supabase has notion_page_id but page no longer exists in Notion)
        for record in linked_records:
            notion_page_id = record.get('notion_page_id')
            
            if notion_page_id not in notion_page_ids:
                # Page was deleted in Notion - soft-delete in Supabase
                record_id = record.get('id')
                record_name = record.get('title') or record.get('name') or record.get('first_name', '') + ' ' + record.get('last_name', '')
                
                try:
                    self.supabase.soft_delete(record_id)
                    self.supabase.update(record_id, {
                        'notion_page_id': None,
                        'notion_updated_at': None
                    })
                    deleted_count += 1
                    self.logger.info(f"Soft-deleted '{record_name}' (Notion page was deleted)")
                except Exception as e:
                    self.logger.error(f"Failed to soft-delete record {record_id}: {e}")
        
        if deleted_count > 0:
            self.logger.info(f"Soft-deleted {deleted_count} records (Notion pages were deleted)")
        else:
            self.logger.info("No Notion deletions detected")
        
        return deleted_count
    
    def _sync_supabase_deletions(self) -> int:
        """
        Sync deletions from Supabase to Notion: Archive Notion pages for soft-deleted Supabase records.
        
        Returns: Number of Notion pages archived
        """
        archived = 0
        
        # Get deleted records that still have notion_page_id
        deleted_records = self.supabase.get_deleted_with_notion_id()
        
        if not deleted_records:
            self.logger.info("No Supabase deletions need Notion archiving")
            return 0
        
        self.logger.info(f"Found {len(deleted_records)} deleted records to archive in Notion")
        
        for record in deleted_records:
            record_id = record.get('id')
            notion_page_id = record.get('notion_page_id')
            record_name = record.get('title') or record.get('name') or record.get('first_name', '') + ' ' + record.get('last_name', '')
            
            try:
                # Archive the Notion page
                self.notion.archive_page(notion_page_id)
                self.logger.info(f"Archived Notion page for deleted record: {record_name}")
                
                # Clear the notion_page_id so we don't try again
                self.supabase.clear_notion_page_id(record_id)
                
                archived += 1
                
            except Exception as e:
                # Page might already be archived, in trash, or not exist
                # 400 = already archived, 404 = not found
                error_str = str(e).lower()
                if "404" in error_str or "400" in error_str or "archived" in error_str or "trash" in error_str:
                    self.logger.info(f"Notion page already archived/deleted: {record_name}")
                    self.supabase.clear_notion_page_id(record_id)
                    archived += 1  # Count as archived since end state is same
                else:
                    self.logger.error(f"Error archiving Notion page for {record_name}: {e}")
        
        if archived > 0:
            self.logger.info(f"Archived {archived} Notion pages for deleted records")
        
        return archived

    def sync(self, full_sync: bool = False, since_hours: int = 24) -> SyncResult:
        """
        Bidirectional sync between Notion and Supabase with comprehensive metrics.
        """
        metrics = SyncMetrics()
        start_time = time.time()
        
        # Step 0a: Sync Notion deletions → Supabase (soft-delete)
        self.logger.info("Phase 0a: Sync Notion Deletions → Supabase")
        phase_start = time.time()
        notion_deletions = self._sync_notion_deletions()
        metrics.notion_deletions_duration = time.time() - phase_start
        
        # Step 0b: Sync Supabase deletions → Notion (archive)
        self.logger.info("Phase 0b: Sync Supabase Deletions → Notion")
        phase_start = time.time()
        supabase_deletions = self._sync_supabase_deletions()
        metrics.supabase_deletions_duration = time.time() - phase_start
        
        # Step 1: Notion → Supabase
        self.logger.info("Phase 1: Notion → Supabase")
        phase_start = time.time()
        result1 = self._sync_notion_to_supabase(full_sync, since_hours, metrics)
        metrics.notion_to_supabase_duration = time.time() - phase_start
        
        # Step 2: Supabase → Notion  
        self.logger.info("Phase 2: Supabase → Notion")
        phase_start = time.time()
        result2 = self._sync_supabase_to_notion(full_sync, since_hours, metrics)
        metrics.supabase_to_notion_duration = time.time() - phase_start
        
        # Combine results
        combined_stats = SyncStats(
            created=result1.stats.created + result2.stats.created,
            updated=result1.stats.updated + result2.stats.updated,
            deleted=result1.stats.deleted + result2.stats.deleted + notion_deletions + supabase_deletions,
            skipped=result1.stats.skipped + result2.stats.skipped,
            errors=result1.stats.errors + result2.stats.errors
        )
        
        # Finalize metrics
        metrics.finish()
        metrics.records_written = combined_stats.created + combined_stats.updated + combined_stats.deleted
        
        elapsed = time.time() - start_time
        result = SyncResult(
            success=result1.success and result2.success,
            direction="bidirectional",
            stats=combined_stats,
            metrics=metrics,
            elapsed_seconds=elapsed
        )
        
        self.sync_logger.log_complete(result)
        return result

    def _sync_notion_to_supabase(self, full_sync: bool, since_hours: int, metrics: Optional[SyncMetrics] = None) -> SyncResult:
        """Sync from Notion to Supabase with metrics tracking."""
        stats = SyncStats()
        start_time = time.time()
        
        try:
            # Build filter
            filter_query = None
            if not full_sync:
                cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
                filter_query = {
                    "timestamp": "last_edited_time",
                    "last_edited_time": {"after": cutoff}
                }
            
            # Fetch from Notion
            notion_records = self.notion.query_database(self.notion_database_id, filter=filter_query)
            self.logger.info(f"Found {len(notion_records)} records in Notion")
            
            if metrics:
                metrics.notion_api_calls += 1
                metrics.source_total = len(notion_records)
                metrics.records_read += len(notion_records)
                # Track staleness - find newest change in Notion
                if notion_records:
                    newest = max(r.get('last_edited_time', '') for r in notion_records)
                    try:
                        metrics.newest_source_change = datetime.fromisoformat(newest.replace('Z', '+00:00'))
                    except (ValueError, AttributeError):
                        pass  # Invalid date format or None
            
            # Get existing for comparison
            existing = {r['notion_page_id']: r for r in self.supabase.select_all() if r.get('notion_page_id')}
            if metrics:
                metrics.supabase_api_calls += 1
                metrics.destination_total = len(existing)
            
            # Safety valve
            is_safe, msg = self.check_safety_valve(len(notion_records), len(existing), "Notion → Supabase")
            if not is_safe and full_sync:
                self.logger.error(msg)
                return SyncResult(success=False, direction="notion_to_supabase", error_message=msg)
            
            # Process records
            for notion_record in notion_records:
                try:
                    notion_id = self.get_source_id(notion_record)
                    existing_record = existing.get(notion_id)

                    # Compare timestamps if record exists
                    if existing_record:
                        # Skip if Supabase has local changes pending sync TO Notion
                        # This prevents overwriting local edits before they sync out
                        if existing_record.get('last_sync_source') == 'supabase':
                            self.logger.debug(f"Skipping {notion_id} - has local Supabase changes pending sync to Notion")
                            stats.skipped += 1
                            continue

                        comparison = self.compare_timestamps(
                            notion_record.get('last_edited_time'),
                            existing_record.get('notion_updated_at')
                        )
                        if comparison <= 0:
                            stats.skipped += 1
                            continue
                        stats.updated += 1
                        if metrics:
                            metrics.conflict_resolutions += 1
                    else:
                        stats.created += 1
                    
                    # Convert and save
                    data = self.convert_from_source(notion_record)
                    data['notion_page_id'] = notion_id
                    data['notion_updated_at'] = notion_record.get('last_edited_time')
                    data['last_sync_source'] = 'notion'
                    data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    
                    self.supabase.upsert(data, conflict_column='notion_page_id')
                    if metrics:
                        metrics.supabase_api_calls += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing from Notion: {format_exception(e)}")
                    stats.errors += 1

            return SyncResult(
                success=True,
                direction="notion_to_supabase",
                stats=stats,
                elapsed_seconds=time.time() - start_time
            )

        except Exception as e:
            return SyncResult(success=False, direction="notion_to_supabase", error_message=format_exception(e))
    
    def _sync_supabase_to_notion(self, full_sync: bool, since_hours: int, metrics: Optional[SyncMetrics] = None) -> SyncResult:
        """Sync from Supabase to Notion with metrics tracking."""
        stats = SyncStats()
        start_time = time.time()
        
        try:
            # Get Supabase records that need syncing
            if full_sync:
                supabase_records = self.supabase.select_all()
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                supabase_records = self.supabase.select_updated_since(cutoff)
            
            if metrics:
                metrics.supabase_api_calls += 1
            
            # Filter to records that need syncing to Notion
            # FIXED: Don't rely on last_sync_source=='supabase' - it's never set when editing directly in Supabase!
            # Instead, compare updated_at vs notion_updated_at to detect local changes
            records_to_sync = []
            for r in supabase_records:
                # Skip soft-deleted records
                if r.get('deleted_at'):
                    continue
                
                # New records without notion_page_id always need syncing
                if not r.get('notion_page_id'):
                    records_to_sync.append(r)
                    continue
                
                # For existing records, check if Supabase is newer than last Notion sync
                updated_at = r.get('updated_at')
                notion_updated_at = r.get('notion_updated_at')
                
                if not notion_updated_at:
                    # No notion timestamp means it was created but never synced back
                    records_to_sync.append(r)
                    continue
                
                # Parse timestamps and compare
                try:
                    from dateutil import parser as date_parser
                    local_time = date_parser.isoparse(updated_at) if isinstance(updated_at, str) else updated_at
                    notion_time = date_parser.isoparse(notion_updated_at) if isinstance(notion_updated_at, str) else notion_updated_at
                    
                    # If Supabase was updated AFTER the last Notion sync, it needs syncing
                    # Add 5 second buffer to account for sync timing
                    if local_time > notion_time + timedelta(seconds=5):
                        records_to_sync.append(r)
                except Exception as e:
                    self.logger.warning(f"Could not compare timestamps for record {r.get('id')}: {e}")
                    # When in doubt, sync it
                    records_to_sync.append(r)
            
            self.logger.info(f"Found {len(records_to_sync)} records to sync to Notion")
            
            # Safety valve
            notion_records = self.notion.query_database(self.notion_database_id)
            if metrics:
                metrics.notion_api_calls += 1
            
            is_safe, msg = self.check_safety_valve(len(records_to_sync), len(notion_records), "Supabase → Notion")
            # For Supabase→Notion we don't abort, just warn
            if not is_safe:
                self.logger.warning(msg)
            
            for record in records_to_sync:
                try:
                    notion_page_id = record.get('notion_page_id')
                    notion_props = self.convert_to_source(record)
                    
                    if notion_page_id:
                        # Update existing
                        updated_page = self.notion.update_page(notion_page_id, notion_props)
                        if metrics:
                            metrics.notion_api_calls += 1
                        
                        # Update Supabase with new Notion timestamp to prevent re-sync loops
                        # This is CRITICAL: without this, `last_sync_source` stays 'supabase'
                        # and future Notion edits would be skipped!
                        self.supabase.update(record['id'], {
                            'notion_updated_at': updated_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        if metrics:
                            metrics.supabase_api_calls += 1
                        
                        stats.updated += 1
                    else:
                        # Create new
                        new_page = self.notion.create_page(self.notion_database_id, notion_props)
                        if metrics:
                            metrics.notion_api_calls += 1
                        # Update Supabase with new Notion ID
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page['id'],
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        if metrics:
                            metrics.supabase_api_calls += 1
                        stats.created += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing to Notion: {format_exception(e)}")
                    stats.errors += 1

            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=time.time() - start_time
            )
            
        except Exception as e:
            return SyncResult(success=False, direction="supabase_to_notion", error_message=format_exception(e))


# ============================================================================
# CLI INTERFACE
# ============================================================================

def create_cli_parser(service_name: str) -> argparse.ArgumentParser:
    """Create a standardized CLI parser for sync services."""
    parser = argparse.ArgumentParser(description=f'{service_name} Sync Service')
    parser.add_argument('--full', action='store_true', help='Full sync (default: incremental)')
    parser.add_argument('--hours', type=int, default=24, help='Hours to look back for incremental sync')
    parser.add_argument('--schema', action='store_true', help='Show database schema')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without applying')
    return parser


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    # Configuration
    'NOTION_API_TOKEN',
    'SUPABASE_URL', 
    'SUPABASE_KEY',
    'SAFETY_VALVE_THRESHOLD',
    
    # Utilities
    'setup_logger',
    'retry_on_error',
    'retry_on_error_async',
    
    # Data classes
    'SyncDirection',
    'SyncStats',
    'SyncMetrics',
    'SyncResult',
    
    # Clients
    'NotionClient',
    'SupabaseClient',
    'SyncLogger',
    
    # Property helpers
    'NotionPropertyExtractor',
    'NotionPropertyBuilder',
    'ContentBlockBuilder',
    
    # Sync services
    'BaseSyncService',
    'OneWaySyncService',
    'TwoWaySyncService',
    
    # CLI
    'create_cli_parser',
]
