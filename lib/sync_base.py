"""
===================================================================================
UNIFIED SYNC ARCHITECTURE - Base Classes and Templates
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
- Standardized entry points and CLI interface
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
class SyncResult:
    """Result of a complete sync operation."""
    success: bool
    direction: str
    stats: SyncStats = field(default_factory=SyncStats)
    source_count: int = 0
    destination_count: int = 0
    elapsed_seconds: float = 0.0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'success': self.success,
            'direction': self.direction,
            'stats': self.stats.to_dict(),
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
        """Archive (soft-delete) a Notion page."""
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
        response = self.client.post(
            f"{self.base_url}/{self.table_name}?on_conflict={conflict_column}",
            json=data
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


# ============================================================================
# SYNC LOGGING SERVICE
# ============================================================================

class SyncLogger:
    """Unified logging to sync_logs table."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY, 'sync_logs')
        self.logger = setup_logger(f'SyncLogger.{service_name}')
    
    def log(self, event_type: str, status: str, message: str, details: Optional[Dict] = None):
        """Log a sync event to the database."""
        try:
            self.supabase.insert({
                'event_type': f"{self.service_name}_{event_type}",
                'status': status,
                'message': message[:500] if message else '',
                'details': details or {},
                'created_at': datetime.now(timezone.utc).isoformat()
            })
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
        self.log('complete', status, 
                 f"Completed: {result.stats.created} created, {result.stats.updated} updated, {result.stats.errors} errors",
                 result.to_dict())


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
        return {"rich_text": [{"text": {"content": value[:2000]}}]}  # Notion limit
    
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
            
            self.logger.info(f"Sync complete: {stats.created} created, {stats.updated} updated, {stats.errors} errors in {elapsed:.1f}s")
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
    
    def sync(self, full_sync: bool = False, since_hours: int = 24) -> SyncResult:
        """
        Bidirectional sync between Notion and Supabase.
        """
        start_time = time.time()
        
        # Step 1: Notion → Supabase
        self.logger.info("Phase 1: Notion → Supabase")
        result1 = self._sync_notion_to_supabase(full_sync, since_hours)
        
        # Step 2: Supabase → Notion  
        self.logger.info("Phase 2: Supabase → Notion")
        result2 = self._sync_supabase_to_notion(full_sync, since_hours)
        
        # Combine results
        combined_stats = SyncStats(
            created=result1.stats.created + result2.stats.created,
            updated=result1.stats.updated + result2.stats.updated,
            deleted=result1.stats.deleted + result2.stats.deleted,
            skipped=result1.stats.skipped + result2.stats.skipped,
            errors=result1.stats.errors + result2.stats.errors
        )
        
        elapsed = time.time() - start_time
        return SyncResult(
            success=result1.success and result2.success,
            direction="bidirectional",
            stats=combined_stats,
            elapsed_seconds=elapsed
        )
    
    def _sync_notion_to_supabase(self, full_sync: bool, since_hours: int) -> SyncResult:
        """Sync from Notion to Supabase."""
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
            
            # Get existing for comparison
            existing = {r['notion_page_id']: r for r in self.supabase.select_all() if r.get('notion_page_id')}
            
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
                        comparison = self.compare_timestamps(
                            notion_record.get('last_edited_time'),
                            existing_record.get('notion_updated_at')
                        )
                        if comparison <= 0:
                            stats.skipped += 1
                            continue
                        stats.updated += 1
                    else:
                        stats.created += 1
                    
                    # Convert and save
                    data = self.convert_from_source(notion_record)
                    data['notion_page_id'] = notion_id
                    data['notion_updated_at'] = notion_record.get('last_edited_time')
                    data['last_sync_source'] = 'notion'
                    data['updated_at'] = datetime.now(timezone.utc).isoformat()
                    
                    self.supabase.upsert(data, conflict_column='notion_page_id')
                    
                except Exception as e:
                    self.logger.error(f"Error syncing from Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="notion_to_supabase",
                stats=stats,
                elapsed_seconds=time.time() - start_time
            )
            
        except Exception as e:
            return SyncResult(success=False, direction="notion_to_supabase", error_message=str(e))
    
    def _sync_supabase_to_notion(self, full_sync: bool, since_hours: int) -> SyncResult:
        """Sync from Supabase to Notion."""
        stats = SyncStats()
        start_time = time.time()
        
        try:
            # Get Supabase records that need syncing
            if full_sync:
                supabase_records = self.supabase.select_all()
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                supabase_records = self.supabase.select_updated_since(cutoff)
            
            # Filter to records without notion_page_id (new) or updated locally
            records_to_sync = [
                r for r in supabase_records 
                if not r.get('notion_page_id') or r.get('last_sync_source') == 'supabase'
            ]
            
            self.logger.info(f"Found {len(records_to_sync)} records to sync to Notion")
            
            # Safety valve
            notion_records = self.notion.query_database(self.notion_database_id)
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
                        self.notion.update_page(notion_page_id, notion_props)
                        stats.updated += 1
                    else:
                        # Create new
                        new_page = self.notion.create_page(self.notion_database_id, notion_props)
                        # Update Supabase with new Notion ID
                        self.supabase.update(record['id'], {
                            'notion_page_id': new_page['id'],
                            'notion_updated_at': new_page.get('last_edited_time'),
                            'last_sync_source': 'notion'
                        })
                        stats.created += 1
                    
                except Exception as e:
                    self.logger.error(f"Error syncing to Notion: {e}")
                    stats.errors += 1
            
            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=time.time() - start_time
            )
            
        except Exception as e:
            return SyncResult(success=False, direction="supabase_to_notion", error_message=str(e))


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
    'SyncResult',
    
    # Clients
    'NotionClient',
    'SupabaseClient',
    'SyncLogger',
    
    # Property helpers
    'NotionPropertyExtractor',
    'NotionPropertyBuilder',
    
    # Sync services
    'BaseSyncService',
    'OneWaySyncService',
    'TwoWaySyncService',
    
    # CLI
    'create_cli_parser',
]
