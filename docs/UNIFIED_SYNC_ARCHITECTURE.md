# Unified Sync Architecture

## Overview

This document describes the new unified sync architecture that standardizes all sync services.

## Architecture Patterns

### 1. ONE-WAY SYNC (Notion → Supabase)
- **Use for**: Books, Highlights, LinkedIn posts
- **Flow**: `Notion → Supabase`
- **Class**: `OneWaySyncService`
- **Supabase is read-only**: Data originates in Notion

### 2. TWO-WAY SYNC (Notion ↔ Supabase)
- **Use for**: Meetings, Tasks, Reflections, Journals
- **Flow**: `Notion ↔ Supabase`
- **Class**: `TwoWaySyncService`
- **Conflict resolution**: Timestamp-based, newer wins

### 3. MULTI-SOURCE SYNC (Google + Notion ↔ Supabase)
- **Use for**: Contacts, Calendar Events
- **Flow**: `Google ↔ Supabase ↔ Notion`
- **Class**: `ContactsSyncService` (custom)
- **Priority**: 
  - Google = source of truth for contact details
  - Notion = source of truth for CRM data

## Base Classes

```
lib/sync_base.py
├── NotionClient           # Unified Notion API client
├── SupabaseClient         # Unified Supabase REST client
├── SyncLogger             # Logs to sync_logs table
├── NotionPropertyExtractor # Helper for reading Notion props
├── NotionPropertyBuilder   # Helper for building Notion props
├── BaseSyncService        # Abstract base class
├── OneWaySyncService      # One-way sync implementation
└── TwoWaySyncService      # Bidirectional sync implementation
```

## Migration Guide

### Before (Old Pattern)
```python
# Each file had its own Notion client implementation (~300 lines)
class NotionClient:
    def __init__(self):
        self.token = NOTION_API_TOKEN
        # ... lots of duplicate code
    
    def query_database(self, ...):
        # ... duplicate implementation

# Manual error handling
try:
    response = requests.post(url, ...)
    if response.status_code != 200:
        # ... error handling
except Exception as e:
    # ... more error handling

# No standardized logging
print(f"Created {count} records")
```

### After (New Pattern)
```python
from lib.sync_base import (
    OneWaySyncService,
    NotionPropertyExtractor as Extract,
    SyncResult
)

class BooksSyncService(OneWaySyncService):
    def __init__(self):
        super().__init__(
            service_name='books_sync',
            notion_database_id=BOOKS_DB_ID,
            supabase_table='books'
        )
    
    def convert_from_source(self, notion_record: Dict) -> Dict:
        props = notion_record.get('properties', {})
        return {
            'title': Extract.title(props, 'Name'),
            'author': Extract.rich_text(props, 'Author'),
            # ... mapping
        }
```

## Service Migration Status

| Service | Status | New File | Pattern |
|---------|--------|----------|---------|
| books | ✅ Created | sync_books_unified.py | ONE-WAY |
| contacts | ✅ Created | sync_contacts_unified.py | MULTI-SOURCE |
| meetings | 🔄 TODO | sync_meetings_unified.py | TWO-WAY |
| tasks | 🔄 TODO | sync_tasks_unified.py | TWO-WAY |
| reflections | 🔄 TODO | sync_reflections_unified.py | TWO-WAY |
| journals | 🔄 TODO | sync_journals_unified.py | TWO-WAY |
| calendar | 🔄 TODO | sync_calendar_unified.py | MULTI-SOURCE |
| gmail | 🔄 TODO | sync_gmail_unified.py | ONE-WAY |

## Key Features

### 1. Automatic Retry
```python
@retry_on_error(max_retries=3, base_delay=1.0)
def query_database(self, ...):
    # Automatically retries on failure
```

### 2. Safety Valves
```python
# Prevents accidental data loss
# Aborts if source has <10% of destination records
is_safe, msg = self.check_safety_valve(source_count, dest_count, "direction")
```

### 3. Unified Logging
```python
# All syncs log to sync_logs table
self.sync_logger.log_success('create', f"Created {count} records")
self.sync_logger.log_error('sync_failed', str(error))
```

### 4. Timestamp Comparison
```python
# Smart conflict resolution with 5-second buffer
comparison = self.compare_timestamps(source_updated, dest_updated)
# Returns: 1 (source newer), -1 (dest newer), 0 (equal/unknown)
```

### 5. Property Helpers
```python
# Reading
Extract.title(props, 'Name')
Extract.rich_text(props, 'Summary')
Extract.multi_select(props, 'Tags')

# Building
Build.title("Meeting Title")
Build.rich_text("Summary text")
Build.multi_select(["tag1", "tag2"])
```

## CLI Interface

All sync services have a standardized CLI:

```bash
# Incremental sync (last 24 hours)
python sync_books_unified.py

# Full sync
python sync_books_unified.py --full

# Custom time range
python sync_books_unified.py --hours 48

# Show database schema
python sync_books_unified.py --schema

# Dry run (preview only)
python sync_books_unified.py --dry-run
```

## Database Requirements

### sync_logs table
```sql
CREATE TABLE sync_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,  -- 'success' | 'error' | 'info'
    message TEXT,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Required columns in synced tables
```sql
-- All synced tables should have:
notion_page_id TEXT UNIQUE,      -- Link to Notion
notion_updated_at TIMESTAMPTZ,   -- Notion's last_edited_time
last_sync_source TEXT,           -- 'notion' | 'supabase' | 'google'
created_at TIMESTAMPTZ,
updated_at TIMESTAMPTZ
```

## Environment Variables

```env
# Required
NOTION_API_TOKEN=secret_xxx
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=eyJxxx

# Optional - for Google sync
GOOGLE_TOKEN_JSON={"access_token": "...", "refresh_token": "...", ...}

# Optional - for Telegram notifications
TELEGRAM_BOT_TOKEN=xxx
TELEGRAM_CHAT_ID=xxx
```

## Next Steps

1. ✅ Create base classes (`lib/sync_base.py`)
2. ✅ Create books sync template (`sync_books_unified.py`)
3. ✅ Create contacts sync template (`sync_contacts_unified.py`)
4. 🔄 Migrate meetings, tasks, reflections, journals
5. 🔄 Update main.py to use unified services
6. 🔄 Deprecate old sync files
7. 🔄 Add LinkedIn sync (one-way)
