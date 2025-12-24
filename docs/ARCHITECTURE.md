# 📐 Sync Service Architecture

## Overview

The Jarvis Sync Service maintains data consistency across three platforms:
- **Notion** (Rich content, CRM, personal databases)
- **Supabase** (Central database, API backend)
- **Google** (Calendar, Contacts, Gmail)

## Sync Patterns

### Pattern 1: One-Way Sync (Source → Supabase)

```
┌────────────┐         ┌────────────┐
│   Source   │ ──────► │  Supabase  │
│  (Notion)  │         │  (Target)  │
└────────────┘         └────────────┘
```

**Used by**: Books, Highlights, Calendar, Gmail

**Characteristics**:
- Source is the authority
- Supabase is read-only replica
- No conflict resolution needed

### Pattern 2: Bidirectional Sync (Notion ↔ Supabase)

```
┌────────────┐         ┌────────────┐
│   Notion   │ ◄─────► │  Supabase  │
│            │         │            │
└────────────┘         └────────────┘
```

**Used by**: Meetings, Tasks, Reflections, Journals

**Characteristics**:
- Either side can be modified
- Conflict resolution: Timestamp-based (newer wins)
- Safety valves prevent mass deletions
- 5-second buffer prevents sync ping-pong

### Pattern 3: Multi-Source Sync (Google ↔ Supabase ↔ Notion)

```
┌────────────┐         ┌────────────┐         ┌────────────┐
│   Google   │ ◄─────► │  Supabase  │ ◄─────► │   Notion   │
│            │         │  (Central) │         │            │
└────────────┘         └────────────┘         └────────────┘
```

**Used by**: Contacts

**Characteristics**:
- Google: Source of truth for contact details (phone, email)
- Notion: Source of truth for CRM data (company, notes)
- Supabase: Central merge point

---

## Base Classes

```
lib/sync_base.py
├── NotionClient              # Notion API wrapper with retry
├── SupabaseClient            # Supabase REST client
├── SyncLogger                # Logs to sync_logs table
├── NotionPropertyExtractor   # Parse Notion properties
├── NotionPropertyBuilder     # Build Notion properties
├── BaseSyncService           # Abstract base class
├── OneWaySyncService         # One-way sync implementation
└── TwoWaySyncService         # Bidirectional sync implementation
```

### Using Base Classes

```python
from lib.sync_base import TwoWaySyncService, SyncResult

class MeetingsSyncService(TwoWaySyncService):
    def __init__(self):
        super().__init__(
            service_name='meetings_sync',
            notion_database_id=NOTION_MEETING_DB_ID,
            supabase_table='meetings'
        )
    
    def notion_to_supabase_record(self, page) -> dict:
        """Transform Notion page to Supabase record."""
        return {
            'title': Extract.title(page, 'Title'),
            'date': Extract.date(page, 'Date'),
            'summary': Extract.rich_text(page, 'Summary'),
            # ...
        }
    
    def supabase_to_notion_properties(self, record) -> dict:
        """Transform Supabase record to Notion properties."""
        return Build.properties({
            'Title': Build.title(record['title']),
            'Date': Build.date(record['date']),
            'Summary': Build.rich_text(record['summary']),
            # ...
        })
```

---

## Sync Lifecycle

### 1. Incremental Sync (Default)

```python
@app.post("/sync/meetings")
async def sync_meetings(hours: int = 24, full: bool = False):
    return await run_meeting_sync(full_sync=full, since_hours=hours)
```

**Process**:
1. Query records modified in last N hours
2. Compare timestamps (with 5s buffer)
3. Sync only changed records
4. Log results to `sync_logs`

### 2. Full Sync (On Demand)

```python
# Triggered with ?full=true
POST /sync/meetings?full=true
```

**Process**:
1. Query ALL records from both sides
2. Rebuild links where missing
3. Update all mismatched records
4. Takes longer but ensures consistency

---

## Safety Mechanisms

### Safety Valve

```python
def check_safety_valve(notion_count: int, supabase_count: int):
    """Abort if source returns <10% of destination count."""
    if notion_count < (supabase_count * 0.1):
        raise SafetyValveError(
            f"Safety valve triggered: Notion has {notion_count} records, "
            f"Supabase has {supabase_count}. Possible API failure."
        )
```

### Timestamp Buffer

```python
def should_sync(notion_ts, supabase_ts):
    """5-second buffer prevents sync ping-pong."""
    buffer = timedelta(seconds=5)
    return abs(notion_ts - supabase_ts) > buffer
```

### Soft Deletes

```python
# Never hard delete - use soft deletes
supabase.table("contacts").update({
    "deleted_at": datetime.utcnow().isoformat()
}).eq("id", contact_id).execute()

# Sync propagates to Notion by archiving
notion.pages.update(page_id=page_id, archived=True)
```

---

## Error Handling

### Retry Decorator

```python
@retry_on_error_sync(max_retries=3, backoff_factor=2)
def notion_api_call():
    """Automatically retries on transient failures."""
    pass
```

### Error Isolation

```python
# Errors in one record don't stop the sync
for record in records:
    try:
        sync_record(record)
    except Exception as e:
        log_error(record['id'], str(e))
        continue  # Process next record
```

### Error Notifications

```python
# Transient errors suppressed until they repeat 5+ times
_error_counts = {}

def should_notify(error_type: str) -> bool:
    _error_counts[error_type] = _error_counts.get(error_type, 0) + 1
    return _error_counts[error_type] >= 5
```

---

## Sync Order

The `/sync/all` endpoint runs syncs in this order:

1. **Contacts** (Notion → Supabase)
2. **Contacts** (Google → Supabase)
3. **Contacts** (Supabase → Notion)
4. **Meetings** (Bidirectional)
5. **Tasks** (Bidirectional)
6. **Reflections** (Bidirectional)
7. **Journals** (Bidirectional)
8. **Calendar** (Google → Supabase)
9. **Gmail** (Google → Supabase)
10. **Books** (Notion → Supabase)
11. **Highlights** (Notion → Supabase)

**Why this order?**
- Contacts first (meetings link to contacts)
- Bidirectional syncs before one-way
- Books/Highlights last (reference data)

---

## Database Tables

### Standard Sync Fields

```sql
-- Every synced table has these columns
notion_page_id      TEXT UNIQUE,     -- Link to Notion page
notion_updated_at   TIMESTAMPTZ,     -- Last Notion edit time
last_sync_source    TEXT,            -- 'notion' | 'supabase' | 'google'
created_at          TIMESTAMPTZ,     -- Record creation
updated_at          TIMESTAMPTZ,     -- Last update
deleted_at          TIMESTAMPTZ      -- Soft delete (NULL = active)
```

### Sync State Table

```sql
CREATE TABLE sync_state (
    key         TEXT PRIMARY KEY,    -- e.g., 'gmail_history_id'
    value       TEXT,                -- Token/ID value
    updated_at  TIMESTAMPTZ
);

-- Used for incremental sync tokens
-- gmail_history_id: Gmail incremental sync
-- calendar_sync_token: Google Calendar incremental sync
```

### Sync Logs Table

```sql
CREATE TABLE sync_logs (
    id          UUID PRIMARY KEY,
    event_type  TEXT NOT NULL,       -- 'meetings_sync', 'create_supabase_contact', etc.
    status      TEXT NOT NULL,       -- 'success' | 'error'
    message     TEXT,                -- Human-readable description
    details     JSONB,               -- Additional context
    created_at  TIMESTAMPTZ
);
```

---

## Monitoring

### Health Endpoint Response

```json
{
  "status": "healthy",
  "sync_status": {
    "sync_in_progress": false,
    "last_sync_start": "2024-12-24T08:15:00Z",
    "last_sync_end": "2024-12-24T08:16:30Z",
    "last_sync_duration_seconds": 90
  },
  "stats": {
    "total_syncs_24h": 96,
    "success_rate": "99.2%"
  }
}
```

### Useful Queries

```sql
-- Recent sync activity
SELECT event_type, status, COUNT(*) 
FROM sync_logs 
WHERE created_at > NOW() - INTERVAL '24 hours'
GROUP BY event_type, status
ORDER BY COUNT(*) DESC;

-- Error patterns
SELECT event_type, message, COUNT(*)
FROM sync_logs
WHERE status = 'error'
GROUP BY event_type, message
ORDER BY COUNT(*) DESC;

-- Sync duration trends
SELECT 
    DATE_TRUNC('hour', created_at) as hour,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration
FROM sync_logs
WHERE event_type = 'full_sync_complete'
GROUP BY hour
ORDER BY hour DESC;
```

---

**Last Updated**: December 24, 2024
