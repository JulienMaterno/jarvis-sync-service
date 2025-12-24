# 🤖 AGENTS.md - LLM Integration Guide

> **⚠️ CAUTION: Production-Critical Service**
> 
> This service handles critical data synchronization between Notion, Supabase, Google (Calendar/Gmail), and ActivityWatch.
> Runs every 15 minutes via Cloud Scheduler. Be careful with modifications.

## 🏗️ Architecture (Unified Sync Services)

```
jarvis-sync-service/
├── main.py                      # FastAPI app with all endpoints
├── reports.py                   # Daily reports & evening journal
├── syncs/                       # 🆕 Unified sync services (use lib/sync_base.py)
│   ├── meetings_sync.py         # Bidirectional: Notion ↔ Supabase
│   ├── tasks_sync.py            # Bidirectional: Notion ↔ Supabase  
│   ├── reflections_sync.py      # Bidirectional: Notion ↔ Supabase
│   └── journals_sync.py         # Bidirectional: Notion ↔ Supabase
├── sync_contacts_unified.py     # Multi-source: Notion ↔ Supabase ↔ Google
├── sync_calendar.py             # One-way: Google → Supabase
├── sync_gmail.py                # One-way: Google → Supabase
├── sync_books.py                # One-way: Notion → Supabase
├── sync_highlights.py           # One-way: Notion → Supabase
├── sync_activitywatch.py        # Local: ActivityWatch → Supabase
└── lib/                         # Core libraries
    ├── sync_base.py             # 🎯 Base classes for all syncs
    └── ...
```

## 🔒 Safety Mechanisms

1. **Data Integrity**: All syncs have safety valves (10% threshold)
2. **Production Traffic**: Cloud Scheduler runs `/sync/all` every 15 minutes
3. **Soft Deletes**: Records are soft-deleted, never hard-deleted
4. **Bidirectional Deletion**: Deletes sync both ways (Notion ↔ Supabase)

## ✅ What You CAN Do

### 1. Call Sync Endpoints
Use the sync service as an internal API. All endpoints accept POST requests.

**Base URL**: `https://jarvis-sync-service-qkz4et4n4q-as.a.run.app`

**Authentication**: Include `Authorization: Bearer {identity_token}` header.

```python
# Example: Trigger meeting sync
import httpx

async def trigger_meeting_sync():
    token = await get_identity_token()  # From Google Cloud
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://jarvis-sync-service-qkz4et4n4q-as.a.run.app/sync/meetings",
            params={"hours": 24, "full": False},
            headers={"Authorization": f"Bearer {token}"}
        )
        return response.json()
```

### 2. Read Sync Logs
Query the `sync_logs` table in Supabase:

```python
# Get recent sync activity
logs = supabase.table("sync_logs") \
    .select("*") \
    .order("created_at", desc=True) \
    .limit(100) \
    .execute()
```

### 3. Check Sync State
Query `sync_state` table for tokens:

```python
# Check calendar sync token
state = supabase.table("sync_state") \
    .select("*") \
    .eq("key", "calendar_sync_token") \
    .execute()
```

## ❌ What You Should NOT Do

1. **Modify sync logic** in any `sync_*.py` file
2. **Change safety valve thresholds** (currently 10%)
3. **Modify Notion property mappings** without schema changes
4. **Add new sync directions** without full testing
5. **Change error handling patterns** 
6. **Modify the `lib/` directory** (core clients and utilities)

## 📊 Database Schema Reference

### contacts
```sql
id                  UUID PRIMARY KEY,
first_name          TEXT,
last_name           TEXT,
email               TEXT UNIQUE,
phone               TEXT,
company             TEXT,
job_title           TEXT,
birthday            DATE,
linkedin_url        TEXT,
location            TEXT,
notes               TEXT,
dynamic_properties  JSONB,           -- Extra Notion properties
notion_page_id      TEXT UNIQUE,
notion_updated_at   TIMESTAMPTZ,
google_contact_id   TEXT,
last_sync_source    TEXT,            -- 'notion' | 'supabase' | 'google'
created_at          TIMESTAMPTZ DEFAULT NOW(),
updated_at          TIMESTAMPTZ DEFAULT NOW(),
deleted_at          TIMESTAMPTZ      -- Soft delete
```

### meetings
```sql
id                  UUID PRIMARY KEY,
title               TEXT NOT NULL,
date                TIMESTAMPTZ,
location            TEXT,
summary             TEXT,
contact_id          UUID REFERENCES contacts(id),
contact_name        TEXT,            -- Denormalized for convenience
topics_discussed    JSONB,
people_mentioned    TEXT[],
action_items        JSONB,
source_file         TEXT,            -- Original audio file name
notion_page_id      TEXT UNIQUE,
notion_updated_at   TIMESTAMPTZ,
last_sync_source    TEXT,
created_at          TIMESTAMPTZ DEFAULT NOW(),
updated_at          TIMESTAMPTZ DEFAULT NOW()
```

### tasks
```sql
id                  UUID PRIMARY KEY,
title               TEXT NOT NULL,
description         TEXT,
status              TEXT,            -- 'Not started' | 'In progress' | 'Done'
priority            TEXT,            -- 'High' | 'Medium' | 'Low'
due_date            DATE,
completed_at        TIMESTAMPTZ,
project             TEXT,
tags                TEXT[],
notion_page_id      TEXT UNIQUE,
notion_updated_at   TIMESTAMPTZ,
last_sync_source    TEXT,
created_at          TIMESTAMPTZ DEFAULT NOW(),
updated_at          TIMESTAMPTZ DEFAULT NOW()
```

### reflections
```sql
id                  UUID PRIMARY KEY,
title               TEXT NOT NULL,
date                DATE,
content             TEXT,
location            TEXT,
mood                TEXT,
energy_level        TEXT,
tags                TEXT[],
people_mentioned    TEXT[],
notion_page_id      TEXT UNIQUE,
notion_updated_at   TIMESTAMPTZ,
last_sync_source    TEXT,
source_file         TEXT,
source_transcript_id UUID,
created_at          TIMESTAMPTZ DEFAULT NOW(),
updated_at          TIMESTAMPTZ DEFAULT NOW(),
deleted_at          TIMESTAMPTZ
```

### journals
```sql
id                  UUID PRIMARY KEY,
date                DATE NOT NULL UNIQUE,
title               TEXT,
content             TEXT,
mood                TEXT,
energy              TEXT,
gratitude           TEXT[],
wins                TEXT[],
challenges          TEXT[],
tomorrow_focus      TEXT[],
notion_page_id      TEXT UNIQUE,
notion_updated_at   TIMESTAMPTZ,
last_sync_source    TEXT,
created_at          TIMESTAMPTZ DEFAULT NOW(),
updated_at          TIMESTAMPTZ DEFAULT NOW()
```

### calendar_events
```sql
id                  UUID PRIMARY KEY,
google_event_id     TEXT UNIQUE NOT NULL,
calendar_id         TEXT DEFAULT 'primary',
summary             TEXT,
description         TEXT,
start_time          TIMESTAMPTZ,
end_time            TIMESTAMPTZ,
location            TEXT,
status              TEXT,            -- 'confirmed' | 'tentative' | 'cancelled'
html_link           TEXT,
attendees           JSONB,
creator             JSONB,
organizer           JSONB,
contact_id          UUID REFERENCES contacts(id),  -- Auto-linked
last_sync_at        TIMESTAMPTZ,
created_at          TIMESTAMPTZ DEFAULT NOW(),
updated_at          TIMESTAMPTZ DEFAULT NOW()
```

### emails
```sql
id                  UUID PRIMARY KEY,
google_message_id   TEXT UNIQUE NOT NULL,
thread_id           TEXT,
subject             TEXT,
sender              TEXT,
recipient           TEXT,
date                TIMESTAMPTZ,
snippet             TEXT,
body_preview        TEXT,
labels              TEXT[],
is_read             BOOLEAN DEFAULT FALSE,
contact_id          UUID REFERENCES contacts(id),  -- Auto-linked
created_at          TIMESTAMPTZ DEFAULT NOW(),
updated_at          TIMESTAMPTZ DEFAULT NOW()
```

### sync_logs
```sql
id                  UUID PRIMARY KEY,
event_type          TEXT NOT NULL,   -- e.g., 'calendar_sync', 'create_supabase_meeting'
status              TEXT NOT NULL,   -- 'success' | 'error'
message             TEXT,
details             JSONB,
created_at          TIMESTAMPTZ DEFAULT NOW()
```

### sync_state
```sql
key                 TEXT PRIMARY KEY,
value               TEXT,
updated_at          TIMESTAMPTZ DEFAULT NOW()
```

## 🔄 Sync Flow Diagrams

### Notion → Supabase (Example: Meetings)
```
1. Query Notion DB with last_edited_time filter
2. For each page:
   a. Check if exists in Supabase (by notion_page_id)
   b. Compare timestamps with 5s buffer
   c. If newer: Update Supabase record
   d. If not exists: Create Supabase record
   e. Set last_sync_source = 'notion'
```

### Supabase → Notion (Example: Meetings)
```
1. Query Supabase for records without notion_page_id OR recently updated
2. For each record:
   a. If has notion_page_id: Compare timestamps, update if newer
   b. If no notion_page_id: Create Notion page, store page_id
   c. Set last_sync_source = 'supabase'
```

## 🚨 Emergency Procedures

### If Sync is Corrupting Data

1. **Stop the Cloud Scheduler job**:
   ```bash
   gcloud scheduler jobs pause jarvis-sync-hourly --location=asia-southeast1
   ```

2. **Check recent sync logs**:
   ```sql
   SELECT * FROM sync_logs 
   WHERE status = 'error' 
   ORDER BY created_at DESC 
   LIMIT 50;
   ```

3. **Restore from backup** (if needed):
   ```bash
   # Backups are in jarvis-478401-backups bucket
   gsutil ls gs://jarvis-478401-backups/
   ```

### If Safety Valve Triggers

This is GOOD - it means the system prevented potential data loss.

1. Check what caused the discrepancy (Notion API issue? Network timeout?)
2. Manually verify data in both systems
3. If safe, run a full sync: `POST /sync/meetings?full=true`

## 📞 Support Endpoints for Other Services

### For Intelligence Service
```python
# After creating a meeting record, trigger sync to push to Notion
await trigger_sync("meetings", hours=1)

# After processing voice memo as reflection
await trigger_sync("reflections", hours=1)
```

### For Telegram Bot
```python
# No direct integration needed - sync runs automatically
# If user wants immediate sync, they can trigger via command
```

---

**Remember**: This service is the backbone of data consistency.
**When in doubt, don't modify - ask the user first.**
