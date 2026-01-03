# Enhanced Sync Logging & Health Checks

This document describes the improvements made to sync logging and health monitoring.

## 🎯 Problem Statement

The user reported:
1. Unclear sync logs - couldn't tell how many items were synced in each direction
2. No validation that Notion and Supabase have matching record counts
3. Suspected discrepancies (e.g., more reflections in Supabase than Notion)
4. Need to verify Google Contacts sync coverage

## ✅ Solution Overview

### 1. Enhanced Bidirectional Sync Logging

**Location**: `lib/sync_base.py` - `SyncLogger.log_complete()`

**What Changed**:
- Added optional `direction_details` parameter to `log_complete()`
- Logs now show detailed per-direction breakdown for bidirectional syncs

**Example Log Format**:
```
Before:
"Completed: 5c/3u/1d/0err | 3.2s | API: 12N/10S"

After (with direction details):
"Notion deletions→Supabase: 1 soft-deleted | Notion→Supabase: 3c/1u/0d | Supabase→Notion: 1c/1u/0d | 3.2s | API: 12N/10S"
```

**How It Works**:
```python
# In TwoWaySyncService.sync():
direction_details = {
    'notion_deletions': 1,              # Records soft-deleted in Supabase
    'supabase_deletions': 0,            # Notion pages archived
    'notion_to_supabase': {
        'created': 3, 'updated': 1, 'deleted': 0
    },
    'supabase_to_notion': {
        'created': 1, 'updated': 1, 'deleted': 0
    }
}
self.sync_logger.log_complete(result, direction_details)
```

**Benefits**:
- Clear visibility into each sync direction
- Easy to spot issues (e.g., "Why are 100 items being created in Notion every sync?")
- Helps debug sync loops or conflicts

### 2. Database Count Consistency Check

**Location**: `lib/health_monitor.py` - `check_notion_supabase_consistency()`

**What Changed**:
- New health check validates record counts between Notion and Supabase
- Checks bidirectional sync databases: meetings, tasks, reflections, journals, contacts
- Validates Google Contacts sync coverage

**How It Works**:
1. Queries Notion database for page count
2. Queries Supabase for active records (excludes soft-deleted via `deleted_at IS NULL`)
3. Compares counts with 10% threshold and 5 record minimum
4. Reports discrepancies with recommendations

**Example Output**:
```json
{
  "name": "Notion↔Supabase Consistency",
  "status": "degraded",
  "message": "2 database(s) have count mismatches",
  "details": {
    "meetings": {"notion": 45, "supabase": 47, "diff": 2},
    "tasks": {"notion": 123, "supabase": 123, "diff": 0},
    "reflections": {"notion": 89, "supabase": 102, "diff": 13},
    "journals": {"notion": 67, "supabase": 67, "diff": 0},
    "contacts": {"notion": 234, "supabase": 234, "diff": 0},
    "contacts_google_sync": {
      "total": 234,
      "synced_to_google": 230,
      "not_synced": 4
    }
  }
}
```

**Alert Thresholds**:
- **HEALTHY**: Counts match within 10% OR difference < 5 records
- **DEGRADED**: Difference > 10% AND difference > 5 records
- **Warning**: If >10 contacts aren't synced to Google

**Benefits**:
- Catches sync issues early before they become data integrity problems
- Identifies which database has the discrepancy
- Helps diagnose sync direction issues

### 3. Improved Calendar & Gmail Sync Logging

**Location**: `sync_calendar.py`, `sync_gmail.py`

**What Changed**:

**Calendar**:
```python
# Before
"Synced 25 events"

# After
"Synced 25 events to Supabase (full sync)"
```

**Gmail**:
```python
# Before
"Synced 43 emails"

# After
"Gmail→Supabase: 12 new, 31 updated"
```

**Benefits**:
- Clear direction indicator (always Google → Supabase for these one-way syncs)
- Breakdown of new vs updated records
- Full sync indicator for calendar

## 📊 How to Use

### Check Sync Logs

Query the `sync_logs` table to see detailed sync activity:

```sql
SELECT created_at, event_type, status, message
FROM sync_logs
WHERE event_type LIKE '%complete'
ORDER BY created_at DESC
LIMIT 10;
```

**Example Results**:
```
2026-01-03 15:30:22 | ReflectionsSync_complete | success | 
  "Notion→Supabase: 2c/1u/0d | Supabase→Notion: 0c/0u/0d | 1.2s | API: 8N/6S"

2026-01-03 15:29:45 | gmail_sync | success | 
  "Gmail→Supabase: 5 new, 12 updated"

2026-01-03 15:29:30 | calendar_sync | success | 
  "Synced 15 events to Supabase"
```

### Run Health Check

**Via API**:
```bash
curl https://jarvis-sync-service-qkz4et4n4q-as.a.run.app/health/full \
  -H "Authorization: Bearer $IDENTITY_TOKEN"
```

**Via CLI**:
```bash
python run_health_check.py
```

**Check Specific Component**:
The consistency check is part of the full health report. Look for:
- Component: "Notion↔Supabase Consistency"
- Status: healthy/degraded/unhealthy
- Details: Count breakdown per database

### Investigate Discrepancies

If the health check reports count mismatches:

1. **Check sync logs** for recent errors:
```sql
SELECT * FROM sync_logs
WHERE status = 'error' AND event_type LIKE '%reflections%'
ORDER BY created_at DESC;
```

2. **Run a full sync** to reconcile:
```bash
# Via API
curl -X POST https://.../sync/reflections?full=true

# Or trigger via /sync/all endpoint
```

3. **Verify soft deletes** aren't causing false positives:
```sql
-- Check soft-deleted records in Supabase
SELECT COUNT(*) FROM reflections WHERE deleted_at IS NOT NULL;

-- These are excluded from the consistency check
```

## 🔍 Troubleshooting

### "Why do counts not match after sync?"

Possible causes:
1. **Soft deletes**: Records deleted in Supabase but Notion pages still exist
   - Solution: Run full sync to archive Notion pages
   
2. **Orphaned records**: Records in Supabase without `notion_page_id`
   - Solution: Check for records with `notion_page_id IS NULL`
   
3. **Sync in progress**: Counts checked mid-sync
   - Solution: Wait for sync to complete, check again
   
4. **Archived Notion pages**: Pages archived in Notion but not soft-deleted in Supabase
   - Solution: Run full sync to detect and sync deletions

### "Health check shows UNKNOWN for consistency"

This means the Notion API token is not available or invalid.
- Check `NOTION_API_TOKEN` environment variable
- Verify token has access to the databases

## 🎓 Best Practices

1. **Monitor consistency daily**: Include in your 8am health report
2. **Set up alerts**: If consistency check stays DEGRADED for >24h
3. **Run full sync weekly**: Catches any drift between systems
4. **Check logs after each sync**: Verify expected change counts
5. **Investigate >10% discrepancies immediately**: Usually indicates a sync bug

## 📝 Technical Notes

### Why 10% threshold?

- Allows for normal sync delays (a few records in flight)
- Catches significant issues (100+ record discrepancies)
- Combined with absolute minimum (5 records) prevents false positives on small datasets

### Why exclude soft deletes?

Soft-deleted records in Supabase (`deleted_at IS NOT NULL`) are:
- No longer active data
- Should have corresponding archived Notion pages
- Excluded from count comparison to avoid false positives

The deletion sync process:
1. User deletes in Notion → Page archived → Supabase record soft-deleted
2. User deletes in Supabase → Record soft-deleted → Notion page archived

Both should result in consistent counts when excluding soft-deletes.

### Performance Considerations

The consistency check:
- Makes 1 API call per Notion database (5 databases = 5 calls)
- Uses pagination for accurate counts (may be slow for large databases)
- Runs as part of health check (not on every sync)
- Recommended frequency: Once per hour or on-demand
