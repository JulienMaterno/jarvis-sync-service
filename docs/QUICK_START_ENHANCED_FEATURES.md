# Quick Start: Using Enhanced Logging & Health Checks

## 🚀 What to Expect After Deployment

### 1. Enhanced Sync Logs

**Where to Look**: Supabase `sync_logs` table

**Query to See New Format**:
```sql
SELECT 
    created_at,
    event_type,
    status,
    message
FROM sync_logs
WHERE event_type LIKE '%_complete'
  AND created_at > NOW() - INTERVAL '1 hour'
ORDER BY created_at DESC;
```

**What You'll See**:

For **Bidirectional Syncs** (meetings, tasks, reflections, journals):
```
ReflectionsSync_complete | success | 
  "Notion→Supabase: 2c/1u/0d | Supabase→Notion: 0c/0u/0d | 1.2s | API: 8N/6S"

TasksSync_complete | success |
  "Notion deletions→Supabase: 1 soft-deleted | Notion→Supabase: 3c/2u/0d | Supabase→Notion: 1c/0u/0d | 3.2s | API: 12N/10S"
```

For **One-Way Syncs** (calendar, gmail):
```
gmail_sync | success | "Gmail→Supabase: 5 new, 12 updated"
calendar_sync | success | "Synced 15 events to Supabase (full sync)"
```

**What Each Part Means**:
- `3c` = 3 created
- `2u` = 2 updated
- `1d` = 1 deleted
- `8N` = 8 Notion API calls
- `6S` = 6 Supabase API calls

### 2. Database Consistency Check

**Where to Look**: `/health/full` API endpoint

**How to Check**:
```bash
# Via curl (requires authentication)
curl https://jarvis-sync-service-qkz4et4n4q-as.a.run.app/health/full \
  -H "Authorization: Bearer $IDENTITY_TOKEN"

# Via Python
python run_health_check.py
```

**What You'll See** (Example):

✅ **All Databases in Sync**:
```json
{
  "name": "Notion↔Supabase Consistency",
  "status": "healthy",
  "message": "Counts match across 5 databases",
  "details": {
    "meetings": {"notion": 45, "supabase": 45, "diff": 0},
    "tasks": {"notion": 123, "supabase": 123, "diff": 0},
    "reflections": {"notion": 89, "supabase": 89, "diff": 0},
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

⚠️ **Discrepancy Detected**:
```json
{
  "name": "Notion↔Supabase Consistency",
  "status": "degraded",
  "message": "1 database(s) have count mismatches",
  "details": {
    "reflections": {"notion": 89, "supabase": 102, "diff": 13},
    "meetings": {"notion": 45, "supabase": 45, "diff": 0}
  },
  "warnings": [
    "reflections: Notion=89, Supabase=102 (13% diff)"
  ],
  "recommendations": [
    "Run full sync to reconcile Notion and Supabase databases"
  ]
}
```

## 🔍 Investigating Issues

### Scenario 1: Reflections Count Mismatch

**Problem**: Health check shows Notion=89, Supabase=102

**Steps**:

1. **Check for soft-deleted records**:
```sql
SELECT COUNT(*) as soft_deleted_count
FROM reflections
WHERE deleted_at IS NOT NULL;
```

If this returns 13, the discrepancy is just soft-deletes. These are excluded from the health check, so this shouldn't show as a mismatch. If it does, there may be a bug in the query.

2. **Check for orphaned records** (records without notion_page_id):
```sql
SELECT COUNT(*) as orphaned_count
FROM reflections
WHERE notion_page_id IS NULL
  AND deleted_at IS NULL;
```

If this returns 13, these are reflections created in Supabase that were never synced to Notion.

3. **Check for archived Notion pages** not reflected in Supabase:
```sql
-- This requires checking Notion directly
-- via the API or manually in the Notion UI
```

4. **Fix**: Run a full sync to reconcile:
```bash
curl -X POST "https://jarvis-sync-service-qkz4et4n4q-as.a.run.app/sync/reflections?full=true" \
  -H "Authorization: Bearer $IDENTITY_TOKEN"
```

5. **Verify**: Check health again after sync completes

### Scenario 2: Calendar Sync Not Logging Details

**Problem**: `sync_logs` shows old format: "Synced 25 events"

**Possible Causes**:
- Changes not deployed yet
- Old code still running

**Solution**:
1. Check Cloud Run deployment version
2. Redeploy if necessary
3. Trigger a new sync to see updated logs

### Scenario 3: Health Check Shows "UNKNOWN"

**Problem**: Consistency check reports status as "unknown"

**Causes**:
- `NOTION_API_TOKEN` environment variable not set
- Token expired or invalid
- Network issue connecting to Notion API

**Solution**:
1. Verify environment variables in Cloud Run
2. Test token manually:
```bash
curl https://api.notion.com/v1/users/me \
  -H "Authorization: Bearer $NOTION_API_TOKEN" \
  -H "Notion-Version: 2022-06-28"
```

## 📊 Monitoring Recommendations

### Daily
- Check `/health/full` in the morning (included in 8am report)
- Review any warnings or degraded statuses
- Investigate if consistency check fails

### Weekly
- Review `sync_logs` for patterns:
  ```sql
  SELECT 
      event_type,
      COUNT(*) as total_syncs,
      SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successes,
      SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as errors
  FROM sync_logs
  WHERE created_at > NOW() - INTERVAL '7 days'
  GROUP BY event_type
  ORDER BY errors DESC;
  ```
- Run full sync on all bidirectional databases if not done automatically

### Monthly
- Review orphaned records:
  ```sql
  SELECT 
      'meetings' as table_name,
      COUNT(*) as orphaned_count
  FROM meetings
  WHERE notion_page_id IS NULL AND deleted_at IS NULL
  UNION ALL
  SELECT 'tasks', COUNT(*) FROM tasks WHERE notion_page_id IS NULL AND deleted_at IS NULL
  UNION ALL
  SELECT 'reflections', COUNT(*) FROM reflections WHERE notion_page_id IS NULL AND deleted_at IS NULL
  UNION ALL
  SELECT 'journals', COUNT(*) FROM journals WHERE notion_page_id IS NULL AND deleted_at IS NULL;
  ```

## 🎯 Success Metrics

After deployment, you should see:

✅ **Improved Visibility**
- Each sync shows exactly what changed in each direction
- Easy to spot unusual patterns (e.g., 100 creates every sync)

✅ **Early Problem Detection**
- Count mismatches caught within 1 hour (next health check)
- Recommendations provided automatically

✅ **Easier Debugging**
- Know immediately which direction has an issue
- Clear error messages with context

✅ **Data Integrity Confidence**
- Regular validation that databases are in sync
- Alerts if drift exceeds acceptable thresholds

## 📞 Getting Help

If you see unexpected behavior:

1. **Check the documentation**: `docs/ENHANCED_LOGGING.md`
2. **Review sync logs**: Look for error messages with context
3. **Run health check**: Get current system state
4. **Check this guide**: Common scenarios covered above

Most issues can be resolved with a full sync:
```bash
curl -X POST "https://jarvis-sync-service-qkz4et4n4q-as.a.run.app/sync/all" \
  -H "Authorization: Bearer $IDENTITY_TOKEN"
```
