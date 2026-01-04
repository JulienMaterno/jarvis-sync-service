-- =============================================================================
-- Sync Audit Table
-- =============================================================================
-- Tracks every sync run for auditing, monitoring, and historical analysis.
-- Records counts from each database before/after sync to detect discrepancies.
-- =============================================================================

-- Drop existing table if needed (for development)
-- DROP TABLE IF EXISTS sync_audit CASCADE;

-- =============================================================================
-- SYNC AUDIT TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS sync_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Sync Run Identification
    run_id UUID NOT NULL,                  -- Groups all entries from one sync run
    sync_type TEXT NOT NULL,               -- 'full', 'incremental', 'triggered'
    
    -- Entity Being Synced
    entity_type TEXT NOT NULL,             -- 'contacts', 'meetings', 'tasks', 'journals', 'reflections', etc.
    
    -- Database Counts (snapshot at sync time)
    supabase_count INTEGER NOT NULL DEFAULT 0,   -- Total active records in Supabase
    notion_count INTEGER NOT NULL DEFAULT 0,     -- Total records in Notion
    google_count INTEGER,                        -- Total in Google (for contacts only)
    
    -- Sync Operations Performed
    created_in_notion INTEGER DEFAULT 0,         -- Records created in Notion
    created_in_supabase INTEGER DEFAULT 0,       -- Records created in Supabase
    updated_in_notion INTEGER DEFAULT 0,         -- Records updated in Notion
    updated_in_supabase INTEGER DEFAULT 0,       -- Records updated in Supabase
    deleted_in_notion INTEGER DEFAULT 0,         -- Records deleted/archived in Notion
    deleted_in_supabase INTEGER DEFAULT 0,       -- Records soft-deleted in Supabase
    
    -- Sync Health Status
    is_in_sync BOOLEAN DEFAULT TRUE,             -- Are counts equal (within tolerance)?
    count_difference INTEGER DEFAULT 0,          -- Difference between databases
    sync_health TEXT DEFAULT 'healthy',          -- 'healthy', 'warning', 'critical'
    
    -- Timing
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    duration_ms INTEGER,                         -- How long sync took
    
    -- Status & Errors
    status TEXT NOT NULL DEFAULT 'running',      -- 'running', 'success', 'partial', 'failed'
    error_message TEXT,                          -- Error details if failed
    
    -- Metadata
    triggered_by TEXT,                           -- 'scheduler', 'api', 'webhook'
    details JSONB,                               -- Additional context/notes
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_sync_audit_run_id 
    ON sync_audit(run_id);

CREATE INDEX IF NOT EXISTS idx_sync_audit_entity_type 
    ON sync_audit(entity_type);

CREATE INDEX IF NOT EXISTS idx_sync_audit_created_at 
    ON sync_audit(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_sync_audit_status 
    ON sync_audit(status);

CREATE INDEX IF NOT EXISTS idx_sync_audit_sync_health 
    ON sync_audit(sync_health);

-- Composite index for time-series queries
CREATE INDEX IF NOT EXISTS idx_sync_audit_entity_time 
    ON sync_audit(entity_type, created_at DESC);

-- =============================================================================
-- SYNC RUN SUMMARY VIEW
-- =============================================================================

CREATE OR REPLACE VIEW sync_run_summary AS
SELECT 
    run_id,
    MIN(started_at) AS started_at,
    MAX(completed_at) AS completed_at,
    EXTRACT(EPOCH FROM (MAX(completed_at) - MIN(started_at))) * 1000 AS total_duration_ms,
    COUNT(*) AS entities_synced,
    triggered_by,
    
    -- Overall status (worst status from all entities)
    CASE 
        WHEN bool_or(status = 'failed') THEN 'failed'
        WHEN bool_or(status = 'partial') THEN 'partial'
        WHEN bool_or(status = 'running') THEN 'running'
        ELSE 'success'
    END AS overall_status,
    
    -- Overall health (worst health from all entities)
    CASE 
        WHEN bool_or(sync_health = 'critical') THEN 'critical'
        WHEN bool_or(sync_health = 'warning') THEN 'warning'
        ELSE 'healthy'
    END AS overall_health,
    
    -- Aggregate operations
    SUM(created_in_notion) AS total_created_notion,
    SUM(created_in_supabase) AS total_created_supabase,
    SUM(updated_in_notion) AS total_updated_notion,
    SUM(updated_in_supabase) AS total_updated_supabase,
    SUM(deleted_in_notion) AS total_deleted_notion,
    SUM(deleted_in_supabase) AS total_deleted_supabase
    
FROM sync_audit
GROUP BY run_id, triggered_by
ORDER BY started_at DESC;

-- =============================================================================
-- ENTITY SYNC HISTORY VIEW
-- =============================================================================

CREATE OR REPLACE VIEW entity_sync_history AS
SELECT 
    entity_type,
    DATE(created_at) AS sync_date,
    COUNT(*) AS sync_runs,
    
    -- Daily averages
    AVG(supabase_count)::INTEGER AS avg_supabase_count,
    AVG(notion_count)::INTEGER AS avg_notion_count,
    
    -- Daily operations
    SUM(created_in_notion) AS created_notion,
    SUM(created_in_supabase) AS created_supabase,
    SUM(updated_in_notion) AS updated_notion,
    SUM(updated_in_supabase) AS updated_supabase,
    
    -- Health summary
    COUNT(*) FILTER (WHERE sync_health = 'healthy') AS healthy_runs,
    COUNT(*) FILTER (WHERE sync_health = 'warning') AS warning_runs,
    COUNT(*) FILTER (WHERE sync_health = 'critical') AS critical_runs
    
FROM sync_audit
WHERE status IN ('success', 'partial')
GROUP BY entity_type, DATE(created_at)
ORDER BY sync_date DESC, entity_type;

-- =============================================================================
-- DATABASE INVENTORY SNAPSHOT VIEW
-- =============================================================================

CREATE OR REPLACE VIEW database_inventory AS
SELECT DISTINCT ON (entity_type)
    entity_type,
    supabase_count,
    notion_count,
    google_count,
    count_difference,
    is_in_sync,
    sync_health,
    created_at AS last_sync_at
FROM sync_audit
WHERE status = 'success'
ORDER BY entity_type, created_at DESC;

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE sync_audit IS 'Tracks every sync operation with before/after counts and operations performed';
COMMENT ON COLUMN sync_audit.run_id IS 'Groups all entity syncs from a single sync run (e.g., /sync/all)';
COMMENT ON COLUMN sync_audit.is_in_sync IS 'TRUE if database counts are equal (within acceptable tolerance)';
COMMENT ON COLUMN sync_audit.sync_health IS 'Overall health: healthy (diff=0), warning (diff 1-5), critical (diff>5)';
COMMENT ON VIEW sync_run_summary IS 'Aggregated view of each sync run across all entities';
COMMENT ON VIEW entity_sync_history IS 'Daily aggregates for each entity type';
COMMENT ON VIEW database_inventory IS 'Latest count snapshot for each entity type';
