-- System Health Monitoring Tables
-- Run this migration on Supabase

-- Drop existing tables if they exist (for clean slate)
-- DROP TABLE IF EXISTS system_errors CASCADE;
-- DROP TABLE IF EXISTS health_checks CASCADE;

-- 1. System Errors Table - Stores all errors from all services
CREATE TABLE IF NOT EXISTS system_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service TEXT NOT NULL,          -- 'sync-service', 'intelligence-service', 'audio-pipeline', 'telegram-bot'
    error_type TEXT NOT NULL,       -- 'calendar_sync', 'gmail_sync', 'notion_sync', 'contact_sync', 'transcript_processing', etc.
    severity TEXT DEFAULT 'error',  -- 'warning', 'error', 'critical'
    message TEXT NOT NULL,
    stack_trace TEXT,
    context JSONB,                  -- Additional context (request_id, user_id, etc.)
    is_transient BOOLEAN DEFAULT FALSE,  -- Network errors, timeouts, etc.
    resolved_at TIMESTAMPTZ,        -- When the error was resolved/acknowledged
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for querying recent errors
CREATE INDEX IF NOT EXISTS idx_system_errors_created ON system_errors(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_errors_service ON system_errors(service);
CREATE INDEX IF NOT EXISTS idx_system_errors_unresolved ON system_errors(resolved_at) WHERE resolved_at IS NULL;

-- 2. Health Checks Table - Stores periodic health check results
CREATE TABLE IF NOT EXISTS health_checks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    check_type TEXT NOT NULL,       -- 'daily', 'hourly', 'manual'
    status TEXT NOT NULL,           -- 'healthy', 'degraded', 'unhealthy'
    summary TEXT,
    details JSONB NOT NULL,         -- Full health check results
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_health_checks_created ON health_checks(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_health_checks_type ON health_checks(check_type);

-- 3. Add function to get error summary
CREATE OR REPLACE FUNCTION get_error_summary(hours_back INT DEFAULT 24)
RETURNS TABLE (
    service TEXT,
    error_type TEXT,
    error_count BIGINT,
    last_occurrence TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.service,
        e.error_type,
        COUNT(*)::BIGINT as error_count,
        MAX(e.created_at) as last_occurrence
    FROM system_errors e
    WHERE e.created_at > NOW() - (hours_back || ' hours')::INTERVAL
    GROUP BY e.service, e.error_type
    ORDER BY error_count DESC;
END;
$$ LANGUAGE plpgsql;

-- 4. Add function to get health status
CREATE OR REPLACE FUNCTION get_health_status()
RETURNS TABLE (
    overall_status TEXT,
    error_count_24h BIGINT,
    unresolved_errors BIGINT,
    last_health_check TIMESTAMPTZ,
    services_status JSONB
) AS $$
DECLARE
    err_count BIGINT;
    unresolved BIGINT;
    last_check TIMESTAMPTZ;
    svc_status JSONB;
BEGIN
    -- Count errors in last 24h
    SELECT COUNT(*) INTO err_count
    FROM system_errors
    WHERE created_at > NOW() - INTERVAL '24 hours';
    
    -- Count unresolved errors
    SELECT COUNT(*) INTO unresolved
    FROM system_errors
    WHERE resolved_at IS NULL;
    
    -- Get last health check
    SELECT created_at INTO last_check
    FROM health_checks
    ORDER BY created_at DESC
    LIMIT 1;
    
    -- Get per-service error counts
    SELECT jsonb_object_agg(service, cnt) INTO svc_status
    FROM (
        SELECT service, COUNT(*) as cnt
        FROM system_errors
        WHERE created_at > NOW() - INTERVAL '24 hours'
        GROUP BY service
    ) s;
    
    RETURN QUERY
    SELECT 
        CASE 
            WHEN err_count = 0 THEN 'healthy'
            WHEN err_count < 10 THEN 'degraded'
            ELSE 'unhealthy'
        END as overall_status,
        err_count,
        unresolved,
        last_check,
        COALESCE(svc_status, '{}'::JSONB);
END;
$$ LANGUAGE plpgsql;

-- Comments
COMMENT ON TABLE system_errors IS 'Centralized error logging for all Jarvis services';
COMMENT ON TABLE health_checks IS 'Periodic health check results and system status snapshots';
