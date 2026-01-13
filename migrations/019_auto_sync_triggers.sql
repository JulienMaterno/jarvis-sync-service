-- Migration: Add automatic sync detection triggers
-- Purpose: Automatically detect ANY change to sync-enabled tables (including direct SQL edits)
-- This removes the need for code to manually set last_sync_source='supabase'
-- 
-- Run: Execute in Supabase SQL Editor

-- =============================================================================
-- TRIGGER FUNCTION: Automatically set sync fields on ANY update
-- =============================================================================

CREATE OR REPLACE FUNCTION trigger_set_sync_source_supabase()
RETURNS TRIGGER AS $$
BEGIN
    -- Only set if the change didn't come from Notion sync
    -- (Notion sync will explicitly set last_sync_source='notion')
    IF NEW.last_sync_source IS NULL OR NEW.last_sync_source != 'notion' THEN
        NEW.last_sync_source := 'supabase';
    END IF;
    
    -- Always update the updated_at timestamp
    NEW.updated_at := NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- APPLICATIONS TABLE TRIGGER
-- =============================================================================

-- Drop existing trigger if any
DROP TRIGGER IF EXISTS applications_auto_sync_source ON applications;

-- Create trigger that fires BEFORE UPDATE
CREATE TRIGGER applications_auto_sync_source
    BEFORE UPDATE ON applications
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- Also handle INSERTs (new records should sync to Notion)
DROP TRIGGER IF EXISTS applications_auto_sync_source_insert ON applications;

CREATE TRIGGER applications_auto_sync_source_insert
    BEFORE INSERT ON applications
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- MEETINGS TABLE TRIGGER
-- =============================================================================

DROP TRIGGER IF EXISTS meetings_auto_sync_source ON meetings;

CREATE TRIGGER meetings_auto_sync_source
    BEFORE UPDATE ON meetings
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

DROP TRIGGER IF EXISTS meetings_auto_sync_source_insert ON meetings;

CREATE TRIGGER meetings_auto_sync_source_insert
    BEFORE INSERT ON meetings
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- TASKS TABLE TRIGGER
-- =============================================================================

DROP TRIGGER IF EXISTS tasks_auto_sync_source ON tasks;

CREATE TRIGGER tasks_auto_sync_source
    BEFORE UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

DROP TRIGGER IF EXISTS tasks_auto_sync_source_insert ON tasks;

CREATE TRIGGER tasks_auto_sync_source_insert
    BEFORE INSERT ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- REFLECTIONS TABLE TRIGGER
-- =============================================================================

DROP TRIGGER IF EXISTS reflections_auto_sync_source ON reflections;

CREATE TRIGGER reflections_auto_sync_source
    BEFORE UPDATE ON reflections
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

DROP TRIGGER IF EXISTS reflections_auto_sync_source_insert ON reflections;

CREATE TRIGGER reflections_auto_sync_source_insert
    BEFORE INSERT ON reflections
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- JOURNALS TABLE TRIGGER
-- =============================================================================

DROP TRIGGER IF EXISTS journals_auto_sync_source ON journals;

CREATE TRIGGER journals_auto_sync_source
    BEFORE UPDATE ON journals
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

DROP TRIGGER IF EXISTS journals_auto_sync_source_insert ON journals;

CREATE TRIGGER journals_auto_sync_source_insert
    BEFORE INSERT ON journals
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- CONTACTS TABLE TRIGGER
-- =============================================================================

DROP TRIGGER IF EXISTS contacts_auto_sync_source ON contacts;

CREATE TRIGGER contacts_auto_sync_source
    BEFORE UPDATE ON contacts
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

DROP TRIGGER IF EXISTS contacts_auto_sync_source_insert ON contacts;

CREATE TRIGGER contacts_auto_sync_source_insert
    BEFORE INSERT ON contacts
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- DOCUMENTS TABLE TRIGGER (if using document sync)
-- =============================================================================

DROP TRIGGER IF EXISTS documents_auto_sync_source ON documents;

CREATE TRIGGER documents_auto_sync_source
    BEFORE UPDATE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

DROP TRIGGER IF EXISTS documents_auto_sync_source_insert ON documents;

CREATE TRIGGER documents_auto_sync_source_insert
    BEFORE INSERT ON documents
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- VERIFY TRIGGERS WERE CREATED
-- =============================================================================

SELECT 
    tgname AS trigger_name,
    relname AS table_name,
    CASE tgenabled 
        WHEN 'O' THEN 'enabled'
        WHEN 'D' THEN 'disabled'
        ELSE tgenabled::text
    END AS status
FROM pg_trigger t
JOIN pg_class c ON t.tgrelid = c.oid
WHERE tgname LIKE '%auto_sync_source%'
ORDER BY relname, tgname;

-- =============================================================================
-- USAGE NOTES
-- =============================================================================
-- 
-- After running this migration:
-- 
-- 1. ANY update to these tables (SQL, API, Dashboard, Claude) will automatically:
--    - Set last_sync_source = 'supabase' 
--    - Update updated_at = NOW()
--
-- 2. The sync service will see these changes and push to Notion
--
-- 3. When Notion sync writes back, it explicitly sets last_sync_source = 'notion',
--    which the trigger respects (IF check at the top)
--
-- 4. No more need for mark_apps_for_sync.py or similar scripts!
--
-- To test:
--   UPDATE applications SET status = 'In Progress' WHERE name = 'Test App';
--   SELECT name, last_sync_source, updated_at FROM applications WHERE name = 'Test App';
--   -- Should show last_sync_source = 'supabase' and fresh updated_at
