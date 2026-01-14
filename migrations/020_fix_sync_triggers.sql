-- Migration: Fix auto sync triggers to properly detect Supabase changes
-- Problem: The original trigger checked if NEW.last_sync_source != 'notion',
--          but when updating a record, NEW still has the OLD value unless
--          explicitly changed. So records that came from Notion would never
--          be marked as 'supabase' even when edited in Supabase.
--
-- Solution: Compare OLD and NEW values to detect if the API explicitly set
--           last_sync_source, OR check if any actual data changed.
--
-- Run: Execute in Supabase SQL Editor

-- =============================================================================
-- DROP OLD TRIGGERS FIRST (to avoid conflicts)
-- =============================================================================

DROP TRIGGER IF EXISTS applications_auto_sync_source ON applications;
DROP TRIGGER IF EXISTS applications_auto_sync_source_insert ON applications;
DROP TRIGGER IF EXISTS meetings_auto_sync_source ON meetings;
DROP TRIGGER IF EXISTS meetings_auto_sync_source_insert ON meetings;
DROP TRIGGER IF EXISTS tasks_auto_sync_source ON tasks;
DROP TRIGGER IF EXISTS tasks_auto_sync_source_insert ON tasks;
DROP TRIGGER IF EXISTS reflections_auto_sync_source ON reflections;
DROP TRIGGER IF EXISTS reflections_auto_sync_source_insert ON reflections;
DROP TRIGGER IF EXISTS journals_auto_sync_source ON journals;
DROP TRIGGER IF EXISTS journals_auto_sync_source_insert ON journals;
DROP TRIGGER IF EXISTS contacts_auto_sync_source ON contacts;
DROP TRIGGER IF EXISTS contacts_auto_sync_source_insert ON contacts;
DROP TRIGGER IF EXISTS documents_auto_sync_source ON documents;
DROP TRIGGER IF EXISTS documents_auto_sync_source_insert ON documents;

-- Drop old function
DROP FUNCTION IF EXISTS trigger_set_sync_source_supabase();

-- =============================================================================
-- NEW TRIGGER FUNCTION: Properly detect API vs Sync updates
-- =============================================================================

CREATE OR REPLACE FUNCTION trigger_set_sync_source_supabase()
RETURNS TRIGGER AS $$
BEGIN
    -- For INSERTs: If sync source not explicitly set, mark as supabase
    IF TG_OP = 'INSERT' THEN
        IF NEW.last_sync_source IS NULL THEN
            NEW.last_sync_source := 'supabase';
        END IF;
        IF NEW.updated_at IS NULL THEN
            NEW.updated_at := NOW();
        END IF;
        RETURN NEW;
    END IF;
    
    -- For UPDATEs: Check if the sync source is being explicitly set by Notion sync
    -- Notion sync ALWAYS explicitly sets last_sync_source='notion' in its update
    -- If the incoming update has last_sync_source=OLD value (unchanged), 
    -- then this is a regular API/SQL update, so mark as 'supabase'
    
    IF TG_OP = 'UPDATE' THEN
        -- If sync source is NOT being explicitly changed in this update,
        -- then this update came from Supabase (user edit, SQL, API)
        IF NEW.last_sync_source IS NOT DISTINCT FROM OLD.last_sync_source THEN
            -- Sync source wasn't changed, so this is a Supabase update
            NEW.last_sync_source := 'supabase';
        END IF;
        -- If NEW.last_sync_source != OLD.last_sync_source, someone explicitly
        -- set it (e.g., Notion sync setting it to 'notion'), so don't override
        
        -- Always update the timestamp
        NEW.updated_at := NOW();
        
        RETURN NEW;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- CREATE TRIGGERS FOR ALL TABLES
-- =============================================================================

-- APPLICATIONS
CREATE TRIGGER applications_auto_sync
    BEFORE INSERT OR UPDATE ON applications
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- MEETINGS
CREATE TRIGGER meetings_auto_sync
    BEFORE INSERT OR UPDATE ON meetings
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- TASKS
CREATE TRIGGER tasks_auto_sync
    BEFORE INSERT OR UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- REFLECTIONS
CREATE TRIGGER reflections_auto_sync
    BEFORE INSERT OR UPDATE ON reflections
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- JOURNALS
CREATE TRIGGER journals_auto_sync
    BEFORE INSERT OR UPDATE ON journals
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- CONTACTS
CREATE TRIGGER contacts_auto_sync
    BEFORE INSERT OR UPDATE ON contacts
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- DOCUMENTS
CREATE TRIGGER documents_auto_sync
    BEFORE INSERT OR UPDATE ON documents
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_sync_source_supabase();

-- =============================================================================
-- VERIFY TRIGGERS
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
WHERE tgname LIKE '%auto_sync%'
ORDER BY relname, tgname;

-- =============================================================================
-- TEST: Should show 'supabase' after running
-- =============================================================================
-- 
-- To test (run these in SQL editor):
--
-- 1. Check current value:
--    SELECT name, status, last_sync_source FROM applications LIMIT 1;
--
-- 2. Update without changing sync source:
--    UPDATE applications SET notes = 'test' WHERE name = 'YOUR_APP_NAME';
--
-- 3. Verify last_sync_source is now 'supabase':
--    SELECT name, status, last_sync_source FROM applications WHERE name = 'YOUR_APP_NAME';
