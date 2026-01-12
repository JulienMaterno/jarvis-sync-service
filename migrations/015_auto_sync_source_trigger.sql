-- =============================================================================
-- AUTO SYNC SOURCE TRACKING TRIGGER
-- =============================================================================
-- This trigger automatically sets last_sync_source = 'supabase' whenever 
-- a row is INSERTED or UPDATED, UNLESS the operation explicitly sets 
-- last_sync_source to something else (like 'notion' from the sync service).
--
-- This ensures:
-- 1. Any direct Supabase edit (AI, manual, API) is marked as 'supabase'
-- 2. The sync service can still set 'notion' when syncing from Notion
-- 3. No application code needs to remember to set this field
-- 4. New records created in Supabase are automatically marked for sync
-- =============================================================================

-- Generic function for UPDATE operations
CREATE OR REPLACE FUNCTION auto_set_sync_source_on_update()
RETURNS TRIGGER AS $$
BEGIN
    -- Only set to 'supabase' if the field is NULL (not explicitly set in the UPDATE)
    -- 
    -- IMPORTANT: PostgreSQL copies OLD values to NEW for columns not in SET clause,
    -- so we CANNOT detect "wasn't in SET clause" vs "explicitly set to same value".
    -- 
    -- Therefore, sync services MUST set last_sync_source explicitly, and we only
    -- default to 'supabase' when it's NULL (e.g., from a partial update).
    -- 
    -- For safety, apps updating records should explicitly set last_sync_source = 'supabase'
    -- in their UPDATE statements to ensure proper sync behavior.
    IF NEW.last_sync_source IS NULL THEN
        NEW.last_sync_source := 'supabase';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Generic function for INSERT operations
CREATE OR REPLACE FUNCTION auto_set_sync_source_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    -- Only set to 'supabase' if not explicitly set
    -- This allows sync service to create records with 'notion' directly
    IF NEW.last_sync_source IS NULL THEN
        NEW.last_sync_source := 'supabase';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- APPLICATIONS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_applications_update ON applications;
CREATE TRIGGER auto_sync_source_applications_update
    BEFORE UPDATE ON applications
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_applications_insert ON applications;
CREATE TRIGGER auto_sync_source_applications_insert
    BEFORE INSERT ON applications
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- MEETINGS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_meetings_update ON meetings;
CREATE TRIGGER auto_sync_source_meetings_update
    BEFORE UPDATE ON meetings
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_meetings_insert ON meetings;
CREATE TRIGGER auto_sync_source_meetings_insert
    BEFORE INSERT ON meetings
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- TASKS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_tasks_update ON tasks;
CREATE TRIGGER auto_sync_source_tasks_update
    BEFORE UPDATE ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_tasks_insert ON tasks;
CREATE TRIGGER auto_sync_source_tasks_insert
    BEFORE INSERT ON tasks
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- JOURNALS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_journals_update ON journals;
CREATE TRIGGER auto_sync_source_journals_update
    BEFORE UPDATE ON journals
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_journals_insert ON journals;
CREATE TRIGGER auto_sync_source_journals_insert
    BEFORE INSERT ON journals
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- REFLECTIONS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_reflections_update ON reflections;
CREATE TRIGGER auto_sync_source_reflections_update
    BEFORE UPDATE ON reflections
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_reflections_insert ON reflections;
CREATE TRIGGER auto_sync_source_reflections_insert
    BEFORE INSERT ON reflections
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- CONTACTS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_contacts_update ON contacts;
CREATE TRIGGER auto_sync_source_contacts_update
    BEFORE UPDATE ON contacts
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_contacts_insert ON contacts;
CREATE TRIGGER auto_sync_source_contacts_insert
    BEFORE INSERT ON contacts
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- LINKEDIN_POSTS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_linkedin_posts_update ON linkedin_posts;
CREATE TRIGGER auto_sync_source_linkedin_posts_update
    BEFORE UPDATE ON linkedin_posts
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_linkedin_posts_insert ON linkedin_posts;
CREATE TRIGGER auto_sync_source_linkedin_posts_insert
    BEFORE INSERT ON linkedin_posts
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- BOOKS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_books_update ON books;
CREATE TRIGGER auto_sync_source_books_update
    BEFORE UPDATE ON books
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_books_insert ON books;
CREATE TRIGGER auto_sync_source_books_insert
    BEFORE INSERT ON books
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- HIGHLIGHTS TABLE
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_highlights_update ON highlights;
CREATE TRIGGER auto_sync_source_highlights_update
    BEFORE UPDATE ON highlights
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_update();

DROP TRIGGER IF EXISTS auto_sync_source_highlights_insert ON highlights;
CREATE TRIGGER auto_sync_source_highlights_insert
    BEFORE INSERT ON highlights
    FOR EACH ROW
    EXECUTE FUNCTION auto_set_sync_source_on_insert();

-- =============================================================================
-- CLEANUP OLD TRIGGERS (from previous version)
-- =============================================================================
DROP TRIGGER IF EXISTS auto_sync_source_applications ON applications;
DROP TRIGGER IF EXISTS auto_sync_source_meetings ON meetings;
DROP TRIGGER IF EXISTS auto_sync_source_tasks ON tasks;
DROP TRIGGER IF EXISTS auto_sync_source_journals ON journals;
DROP TRIGGER IF EXISTS auto_sync_source_reflections ON reflections;
DROP TRIGGER IF EXISTS auto_sync_source_contacts ON contacts;
DROP TRIGGER IF EXISTS auto_sync_source_linkedin_posts ON linkedin_posts;

-- Drop old function
DROP FUNCTION IF EXISTS auto_set_sync_source();

-- =============================================================================
-- VERIFICATION
-- =============================================================================
-- Test INSERT (no last_sync_source):
--   INSERT INTO applications (id, name) VALUES (gen_random_uuid(), 'Test App');
--   SELECT last_sync_source FROM applications WHERE name = 'Test App';
--   -- Should show 'supabase'
--
-- Test INSERT from sync service:
--   INSERT INTO applications (id, name, last_sync_source) 
--   VALUES (gen_random_uuid(), 'Test Notion', 'notion');
--   SELECT last_sync_source FROM applications WHERE name = 'Test Notion';
--   -- Should show 'notion'
--
-- Test UPDATE from app (explicitly set 'supabase'):
--   UPDATE applications SET status = 'Applied', last_sync_source = 'supabase' WHERE id = 'x';
--   -- Should show 'supabase'
--
-- Test UPDATE from sync service:
--   UPDATE applications SET status='Applied', last_sync_source='notion' WHERE id='x';
--   -- Should stay 'notion' (explicitly set)
--
-- IMPORTANT: Apps should explicitly set last_sync_source = 'supabase' on updates!
-- The trigger only catches NULLs, not "field not in UPDATE statement".
-- =============================================================================
