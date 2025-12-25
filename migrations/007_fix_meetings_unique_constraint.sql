-- ============================================================================
-- FIX: Add UNIQUE constraints on notion_page_id for synced tables
-- ============================================================================
-- 
-- PROBLEM: The sync services use upsert with ON CONFLICT (notion_page_id),
-- which requires a UNIQUE constraint on the column. Without it, duplicates
-- are created instead of updates.
--
-- RUN THIS IN SUPABASE SQL EDITOR:
-- https://supabase.com/dashboard/project/ojnllduebzfxqmiyinhx/sql
-- ============================================================================

-- ============================================================================
-- MEETINGS TABLE (had 2783 duplicates - cleaned up Dec 25, 2025)
-- ============================================================================

-- Step 1: Verify no duplicates exist
SELECT 'meetings' as table_name, notion_page_id, COUNT(*) as cnt 
FROM meetings 
WHERE notion_page_id IS NOT NULL 
GROUP BY notion_page_id 
HAVING COUNT(*) > 1;

-- Step 2: Add UNIQUE constraint
ALTER TABLE meetings 
ADD CONSTRAINT meetings_notion_page_id_unique UNIQUE (notion_page_id);

-- ============================================================================
-- OTHER TABLES (preventive - add constraints before issues occur)
-- ============================================================================

-- Tasks
ALTER TABLE tasks 
ADD CONSTRAINT tasks_notion_page_id_unique UNIQUE (notion_page_id);

-- Reflections
ALTER TABLE reflections 
ADD CONSTRAINT reflections_notion_page_id_unique UNIQUE (notion_page_id);

-- Journals
ALTER TABLE journals 
ADD CONSTRAINT journals_notion_page_id_unique UNIQUE (notion_page_id);

-- Contacts
ALTER TABLE contacts 
ADD CONSTRAINT contacts_notion_page_id_unique UNIQUE (notion_page_id);

-- Books
ALTER TABLE books 
ADD CONSTRAINT books_notion_page_id_unique UNIQUE (notion_page_id);

-- Highlights
ALTER TABLE highlights 
ADD CONSTRAINT highlights_notion_page_id_unique UNIQUE (notion_page_id);

-- ============================================================================
-- VERIFY ALL CONSTRAINTS
-- ============================================================================
SELECT table_name, constraint_name 
FROM information_schema.table_constraints 
WHERE constraint_type = 'UNIQUE' 
  AND constraint_name LIKE '%notion_page_id%'
ORDER BY table_name;
