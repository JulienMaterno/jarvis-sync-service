-- ============================================================================
-- Add sync tracking columns to meetings and tasks tables
-- ============================================================================
-- This migration adds columns needed for robust bidirectional sync between 
-- Notion and Supabase, matching the structure of the contacts table.
--
-- Columns added:
-- - last_sync_source: Tracks sync source ("notion" or "supabase")
-- - deleted_at: Soft delete timestamp (tasks already has this)
-- - notion_page_id: Link to Notion page (meetings already has this)
-- - notion_updated_at: Notion's last_edited_time (meetings already has this)
--
-- Run this in your Supabase SQL Editor
-- ============================================================================

-- ============================================================================
-- MEETINGS TABLE
-- ============================================================================

-- Add last_sync_source column
ALTER TABLE meetings 
ADD COLUMN IF NOT EXISTS last_sync_source TEXT;

COMMENT ON COLUMN meetings.last_sync_source IS 
'Tracks the source of the last sync operation: "notion" or "supabase"';

-- Verify deleted_at exists (should already be there)
-- This is just for documentation - column already exists
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'meetings' AND column_name = 'deleted_at'
    ) THEN
        EXECUTE 'ALTER TABLE meetings ADD COLUMN deleted_at TIMESTAMP WITH TIME ZONE';
        RAISE NOTICE 'Added deleted_at to meetings';
    ELSE
        RAISE NOTICE 'meetings.deleted_at already exists';
    END IF;
END $$;

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_meetings_last_sync_source 
ON meetings(last_sync_source);

CREATE INDEX IF NOT EXISTS idx_meetings_deleted_at 
ON meetings(deleted_at) 
WHERE deleted_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_meetings_notion_page_id 
ON meetings(notion_page_id) 
WHERE notion_page_id IS NOT NULL;


-- ============================================================================
-- TASKS TABLE
-- ============================================================================

-- Add last_sync_source column
ALTER TABLE tasks 
ADD COLUMN IF NOT EXISTS last_sync_source TEXT;

COMMENT ON COLUMN tasks.last_sync_source IS 
'Tracks the source of the last sync operation: "notion" or "supabase"';

-- Verify notion_page_id exists (should already be there)
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tasks' AND column_name = 'notion_page_id'
    ) THEN
        EXECUTE 'ALTER TABLE tasks ADD COLUMN notion_page_id TEXT';
        RAISE NOTICE 'Added notion_page_id to tasks';
    ELSE
        RAISE NOTICE 'tasks.notion_page_id already exists';
    END IF;
END $$;

-- Verify notion_updated_at exists
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tasks' AND column_name = 'notion_updated_at'
    ) THEN
        EXECUTE 'ALTER TABLE tasks ADD COLUMN notion_updated_at TIMESTAMP WITH TIME ZONE';
        RAISE NOTICE 'Added notion_updated_at to tasks';
    ELSE
        RAISE NOTICE 'tasks.notion_updated_at already exists';
    END IF;
END $$;

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_tasks_last_sync_source 
ON tasks(last_sync_source);

CREATE INDEX IF NOT EXISTS idx_tasks_deleted_at 
ON tasks(deleted_at) 
WHERE deleted_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tasks_notion_page_id 
ON tasks(notion_page_id) 
WHERE notion_page_id IS NOT NULL;


-- ============================================================================
-- VERIFICATION
-- ============================================================================

DO $$ 
DECLARE
    meetings_cols TEXT[];
    tasks_cols TEXT[];
    required_cols TEXT[] := ARRAY['deleted_at', 'notion_page_id', 'notion_updated_at', 'last_sync_source'];
    col TEXT;
BEGIN
    -- Check meetings table
    SELECT ARRAY_AGG(column_name) INTO meetings_cols
    FROM information_schema.columns 
    WHERE table_name = 'meetings' 
    AND column_name = ANY(required_cols);
    
    RAISE NOTICE 'MEETINGS table sync columns: %', meetings_cols;
    
    -- Check tasks table
    SELECT ARRAY_AGG(column_name) INTO tasks_cols
    FROM information_schema.columns 
    WHERE table_name = 'tasks' 
    AND column_name = ANY(required_cols);
    
    RAISE NOTICE 'TASKS table sync columns: %', tasks_cols;
    
    -- Verify all required columns exist
    FOREACH col IN ARRAY required_cols LOOP
        IF NOT (col = ANY(meetings_cols)) THEN
            RAISE WARNING 'Missing column in meetings: %', col;
        END IF;
        IF NOT (col = ANY(tasks_cols)) THEN
            RAISE WARNING 'Missing column in tasks: %', col;
        END IF;
    END LOOP;
    
    RAISE NOTICE '✓ Migration complete! Both tables ready for bidirectional sync.';
END $$;

