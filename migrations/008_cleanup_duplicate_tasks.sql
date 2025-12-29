-- ============================================================================
-- FIX: Clean up duplicate tasks + Create chat_messages table
-- ============================================================================
-- 
-- PROBLEM 1: The Intelligence Service was creating duplicate tasks without
-- checking if they already existed.
--
-- PROBLEM 2: Chat history was stored in-memory only, lost on bot restart.
-- We need persistent storage for future AI memory features.
--
-- RUN THIS IN SUPABASE SQL EDITOR:
-- https://supabase.com/dashboard/project/ojnllduebzfxqmiyinhx/sql
-- ============================================================================

-- ============================================================================
-- PART 1: CHAT MESSAGES TABLE (for AI memory)
-- ============================================================================

CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id BIGINT NOT NULL,                      -- Telegram user ID
    role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
    content TEXT NOT NULL,
    tools_used TEXT[],                            -- Which tools the AI used (if assistant)
    metadata JSONB,                               -- Extra data (tokens used, model, etc.)
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast retrieval by user
CREATE INDEX IF NOT EXISTS idx_chat_messages_user_id ON chat_messages(user_id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_created_at ON chat_messages(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_chat_messages_user_recent ON chat_messages(user_id, created_at DESC);

-- Comment
COMMENT ON TABLE chat_messages IS 'Permanent storage of all chat messages for AI memory features';

-- ============================================================================
-- PART 2: CLEAN UP DUPLICATE TASKS
-- ============================================================================

-- Step 1: Preview duplicates (DO NOT SKIP - verify before deleting!)
SELECT 
    title,
    COUNT(*) as duplicate_count,
    array_agg(id ORDER BY created_at) as all_ids,
    MIN(created_at) as first_created,
    MAX(created_at) as last_created
FROM tasks 
WHERE deleted_at IS NULL
GROUP BY title 
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Step 2: Mark duplicates as deleted (keeping the OLDEST one)
WITH oldest_tasks AS (
    SELECT DISTINCT ON (title) id, title
    FROM tasks
    WHERE deleted_at IS NULL
    ORDER BY title, created_at ASC
),
duplicates_to_delete AS (
    SELECT t.id, t.title
    FROM tasks t
    WHERE t.deleted_at IS NULL
      AND t.title IN (
          SELECT title 
          FROM tasks 
          WHERE deleted_at IS NULL 
          GROUP BY title 
          HAVING COUNT(*) > 1
      )
      AND t.id NOT IN (SELECT id FROM oldest_tasks)
)
UPDATE tasks 
SET deleted_at = NOW(),
    updated_at = NOW()
WHERE id IN (SELECT id FROM duplicates_to_delete);

-- Step 3: Verify cleanup worked
SELECT 
    title,
    COUNT(*) as count
FROM tasks 
WHERE deleted_at IS NULL
GROUP BY title 
HAVING COUNT(*) > 1;
-- Should return 0 rows if cleanup was successful

-- Step 4: Count results
SELECT 
    'Total active tasks' as metric,
    COUNT(*) as count
FROM tasks 
WHERE deleted_at IS NULL
UNION ALL
SELECT 
    'Soft-deleted duplicates' as metric,
    COUNT(*) as count
FROM tasks 
WHERE deleted_at IS NOT NULL 
  AND deleted_at > NOW() - INTERVAL '1 hour'
UNION ALL
SELECT
    'Chat messages table created' as metric,
    1 as count
WHERE EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'chat_messages');
