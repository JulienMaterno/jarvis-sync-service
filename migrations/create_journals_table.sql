-- ============================================================================
-- JOURNAL ENTRIES TABLE
-- For daily journaling with mood, activities, and structured reflections
-- ============================================================================

-- Create the journals table
CREATE TABLE IF NOT EXISTS public.journals (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Core fields
    date DATE NOT NULL UNIQUE,  -- One entry per day
    title TEXT,                 -- Auto-generated like "Journal Entry - Dec 19"
    summary TEXT,               -- Quick AI-generated summary
    
    -- Daily tracking fields (from Notion)
    wakeup_time TEXT,           -- e.g., "6:00", "7:30"
    mood TEXT,                  -- e.g., "Great", "Good", "Okay", "Tired"
    effort TEXT,                -- e.g., "High", "Medium", "Low"
    nutrition TEXT,             -- e.g., "Good", "Okay", "Poor"
    sports TEXT[],              -- Array of activities: ["Running", "Gym", "Yoga"]
    
    -- Content
    note TEXT,                  -- Quick note/raw input from Notion
    content TEXT,               -- Full journal content (transcript or expanded)
    sections JSONB,             -- Structured sections from AI analysis
    
    -- AI-extracted insights
    key_events TEXT[],          -- Main things that happened
    accomplishments TEXT[],     -- What was achieved
    challenges TEXT[],          -- Difficulties faced
    gratitude TEXT[],           -- Things to be grateful for
    tomorrow_focus TEXT[],      -- Priorities for next day
    
    -- Links
    tasks_extracted UUID[],     -- Task IDs extracted from journal
    reflections_extracted UUID[], -- Reflection IDs if deeper thoughts extracted
    
    -- Source tracking
    source TEXT,                -- 'voice', 'notion', 'manual'
    source_file TEXT,           -- Original filename if from voice
    audio_duration_seconds INTEGER,
    transcript_id UUID,         -- Link to raw transcript
    
    -- Notion sync
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMPTZ,
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    last_sync_source TEXT       -- 'notion' or 'supabase' - who updated last
);

-- Index for date lookups (most common query)
CREATE INDEX IF NOT EXISTS idx_journals_date ON public.journals(date DESC);

-- Index for Notion sync
CREATE INDEX IF NOT EXISTS idx_journals_notion_page_id ON public.journals(notion_page_id) WHERE notion_page_id IS NOT NULL;

-- Index for non-deleted entries
CREATE INDEX IF NOT EXISTS idx_journals_active ON public.journals(date DESC) WHERE deleted_at IS NULL;

-- Trigger to update updated_at
CREATE OR REPLACE FUNCTION update_journals_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_journals_updated_at ON public.journals;
CREATE TRIGGER trigger_journals_updated_at
    BEFORE UPDATE ON public.journals
    FOR EACH ROW
    EXECUTE FUNCTION update_journals_updated_at();

-- Grant permissions (adjust role name as needed)
GRANT ALL ON public.journals TO authenticated;
GRANT ALL ON public.journals TO service_role;

-- ============================================================================
-- SAMPLE QUERIES
-- ============================================================================

-- Get today's journal
-- SELECT * FROM journals WHERE date = CURRENT_DATE;

-- Get journals for the week
-- SELECT * FROM journals WHERE date >= CURRENT_DATE - INTERVAL '7 days' ORDER BY date DESC;

-- Get journals with good mood
-- SELECT * FROM journals WHERE mood = 'Great' ORDER BY date DESC;

-- Get journals where I did a specific sport
-- SELECT * FROM journals WHERE 'Running' = ANY(sports) ORDER BY date DESC;

-- ============================================================================
-- VIEW: Journal summary for dashboard
-- ============================================================================

CREATE OR REPLACE VIEW journal_summary AS
SELECT 
    date,
    title,
    mood,
    effort,
    sports,
    summary,
    COALESCE(array_length(tasks_extracted, 1), 0) as tasks_count,
    COALESCE(array_length(key_events, 1), 0) as events_count
FROM journals
WHERE deleted_at IS NULL
ORDER BY date DESC;
