-- =============================================================================
-- APPLICATIONS TABLE
-- =============================================================================
-- Syncs with Notion "Applications" database (bfb77dff-9721-47b6-9bab-0cd0b315a298)
-- For tracking: Grants, Fellowships, Accelerators, Programs, Residencies
--
-- Notion Properties:
--   Name (title) -> name
--   Type (select) -> application_type
--   Status (select) -> status
--   Institution (rich_text) -> institution
--   Website (url) -> website
--   Grant Amount (rich_text) -> grant_amount
--   Deadline (date) -> deadline
--   Context (rich_text) -> context
--   Notes (rich_text) -> notes
--   [Page content] -> content (full page body text)
-- =============================================================================

CREATE TABLE IF NOT EXISTS applications (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Core fields from Notion
    name TEXT NOT NULL,
    application_type TEXT,  -- 'Grant', 'Fellowship', 'Program', 'Accelerator', 'Residency'
    status TEXT DEFAULT 'Not Started',  -- 'Not Started', 'Researching', 'In Progress', 'Applied', 'Accepted'
    institution TEXT,
    website TEXT,
    grant_amount TEXT,  -- Stored as text since it can be ranges like "$1K - $10K" or "?"
    deadline DATE,
    context TEXT,  -- Brief context about why this application
    notes TEXT,  -- Additional notes
    content TEXT,  -- Full page content (questions, answers, etc.)
    
    -- Notion sync tracking
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMPTZ,
    last_sync_source TEXT CHECK (last_sync_source IN ('notion', 'supabase')),
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ  -- Soft delete
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_applications_status ON applications(status);
CREATE INDEX IF NOT EXISTS idx_applications_type ON applications(application_type);
CREATE INDEX IF NOT EXISTS idx_applications_deadline ON applications(deadline);
CREATE INDEX IF NOT EXISTS idx_applications_notion_page_id ON applications(notion_page_id);
CREATE INDEX IF NOT EXISTS idx_applications_updated_at ON applications(updated_at);

-- Update trigger
CREATE OR REPLACE FUNCTION update_applications_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS applications_updated_at ON applications;
CREATE TRIGGER applications_updated_at
    BEFORE UPDATE ON applications
    FOR EACH ROW
    EXECUTE FUNCTION update_applications_updated_at();


-- =============================================================================
-- LINKEDIN_POSTS TABLE
-- =============================================================================
-- Syncs with Notion "LinkedIn Posts" database (2d1068b5-e624-81f2-8be0-fd6783c4763f)
-- For tracking LinkedIn content ideas and posted content
--
-- Notion Properties:
--   Name (title) -> title
--   Date (date) -> post_date
--   Status (select) -> status
--   Pillar (select) -> pillar
--   Likes (rich_text) -> likes
--   [Page content] -> content (full post text)
-- =============================================================================

CREATE TABLE IF NOT EXISTS linkedin_posts (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Core fields from Notion
    title TEXT NOT NULL,
    post_date DATE,
    status TEXT DEFAULT 'Idea',  -- 'Idea', 'Posted'
    pillar TEXT,  -- 'Personal', 'Longevity', 'Algenie'
    likes TEXT,  -- Stored as text since it can be formatted
    content TEXT,  -- Full post content
    
    -- Notion sync tracking
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMPTZ,
    last_sync_source TEXT CHECK (last_sync_source IN ('notion', 'supabase')),
    
    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ  -- Soft delete
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_linkedin_posts_status ON linkedin_posts(status);
CREATE INDEX IF NOT EXISTS idx_linkedin_posts_pillar ON linkedin_posts(pillar);
CREATE INDEX IF NOT EXISTS idx_linkedin_posts_post_date ON linkedin_posts(post_date);
CREATE INDEX IF NOT EXISTS idx_linkedin_posts_notion_page_id ON linkedin_posts(notion_page_id);
CREATE INDEX IF NOT EXISTS idx_linkedin_posts_updated_at ON linkedin_posts(updated_at);

-- Update trigger
CREATE OR REPLACE FUNCTION update_linkedin_posts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS linkedin_posts_updated_at ON linkedin_posts;
CREATE TRIGGER linkedin_posts_updated_at
    BEFORE UPDATE ON linkedin_posts
    FOR EACH ROW
    EXECUTE FUNCTION update_linkedin_posts_updated_at();


-- =============================================================================
-- Grant RLS policies (adjust as needed for your security model)
-- =============================================================================

-- Applications table
ALTER TABLE applications ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow all for authenticated users" ON applications;
CREATE POLICY "Allow all for authenticated users" ON applications
    FOR ALL
    USING (true)
    WITH CHECK (true);

-- LinkedIn Posts table
ALTER TABLE linkedin_posts ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow all for authenticated users" ON linkedin_posts;
CREATE POLICY "Allow all for authenticated users" ON linkedin_posts
    FOR ALL
    USING (true)
    WITH CHECK (true);
