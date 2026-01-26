-- Articles Knowledge Base Schema
-- Stores captured online articles for reading, highlighting, and LLM analysis

-- =============================================================================
-- ARTICLES TABLE - Stores article metadata and full content
-- =============================================================================
CREATE TABLE IF NOT EXISTS articles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Article information
    title TEXT NOT NULL,
    author TEXT,                                -- Author name(s)
    source_name TEXT,                           -- "Substack", "Medium", "gwern.net", etc.
    source_type TEXT DEFAULT 'article',         -- 'article', 'blog', 'newsletter', 'paper'
    url TEXT UNIQUE,                            -- Original URL (unique to prevent duplicates)

    -- Reading status (mirrors books pattern)
    status TEXT DEFAULT 'To Read',              -- 'To Read', 'Reading', 'Finished', 'Archived'
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),

    -- Content
    summary TEXT,                               -- AI-generated or manual summary
    notes TEXT,                                 -- Personal notes
    tags TEXT[],                                -- Categories/topics
    full_text TEXT,                             -- Complete article content for LLM analysis
    word_count INTEGER,                         -- For statistics

    -- Bookfusion integration
    bookfusion_id TEXT UNIQUE,                  -- Bookfusion upload ID after upload
    bookfusion_url TEXT,                        -- URL in Bookfusion (if available)

    -- Notion sync (like books)
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMPTZ,
    last_sync_source TEXT,                      -- 'supabase', 'notion', 'bookfusion'

    -- Timestamps
    published_at TIMESTAMPTZ,                   -- Original article publish date
    captured_at TIMESTAMPTZ DEFAULT NOW(),      -- When captured into system
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ                      -- Soft delete
);

-- Indexes for articles
CREATE INDEX IF NOT EXISTS idx_articles_status ON articles(status);
CREATE INDEX IF NOT EXISTS idx_articles_source_type ON articles(source_type);
CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);
CREATE INDEX IF NOT EXISTS idx_articles_bookfusion_id ON articles(bookfusion_id);
CREATE INDEX IF NOT EXISTS idx_articles_notion_page_id ON articles(notion_page_id);
CREATE INDEX IF NOT EXISTS idx_articles_captured_at ON articles(captured_at);
CREATE INDEX IF NOT EXISTS idx_articles_source_name ON articles(source_name);

-- =============================================================================
-- COMMENTS
-- =============================================================================
COMMENT ON TABLE articles IS 'Online articles captured for reading and highlighting in Bookfusion';
COMMENT ON COLUMN articles.full_text IS 'Complete article text for LLM queries and RAG indexing';
COMMENT ON COLUMN articles.bookfusion_id IS 'Bookfusion upload ID - used to link highlights back to articles';
COMMENT ON COLUMN articles.source_name IS 'Platform name like Substack, Medium, personal blog domain';
COMMENT ON COLUMN articles.source_type IS 'Content type: article (general), blog (personal), newsletter (subscription), paper (academic)';
