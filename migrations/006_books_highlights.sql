-- Books and Highlights Integration Schema
-- Syncs reading data from Notion (Content and Highlights databases)

-- =============================================================================
-- BOOKS TABLE - Tracks reading list and progress
-- =============================================================================
CREATE TABLE IF NOT EXISTS books (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Book information
    title TEXT NOT NULL,
    author TEXT,                                -- Author name (denormalized for convenience)
    author_id UUID,                             -- Future: link to authors table
    
    -- Reading status
    status TEXT DEFAULT 'To Read',              -- 'To Read', 'Reading', 'Finished', 'Abandoned'
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),  -- 1-5 star rating
    
    -- Progress tracking
    current_page INTEGER DEFAULT 0,
    total_pages INTEGER,
    progress_percent FLOAT GENERATED ALWAYS AS (
        CASE WHEN total_pages > 0 THEN ROUND((current_page::float / total_pages * 100)::numeric, 1)
        ELSE 0 END
    ) STORED,
    
    -- Dates
    started_at DATE,                            -- When started reading
    finished_at DATE,                           -- When finished
    
    -- Content
    summary TEXT,                               -- AI-generated or manual summary
    notes TEXT,                                 -- Personal notes
    tags TEXT[],                                -- Categories/genres
    
    -- External links
    cover_url TEXT,                             -- Book cover image URL
    goodreads_url TEXT,                         -- Goodreads link
    amazon_url TEXT,                            -- Amazon link
    
    -- Notion sync
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMPTZ,
    last_sync_source TEXT,                      -- 'notion' or 'supabase'
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ                      -- Soft delete
);

-- Indexes for books
CREATE INDEX IF NOT EXISTS idx_books_status ON books(status);
CREATE INDEX IF NOT EXISTS idx_books_notion_page_id ON books(notion_page_id);
CREATE INDEX IF NOT EXISTS idx_books_author ON books(author);
CREATE INDEX IF NOT EXISTS idx_books_rating ON books(rating);

-- =============================================================================
-- HIGHLIGHTS TABLE - Stores book highlights/annotations
-- =============================================================================
CREATE TABLE IF NOT EXISTS highlights (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Link to book
    book_id UUID REFERENCES books(id) ON DELETE CASCADE,
    book_title TEXT,                            -- Denormalized for convenience
    
    -- Highlight content
    content TEXT NOT NULL,                      -- The highlighted text
    note TEXT,                                  -- Personal annotation/thought
    
    -- Location in book
    page_number INTEGER,
    chapter TEXT,
    location TEXT,                              -- Kindle location or other reference
    
    -- Categorization
    highlight_type TEXT DEFAULT 'highlight',    -- 'highlight', 'note', 'quote', 'insight'
    tags TEXT[],                                -- Categories for filtering
    is_favorite BOOLEAN DEFAULT FALSE,          -- Mark important highlights
    
    -- Notion sync
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMPTZ,
    last_sync_source TEXT,
    
    -- Metadata
    highlighted_at TIMESTAMPTZ,                 -- When the highlight was made
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

-- Indexes for highlights
CREATE INDEX IF NOT EXISTS idx_highlights_book_id ON highlights(book_id);
CREATE INDEX IF NOT EXISTS idx_highlights_notion_page_id ON highlights(notion_page_id);
CREATE INDEX IF NOT EXISTS idx_highlights_is_favorite ON highlights(is_favorite);
CREATE INDEX IF NOT EXISTS idx_highlights_highlighted_at ON highlights(highlighted_at);
CREATE INDEX IF NOT EXISTS idx_highlights_type ON highlights(highlight_type);

-- =============================================================================
-- AUTHORS TABLE (Optional - for future use)
-- =============================================================================
CREATE TABLE IF NOT EXISTS authors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    name TEXT NOT NULL,
    bio TEXT,
    website_url TEXT,
    wikipedia_url TEXT,
    
    -- Notion sync
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMPTZ,
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_authors_name ON authors(name);
CREATE INDEX IF NOT EXISTS idx_authors_notion_page_id ON authors(notion_page_id);

-- =============================================================================
-- READING SESSIONS TABLE (Optional - for detailed tracking)
-- =============================================================================
CREATE TABLE IF NOT EXISTS reading_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    book_id UUID REFERENCES books(id) ON DELETE CASCADE,
    
    -- Session details
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    duration_minutes INTEGER,
    
    -- Progress
    pages_read INTEGER,
    start_page INTEGER,
    end_page INTEGER,
    
    -- Notes
    notes TEXT,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reading_sessions_book_id ON reading_sessions(book_id);
CREATE INDEX IF NOT EXISTS idx_reading_sessions_started_at ON reading_sessions(started_at);

-- =============================================================================
-- VIEWS FOR EASY QUERYING
-- =============================================================================

-- Currently reading books with progress
CREATE OR REPLACE VIEW currently_reading AS
SELECT 
    id,
    title,
    author,
    current_page,
    total_pages,
    progress_percent,
    started_at,
    EXTRACT(days FROM NOW() - started_at) as days_reading
FROM books
WHERE status = 'Reading'
    AND deleted_at IS NULL
ORDER BY started_at DESC;

-- Recent highlights (last 7 days)
CREATE OR REPLACE VIEW recent_highlights AS
SELECT 
    h.id,
    h.content,
    h.note,
    h.page_number,
    h.is_favorite,
    h.highlighted_at,
    b.title as book_title,
    b.author
FROM highlights h
JOIN books b ON h.book_id = b.id
WHERE h.highlighted_at >= NOW() - INTERVAL '7 days'
    AND h.deleted_at IS NULL
ORDER BY h.highlighted_at DESC;

-- Daily highlight count for journal prompts
CREATE OR REPLACE VIEW daily_highlight_stats AS
SELECT 
    DATE(highlighted_at) as date,
    COUNT(*) as highlight_count,
    COUNT(DISTINCT book_id) as books_with_highlights,
    ARRAY_AGG(DISTINCT b.title) as book_titles
FROM highlights h
JOIN books b ON h.book_id = b.id
WHERE h.deleted_at IS NULL
GROUP BY DATE(highlighted_at)
ORDER BY date DESC;

-- =============================================================================
-- COMMENTS
-- =============================================================================
COMMENT ON TABLE books IS 'Reading list synced from Notion Content database';
COMMENT ON TABLE highlights IS 'Book highlights/annotations synced from Notion Highlights database';
COMMENT ON TABLE authors IS 'Author information (optional, for future relation support)';
COMMENT ON TABLE reading_sessions IS 'Detailed reading session tracking (optional)';

COMMENT ON VIEW currently_reading IS 'Books currently being read with progress';
COMMENT ON VIEW recent_highlights IS 'Highlights from the last 7 days';
COMMENT ON VIEW daily_highlight_stats IS 'Daily highlight statistics for journal prompts';
