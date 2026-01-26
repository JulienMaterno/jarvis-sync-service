-- Extend highlights table to support articles
-- Highlights can now belong to either a book OR an article

-- =============================================================================
-- ADD ARTICLE COLUMNS TO HIGHLIGHTS
-- =============================================================================
ALTER TABLE highlights ADD COLUMN IF NOT EXISTS article_id UUID REFERENCES articles(id) ON DELETE CASCADE;
ALTER TABLE highlights ADD COLUMN IF NOT EXISTS article_title TEXT;

-- Index for article highlights
CREATE INDEX IF NOT EXISTS idx_highlights_article_id ON highlights(article_id);

-- =============================================================================
-- COMMENTS
-- =============================================================================
COMMENT ON COLUMN highlights.article_id IS 'Link to article (alternative to book_id for article highlights)';
COMMENT ON COLUMN highlights.article_title IS 'Denormalized article title for convenience';

-- =============================================================================
-- NOTE ON CONSTRAINTS
-- =============================================================================
-- We intentionally do NOT add a check constraint requiring either book_id OR article_id
-- because highlights sync from Notion first (with book_title) and are linked later.
-- The link_highlights_to_articles.py script handles matching unlinked highlights.
