-- Extend anki_cards table to support articles
-- Cards can now be generated from article highlights

-- =============================================================================
-- ADD ARTICLE COLUMN TO ANKI_CARDS
-- =============================================================================
ALTER TABLE anki_cards ADD COLUMN IF NOT EXISTS article_id UUID REFERENCES articles(id) ON DELETE CASCADE;

-- Index for article-based cards
CREATE INDEX IF NOT EXISTS idx_anki_cards_article_id ON anki_cards(article_id);

-- =============================================================================
-- COMMENTS
-- =============================================================================
COMMENT ON COLUMN anki_cards.article_id IS 'Link to article (alternative to book_id for article-based cards)';
