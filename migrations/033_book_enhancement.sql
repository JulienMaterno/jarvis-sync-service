-- Book Enhancement for Learning
-- Adds columns to support AI-generated chapter previews, learning questions,
-- and enhanced EPUB tracking for the reading enhancement pipeline.

-- =============================================================================
-- ADD ENHANCEMENT COLUMNS TO BOOKS TABLE
-- =============================================================================

-- Original EPUB (unmodified) stored separately from current drive_file_id
ALTER TABLE books ADD COLUMN IF NOT EXISTS original_drive_file_id TEXT;
ALTER TABLE books ADD COLUMN IF NOT EXISTS original_drive_url TEXT;

-- Enhanced EPUB with injected previews/questions
ALTER TABLE books ADD COLUMN IF NOT EXISTS enhanced_drive_file_id TEXT;
ALTER TABLE books ADD COLUMN IF NOT EXISTS enhanced_drive_url TEXT;

-- Bookfusion integration for Boox sync
ALTER TABLE books ADD COLUMN IF NOT EXISTS bookfusion_id TEXT;

-- Cover image URL (extracted from EPUB or fetched)
ALTER TABLE books ADD COLUMN IF NOT EXISTS cover_image_url TEXT;

-- Processing timestamp
ALTER TABLE books ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;

-- Indexes
CREATE INDEX IF NOT EXISTS idx_books_bookfusion_id ON books(bookfusion_id);
CREATE INDEX IF NOT EXISTS idx_books_processed_at ON books(processed_at);

-- Comments
COMMENT ON COLUMN books.original_drive_file_id IS 'Google Drive file ID for the original unmodified EPUB';
COMMENT ON COLUMN books.original_drive_url IS 'Direct link to original EPUB in Drive';
COMMENT ON COLUMN books.enhanced_drive_file_id IS 'Google Drive file ID for the enhanced EPUB with learning aids';
COMMENT ON COLUMN books.enhanced_drive_url IS 'Direct link to enhanced EPUB in Drive';
COMMENT ON COLUMN books.bookfusion_id IS 'Bookfusion book ID for Boox e-reader sync';
COMMENT ON COLUMN books.cover_image_url IS 'URL to book cover image';
COMMENT ON COLUMN books.processed_at IS 'When the book was processed through the enhancement pipeline';

-- =============================================================================
-- ADD ENHANCEMENT COLUMNS TO BOOK_CHAPTERS TABLE
-- =============================================================================

-- Chapter preview (injected at chapter start)
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS preview_summary TEXT;

-- Learning questions (injected at chapter end)
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS learning_questions JSONB;

-- Flag for leaf chapters (actual content vs section headers)
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS is_leaf_chapter BOOLEAN DEFAULT TRUE;

-- Track enhancement status
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS enhanced_at TIMESTAMPTZ;

-- Comments
COMMENT ON COLUMN book_chapters.preview_summary IS 'AI-generated preview of what the chapter covers (injected at start)';
COMMENT ON COLUMN book_chapters.learning_questions IS 'JSON array of retrieval practice questions (injected at end)';
COMMENT ON COLUMN book_chapters.is_leaf_chapter IS 'True if this is a real content chapter (not a section header)';
COMMENT ON COLUMN book_chapters.enhanced_at IS 'When AI enhancements were generated for this chapter';

-- =============================================================================
-- UPDATE VIEW TO INCLUDE ENHANCEMENT STATUS
-- =============================================================================

CREATE OR REPLACE VIEW books_with_epub_status AS
SELECT
    b.id,
    b.title,
    b.author,
    b.status,
    b.progress_percent,
    -- EPUB availability (Drive or legacy Supabase)
    CASE
        WHEN b.drive_file_id IS NOT NULL THEN TRUE
        WHEN b.epub_file IS NOT NULL THEN TRUE
        ELSE FALSE
    END as has_epub,
    -- Enhancement status
    CASE
        WHEN b.enhanced_drive_file_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END as has_enhanced_epub,
    b.epub_filename,
    b.epub_uploaded_at,
    b.drive_file_id,
    b.drive_url,
    b.enhanced_drive_file_id,
    b.enhanced_drive_url,
    b.bookfusion_id,
    b.cover_image_url,
    b.processed_at,
    b.epub_status,
    COUNT(DISTINCT c.id) as chapter_count,
    COUNT(DISTINCT CASE WHEN c.preview_summary IS NOT NULL THEN c.id END) as enhanced_chapter_count,
    COUNT(DISTINCT h.id) as highlight_count,
    COUNT(DISTINCT a.id) as anki_card_count
FROM books b
LEFT JOIN book_chapters c ON c.book_id = b.id
LEFT JOIN highlights h ON h.book_id = b.id AND h.deleted_at IS NULL
LEFT JOIN anki_cards a ON a.book_id = b.id AND a.deleted_at IS NULL
WHERE b.deleted_at IS NULL
GROUP BY b.id
ORDER BY b.updated_at DESC;

COMMENT ON VIEW books_with_epub_status IS 'Books with EPUB status, enhancement status, and content counts';
