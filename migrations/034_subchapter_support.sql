-- Sub-Chapter Support for EPUB Enhancement
-- Adds hierarchical chapter structure (Book -> Chapter -> Sub-chapter)
-- and book-level word count for reading assessment.

-- =============================================================================
-- ADD WORD COUNT TO BOOKS TABLE
-- =============================================================================

-- Total word count for reading time assessment
ALTER TABLE books ADD COLUMN IF NOT EXISTS word_count INTEGER;

COMMENT ON COLUMN books.word_count IS 'Total word count of the book (sum of all chapter word counts)';

-- Index for filtering/sorting by length
CREATE INDEX IF NOT EXISTS idx_books_word_count ON books(word_count);

-- =============================================================================
-- ADD SUB-CHAPTER SUPPORT TO BOOK_CHAPTERS TABLE
-- =============================================================================

-- Enable hierarchical chapters (sub-chapters reference their parent)
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS parent_chapter_id UUID
    REFERENCES book_chapters(id) ON DELETE CASCADE;

-- Order within parent chapter (1, 2, 3...)
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS subchapter_number INTEGER;

-- Context bridge: what reader needs to know from previous sections
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS context_bridge TEXT;

-- Original section header text from EPUB (h2/h3)
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS section_header TEXT;

-- How sub-chapter was detected: 'header' (parsed h2/h3) or 'ai' (LLM-identified)
ALTER TABLE book_chapters ADD COLUMN IF NOT EXISTS detection_method TEXT;

-- Index for efficient parent lookups
CREATE INDEX IF NOT EXISTS idx_book_chapters_parent ON book_chapters(parent_chapter_id);

-- Comments
COMMENT ON COLUMN book_chapters.parent_chapter_id IS 'Parent chapter ID for sub-chapters (NULL for top-level chapters)';
COMMENT ON COLUMN book_chapters.subchapter_number IS 'Order within parent chapter (1, 2, 3...)';
COMMENT ON COLUMN book_chapters.context_bridge IS 'What reader needs to know from previous sections to understand this one';
COMMENT ON COLUMN book_chapters.section_header IS 'Original section header text from EPUB (h2/h3)';
COMMENT ON COLUMN book_chapters.detection_method IS 'How sub-chapter was detected: header (parsed h2/h3) or ai (LLM-identified)';

-- =============================================================================
-- UPDATE VIEW TO INCLUDE SUB-CHAPTER INFO
-- =============================================================================

CREATE OR REPLACE VIEW books_with_epub_status AS
SELECT
    b.id,
    b.title,
    b.author,
    b.status,
    b.progress_percent,
    b.word_count,
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
    COUNT(DISTINCT CASE WHEN c.parent_chapter_id IS NULL THEN c.id END) as chapter_count,
    COUNT(DISTINCT CASE WHEN c.parent_chapter_id IS NOT NULL THEN c.id END) as subchapter_count,
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

COMMENT ON VIEW books_with_epub_status IS 'Books with EPUB status, enhancement status, word count, and content counts including sub-chapters';
