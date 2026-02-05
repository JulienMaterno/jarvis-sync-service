"""
Chapter Filtering Utilities

Shared logic for identifying content vs non-content chapters in books.
Used by both the book enhancement pipeline and Anki card generation.
"""

from typing import Union

# Titles that indicate non-content chapters (front/back matter)
NON_CONTENT_TITLES = {
    # Front matter
    'copyright', 'copyright page', 'contents', 'table of contents', 'toc',
    'title page', 'half title', 'frontispiece', 'dedication', 'epigraph',
    'also by', 'other books by', 'praise for', 'advance praise', 'reviews',

    # Back matter
    'notes', 'endnotes', 'footnotes', 'bibliography', 'references',
    'further reading', 'index', 'glossary', 'appendix', 'appendices',
    'about the author', 'about the authors', 'acknowledgments', 'acknowledgements',
    'credits', 'permissions', 'colophon', 'afterword',
}

# Minimum word count for a chapter to be considered "content"
MIN_CONTENT_WORDS = 300


def is_content_chapter(
    chapter_title: str,
    word_count: int,
    min_words: int = MIN_CONTENT_WORDS
) -> bool:
    """
    Check if a chapter is actual content (not front/back matter).

    Args:
        chapter_title: Title of the chapter
        word_count: Number of words in the chapter
        min_words: Minimum word count threshold (default 300)

    Returns:
        True if this appears to be a content chapter
    """
    # Check word count
    if word_count < min_words:
        return False

    # Check title against non-content patterns
    title_lower = (chapter_title or '').lower().strip()

    # Exact match
    if title_lower in NON_CONTENT_TITLES:
        return False

    # Starts with non-content prefix
    for non_content in NON_CONTENT_TITLES:
        if title_lower.startswith(non_content):
            return False

    return True


def filter_content_chapters(
    chapters: list,
    title_key: str = 'chapter_title',
    word_count_key: str = 'word_count',
    min_words: int = MIN_CONTENT_WORDS
) -> tuple[list, int]:
    """
    Filter a list of chapters to only content chapters.

    Works with both dictionaries and objects with attributes.

    Args:
        chapters: List of chapter objects/dicts
        title_key: Key/attribute name for chapter title
        word_count_key: Key/attribute name for word count
        min_words: Minimum word count threshold

    Returns:
        Tuple of (filtered_chapters, skipped_count)
    """
    def get_value(obj, key):
        """Get value from dict or object attribute."""
        if isinstance(obj, dict):
            return obj.get(key)
        return getattr(obj, key, None)

    content_chapters = []
    for ch in chapters:
        title = get_value(ch, title_key) or get_value(ch, 'title') or ''
        words = get_value(ch, word_count_key) or 0

        if is_content_chapter(title, words, min_words):
            content_chapters.append(ch)

    skipped = len(chapters) - len(content_chapters)
    return content_chapters, skipped
