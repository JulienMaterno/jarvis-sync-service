"""
Sub-Chapter Detection for EPUB Book Enhancement

Detects logical sub-sections within book chapters using:
1. Header-based detection (fast, deterministic) - parses ## headers from markdown
2. AI-based detection (fallback) - uses LLM to identify logical section boundaries

Used by the book enhancement pipeline to create three-level hierarchy:
Book -> Chapter -> Sub-chapter
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class SubChapter:
    """Represents a detected sub-chapter within a chapter."""
    number: int              # Order within parent (1, 2, 3...)
    title: str               # Section title
    content: str             # Full text content
    word_count: int          # Number of words
    section_header: str      # Original header text from EPUB
    detection_method: str    # 'header' or 'ai'
    start_pos: int = 0       # Start position in parent content
    end_pos: int = 0         # End position in parent content


def detect_subchapters_from_headers(
    chapter_content: str,
    min_words: int = 300,
    header_level: str = '##'
) -> list[SubChapter]:
    """
    Parse markdown content for headers to identify sub-sections.

    The EPUB parser preserves h2/h3 headers as ## in markdown format.
    This function finds those headers and splits content into sections.

    Args:
        chapter_content: Markdown content of the chapter
        min_words: Minimum word count for a section (default 300)
        header_level: Header pattern to match (default '##' for h2)

    Returns:
        List of SubChapter objects, empty if fewer than 2 sections found
    """
    # Match ## headers at start of line
    # Escape the header_level in case it has special regex chars
    escaped_header = re.escape(header_level)
    header_pattern = rf'^{escaped_header}\s+(.+)$'

    headers = list(re.finditer(header_pattern, chapter_content, re.MULTILINE))

    if len(headers) < 2:
        logger.debug(f"Found only {len(headers)} headers, need at least 2 for sub-chapters")
        return []

    sections: list[SubChapter] = []

    for i, match in enumerate(headers):
        title = match.group(1).strip()
        start_pos = match.start()

        # End position is start of next header or end of content
        end_pos = headers[i + 1].start() if i + 1 < len(headers) else len(chapter_content)

        section_content = chapter_content[start_pos:end_pos].strip()
        word_count = len(section_content.split())

        # Skip very short sections (likely just headers or transitions)
        if word_count < min_words:
            logger.debug(f"Skipping short section '{title}' ({word_count} words < {min_words})")
            continue

        sections.append(SubChapter(
            number=len(sections) + 1,
            title=title,
            content=section_content,
            word_count=word_count,
            section_header=title,
            detection_method='header',
            start_pos=start_pos,
            end_pos=end_pos
        ))

    # Renumber after filtering
    for i, section in enumerate(sections):
        section.number = i + 1

    logger.info(f"Detected {len(sections)} sub-chapters from headers")
    return sections


def detect_subchapters_with_ai(
    chapter_content: str,
    chapter_title: str,
    book_title: str,
    anthropic_client,
    min_sections: int = 3,
    max_sections: int = 8
) -> list[SubChapter]:
    """
    Use AI to identify logical section boundaries in headerless chapters.

    This is a fallback for chapters that don't have explicit ## headers
    but are long enough to benefit from sub-chapter splitting.

    Args:
        chapter_content: Full text content of the chapter
        chapter_title: Title of the chapter
        book_title: Title of the book
        anthropic_client: Anthropic API client
        min_sections: Minimum number of sections to identify
        max_sections: Maximum number of sections to identify

    Returns:
        List of SubChapter objects based on AI-identified boundaries
    """
    word_count = len(chapter_content.split())

    # Truncate if too long (keep first 8000 words for context)
    truncated_content = chapter_content
    words = chapter_content.split()
    if len(words) > 8000:
        truncated_content = ' '.join(words[:8000]) + '\n\n[...chapter continues...]'

    prompt = f"""Identify {min_sections}-{max_sections} logical sections in this chapter.

BOOK: {book_title}
CHAPTER: {chapter_title}
WORD COUNT: {word_count}

CHAPTER CONTENT:
{truncated_content}

For each section, provide:
1. A descriptive title (2-6 words) that captures the section's main topic
2. The EXACT first sentence of the section (for boundary detection)
3. Estimated word count

Look for:
- Topic shifts or new arguments being introduced
- Narrative breaks or transitions
- New examples or case studies beginning
- Conceptual divisions in the author's argument

Each section should be substantial (500+ words preferred). Don't split at every paragraph.

Return JSON:
```json
{{
  "sections": [
    {{
      "title": "Section title here",
      "first_sentence": "The exact first sentence of this section...",
      "estimated_words": 1500
    }}
  ],
  "reasoning": "Brief explanation of how you identified these boundaries"
}}
```
"""

    try:
        response = anthropic_client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}]
        )

        text = response.content[0].text

        # Extract JSON from response
        json_match = re.search(r'```json\s*(\{[\s\S]*?\})\s*```', text)
        if not json_match:
            logger.warning("Could not extract JSON from AI response")
            return []

        data = json.loads(json_match.group(1))
        ai_sections = data.get('sections', [])

        if not ai_sections:
            return []

        # Convert AI output to SubChapter objects by finding boundaries
        sections: list[SubChapter] = []

        for i, ai_section in enumerate(ai_sections):
            title = ai_section.get('title', f'Section {i + 1}')
            first_sentence = ai_section.get('first_sentence', '')

            # Find the position of this section's first sentence
            start_pos = chapter_content.find(first_sentence)
            if start_pos == -1:
                # Try partial match (first 50 chars)
                partial = first_sentence[:50] if len(first_sentence) > 50 else first_sentence
                start_pos = chapter_content.find(partial)

            if start_pos == -1:
                logger.warning(f"Could not find section '{title}' in content")
                continue

            # End position is start of next section or end of content
            end_pos = len(chapter_content)
            if i + 1 < len(ai_sections):
                next_sentence = ai_sections[i + 1].get('first_sentence', '')
                next_pos = chapter_content.find(next_sentence)
                if next_pos > start_pos:
                    end_pos = next_pos

            section_content = chapter_content[start_pos:end_pos].strip()
            word_count = len(section_content.split())

            sections.append(SubChapter(
                number=len(sections) + 1,
                title=title,
                content=section_content,
                word_count=word_count,
                section_header=title,
                detection_method='ai',
                start_pos=start_pos,
                end_pos=end_pos
            ))

        # Renumber after any filtering
        for i, section in enumerate(sections):
            section.number = i + 1

        logger.info(f"AI detected {len(sections)} sub-chapters")
        return sections

    except Exception as e:
        logger.error(f"AI sub-chapter detection failed: {e}")
        return []


def detect_subchapters(
    chapter_content: str,
    chapter_title: str,
    book_title: str,
    chapter_word_count: int,
    anthropic_client=None,
    min_chapter_words: int = 5000,
    min_section_words: int = 300
) -> list[SubChapter]:
    """
    Main entry point for sub-chapter detection.

    Uses a two-phase approach:
    1. First tries header-based detection (fast, deterministic)
    2. Falls back to AI detection for long chapters without headers

    Args:
        chapter_content: Markdown content of the chapter
        chapter_title: Title of the chapter
        book_title: Title of the book
        chapter_word_count: Total word count of the chapter
        anthropic_client: Optional Anthropic client for AI fallback
        min_chapter_words: Minimum chapter size to consider splitting (default 5000)
        min_section_words: Minimum section size (default 300)

    Returns:
        List of SubChapter objects, empty if no meaningful split possible
    """
    # Skip short chapters
    if chapter_word_count < min_chapter_words:
        logger.debug(f"Chapter too short for sub-chapters ({chapter_word_count} < {min_chapter_words})")
        return []

    # Phase 1: Try header-based detection
    sections = detect_subchapters_from_headers(
        chapter_content,
        min_words=min_section_words
    )

    if len(sections) >= 2:
        return sections

    # Phase 2: AI fallback for headerless chapters
    if anthropic_client and chapter_word_count >= min_chapter_words:
        logger.info(f"No headers found, trying AI detection for '{chapter_title}'")
        sections = detect_subchapters_with_ai(
            chapter_content,
            chapter_title,
            book_title,
            anthropic_client
        )

    return sections
