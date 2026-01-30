#!/usr/bin/env python3
"""
Generate Anki cards from book highlights using chapter context.

This creates varied card types:
- Q&A: Direct questions about concepts
- Cloze: Fill-in-the-blank from highlight text
- Story Retell: Summarize what's happening in the chapter
- Reflection: Personal application prompts
- Chapter Summary: Generated when a chapter is completed
- Book Summary: Generated when a book is finished

Triggers:
- Weekly job processes new highlights without existing cards
- Chapter completion detected when highlight appears in next chapter
- Book completion detected when status changes to "Finished"

Usage:
    python generate_anki_cards.py                     # Generate for new highlights (weekly)
    python generate_anki_cards.py --book "Regenesis"  # Generate for specific book
    python generate_anki_cards.py --days 7            # Last 7 days of highlights
    python generate_anki_cards.py --check-completions # Check for completed chapters/books
    python generate_anki_cards.py --preview           # Preview without saving
"""

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
from supabase import create_client
import anthropic

load_dotenv()

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')

# =============================================================================
# CARD GENERATION CONFIGURATION
# =============================================================================

# Target books to generate cards for (partial title match)
# Set to None to process all non-fiction books
TARGET_BOOKS = [
    'Regenesis',
    'How to Avoid a Climate Disaster',
    'Where Good Ideas Come From',
    'The Culture Map',
    'Abundance',
]

# Tags/genres to exclude (fiction books)
EXCLUDED_GENRES = ['Fiction', 'Science Fiction', 'Fantasy', 'Novel', 'Thriller', 'Mystery']

def is_fiction_book(book_tags: list) -> bool:
    """Check if a book is fiction based on its tags/genres."""
    if not book_tags:
        return False
    return any(
        genre.lower() in [t.lower() for t in book_tags]
        for genre in EXCLUDED_GENRES
    )


CHAPTER_SUMMARY_PROMPT = """You are creating ONE Anki flashcard to capture the essence of a book chapter.

Your goal: Distill the entire chapter into a SINGLE powerful card that tests understanding of the main idea.

## Rules
- Create exactly 1 card - the chapter's core thesis or most important insight
- The question should be thought-provoking, not trivia
- CRITICAL: Keep the back/answer to 50-100 words MAX. Be concise!
- The answer should be self-contained - reader should understand without re-reading the book
- Introduce key people/concepts briefly if needed
- Focus on WHY this matters, not just WHAT happened

## Book: {book_title}
## Chapter: {chapter_title}

### Chapter Content:
{chapter_content}

### Highlights Made by Reader:
{highlights}

Create exactly 1 flashcard. The back MUST be 50-100 words - no longer!

```json
[
  {{
    "card_type": "chapter_summary",
    "front": "A thought-provoking question about the chapter's core idea",
    "back": "Concise answer in 50-100 words with key insight",
    "summary_type": "thesis"
  }}
]
```
"""

BOOK_SUMMARY_PROMPT = """You are creating Anki flashcards to help someone retain the key insights from a book they just finished.

Your goal is to capture the MOST IMPORTANT ideas that will stick with the reader for years. Focus on transformative insights, not trivia.

## Rules
- Create 5-7 cards maximum - only the truly essential ideas
- Cards must be self-contained and understandable without having read the book
- Introduce any people, places, or concepts mentioned (don't assume reader remembers names)
- Focus on insights that change how you think or act, not just interesting facts
- Ask questions that a curious person would genuinely want answered

## Card Types to Create

1. **Book Thesis** (1 card) - The book's main argument or transformative idea
2. **Key Framework** (1-2 cards) - Mental models that change how you see the world
3. **Memorable Story** (1-2 cards) - Stories/examples that make abstract ideas concrete
4. **Personal Application** (1-2 cards) - Practical ways to apply these ideas
5. **Connection** (0-1 card) - How this connects to other important ideas

## Book: {book_title}
## Author: {author}

### Chapters:
{chapter_summaries}

### All Highlights:
{highlights}

Create 5-7 flashcards. Each MUST be understandable without having read the book.
```json
[
  {{
    "card_type": "book_summary",
    "front": "...",
    "back": "...",
    "summary_type": "thesis|framework|example|application|connection"
  }}
]
```
"""

CARD_GENERATION_PROMPT = """You are reviewing highlights from a book to create Anki flashcards. Your job is to identify which highlights contain genuine INSIGHTS worth remembering.

## What Makes a Good Card
- Tests UNDERSTANDING, not memorization
- Captures a fact, story, framework, or insight the reader would want to remember
- Is self-contained - understandable without the book
- Introduces any names/concepts needed ("Tolly, an organic farmer who...")

## What to SKIP (return empty array)
- Highlights that are just nice prose without a memorable insight
- Biographical details that aren't relevant to the main ideas
- Context-setting passages that don't teach anything
- Quotes that only make sense in the book's context

## Card Types

1. **fact** - A surprising or important piece of information
   - "How much of humanity's calories come from just 4 crops?" → "Four crops (wheat, rice, maize, soybeans) provide the majority of calories..."

2. **insight** - A paradigm shift or key understanding
   - "Why does food system concentration create fragility?" → "Because all nodes behave similarly, so a shock to one affects all..."

3. **story** - A memorable example that illustrates a bigger point
   - "How did irrigation in India unexpectedly affect East Africa?" → "Water vapor from irrigated Indian fields travels..."

## Book: {book_title}
## Chapter: {chapter_title}

### Chapter Context:
{chapter_context}

### Highlighted Text:
"{highlight}"

### User's Note (if any):
{note}

Create 0-2 flashcards. Return empty array [] if no card-worthy content.

```json
[
  {{
    "card_type": "fact|insight|story",
    "front": "Question that a curious person would ask",
    "back": "Self-contained answer with full context"
  }}
]
```
"""

BATCH_CARD_GENERATION_PROMPT = """You are a master educator reviewing book highlights to create memorable Anki flashcards.

Your goal: Transform raw highlights into POWERFUL learning cards. You have FULL CREATIVE FREEDOM to:
- Bundle multiple related highlights into one comprehensive card
- Skip highlights that are just nice prose without real insight
- Create cards for surprising facts, paradigm shifts, or memorable stories
- Decide the optimal number of cards (could be 0, could be 5+)

## What Makes a GREAT Card
- Tests UNDERSTANDING, not trivia
- Would be valuable to remember 5 years from now
- Is completely self-contained (reader doesn't need the book)
- Introduces any people/concepts needed ("Tolly, an organic farmer who...")
- Back answer is 50-100 words MAX - be concise!

## What to SKIP
- Beautiful prose without a specific takeaway
- Background info that doesn't teach anything standalone
- Redundant points (combine into one card instead)
- Things only interesting in the book's context

## Card Types (choose what fits best)

1. **fact** - Surprising information worth remembering
   "What % of calories come from just 4 crops?" → "75%: wheat, rice, maize, soybeans..."

2. **insight** - A mental model or paradigm shift
   "Why does monoculture create fragility?" → "All nodes behave identically, so one shock affects all..."

3. **story** - A memorable example illustrating a bigger point
   "How did Indian irrigation unexpectedly affect East Africa?" → "Water vapor travels via monsoons..."

4. **concept** - A framework or definition worth internalizing
   "What is the 'adjacent possible'?" → "The set of next innovations unlocked by current capabilities..."

## Book: {book_title}
## Chapter: {chapter_title}

### Chapter Context:
{chapter_context}

### All Highlights from this Chapter:
{highlights}

USE YOUR JUDGMENT: Create as many (or few) cards as the content deserves.
Bundle related highlights. Skip fluff. Focus on what's genuinely worth remembering.

Return JSON array (empty [] if nothing card-worthy):

```json
[
  {{
    "card_type": "fact|insight|story",
    "front": "Question that a curious person would ask",
    "back": "Self-contained answer with full context",
    "highlight_ids": ["id1", "id2"]
  }}
]
```
"""


class AnkiCardGenerator:
    """Generates Anki cards from highlights using AI."""

    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.anthropic = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    def get_highlights_without_cards(
        self,
        book_title: str = None,
        days: int = None,
        limit: int = 50
    ) -> list[dict]:
        """Get highlights that don't have cards yet."""
        # Get highlight IDs that already have cards
        existing = self.supabase.table('anki_cards').select(
            'highlight_id'
        ).not_.is_('highlight_id', 'null').execute()
        existing_ids = {r['highlight_id'] for r in existing.data}

        # Get highlights with chapter context
        highlights = self.get_highlights_with_context(
            book_title=book_title,
            days=days,
            limit=limit * 2  # Get extra in case many already have cards
        )

        # Filter out those with existing cards
        new_highlights = [h for h in highlights if h['id'] not in existing_ids]
        return new_highlights[:limit]

    def get_chapters_needing_summaries(self, book_title: str = None) -> list[dict]:
        """
        Find chapters that are "unlocked" for summary generation.

        Progressive unlocking logic:
        - A chapter is unlocked if there's a highlight in a LATER chapter
          (meaning you've read past it)
        - OR if the book is marked as "Finished"/"Summarized"
        - The chapter must have content and not already have a summary card

        Args:
            book_title: Optional book title filter (partial match)

        Returns:
            List of chapters eligible for summary cards
        """
        # Get book filter if specified - do this FIRST to optimize the query
        book_filter_ids = None
        if book_title:
            books = self.supabase.table('books').select('id, title, tags').ilike(
                'title', f'%{book_title}%'
            ).execute().data
            # Filter out fiction books
            books = [b for b in books if not is_fiction_book(b.get('tags', []))]
            book_filter_ids = {b['id'] for b in books}
            if not book_filter_ids:
                return []  # No matching books
        elif TARGET_BOOKS:
            # Use target books configuration if no specific book requested
            all_target_books = []
            for target in TARGET_BOOKS:
                matches = self.supabase.table('books').select('id, title, tags').ilike(
                    'title', f'%{target}%'
                ).execute().data
                all_target_books.extend(matches)
            # Filter out fiction
            all_target_books = [b for b in all_target_books if not is_fiction_book(b.get('tags', []))]
            book_filter_ids = {b['id'] for b in all_target_books}
            if not book_filter_ids:
                return []

        # Get chapters with content - filter by book if specified to avoid 1000 row limit
        query = self.supabase.table('book_chapters').select(
            'id, book_id, chapter_number, chapter_title, content, word_count'
        ).not_.is_('content', 'null')

        if book_filter_ids:
            # Query for each book to avoid limit issues
            all_chapters = []
            for bid in book_filter_ids:
                chapters = query.eq('book_id', bid).execute().data
                all_chapters.extend(chapters)
        else:
            # No filter - paginate to get all chapters
            all_chapters = []
            offset = 0
            while True:
                batch = query.range(offset, offset + 999).execute().data
                if not batch:
                    break
                all_chapters.extend(batch)
                if len(batch) < 1000:
                    break
                offset += 1000

        # Filter to chapters with actual content (not just whitespace)
        all_chapters = [c for c in all_chapters if c.get('content') and len(c['content'].strip()) > 100]

        # Filter out non-content chapters (front/back matter)
        NON_CONTENT_TITLES = {
            'copyright', 'copyright page', 'contents', 'table of contents',
            'acknowledgments', 'acknowledgements', 'notes', 'index', 'endnotes',
            'about the author', 'about the authors', 'dedication', 'epigraph',
            'also by', 'title page', 'half title', 'frontispiece', 'colophon',
            'bibliography', 'references', 'further reading', 'glossary',
            'praise for', 'advance praise', 'reviews', 'credits', 'permissions'
        }
        all_chapters = [
            c for c in all_chapters
            if not any(
                non_content in (c.get('chapter_title') or '').lower()
                for non_content in NON_CONTENT_TITLES
            )
        ]

        # Get finished books (all chapters unlocked)
        finished_query = self.supabase.table('books').select('id').in_(
            'status', ['Finished', 'Summarized']
        )
        if book_filter_ids:
            finished_query = finished_query.in_('id', list(book_filter_ids))
        finished_books = {b['id'] for b in finished_query.execute().data}

        # Get all highlights with chapter_id to find reading progress
        highlights = self.supabase.table('highlights').select(
            'book_id, chapter_id'
        ).not_.is_('chapter_id', 'null').execute().data

        # Build map: book_id -> set of chapter_ids with highlights
        book_highlighted_chapters = {}
        for h in highlights:
            bid = h['book_id']
            if bid not in book_highlighted_chapters:
                book_highlighted_chapters[bid] = set()
            book_highlighted_chapters[bid].add(h['chapter_id'])

        # Build map: chapter_id -> chapter_number for lookup
        chapter_numbers = {c['id']: c['chapter_number'] for c in all_chapters}

        # For each book, find the max highlighted chapter number
        book_max_highlighted = {}
        for book_id, ch_ids in book_highlighted_chapters.items():
            max_num = max((chapter_numbers.get(cid, 0) for cid in ch_ids), default=0)
            book_max_highlighted[book_id] = max_num

        # Filter out chapters that already have summary cards (exclude soft-deleted)
        existing_summaries = self.supabase.table('anki_cards').select(
            'chapter_id'
        ).eq('card_type', 'chapter_summary').is_('deleted_at', 'null').execute()
        summarized_chapters = {r['chapter_id'] for r in existing_summaries.data if r['chapter_id']}

        # Determine which chapters are unlocked
        unlocked = []
        for ch in all_chapters:
            if ch['id'] in summarized_chapters:
                continue  # Already has summary

            book_id = ch['book_id']
            ch_num = ch['chapter_number']

            # Check if unlocked
            if book_id in finished_books:
                # Book is finished - all chapters unlocked
                unlocked.append(ch)
            elif book_id in book_max_highlighted:
                # Chapter is unlocked if it's BEFORE the max highlighted chapter
                # (meaning there's a highlight in a later chapter)
                if ch_num < book_max_highlighted[book_id]:
                    unlocked.append(ch)

        # Sort by book and chapter number
        unlocked.sort(key=lambda c: (c['book_id'], c['chapter_number']))

        return unlocked

    def get_completed_chapters(self, book_title: str = None) -> list[dict]:
        """
        Alias for get_chapters_needing_summaries for backwards compatibility.
        """
        return self.get_chapters_needing_summaries(book_title=book_title)

    def get_finished_books_without_summary(self, book_title: str = None) -> list[dict]:
        """Get books marked as Finished that don't have book summary cards.

        Args:
            book_title: Optional book title filter (partial match)
        """
        # Get finished books
        query = self.supabase.table('books').select(
            'id, title, author'
        ).in_('status', ['Finished', 'Summarized'])

        if book_title:
            query = query.ilike('title', f'%{book_title}%')

        finished = query.execute().data

        # Get books that already have summary cards
        existing = self.supabase.table('anki_cards').select(
            'book_id'
        ).eq('card_type', 'book_summary').execute()
        summarized_books = {r['book_id'] for r in existing.data if r['book_id']}

        return [b for b in finished if b['id'] not in summarized_books]

    def get_highlights_with_context(
        self,
        book_title: str = None,
        days: int = None,
        limit: int = 20,
        require_chapter_id: bool = False
    ) -> list[dict]:
        """Get highlights with their chapter context.

        Args:
            require_chapter_id: If True, only return highlights with chapter_id.
                              If False, also include highlights with just 'chapter' text field.
        """
        query = self.supabase.table('highlights').select(
            'id, content, note, chapter, book_id, book_title, chapter_id, highlighted_at'
        )

        if require_chapter_id:
            query = query.not_.is_('chapter_id', 'null')

        if book_title:
            query = query.ilike('book_title', f'%{book_title}%')
        elif TARGET_BOOKS:
            # Filter to target books only - need to get book_ids first
            target_book_ids = set()
            for target in TARGET_BOOKS:
                matches = self.supabase.table('books').select('id, tags').ilike(
                    'title', f'%{target}%'
                ).execute().data
                # Exclude fiction
                for b in matches:
                    if not is_fiction_book(b.get('tags', [])):
                        target_book_ids.add(b['id'])
            if target_book_ids:
                query = query.in_('book_id', list(target_book_ids))

        if days:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            query = query.gte('highlighted_at', cutoff)

        query = query.order('highlighted_at', desc=True).limit(limit)
        highlights = query.execute().data

        # Fetch chapter content for each highlight that has chapter_id
        for h in highlights:
            if h.get('chapter_id'):
                try:
                    chapter = self.supabase.table('book_chapters').select(
                        'chapter_title, content, word_count'
                    ).eq('id', h['chapter_id']).single().execute().data

                    if chapter:
                        h['chapter_title'] = chapter['chapter_title']
                        h['chapter_content'] = chapter['content']
                        h['chapter_word_count'] = chapter['word_count']
                except Exception:
                    pass  # Chapter not found, continue without content

            # Use 'chapter' text field as fallback for chapter_title
            if not h.get('chapter_title') and h.get('chapter'):
                h['chapter_title'] = h['chapter']

        return highlights

    def generate_cards_for_highlight(self, highlight: dict) -> list[dict]:
        """Generate Anki cards for a single highlight."""
        # Get context around the highlight, not just from the start
        chapter_context = highlight.get('chapter_content', '')
        highlight_text = highlight.get('content', '')

        # Find where the highlight appears in the chapter
        highlight_pos = chapter_context.lower().find(highlight_text.lower()[:50])

        words = chapter_context.split()
        max_context_words = 2000

        if len(words) > max_context_words:
            if highlight_pos > 0:
                # Estimate word position from character position
                chars_per_word = len(chapter_context) / len(words) if words else 50
                highlight_word_pos = int(highlight_pos / chars_per_word)

                # Center context around the highlight
                context_before = max_context_words // 3  # 1/3 before
                context_after = max_context_words - context_before  # 2/3 after

                start_word = max(0, highlight_word_pos - context_before)
                end_word = min(len(words), start_word + max_context_words)

                # Adjust if we hit the end
                if end_word == len(words):
                    start_word = max(0, end_word - max_context_words)

                prefix = '[...] ' if start_word > 0 else ''
                suffix = ' [...]' if end_word < len(words) else ''
                chapter_context = prefix + ' '.join(words[start_word:end_word]) + suffix
            else:
                # Highlight not found, fall back to start
                chapter_context = ' '.join(words[:max_context_words]) + '\n\n[...chapter continues...]'

        prompt = CARD_GENERATION_PROMPT.format(
            book_title=highlight.get('book_title', 'Unknown'),
            chapter_title=highlight.get('chapter_title', highlight.get('chapter', 'Unknown')),
            chapter_context=chapter_context,
            highlight=highlight.get('content', ''),
            note=highlight.get('note') or '(no note)'
        )

        response = self.anthropic.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )

        # Parse JSON from response
        import json
        import re

        text = response.content[0].text
        json_match = re.search(r'\[[\s\S]*\]', text)
        if json_match:
            cards = json.loads(json_match.group())
            # Add metadata
            for card in cards:
                card['book_id'] = highlight.get('book_id')
                card['chapter_id'] = highlight.get('chapter_id')
                card['highlight_id'] = highlight.get('id')
                card['tags'] = [
                    highlight.get('book_title', 'unknown').replace(' ', '_'),
                    card.get('card_type', 'qa')
                ]
            return cards
        return []

    def generate_cards_for_chapter_highlights(self, chapter_id: str, highlights: list[dict]) -> list[dict]:
        """
        Generate cards for ALL highlights in a chapter at once.
        This allows the LLM to combine related highlights and skip redundant ones.
        """
        if not highlights:
            return []

        # Get chapter info
        chapter = self.supabase.table('book_chapters').select(
            'id, book_id, chapter_title, content, word_count'
        ).eq('id', chapter_id).single().execute().data

        if not chapter:
            return []

        book = self.supabase.table('books').select(
            'title'
        ).eq('id', chapter['book_id']).single().execute().data

        # Format all highlights
        highlights_text = "\n\n".join([
            f"[{i+1}] ID: {h['id']}\n\"{h['content']}\""
            + (f"\nNote: {h.get('note')}" if h.get('note') else "")
            for i, h in enumerate(highlights)
        ])

        # Truncate chapter content if needed
        content = chapter['content']
        words = content.split()
        if len(words) > 3000:
            content = ' '.join(words[:3000]) + '\n\n[...chapter continues...]'

        prompt = BATCH_CARD_GENERATION_PROMPT.format(
            book_title=book['title'],
            chapter_title=chapter['chapter_title'],
            chapter_context=content,
            highlights=highlights_text
        )

        response = self.anthropic.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )

        import json
        import re

        # Build set of valid highlight IDs for validation
        valid_ids = {h['id'] for h in highlights}

        text = response.content[0].text

        # Try to extract JSON array - look for ```json block first
        json_block = re.search(r'```json\s*(\[[\s\S]*?\])\s*```', text)
        if json_block:
            json_str = json_block.group(1)
        else:
            # Fall back to finding array
            json_match = re.search(r'\[[\s\S]*?\]', text)
            json_str = json_match.group() if json_match else None

        if json_str:
            try:
                cards = json.loads(json_str)
            except json.JSONDecodeError:
                cards = []
            for card in cards:
                card['book_id'] = chapter['book_id']
                card['chapter_id'] = chapter_id
                # Validate and link highlight_ids
                highlight_ids = card.get('highlight_ids', [])
                # Filter to only valid IDs
                valid_highlight_ids = [hid for hid in highlight_ids if hid in valid_ids]
                if valid_highlight_ids:
                    card['highlight_id'] = valid_highlight_ids[0]
                else:
                    # Fall back to first highlight in the batch if no valid IDs
                    card['highlight_id'] = highlights[0]['id'] if highlights else None
                card['source_type'] = 'highlight'
                card['tags'] = [
                    book['title'].replace(' ', '_'),
                    'highlight',
                    card.get('card_type', 'insight')
                ]
            return cards
        return []

    def generate_cards_without_chapter_content(self, highlights: list[dict], chapter_name: str) -> list[dict]:
        """
        Generate cards for highlights when we don't have chapter content.
        Uses just the highlights themselves with book context.
        """
        if not highlights:
            return []

        book_title = highlights[0].get('book_title', 'Unknown Book')
        book_id = highlights[0].get('book_id')

        # Format all highlights
        highlights_text = "\n\n".join([
            f"[{i+1}] ID: {h['id']}\n\"{h['content']}\""
            + (f"\nNote: {h.get('note')}" if h.get('note') else "")
            for i, h in enumerate(highlights)
        ])

        # Modified prompt for when we don't have chapter content
        prompt = f"""You are reviewing highlights from a book to create Anki flashcards.
Each highlight was marked by the reader because it contained something valuable.

## What Makes a Good Card
- Tests UNDERSTANDING, not memorization
- Captures a fact, story, framework, or insight worth remembering
- Is self-contained - understandable without the book
- Introduces any names/concepts needed ("Tolly, an organic farmer who...")

## Card Types

1. **fact** - A surprising or important piece of information
2. **insight** - A paradigm shift or key understanding
3. **story** - A memorable example that illustrates a bigger point

## Book: {book_title}
## Chapter: {chapter_name}

### Highlights:
{highlights_text}

IMPORTANT: Create at least one card if there's any genuine insight. Even a single highlight
can make a valuable card. Only return empty array if the highlight is truly just prose
with no memorable content.

Create 1 card per meaningful highlight (combine only if truly redundant).

Return JSON array with highlight_ids field to track which highlights each card covers:

```json
[
  {{
    "card_type": "fact|insight|story",
    "front": "Question that a curious person would ask",
    "back": "Self-contained answer with full context",
    "highlight_ids": ["id1", "id2"]
  }}
]
```
"""

        response = self.anthropic.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )

        import json
        import re

        # Build set of valid highlight IDs for validation
        valid_ids = {h['id'] for h in highlights}

        text = response.content[0].text

        # Try to extract JSON array - look for ```json block first
        json_block = re.search(r'```json\s*(\[[\s\S]*?\])\s*```', text)
        if json_block:
            json_str = json_block.group(1)
        else:
            # Fall back to finding array
            json_match = re.search(r'\[[\s\S]*?\]', text)
            json_str = json_match.group() if json_match else None

        cards = []
        if json_str:
            try:
                parsed = json.loads(json_str)
                # Ensure we have a list of dicts
                if isinstance(parsed, list):
                    cards = [c for c in parsed if isinstance(c, dict)]
            except json.JSONDecodeError:
                cards = []

        # Clean book title for tags (remove subtitle after colon)
        clean_title = book_title.split(':')[0].strip().replace(' ', '_')

        for card in cards:
            card['book_id'] = book_id
            card['chapter_id'] = None  # No chapter_id available
            # Validate and link highlight_ids
            highlight_ids = card.get('highlight_ids', [])
            # Filter to only valid IDs
            valid_highlight_ids = [hid for hid in highlight_ids if hid in valid_ids]
            if valid_highlight_ids:
                card['highlight_id'] = valid_highlight_ids[0]
            else:
                # Fall back to first highlight in the batch if no valid IDs
                card['highlight_id'] = highlights[0]['id'] if highlights else None
            card['source_type'] = 'highlight'
            card['tags'] = [
                clean_title,
                'highlight',
                card.get('card_type', 'insight')
            ]
        return cards

    def process_highlights_by_chapter(
        self,
        highlights: list[dict],
        preview: bool = False
    ) -> dict:
        """
        Process highlights grouped by chapter for batch card generation.
        Returns dict with cards created and stats.

        Groups by chapter_id if available, otherwise by the 'chapter' text field.
        """
        # Group by chapter - use chapter_id if available, otherwise 'chapter' text
        by_chapter = {}
        no_chapter = []

        for h in highlights:
            chapter_id = h.get('chapter_id')
            chapter_text = h.get('chapter')

            if chapter_id:
                key = ('id', chapter_id)
            elif chapter_text:
                key = ('text', chapter_text)
            else:
                no_chapter.append(h)
                continue

            if key not in by_chapter:
                by_chapter[key] = []
            by_chapter[key].append(h)

        results = {
            'chapters_processed': 0,
            'cards_created': 0,
            'highlights_covered': 0,
            'cards': []
        }

        for (key_type, key_value), chapter_highlights in by_chapter.items():
            chapter_name = key_value if key_type == 'text' else 'chapter'
            print(f"  Processing {len(chapter_highlights)} highlights from {chapter_name}...")

            if preview:
                # In preview mode, just estimate
                results['chapters_processed'] += 1
                results['highlights_covered'] += len(chapter_highlights)
                continue

            if key_type == 'id':
                # Use chapter content from book_chapters table
                cards = self.generate_cards_for_chapter_highlights(key_value, chapter_highlights)
            else:
                # No chapter_id - generate cards using just the highlights (no chapter content)
                cards = self.generate_cards_without_chapter_content(chapter_highlights, key_value)

            if cards:
                # Get or create deck for the book
                book_title = chapter_highlights[0]['book_title']
                deck_id = self.get_or_create_book_deck(book_title)

                saved = self.save_cards(cards, deck_id)
                results['cards_created'] += saved
                results['cards'].extend(cards)

            results['chapters_processed'] += 1
            results['highlights_covered'] += len(chapter_highlights)

            for card in cards:
                print(f"    -> [{card.get('card_type', 'unknown')}] {card['front'][:60]}...")

        # Handle highlights without any chapter info
        if no_chapter:
            print(f"  Processing {len(no_chapter)} highlights without chapter info...")
            if not preview:
                cards = self.generate_cards_without_chapter_content(no_chapter, 'Unknown Chapter')
                if cards:
                    book_title = no_chapter[0]['book_title']
                    deck_id = self.get_or_create_book_deck(book_title)
                    saved = self.save_cards(cards, deck_id)
                    results['cards_created'] += saved
                    results['cards'].extend(cards)

                    for card in cards:
                        print(f"    -> [{card.get('card_type', 'unknown')}] {card['front'][:60]}...")

            results['highlights_covered'] += len(no_chapter)

        return results

    def save_cards(self, cards: list[dict], deck_id: str = None) -> int:
        """Save generated cards to database."""
        if not cards:
            return 0

        # Transform cards to match table schema
        db_cards = []
        for card in cards:
            db_card = {
                'front': card['front'],
                'back': card['back'],
                'card_type': card.get('card_type', 'qa'),
                'tags': card.get('tags', []),
                'book_id': card.get('book_id'),
                'chapter_id': card.get('chapter_id'),
                'highlight_id': card.get('highlight_id'),
                'source_type': card.get('source_type', 'highlight'),
                'source_id': card.get('highlight_id') or card.get('chapter_id') or card.get('book_id'),
                'metadata': {
                    'difficulty': card.get('difficulty', 'medium'),
                    'summary_type': card.get('summary_type')
                }
            }
            if deck_id:
                db_card['deck_id'] = deck_id
            db_cards.append(db_card)

        # Insert cards
        self.supabase.table('anki_cards').insert(db_cards).execute()
        return len(db_cards)

    def generate_chapter_summary_cards(self, chapter: dict) -> list[dict]:
        """Generate summary cards for a completed chapter."""
        # Get chapter content
        chapter_data = self.supabase.table('book_chapters').select(
            'id, book_id, chapter_title, content, word_count'
        ).eq('id', chapter['id']).single().execute().data

        # Get book info
        book = self.supabase.table('books').select(
            'title'
        ).eq('id', chapter_data['book_id']).single().execute().data

        # Get highlights for this chapter
        highlights = self.supabase.table('highlights').select(
            'content, note'
        ).eq('chapter_id', chapter['id']).execute().data

        highlights_text = "\n".join([
            f"- \"{h['content']}\"" + (f" [Note: {h['note']}]" if h.get('note') else "")
            for h in highlights
        ]) or "(No highlights)"

        # Truncate chapter content if too long
        content = chapter_data['content']
        words = content.split()
        if len(words) > 4000:
            content = ' '.join(words[:4000]) + '\n\n[...chapter continues...]'

        prompt = CHAPTER_SUMMARY_PROMPT.format(
            book_title=book['title'],
            chapter_title=chapter_data['chapter_title'],
            chapter_content=content,
            highlights=highlights_text
        )

        response = self.anthropic.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )

        import json
        import re

        text = response.content[0].text
        json_match = re.search(r'\[[\s\S]*\]', text)
        if json_match:
            cards = json.loads(json_match.group())
            for card in cards:
                card['book_id'] = chapter_data['book_id']
                card['chapter_id'] = chapter['id']
                card['source_type'] = 'chapter'
                card['tags'] = [
                    book['title'].replace(' ', '_'),
                    'chapter_summary',
                    card.get('summary_type', 'concept')
                ]
            return cards
        return []

    def generate_book_summary_cards(self, book: dict) -> list[dict]:
        """Generate summary cards for a finished book."""
        # Get all chapters
        chapters = self.supabase.table('book_chapters').select(
            'chapter_number, chapter_title, word_count'
        ).eq('book_id', book['id']).order('chapter_number').execute().data

        chapter_summaries = "\n".join([
            f"{c['chapter_number']}. {c['chapter_title']} ({c['word_count']} words)"
            for c in chapters
        ])

        # Get all highlights
        highlights = self.supabase.table('highlights').select(
            'content, note, chapter'
        ).eq('book_id', book['id']).execute().data

        highlights_text = "\n".join([
            f"[{h.get('chapter', 'Unknown')}] \"{h['content']}\""
            + (f" - Note: {h['note']}" if h.get('note') else "")
            for h in highlights[:50]  # Limit to 50 highlights
        ]) or "(No highlights)"

        prompt = BOOK_SUMMARY_PROMPT.format(
            book_title=book['title'],
            author=book.get('author', 'Unknown'),
            chapter_summaries=chapter_summaries,
            highlights=highlights_text
        )

        response = self.anthropic.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )

        import json
        import re

        text = response.content[0].text
        json_match = re.search(r'\[[\s\S]*\]', text)
        if json_match:
            cards = json.loads(json_match.group())
            for card in cards:
                card['book_id'] = book['id']
                card['source_type'] = 'book'
                card['tags'] = [
                    book['title'].replace(' ', '_'),
                    'book_summary',
                    card.get('summary_type', 'concept')
                ]
            return cards
        return []

    def process_completions(self, preview: bool = False, book_title: str = None) -> dict:
        """Process chapters and books, generating summary cards.

        Args:
            preview: If True, only show what would be generated
            book_title: Optional book title filter (partial match)
        """
        results = {
            'chapters_processed': 0,
            'chapter_cards': 0,
            'books_processed': 0,
            'book_cards': 0
        }

        # Process chapters needing summaries
        print("\n--- Checking for Chapters Needing Summaries ---")
        chapters_to_process = self.get_chapters_needing_summaries(book_title=book_title)
        print(f"Found {len(chapters_to_process)} chapters needing summaries")

        for chapter in chapters_to_process:
            book = self.supabase.table('books').select('title').eq(
                'id', chapter['book_id']
            ).single().execute().data

            print(f"\n  Processing: {book['title']} - {chapter['chapter_title']}")

            try:
                cards = self.generate_chapter_summary_cards(chapter)
                if preview:
                    for card in cards:
                        print(f"    -> [{card.get('summary_type')}] {card['front'][:60]}...")
                else:
                    deck_id = self.get_or_create_book_deck(book['title'])
                    self.save_cards(cards, deck_id=deck_id)
                    print(f"    Saved {len(cards)} summary cards")

                results['chapters_processed'] += 1
                results['chapter_cards'] += len(cards)

            except Exception as e:
                print(f"    ERROR: {e}")

        # Process finished books
        print("\n--- Checking for Finished Books ---")
        finished_books = self.get_finished_books_without_summary(book_title=book_title)
        print(f"Found {len(finished_books)} books needing summaries")

        for book in finished_books:
            print(f"\n  Processing: {book['title']}")

            try:
                cards = self.generate_book_summary_cards(book)
                if preview:
                    for card in cards:
                        print(f"    -> [{card.get('summary_type')}] {card['front'][:60]}...")
                else:
                    deck_id = self.get_or_create_book_deck(book['title'])
                    self.save_cards(cards, deck_id=deck_id)
                    print(f"    Saved {len(cards)} book summary cards")

                results['books_processed'] += 1
                results['book_cards'] += len(cards)

            except Exception as e:
                print(f"    ERROR: {e}")

        return results

    def get_or_create_book_deck(self, book_title: str) -> str:
        """Get or create a deck for a book under 00_Current."""
        deck_name = f"00_Current::{book_title}"

        # Check if deck exists
        response = self.supabase.table('anki_decks').select('id').eq(
            'name', deck_name
        ).execute()

        if response.data:
            return response.data[0]['id']

        # Create new deck
        response = self.supabase.table('anki_decks').insert({
            'name': deck_name,
            'description': f'Flashcards generated from {book_title}'
        }).execute()

        return response.data[0]['id']

    def generate_and_save(
        self,
        book_title: str = None,
        days: int = None,
        limit: int = 20,
        preview: bool = False,
        skip_existing: bool = True
    ) -> dict:
        """
        Generate cards for multiple highlights using batch processing.
        Groups highlights by chapter and processes them together so the LLM can:
        - Combine related highlights into single cards
        - Skip redundant or low-value highlights
        - Create more coherent, higher-quality cards
        """
        if skip_existing:
            highlights = self.get_highlights_without_cards(
                book_title=book_title,
                days=days,
                limit=limit
            )
            print(f"Found {len(highlights)} NEW highlights (without existing cards)")
        else:
            highlights = self.get_highlights_with_context(
                book_title=book_title,
                days=days,
                limit=limit
            )
            print(f"Found {len(highlights)} highlights with chapter context")

        if not highlights:
            return {'generated': 0, 'saved': 0}

        # Process highlights by chapter (batch processing)
        print(f"\nProcessing highlights by chapter (batch mode)...")
        results = self.process_highlights_by_chapter(highlights, preview=preview)

        if preview:
            print(f"\n[PREVIEW MODE] Would process {results['highlights_covered']} highlights into ~{results['cards_created'] or 'estimated'} cards")
            return {
                'generated': len(results.get('cards', [])),
                'saved': 0,
                'chapters_processed': results['chapters_processed'],
                'highlights_covered': results['highlights_covered']
            }

        print(f"\nCreated {results['cards_created']} cards from {results['highlights_covered']} highlights across {results['chapters_processed']} chapters")

        return {
            'generated': results['cards_created'],
            'saved': results['cards_created'],
            'chapters_processed': results['chapters_processed'],
            'highlights_covered': results['highlights_covered']
        }


def main():
    parser = argparse.ArgumentParser(description='Generate Anki cards from highlights')
    parser.add_argument('--book', help='Book title to filter by')
    parser.add_argument('--days', type=int, help='Days of highlights to process')
    parser.add_argument('--limit', type=int, default=50, help='Max highlights to process')
    parser.add_argument('--preview', action='store_true', help='Preview without saving')
    parser.add_argument('--check-completions', action='store_true',
                        help='Check for completed chapters/books and generate summaries')
    parser.add_argument('--all', action='store_true',
                        help='Process highlights AND check completions (weekly job mode)')
    parser.add_argument('--force', action='store_true',
                        help='Process highlights even if they already have cards')
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY required")
        sys.exit(1)

    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY required for card generation")
        sys.exit(1)

    generator = AnkiCardGenerator()

    total_generated = 0
    total_saved = 0

    # Process highlights (unless only checking completions)
    if not args.check_completions or args.all:
        print("=" * 60)
        print("GENERATING CARDS FROM HIGHLIGHTS")
        print("=" * 60)

        result = generator.generate_and_save(
            book_title=args.book,
            days=args.days,
            limit=args.limit,
            preview=args.preview,
            skip_existing=not args.force
        )
        total_generated += result['generated']
        total_saved += result['saved']

    # Process completions
    if args.check_completions or args.all:
        print("\n" + "=" * 60)
        print("CHECKING COMPLETIONS")
        print("=" * 60)

        completion_result = generator.process_completions(preview=args.preview, book_title=args.book)
        total_generated += completion_result['chapter_cards'] + completion_result['book_cards']
        if not args.preview:
            total_saved += completion_result['chapter_cards'] + completion_result['book_cards']

        print(f"\nCompletion Summary:")
        print(f"  Chapters processed: {completion_result['chapters_processed']}")
        print(f"  Chapter cards: {completion_result['chapter_cards']}")
        print(f"  Books processed: {completion_result['books_processed']}")
        print(f"  Book cards: {completion_result['book_cards']}")

    print(f"\n{'=' * 60}")
    print(f"TOTAL: Generated {total_generated} cards, saved {total_saved}")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
