#!/usr/bin/env python3
"""
Batch process all agrifood books with Haiku (summaries only, no questions).

Cost optimization:
- Uses Haiku 4.5 instead of Sonnet (~90% cheaper)
- Skips learning questions (only generates summaries)
- Skips Drive/Bookfusion uploads (faster processing)

Estimated cost: ~$2.60 for 50 books
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from process_new_book import BookProcessingPipeline, EPUBLearningEnhancer
from dotenv import load_dotenv
import os

load_dotenv()

sys.stdout.reconfigure(encoding='utf-8')

EPUB_DIR = Path(__file__).parent.parent / "data" / "epubs"
CONVERTED_DIR = Path(__file__).parent.parent / "data" / "converted_epubs"
ENHANCED_DIR = Path(__file__).parent.parent / "data" / "enhanced_haiku"

# Override model to Haiku in the enhancer
HAIKU_MODEL = "claude-haiku-4-5-20251001"

def get_all_epub_files() -> list[Path]:
    """Get all EPUB files from both directories."""
    epubs = []

    # Main directory
    if EPUB_DIR.exists():
        epubs.extend(EPUB_DIR.glob("*.epub"))

    # Converted PDFs
    if CONVERTED_DIR.exists():
        for epub in CONVERTED_DIR.glob("*.epub"):
            # Skip test files
            if "test" not in epub.stem.lower() and "conversion_report" not in epub.stem:
                epubs.append(epub)

    return sorted(epubs, key=lambda p: p.stem)


class HaikuEnhancer(EPUBLearningEnhancer):
    """
    Modified enhancer that uses Haiku and skips learning questions.
    """

    def generate_chapter_enhancement(self, chapter, book_title: str, book_author: str = None) -> dict:
        """Generate ONLY preview summary (no learning questions)."""
        client = self._get_client()

        # Build context section
        context_section = ""
        if self.reader_context:
            context_section = self.reader_context.to_prompt_section() + "\n\n"

        # Truncate chapter content if too long
        content = chapter.content
        words = content.split()
        if len(words) > 3000:
            content = ' '.join(words[:3000]) + '\n\n[...chapter continues...]'

        prompt = f"""{context_section}Create a factual chapter summary to help the reader decide whether to read or skip this chapter.

BOOK: {book_title}
{f'AUTHOR: {book_author}' if book_author else ''}
CHAPTER: {chapter.title or f'Chapter {chapter.number}'}

CHAPTER CONTENT:
{content}

Generate a CHAPTER OVERVIEW (50-100 words):
A factual, objective description of what this chapter contains. NOT a teaser or marketing pitch.
Help the reader decide: "Should I read this chapter or skip it?"

Include:
- What topics/concepts are covered
- Key frameworks, models, or ideas introduced (if any)
- What type of content it is (theory, examples, practical exercises, stories, etc.)

Style: Informative, neutral, like an enhanced table of contents entry.
You can use bullet points if it helps clarity.

BAD example (too salesy): "Discover the shocking truth about time management that will transform your life!"
GOOD example: "Introduces the 'energy audit' framework. Covers: identifying energy drains, categorizing tasks by cognitive load, and the 2-hour focus block method. Heavy on practical exercises."

Return ONLY the chapter overview text (no JSON, no extra formatting).
"""

        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}]
        )

        summary = response.content[0].text.strip()

        # Remove any JSON formatting if present
        import re
        json_match = re.search(r'```json\s*\{[\s\S]*"preview_summary":\s*"([^"]+)"[\s\S]*\}\s*```', summary)
        if json_match:
            summary = json_match.group(1)

        return {
            "preview_summary": summary,
            "learning_questions": []  # Empty - we're skipping questions
        }

    def generate_book_summary(self, book_title: str, book_author: str, chapters: list, chapter_previews: dict) -> dict:
        """Generate book-level summary (100-150 words)."""
        client = self._get_client()

        # Build chapter list with previews
        chapter_list = []
        for ch in chapters:
            preview = chapter_previews.get(ch.number, {}).get('preview_summary', '')
            if preview:
                chapter_list.append(f"**{ch.title or f'Chapter {ch.number}'}** ({ch.word_count:,} words)\n{preview}")
            elif ch.word_count > 300:
                chapter_list.append(f"**{ch.title or f'Chapter {ch.number}'}** ({ch.word_count:,} words)")

        chapters_text = '\n\n'.join(chapter_list)

        prompt = f"""Create a book overview to help a reader decide whether to read this book.

BOOK: {book_title}
{f'AUTHOR: {book_author}' if book_author else ''}

CHAPTERS AND PREVIEWS:
{chapters_text}

Generate:

1. BOOK OVERVIEW (100-150 words)
   A factual description of what this book is about, its main thesis, and what kind of reader it's for.
   Help the reader decide: "Is this book worth my time?"

2. KEY THEMES (3-5 bullet points)
   The major ideas or threads that run through the book.

Format as JSON:
```json
{{
  "book_overview": "Overall description of the book...",
  "key_themes": [
    "Theme 1: ...",
    "Theme 2: ..."
  ]
}}
```
"""

        response = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )

        import re
        import json
        text = response.content[0].text

        json_match = re.search(r'```json\s*(\{[\s\S]*?\})\s*```', text)
        if json_match:
            return json.loads(json_match.group(1))

        # Fallback
        return {
            "book_overview": f"A book about {book_title}.",
            "key_themes": ["See individual chapters for details."]
        }

    def generate_subchapter_enhancement(self, subchapter, chapter_title: str, book_title: str,
                                       section_number: int, total_sections: int,
                                       previous_sections: list, book_author: str = None) -> dict:
        """Generate sub-chapter summary (30-50 words) with context bridge."""
        client = self._get_client()

        # Build previous sections summary for context
        prev_summary = ""
        if previous_sections:
            prev_parts = []
            for ps in previous_sections[-3:]:  # Last 3 sections for context
                prev_parts.append(f"- **{ps.title}** ({ps.word_count} words)")
            prev_summary = "Previous sections in this chapter:\n" + "\n".join(prev_parts)
        else:
            prev_summary = "This is the first section of the chapter."

        # Truncate content if too long
        content = subchapter.content
        words = content.split()
        if len(words) > 2500:
            content = ' '.join(words[:2500]) + '\n\n[...section continues...]'

        prompt = f"""Create a factual summary for this book section.

BOOK: {book_title}
{f'AUTHOR: {book_author}' if book_author else ''}
CHAPTER: {chapter_title}
SECTION: {subchapter.title} (Section {section_number} of {total_sections})
WORD COUNT: {subchapter.word_count}

{prev_summary}

SECTION CONTENT:
{content}

Generate:

1. SECTION OVERVIEW (30-50 words)
   Factual description to help reader decide: read or skip this section?
   - What topics/arguments are presented
   - Key examples or evidence used

2. CONTEXT BRIDGE (1 sentence)
   What does reader need from PREVIOUS sections to follow this?

   Examples:
   - "Builds on the enclosure movement from the previous section."
   - "Standalone section - no prior context needed."

Return JSON:
```json
{{
  "section_summary": "Your section overview here...",
  "context_bridge": "What reader needs to know..."
}}
```
"""

        # Retry logic
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                response = client.messages.create(
                    model=HAIKU_MODEL,
                    max_tokens=512,
                    messages=[{"role": "user", "content": prompt}]
                )

                import re
                import json
                text = response.content[0].text

                json_match = re.search(r'```json\s*(\{[\s\S]*?\})\s*```', text)
                if json_match:
                    result = json.loads(json_match.group(1))

                    if not result.get('section_summary'):
                        raise ValueError("Missing section_summary")

                    return {
                        "section_summary": result.get('section_summary', ''),
                        "context_bridge": result.get('context_bridge', ''),
                        "learning_questions": []  # Skip questions
                    }

                if attempt < max_retries:
                    continue

            except Exception as e:
                if attempt < max_retries:
                    time.sleep(1)
                    continue

        # Fallback
        return {
            "section_summary": f"This section covers {subchapter.title}.",
            "context_bridge": "Continue from the previous section.",
            "learning_questions": []
        }


def main():
    print("=" * 70)
    print("BATCH PROCESSING: Agrifood Books with Haiku")
    print("=" * 70)
    print(f"Model: {HAIKU_MODEL}")
    print(f"Mode: Summaries only (no learning questions)")
    print(f"Output: {ENHANCED_DIR}")
    print()

    # Check environment
    if not os.environ.get('ANTHROPIC_API_KEY'):
        print("ERROR: ANTHROPIC_API_KEY not set")
        sys.exit(1)

    if not os.environ.get('SUPABASE_URL') or not os.environ.get('SUPABASE_KEY'):
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set")
        sys.exit(1)

    # Get all EPUBs
    epub_files = get_all_epub_files()
    print(f"Found {len(epub_files)} EPUB files\n")

    if not epub_files:
        print("No EPUB files found!")
        sys.exit(1)

    # Create output directory
    ENHANCED_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize pipeline (skip Drive and Bookfusion)
    pipeline = BookProcessingPipeline(
        supabase_url=os.environ['SUPABASE_URL'],
        supabase_key=os.environ['SUPABASE_KEY'],
        anthropic_api_key=os.environ['ANTHROPIC_API_KEY'],
        use_drive=False,  # Skip to speed up
        use_bookfusion=False  # Skip to speed up
    )

    # Replace the enhancer class with our Haiku version
    # This is a bit hacky but avoids modifying the main pipeline code
    import process_new_book
    process_new_book.EPUBLearningEnhancer = HaikuEnhancer

    # Process each book
    results = []
    total_start = time.time()

    for i, epub_path in enumerate(epub_files):
        print(f"\n{'=' * 70}")
        print(f"[{i+1}/{len(epub_files)}] {epub_path.stem}")
        print(f"{'=' * 70}")

        # Output path in enhanced directory
        output_path = ENHANCED_DIR / f"{epub_path.stem}_enhanced.epub"

        try:
            result = pipeline.process(
                epub_path=epub_path,
                output_path=output_path,
                preview=False
            )
            results.append({
                'file': epub_path.name,
                'success': result['success'],
                'book_id': result.get('book_id'),
                'enhancements': result.get('enhancements_generated', 0),
                'subchapters': result.get('subchapters_generated', 0)
            })

        except Exception as e:
            print(f"\nERROR processing {epub_path.name}: {e}")
            results.append({
                'file': epub_path.name,
                'success': False,
                'error': str(e)
            })

        # Small delay between books
        time.sleep(2)

    # Final summary
    total_time = time.time() - total_start
    successful = [r for r in results if r.get('success')]
    failed = [r for r in results if not r.get('success')]

    print(f"\n{'=' * 70}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'=' * 70}")
    print(f"Total time: {total_time / 60:.1f} minutes")
    print(f"Processed: {len(successful)}/{len(epub_files)} books")
    print(f"Failed: {len(failed)} books")

    total_enhancements = sum(r.get('enhancements', 0) for r in results)
    total_subchapters = sum(r.get('subchapters', 0) for r in results)
    print(f"Total chapter enhancements: {total_enhancements}")
    print(f"Total sub-chapter enhancements: {total_subchapters}")

    if failed:
        print(f"\nFailed books:")
        for r in failed:
            print(f"  - {r['file']}: {r.get('error', 'Unknown error')}")

    print(f"\nEnhanced EPUBs saved to: {ENHANCED_DIR}")

    # Save results
    import json
    report_path = ENHANCED_DIR / "batch_processing_report.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump({
            'total_books': len(epub_files),
            'successful': len(successful),
            'failed': len(failed),
            'total_time_minutes': total_time / 60,
            'model': HAIKU_MODEL,
            'results': results
        }, f, indent=2, ensure_ascii=False)

    print(f"Report saved to: {report_path}")


if __name__ == '__main__':
    main()
