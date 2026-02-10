#!/usr/bin/env python3
"""
=============================================================================
CREATE SUMMARY BOOK - Reusable Pipeline
=============================================================================

End-to-end pipeline to create a summary EPUB for any topic:
  1. Generate/load book list (AI-assisted or from file)
  2. Search & download EPUBs from LibGen
  3. Convert PDFs to EPUB if needed
  4. Process all books with Haiku (parallel summaries)
  5. Compile into a single summary EPUB
  6. Upload to Bookfusion

Usage:
  # From a book list file (CSV or JSON)
  python create_summary_book.py --from-list books.json --title "Climate Tech"

  # AI-generated book list for a topic
  python create_summary_book.py --topic "synthetic biology" --max-books 30

  # Resume from a specific step
  python create_summary_book.py --project-id abc123 --resume-from process

  # Just compile (skip download/process)
  python create_summary_book.py --project-id abc123 --compile-only

Environment:
  ANTHROPIC_API_KEY, SUPABASE_URL, SUPABASE_KEY, BOOKFUSION_API_KEY
"""
import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_DATA_DIR = Path(__file__).parent.parent / "data"
HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-5-20250929"
NUM_WORKERS = 8


def get_supabase():
    return create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])


def get_anthropic():
    import anthropic
    return anthropic.Anthropic(api_key=os.environ['ANTHROPIC_API_KEY'])


# =============================================================================
# STEP 1: GENERATE BOOK LIST
# =============================================================================

def generate_book_list(topic: str, max_books: int = 30, context: str = "") -> list[dict]:
    """
    Use AI to generate a curated book list for a topic.

    Returns list of dicts with: title, author, why (reason for inclusion)
    """
    client = get_anthropic()

    prompt = f"""Generate a curated list of {max_books} books about: {topic}

{f'Additional context: {context}' if context else ''}

Requirements:
- Focus on the most important, well-regarded books on this topic
- Mix of foundational texts, recent publications, and practical guides
- Include both popular and academic works
- Prioritize books likely available as EPUBs on LibGen

For each book, provide:
- title: Full book title
- author: Author name(s)
- year: Publication year
- why: 1 sentence on why this book is important for the topic (this helps assess relevance)

Return as a JSON array:
```json
[
  {{"title": "Book Title", "author": "Author Name", "year": 2020, "why": "Foundational text on X"}},
  ...
]
```
"""

    response = client.messages.create(
        model=SONNET_MODEL,  # Use Sonnet for better book curation
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}]
    )

    import re
    text = response.content[0].text
    json_match = re.search(r'```json\s*(\[[\s\S]*?\])\s*```', text)
    if json_match:
        return json.loads(json_match.group(1))

    # Try to find raw JSON array
    json_match = re.search(r'\[[\s\S]*\]', text)
    if json_match:
        return json.loads(json_match.group())

    raise ValueError("Could not parse book list from AI response")


def load_book_list(file_path: Path) -> list[dict]:
    """Load book list from JSON or CSV file."""
    if file_path.suffix == '.json':
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    elif file_path.suffix == '.csv':
        import csv
        books = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                books.append({
                    'title': row.get('title', ''),
                    'author': row.get('author', ''),
                    'year': row.get('year', ''),
                    'why': row.get('why', row.get('reason', ''))
                })
        return books
    else:
        raise ValueError(f"Unsupported file format: {file_path.suffix}")


# =============================================================================
# STEP 2: SEARCH & DOWNLOAD
# =============================================================================

def download_books(book_list: list[dict], output_dir: Path) -> dict:
    """
    Search LibGen and download EPUBs for each book.

    Returns download report dict.
    """
    from scripts.batch_download import search_and_download

    output_dir.mkdir(parents=True, exist_ok=True)
    report = {
        'total': len(book_list),
        'found_epub': 0,
        'found_pdf': 0,
        'not_found': 0,
        'books': []
    }

    for i, book in enumerate(book_list, 1):
        title = book['title']
        author = book.get('author', '')
        print(f"  [{i}/{len(book_list)}] Searching: {title[:50]}...")

        try:
            result = search_and_download(
                title=title,
                author=author,
                output_dir=output_dir,
                prefer_epub=True
            )
            report['books'].append(result)

            if result.get('format') == 'epub':
                report['found_epub'] += 1
            elif result.get('format') == 'pdf':
                report['found_pdf'] += 1
            else:
                report['not_found'] += 1

        except Exception as e:
            print(f"    Error: {e}")
            report['books'].append({
                'title': title,
                'error': str(e),
                'status': 'failed'
            })
            report['not_found'] += 1

        time.sleep(2)  # Rate limiting

    return report


# =============================================================================
# STEP 3: CONVERT PDFs
# =============================================================================

def convert_pdfs(epub_dir: Path, pdf_dir: Path, converted_dir: Path) -> int:
    """Convert any PDFs to EPUB using Calibre."""
    from scripts.convert_pdfs_to_epub import convert_pdf_to_epub

    converted_dir.mkdir(parents=True, exist_ok=True)
    pdf_files = list(pdf_dir.glob("*.pdf")) if pdf_dir.exists() else []

    if not pdf_files:
        print("  No PDFs to convert")
        return 0

    converted = 0
    for pdf in pdf_files:
        epub_path = converted_dir / f"{pdf.stem}.epub"
        if epub_path.exists():
            converted += 1
            continue

        print(f"  Converting: {pdf.stem[:50]}...")
        if convert_pdf_to_epub(pdf, epub_path):
            converted += 1
        else:
            print(f"    Conversion failed")

    return converted


# =============================================================================
# STEP 4: PROCESS WITH HAIKU
# =============================================================================

def process_books_parallel(epub_dir: Path, converted_dir: Path, enhanced_dir: Path) -> dict:
    """Process all books with Haiku in parallel."""
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from scripts.batch_process_parallel import process_single_book
    from scripts.batch_process_haiku import HaikuEnhancer
    import process_new_book

    # Override enhancer with Haiku
    process_new_book.EPUBLearningEnhancer = HaikuEnhancer

    enhanced_dir.mkdir(parents=True, exist_ok=True)

    # Collect all EPUBs
    epub_files = []
    if epub_dir.exists():
        epub_files.extend(epub_dir.glob("*.epub"))
    if converted_dir.exists():
        for f in converted_dir.glob("*.epub"):
            if "test" not in f.stem.lower():
                epub_files.append(f)

    results = {'total': len(epub_files), 'processed': 0, 'skipped': 0, 'failed': 0}
    print(f"  Processing {len(epub_files)} books with {NUM_WORKERS} workers...")

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_epub = {
            executor.submit(process_single_book, p): p for p in epub_files
        }

        for future in as_completed(future_to_epub):
            try:
                result = future.result()
                if result.get('skipped'):
                    results['skipped'] += 1
                    print(f"    SKIP: {result['file'][:50]}")
                elif result['success']:
                    results['processed'] += 1
                    print(f"    OK: {result['file'][:50]}")
                else:
                    results['failed'] += 1
                    print(f"    FAIL: {result['file'][:50]} - {result.get('error', '')[:40]}")
            except Exception as e:
                results['failed'] += 1

    return results


# =============================================================================
# STEP 5: COMPILE EPUB
# =============================================================================

def compile_summary_epub(title: str, output_path: Path) -> Path:
    """Compile all processed books into a single summary EPUB."""
    # Import and run the compile script
    from scripts.compile_summary_epub_v2 import (
        fetch_all_chapters, create_epub_structure, escape, markdown_to_html,
        clean_summary, is_junk_chapter
    )

    # We need to run compile_summary_epub_v2.main() but with a custom title
    # For now, just call it as a subprocess with the right config
    import subprocess
    result = subprocess.run(
        [sys.executable, str(Path(__file__).parent / 'compile_summary_epub_v2.py')],
        capture_output=True, text=True, cwd=str(Path(__file__).parent.parent)
    )

    if result.returncode != 0:
        print(f"  Compilation error: {result.stderr[:200]}")
        raise RuntimeError("EPUB compilation failed")

    print(result.stdout)
    return output_path


# =============================================================================
# STEP 6: UPLOAD
# =============================================================================

def upload_to_bookfusion(epub_path: Path, title: str) -> Optional[str]:
    """Upload compiled EPUB to Bookfusion."""
    import asyncio
    from lib.bookfusion_client import BookfusionClient, BookfusionMetadata

    api_key = os.environ.get('BOOKFUSION_API_KEY')
    if not api_key:
        print("  BOOKFUSION_API_KEY not set - skipping upload")
        return None

    async def _upload():
        metadata = BookfusionMetadata(
            title=title,
            author_list=['Jarvis AI'],
            language='eng',
            bookshelves=['Books']
        )
        async with BookfusionClient(api_key) as client:
            result = await client.upload_book(str(epub_path), metadata)
        return result

    result = asyncio.run(_upload())
    if result.success:
        return result.bookfusion_id
    else:
        print(f"  Upload failed: {result.error}")
        return None


# =============================================================================
# PROJECT MANAGEMENT
# =============================================================================

def save_project(project_id: str, config: dict) -> None:
    """Save project config to a JSON file for resume capability."""
    project_dir = BASE_DATA_DIR / "summary_projects"
    project_dir.mkdir(parents=True, exist_ok=True)

    project_file = project_dir / f"{project_id}.json"
    config['updated_at'] = datetime.now(timezone.utc).isoformat()
    with open(project_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def load_project(project_id: str) -> dict:
    """Load project config."""
    project_file = BASE_DATA_DIR / "summary_projects" / f"{project_id}.json"
    if not project_file.exists():
        raise FileNotFoundError(f"Project not found: {project_id}")
    with open(project_file, 'r', encoding='utf-8') as f:
        return json.load(f)


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def run_pipeline(
    title: str,
    book_list: list[dict],
    project_id: Optional[str] = None,
    resume_from: Optional[str] = None,
    compile_only: bool = False,
    skip_upload: bool = False
) -> dict:
    """
    Run the full summary book pipeline.

    Args:
        title: Title for the summary book (e.g., "Summary: Climate Tech")
        book_list: List of books to process
        project_id: Unique project ID (auto-generated if not provided)
        resume_from: Step to resume from ('download', 'convert', 'process', 'compile', 'upload')
        compile_only: Skip all steps except compile + upload
        skip_upload: Don't upload to Bookfusion

    Returns:
        Pipeline results dict
    """
    if not project_id:
        project_id = str(uuid.uuid4())[:8]

    # Set up directories
    project_dir = BASE_DATA_DIR / f"summary_{project_id}"
    epub_dir = project_dir / "epubs"
    pdf_dir = project_dir / "pdfs"
    converted_dir = project_dir / "converted"
    enhanced_dir = project_dir / "enhanced"
    output_path = project_dir / f"{title}.epub"

    # Determine which steps to run
    steps = ['download', 'convert', 'process', 'compile', 'upload']
    if compile_only:
        steps = ['compile', 'upload']
    elif resume_from and resume_from in steps:
        steps = steps[steps.index(resume_from):]
    if skip_upload and 'upload' in steps:
        steps.remove('upload')

    # Save project config
    config = {
        'project_id': project_id,
        'title': title,
        'book_list': book_list,
        'steps': steps,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'status': 'running'
    }
    save_project(project_id, config)

    results = {
        'project_id': project_id,
        'title': title,
        'total_books': len(book_list),
        'steps_completed': [],
        'output_path': None,
        'bookfusion_id': None
    }

    pipeline_start = time.time()

    print("=" * 70)
    print(f"SUMMARY BOOK PIPELINE: {title}")
    print("=" * 70)
    print(f"Project ID: {project_id}")
    print(f"Books: {len(book_list)}")
    print(f"Steps: {' → '.join(steps)}")
    print(f"Output: {project_dir}")
    print()

    try:
        # STEP 1: Download
        if 'download' in steps:
            print("[1/5] DOWNLOADING BOOKS...")
            download_report = download_books(book_list, epub_dir)
            results['download'] = download_report
            results['steps_completed'].append('download')
            print(f"  Done: {download_report['found_epub']} EPUBs, {download_report['found_pdf']} PDFs, {download_report['not_found']} not found")
            print()

        # STEP 2: Convert PDFs
        if 'convert' in steps:
            print("[2/5] CONVERTING PDFs TO EPUB...")
            converted = convert_pdfs(epub_dir, pdf_dir, converted_dir)
            results['converted'] = converted
            results['steps_completed'].append('convert')
            print(f"  Done: {converted} PDFs converted")
            print()

        # STEP 3: Process with Haiku
        if 'process' in steps:
            print("[3/5] PROCESSING WITH HAIKU (parallel)...")
            process_results = process_books_parallel(epub_dir, converted_dir, enhanced_dir)
            results['processing'] = process_results
            results['steps_completed'].append('process')
            print(f"  Done: {process_results['processed']} processed, {process_results['skipped']} skipped, {process_results['failed']} failed")
            print()

        # STEP 4: Compile EPUB
        if 'compile' in steps:
            print("[4/5] COMPILING SUMMARY EPUB...")
            compile_summary_epub(title, output_path)
            results['output_path'] = str(output_path)
            results['steps_completed'].append('compile')
            print()

        # STEP 5: Upload
        if 'upload' in steps:
            print("[5/5] UPLOADING TO BOOKFUSION...")
            epub_to_upload = Path(BASE_DATA_DIR / "enhanced_haiku" / f"Summary Agrifood books.epub")  # TODO: make dynamic
            if epub_to_upload.exists():
                bf_id = upload_to_bookfusion(epub_to_upload, f"Summary: {title}")
                if bf_id:
                    results['bookfusion_id'] = bf_id
                    print(f"  Uploaded! Bookfusion ID: {bf_id}")
            results['steps_completed'].append('upload')
            print()

    except Exception as e:
        print(f"\nPIPELINE ERROR: {e}")
        config['status'] = 'failed'
        config['error'] = str(e)
        save_project(project_id, config)
        raise

    # Final summary
    total_time = time.time() - pipeline_start
    results['total_time_minutes'] = total_time / 60

    config['status'] = 'completed'
    config['results'] = results
    save_project(project_id, config)

    print("=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)
    print(f"  Project: {project_id}")
    print(f"  Title: {title}")
    print(f"  Time: {total_time / 60:.1f} minutes")
    print(f"  Steps: {' → '.join(results['steps_completed'])}")
    if results.get('bookfusion_id'):
        print(f"  Bookfusion ID: {results['bookfusion_id']}")

    return results


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Create a summary book for any topic',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From AI-generated list
  python create_summary_book.py --topic "synthetic biology" --max-books 30

  # From existing book list
  python create_summary_book.py --from-list my_books.json --title "Climate Tech"

  # Generate book list only (for review before processing)
  python create_summary_book.py --topic "regenerative agriculture" --list-only

  # Resume a failed pipeline
  python create_summary_book.py --project-id abc123 --resume-from process

  # Just recompile the EPUB
  python create_summary_book.py --project-id abc123 --compile-only
"""
    )

    parser.add_argument('--topic', type=str, help='Topic for AI to generate book list')
    parser.add_argument('--max-books', type=int, default=30, help='Max books to generate (default: 30)')
    parser.add_argument('--context', type=str, default='', help='Additional context for AI book selection')
    parser.add_argument('--from-list', type=Path, help='Load book list from JSON or CSV file')
    parser.add_argument('--title', type=str, help='Title for the summary book')
    parser.add_argument('--project-id', type=str, help='Existing project ID (for resume)')
    parser.add_argument('--resume-from', type=str, choices=['download', 'convert', 'process', 'compile', 'upload'])
    parser.add_argument('--compile-only', action='store_true', help='Only compile + upload')
    parser.add_argument('--skip-upload', action='store_true', help='Skip Bookfusion upload')
    parser.add_argument('--list-only', action='store_true', help='Only generate book list (no processing)')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel workers (default: 8)')

    args = parser.parse_args()

    global NUM_WORKERS
    NUM_WORKERS = args.workers

    # Check environment
    required = ['ANTHROPIC_API_KEY', 'SUPABASE_URL', 'SUPABASE_KEY']
    for var in required:
        if not os.environ.get(var):
            print(f"ERROR: {var} not set")
            sys.exit(1)

    # Determine book list and title
    book_list = None
    title = args.title

    if args.project_id:
        # Resume existing project
        project = load_project(args.project_id)
        book_list = project['book_list']
        title = title or project['title']
        print(f"Resuming project: {args.project_id} ({title})")

    elif args.from_list:
        # Load from file
        book_list = load_book_list(args.from_list)
        title = title or args.from_list.stem.replace('_', ' ').title()
        print(f"Loaded {len(book_list)} books from {args.from_list}")

    elif args.topic:
        # Generate with AI
        print(f"Generating book list for topic: {args.topic}")
        book_list = generate_book_list(args.topic, args.max_books, args.context)
        title = title or f"Summary: {args.topic.title()}"
        print(f"Generated {len(book_list)} books")

        # Save the generated list for reference
        list_dir = BASE_DATA_DIR / "summary_projects"
        list_dir.mkdir(parents=True, exist_ok=True)
        list_file = list_dir / f"{args.topic.lower().replace(' ', '_')}_books.json"
        with open(list_file, 'w', encoding='utf-8') as f:
            json.dump(book_list, f, indent=2, ensure_ascii=False)
        print(f"Saved book list to: {list_file}")

    else:
        parser.error("Must provide --topic, --from-list, or --project-id")

    # Show book list
    print(f"\nBook list ({len(book_list)} books):")
    for i, book in enumerate(book_list, 1):
        why = book.get('why', '')
        print(f"  {i:2d}. {book['title'][:50]} - {book.get('author', 'Unknown')[:20]}")
        if why:
            print(f"      {why[:70]}")

    if args.list_only:
        print(f"\n--list-only mode: Book list saved. Review and run again without --list-only to process.")
        return

    # Run pipeline
    print()
    results = run_pipeline(
        title=title,
        book_list=book_list,
        project_id=args.project_id,
        resume_from=args.resume_from,
        compile_only=args.compile_only,
        skip_upload=args.skip_upload
    )


if __name__ == '__main__':
    main()
