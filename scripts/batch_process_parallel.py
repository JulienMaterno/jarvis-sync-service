#!/usr/bin/env python3
"""
Batch process books in PARALLEL using multiprocessing.

Processes 4 books simultaneously to speed up processing by 4x.
"""
import sys
import time
import multiprocessing as mp
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import os

load_dotenv()

sys.stdout.reconfigure(encoding='utf-8')

EPUB_DIR = Path(__file__).parent.parent / "data" / "epubs"
CONVERTED_DIR = Path(__file__).parent.parent / "data" / "converted_epubs"
ENHANCED_DIR = Path(__file__).parent.parent / "data" / "enhanced_haiku"

# Number of parallel workers
NUM_WORKERS = 8  # Increased from 4 for faster processing


def get_all_epub_files() -> list[Path]:
    """Get all EPUB files from both directories."""
    epubs = []

    # Main directory
    if EPUB_DIR.exists():
        epubs.extend(EPUB_DIR.glob("*.epub"))

    # Converted PDFs
    if CONVERTED_DIR.exists():
        for epub in CONVERTED_DIR.glob("*.epub"):
            if "test" not in epub.stem.lower() and "conversion_report" not in epub.stem:
                epubs.append(epub)

    return sorted(epubs, key=lambda p: p.stem)


def process_single_book(epub_path: Path) -> dict:
    """
    Process a single book (runs in separate process).

    This function will be called by each worker process.
    """
    # Import here to avoid pickling issues
    from process_new_book import BookProcessingPipeline
    from scripts.batch_process_haiku import HaikuEnhancer
    import process_new_book

    # Override enhancer with Haiku version
    process_new_book.EPUBLearningEnhancer = HaikuEnhancer

    # Output path
    output_path = ENHANCED_DIR / f"{epub_path.stem}_enhanced.epub"

    # Skip if already processed
    if output_path.exists():
        return {
            'file': epub_path.name,
            'success': True,
            'skipped': True,
            'reason': 'Already exists'
        }

    try:
        # Initialize pipeline (skip uploads for speed)
        pipeline = BookProcessingPipeline(
            supabase_url=os.environ['SUPABASE_URL'],
            supabase_key=os.environ['SUPABASE_KEY'],
            anthropic_api_key=os.environ['ANTHROPIC_API_KEY'],
            use_drive=False,
            use_bookfusion=False
        )

        # Process book
        result = pipeline.process(
            epub_path=epub_path,
            output_path=output_path,
            preview=False
        )

        return {
            'file': epub_path.name,
            'success': result['success'],
            'skipped': False,
            'book_id': result.get('book_id'),
            'enhancements': result.get('enhancements_generated', 0),
            'subchapters': result.get('subchapters_generated', 0)
        }

    except Exception as e:
        return {
            'file': epub_path.name,
            'success': False,
            'skipped': False,
            'error': str(e)
        }


def main():
    print("=" * 70)
    print("PARALLEL BATCH PROCESSING: Agrifood Books with Haiku")
    print("=" * 70)
    print(f"Workers: {NUM_WORKERS} parallel processes")
    print(f"Output: {ENHANCED_DIR}")
    print()

    # Check environment
    required_vars = ['ANTHROPIC_API_KEY', 'SUPABASE_URL', 'SUPABASE_KEY']
    for var in required_vars:
        if not os.environ.get(var):
            print(f"ERROR: {var} not set")
            sys.exit(1)

    # Get all EPUBs
    epub_files = get_all_epub_files()
    print(f"Found {len(epub_files)} EPUB files\n")

    if not epub_files:
        print("No EPUB files found!")
        sys.exit(1)

    # Create output directory
    ENHANCED_DIR.mkdir(parents=True, exist_ok=True)

    # Process books in parallel
    results = []
    completed = 0
    skipped = 0
    failed = 0
    total_start = time.time()

    print(f"Processing {len(epub_files)} books with {NUM_WORKERS} workers...\n")

    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        # Submit all jobs
        future_to_epub = {
            executor.submit(process_single_book, epub_path): epub_path
            for epub_path in epub_files
        }

        # Process results as they complete
        for future in as_completed(future_to_epub):
            epub_path = future_to_epub[future]

            try:
                result = future.result()
                results.append(result)

                if result.get('skipped'):
                    skipped += 1
                    print(f"[{completed + skipped}/{len(epub_files)}] ⊘ SKIPPED: {result['file'][:60]}")
                elif result['success']:
                    completed += 1
                    enhancements = result.get('enhancements', 0)
                    subchapters = result.get('subchapters', 0)
                    print(f"[{completed + skipped}/{len(epub_files)}] ✓ {result['file'][:60]}")
                    print(f"    {enhancements} chapters, {subchapters} sub-chapters")
                else:
                    failed += 1
                    error = result.get('error', 'Unknown error')[:80]
                    print(f"[{completed + skipped}/{len(epub_files)}] ✗ FAILED: {result['file'][:60]}")
                    print(f"    Error: {error}")

            except Exception as e:
                failed += 1
                print(f"[{completed + skipped + failed}/{len(epub_files)}] ✗ ERROR: {epub_path.name[:60]}")
                print(f"    {type(e).__name__}: {e}")
                results.append({
                    'file': epub_path.name,
                    'success': False,
                    'error': str(e)
                })

    # Final summary
    total_time = time.time() - total_start

    print(f"\n{'=' * 70}")
    print("BATCH PROCESSING COMPLETE")
    print(f"{'=' * 70}")
    print(f"Total time: {total_time / 60:.1f} minutes")
    print(f"Processed: {completed}/{len(epub_files)} books")
    print(f"Skipped (already done): {skipped}")
    print(f"Failed: {failed}")

    total_enhancements = sum(r.get('enhancements', 0) for r in results)
    total_subchapters = sum(r.get('subchapters', 0) for r in results)
    print(f"Total chapter enhancements: {total_enhancements}")
    print(f"Total sub-chapter enhancements: {total_subchapters}")

    if failed > 0:
        print(f"\nFailed books:")
        for r in results:
            if not r.get('success') and not r.get('skipped'):
                print(f"  - {r['file']}: {r.get('error', 'Unknown error')[:60]}")

    print(f"\nEnhanced EPUBs saved to: {ENHANCED_DIR}")

    # Save results
    import json
    report_path = ENHANCED_DIR / "batch_processing_report_parallel.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump({
            'total_books': len(epub_files),
            'successful': completed,
            'skipped': skipped,
            'failed': failed,
            'total_time_minutes': total_time / 60,
            'workers': NUM_WORKERS,
            'results': results
        }, f, indent=2, ensure_ascii=False)

    print(f"Report saved to: {report_path}")


if __name__ == '__main__':
    # Required for Windows multiprocessing
    mp.freeze_support()
    main()
