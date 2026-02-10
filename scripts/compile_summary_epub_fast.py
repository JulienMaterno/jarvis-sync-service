#!/usr/bin/env python3
"""
FAST version: Compile all book summaries into a single EPUB.
Uses optimized queries to avoid timeouts.
"""
import sys
import zipfile
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import os
from supabase import create_client

load_dotenv()

sys.stdout.reconfigure(encoding='utf-8')

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "enhanced_haiku"
OUTPUT_FILE = OUTPUT_DIR / "Summary Agrifood books.epub"


def create_epub_structure(temp_dir: Path) -> None:
    """Create basic EPUB 3 structure."""
    # mimetype
    (temp_dir / 'mimetype').write_text('application/epub+zip', encoding='utf-8')

    # META-INF
    meta_inf = temp_dir / 'META-INF'
    meta_inf.mkdir()
    (meta_inf / 'container.xml').write_text('''<?xml version="1.0" encoding="UTF-8"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>
''', encoding='utf-8')

    # OEBPS
    oebps = temp_dir / 'OEBPS'
    oebps.mkdir()


def main():
    print("=" * 70)
    print("COMPILING SUMMARY EPUB: Agrifood Books")
    print("=" * 70)

    # Connect to database
    supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

    # Fetch ALL books with summaries in ONE query
    print("\nFetching books from database...")
    books_response = supabase.table('books').select(
        'id, title, author, summary, word_count'
    ).not_.is_('word_count', 'null').order('title').execute()

    books = books_response.data
    print(f"  Found {len(books)} books")

    if not books:
        print("No books found!")
        return

    # Fetch ALL chapters for ALL books in ONE query (faster than per-book queries)
    print("\nFetching chapters...")
    all_book_ids = [b['id'] for b in books]

    # Fetch parent chapters only (no sub-chapters for now - keep it simple)
    chapters_response = supabase.table('book_chapters').select(
        'book_id, chapter_number, chapter_title, preview_summary, word_count, context_bridge'
    ).in_('book_id', all_book_ids).is_('parent_chapter_id', 'null').order(
        'book_id, chapter_number'
    ).execute()

    # Group chapters by book_id
    chapters_by_book = {}
    for ch in chapters_response.data:
        book_id = ch['book_id']
        if book_id not in chapters_by_book:
            chapters_by_book[book_id] = []
        chapters_by_book[book_id].append(ch)

    print(f"  Found {len(chapters_response.data)} total chapters")

    # Attach chapters to books
    for book in books:
        book['chapters'] = chapters_by_book.get(book['id'], [])

    # Build EPUB
    print("\nBuilding EPUB...")
    import tempfile
    import shutil

    temp_dir = Path(tempfile.mkdtemp())

    try:
        create_epub_structure(temp_dir)

        # Create OPF
        opf_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="book-id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Summary: Agrifood Books</dc:title>
    <dc:creator>Compiled by Jarvis AI</dc:creator>
    <dc:language>en</dc:language>
    <dc:date>{datetime.now().strftime('%Y-%m-%d')}</dc:date>
    <dc:description>Comprehensive summaries of {len(books)} agrifood and sustainability books.</dc:description>
    <dc:identifier id="book-id">agrifood-summaries-{datetime.now().strftime('%Y%m%d')}</dc:identifier>
  </metadata>
  <manifest>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
    <item id="title" href="title.xhtml" media-type="application/xhtml+xml"/>
    <item id="content" href="content.xhtml" media-type="application/xhtml+xml"/>
    <item id="css" href="styles.css" media-type="text/css"/>
  </manifest>
  <spine>
    <itemref idref="title"/>
    <itemref idref="content"/>
  </spine>
</package>
'''
        (temp_dir / 'OEBPS' / 'content.opf').write_text(opf_content, encoding='utf-8')

        # Create title page
        title_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <title>Summary: Agrifood Books</title>
  <link rel="stylesheet" type="text/css" href="styles.css"/>
</head>
<body>
  <div class="title-page">
    <h1>Summary: Agrifood Books</h1>
    <p class="subtitle">{len(books)} books on agriculture, food systems, and sustainability</p>
    <p class="compiled-by">Compiled by Jarvis AI • {datetime.now().strftime('%B %Y')}</p>
    <div class="intro">
      <h2>How to Use This Book</h2>
      <p>This compilation contains AI-generated summaries at multiple levels:</p>
      <ul>
        <li><strong>Book Overview</strong> - What the book is about and who it's for</li>
        <li><strong>Chapter Summaries</strong> - What each chapter covers</li>
      </ul>
      <p>Use these summaries to decide what to read in detail.</p>
    </div>
  </div>
</body>
</html>
'''
        (temp_dir / 'OEBPS' / 'title.xhtml').write_text(title_content, encoding='utf-8')

        # Create navigation
        nav_items = []
        for i, book in enumerate(books, 1):
            title = book['title'][:60]
            nav_items.append(f'      <li><a href="content.xhtml#book-{i}">{title}</a></li>')

        nav_content = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head><title>Navigation</title></head>
<body>
  <nav epub:type="toc">
    <h1>Table of Contents</h1>
    <ol>
{chr(10).join(nav_items)}
    </ol>
  </nav>
</body>
</html>
'''
        (temp_dir / 'OEBPS' / 'nav.xhtml').write_text(nav_content, encoding='utf-8')

        # Create content page
        print("  Building content...")
        book_sections = []

        for i, book in enumerate(books, 1):
            title = book['title']
            author = book.get('author', 'Unknown')
            summary = book.get('summary', '')
            word_count = book.get('word_count', 0)
            chapters = book.get('chapters', [])

            book_html = f'''
  <div class="book-section" id="book-{i}">
    <h1 class="book-title">{i}. {title}</h1>
    <p class="book-author">by {author}</p>
    <p class="book-meta">{word_count:,} words • {len(chapters)} chapters</p>

    <div class="book-overview">
      <h2>📚 Book Overview</h2>
      <div class="overview-content">
        {summary if summary else '<p><em>No summary available.</em></p>'}
      </div>
    </div>
'''

            if chapters:
                book_html += '    <div class="chapters">\n      <h2>Chapter Guide</h2>\n'

                for ch in chapters[:50]:  # Limit to 50 chapters per book
                    ch_title = ch.get('chapter_title', f"Chapter {ch.get('chapter_number', '?')}")
                    ch_summary = ch.get('preview_summary', '')
                    ch_word_count = ch.get('word_count', 0)

                    book_html += f'''
      <div class="chapter">
        <h3>{ch_title}</h3>
        <p class="chapter-meta">{ch_word_count:,} words</p>
        <p class="chapter-summary">{ch_summary if ch_summary else '<em>No summary</em>'}</p>
      </div>
'''
                book_html += '    </div>\n'

            book_html += '  </div>\n'
            book_sections.append(book_html)

            if i % 10 == 0:
                print(f"    Processed {i}/{len(books)} books...")

        content_html = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <title>Agrifood Book Summaries</title>
  <link rel="stylesheet" type="text/css" href="styles.css"/>
</head>
<body>
{''.join(book_sections)}
</body>
</html>
'''
        (temp_dir / 'OEBPS' / 'content.xhtml').write_text(content_html, encoding='utf-8')

        # Create CSS
        css = '''
body { font-family: Georgia, serif; line-height: 1.6; max-width: 800px; margin: 0 auto; padding: 1em; }
h1, h2, h3 { font-family: -apple-system, sans-serif; }
.title-page { text-align: center; margin: 4em 2em; }
.title-page h1 { font-size: 2.5em; color: #2c3e50; }
.subtitle { font-size: 1.2em; color: #7f8c8d; }
.book-section { margin: 3em 0; padding: 2em 0; border-top: 3px solid #3498db; }
.book-title { font-size: 2em; color: #2c3e50; }
.book-author { font-size: 1.2em; color: #7f8c8d; }
.book-overview { background: #f8f9fa; padding: 1.5em; margin: 2em 0; border-left: 4px solid #3498db; }
.chapter { margin: 1.5em 0; padding: 1em 0; }
.chapter h3 { color: #34495e; }
.chapter-summary { line-height: 1.7; color: #555; }
'''
        (temp_dir / 'OEBPS' / 'styles.css').write_text(css, encoding='utf-8')

        # Package EPUB
        print("\n  Packaging EPUB...")
        with zipfile.ZipFile(OUTPUT_FILE, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Add mimetype first, uncompressed
            zf.write(temp_dir / 'mimetype', 'mimetype', compress_type=zipfile.ZIP_STORED)

            # Add all other files
            for file_path in temp_dir.rglob('*'):
                if file_path.is_file() and file_path.name != 'mimetype':
                    arcname = file_path.relative_to(temp_dir)
                    zf.write(file_path, arcname)

        file_size_mb = OUTPUT_FILE.stat().st_size / 1024 / 1024
        print(f"\n✓ EPUB created: {OUTPUT_FILE}")
        print(f"  Size: {file_size_mb:.1f} MB")
        print(f"  Total books: {len(books)}")
        print(f"  Total chapters: {len(chapters_response.data)}")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    main()
