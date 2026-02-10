#!/usr/bin/env python3
"""
Compile all book summaries into a single EPUB: "Summary Agrifood books.epub"

V2: Fixed Supabase 1000-row limit bug + each book gets its own XHTML file
for proper e-reader navigation and highlight tracking.
"""
import sys
import zipfile
import html
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


def escape(text: str) -> str:
    """HTML-escape text for safe embedding."""
    if not text:
        return ""
    return html.escape(str(text))


def markdown_to_html(text: str) -> str:
    """Convert markdown-style formatting to HTML."""
    import re
    if not text:
        return ""

    # Escape HTML first
    text = html.escape(text)

    # Convert **bold** to <strong>
    text = re.sub(r'\*\*([^*]+)\*\*', r'<strong>\1</strong>', text)

    # Convert bullet points: lines starting with "- " to list items
    lines = text.split('\n')
    result = []
    in_list = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('- '):
            if not in_list:
                result.append('<ul>')
                in_list = True
            result.append(f'<li>{stripped[2:]}</li>')
        else:
            if in_list:
                result.append('</ul>')
                in_list = False
            if stripped:
                result.append(f'<p>{stripped}</p>')
    if in_list:
        result.append('</ul>')

    return '\n'.join(result)


def clean_summary(summary: str, chapter_title: str = '') -> str:
    """Clean up summary text: remove redundant headers, convert markdown."""
    import re
    if not summary:
        return ""

    text = summary.strip()

    # Remove leading "# Chapter Overview" or similar headers
    patterns_to_strip = [
        r'^#+\s*(Chapter\s+\d*\s*Overview[:\s]*)',
        r'^#+\s*(CHAPTER\s+\d*\s*OVERVIEW[:\s]*)',
        r'^#+\s*(Section\s+\d*\s*Overview[:\s]*)',
        r'^#+\s*(Chapter\s+Overview[:\s]*)',
        r'^#+\s*',  # Any leading markdown header
        r'^\*\*Chapter\s+Overview[:\s]*\*\*\s*',
        r'^\*\*CHAPTER\s+OVERVIEW[:\s]*\*\*\s*',
    ]
    for pattern in patterns_to_strip:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE).strip()

    # Remove leading ": Chapter Title" (duplicate of heading)
    if chapter_title and text.startswith(f': {chapter_title}'):
        text = text[len(f': {chapter_title}'):].strip()
    elif text.startswith(':'):
        text = text[1:].strip()

    # Remove leading chapter title if it's repeated
    if chapter_title:
        # Check if summary starts with the chapter title text
        title_clean = chapter_title.strip()
        if text.lower().startswith(title_clean.lower()):
            text = text[len(title_clean):].strip()

    return text


def is_junk_chapter(chapter: dict) -> bool:
    """Filter out non-content chapters (endnotes, page numbers, etc.)."""
    title = str(chapter.get('chapter_title', '') or '').strip()
    word_count = chapter.get('word_count', 0) or 0
    summary = chapter.get('preview_summary', '') or ''

    # Skip chapters with just numbers as titles (page numbers from PDF conversion)
    if title.isdigit():
        return True

    # Skip common non-content sections
    skip_patterns = [
        'copyright', 'endnote', 'footnote', 'bibliography',
        'index', 'list of images', 'list of tables', 'list of figures',
        'about the author', 'also by', 'title page', 'half title',
        'frontispiece', 'colophon', 'dedication',
    ]
    title_lower = title.lower()
    for pattern in skip_patterns:
        if pattern in title_lower:
            return True

    # Skip very short chapters with no summary
    if word_count < 200 and not summary:
        return True

    return False


def fetch_all_chapters(supabase, book_ids: list[str]) -> list[dict]:
    """Fetch ALL chapters, paginating past Supabase's 1000-row limit."""
    all_chapters = []
    page_size = 500
    offset = 0

    while True:
        response = supabase.table('book_chapters').select(
            'book_id, chapter_number, chapter_title, preview_summary, word_count, context_bridge'
        ).in_('book_id', book_ids).is_(
            'parent_chapter_id', 'null'
        ).order('chapter_number').range(offset, offset + page_size - 1).execute()

        batch = response.data
        all_chapters.extend(batch)

        if len(batch) < page_size:
            break  # No more pages
        offset += page_size

    return all_chapters


def main():
    print("=" * 70)
    print("COMPILING SUMMARY EPUB V2: Agrifood Books")
    print("=" * 70)

    supabase = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_KEY'])

    # Fetch books
    print("\nFetching books...")
    books_response = supabase.table('books').select(
        'id, title, author, summary, word_count'
    ).not_.is_('word_count', 'null').order('title').execute()

    books = books_response.data
    print(f"  Found {len(books)} books")

    # Fetch ALL chapters with pagination
    print("\nFetching chapters (paginated)...")
    all_book_ids = [b['id'] for b in books]
    all_chapters = fetch_all_chapters(supabase, all_book_ids)
    print(f"  Found {len(all_chapters)} total chapters")

    # Group chapters by book_id
    chapters_by_book = {}
    for ch in all_chapters:
        book_id = ch['book_id']
        if book_id not in chapters_by_book:
            chapters_by_book[book_id] = []
        chapters_by_book[book_id].append(ch)

    for book in books:
        book['chapters'] = chapters_by_book.get(book['id'], [])

    # Show stats
    for book in books:
        ch_count = len(book['chapters'])
        with_summary = sum(1 for ch in book['chapters'] if ch.get('preview_summary'))
        has_overview = bool(book.get('summary') and len(book['summary']) > 20)
        flag = "" if (ch_count > 0 and has_overview) else " <-- MISSING DATA"
        print(f"  {book['title'][:45]:45s} | {ch_count:3d} ch | {with_summary:3d} summaries | overview: {'Y' if has_overview else 'N'}{flag}")

    # Build EPUB
    print("\nBuilding EPUB...")
    import tempfile
    import shutil

    temp_dir = Path(tempfile.mkdtemp())

    try:
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

        # OEBPS dir
        oebps = temp_dir / 'OEBPS'
        oebps.mkdir()

        # === Create one XHTML file per book (+ title + overview pages) ===
        book_files = []  # (id, filename, title)

        # Compute stats for overview
        total_words = sum(b.get('word_count', 0) or 0 for b in books)
        books_with_overviews = sum(1 for b in books if b.get('summary') and len(b['summary']) > 20)

        # Categorize books by size
        def reading_time(words):
            hours = words / 15000  # ~250 wpm, ~15k words/hour
            if hours < 3:
                return "Short read"
            elif hours < 8:
                return "Medium read"
            else:
                return "Long read"

        # Title page
        title_html = f'''<?xml version="1.0" encoding="UTF-8"?>
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
    <p class="compiled-by">Compiled by Jarvis AI - {datetime.now().strftime('%B %Y')}</p>
    <div class="intro">
      <h2>How to Use This Book</h2>
      <p>This compilation contains AI-generated summaries to help you decide what to read in detail.</p>
      <p>Each book includes:</p>
      <ul>
        <li><strong>Book Overview</strong> - Main thesis and who it's for (100-150 words)</li>
        <li><strong>Chapter Summaries</strong> - What each chapter covers (50-100 words)</li>
      </ul>
      <p>Highlight sections you find interesting - each book is clearly labeled so you can trace highlights back to specific books and chapters.</p>
      <p class="stats"><strong>Library stats:</strong> {len(books)} books | {total_words:,} total words | ~{total_words // 15000} hours of reading</p>
    </div>
  </div>
</body>
</html>
'''
        (oebps / 'title.xhtml').write_text(title_html, encoding='utf-8')

        # Library Overview page (table of contents with word counts)
        toc_rows = []
        for i, book in enumerate(books, 1):
            bw = book.get('word_count', 0) or 0
            ba = escape(book.get('author', 'Unknown') or 'Unknown')
            bt = escape(book['title'])
            hours = bw / 15000
            time_str = f"{hours:.1f}h" if hours >= 1 else f"{int(hours * 60)}min"
            size_label = reading_time(bw)
            has_overview = "Yes" if (book.get('summary') and len(book['summary']) > 20) else "No"

            toc_rows.append(f'''
    <tr>
      <td class="toc-num">{i}</td>
      <td class="toc-title"><a href="book_{i:02d}.xhtml">{bt}</a><br/><span class="toc-author">{ba}</span></td>
      <td class="toc-words">{bw:,}</td>
      <td class="toc-time">{time_str}</td>
      <td class="toc-size">{size_label}</td>
    </tr>''')

        overview_html = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <title>Library Overview</title>
  <link rel="stylesheet" type="text/css" href="styles.css"/>
</head>
<body>
  <div class="overview-page">
    <h1>Library Overview</h1>
    <p>{len(books)} books | {total_words:,} total words | {books_with_overviews} with AI summaries</p>

    <table class="toc-table">
      <thead>
        <tr>
          <th>#</th>
          <th>Book</th>
          <th>Words</th>
          <th>Time</th>
          <th>Length</th>
        </tr>
      </thead>
      <tbody>
        {''.join(toc_rows)}
      </tbody>
    </table>

    <div class="legend">
      <p><strong>Reading time</strong> assumes ~250 words per minute (15,000 words/hour).</p>
      <p><strong>Short read</strong> = under 3 hours | <strong>Medium read</strong> = 3-8 hours | <strong>Long read</strong> = 8+ hours</p>
    </div>
  </div>
</body>
</html>
'''
        (oebps / 'overview.xhtml').write_text(overview_html, encoding='utf-8')

        # Create each book's XHTML file
        for i, book in enumerate(books, 1):
            filename = f'book_{i:02d}.xhtml'
            title = escape(book['title'])
            author = escape(book.get('author', 'Unknown'))
            summary = book.get('summary', '') or ''
            word_count = book.get('word_count', 0) or 0
            chapters = book.get('chapters', [])

            # Format summary - convert markdown to HTML
            if summary:
                summary_html = markdown_to_html(summary)
            else:
                summary_html = '<p><em>No summary available yet.</em></p>'

            # Filter out junk chapters (endnotes, page numbers, etc.)
            real_chapters = [ch for ch in chapters if not is_junk_chapter(ch)]

            # Build chapters HTML
            chapters_html = ''
            if real_chapters:
                skipped = len(chapters) - len(real_chapters)
                chapters_html = '<div class="chapters"><h2>Chapter Guide</h2>\n'
                if skipped > 0:
                    chapters_html += f'<p class="chapter-meta"><em>Showing {len(real_chapters)} content chapters (skipped {skipped} non-content sections)</em></p>\n'

                for ch in real_chapters:
                    ch_title_raw = ch.get('chapter_title') or f"Chapter {ch.get('chapter_number', '?')}"
                    ch_title = escape(ch_title_raw)
                    ch_summary = ch.get('preview_summary', '') or ''
                    ch_word_count = ch.get('word_count', 0) or 0

                    # Clean summary: remove redundant headers, convert markdown
                    ch_summary_clean = clean_summary(ch_summary, ch_title_raw)
                    ch_summary_html = markdown_to_html(ch_summary_clean) if ch_summary_clean else ''

                    if ch_summary_html:
                        chapters_html += f'''
  <div class="chapter">
    <h3>{ch_title}</h3>
    <p class="chapter-meta">{ch_word_count:,} words</p>
    <div class="chapter-summary">{ch_summary_html}</div>
  </div>
'''
                    else:
                        chapters_html += f'''
  <div class="chapter">
    <h3>{ch_title}</h3>
    <p class="chapter-meta">{ch_word_count:,} words</p>
  </div>
'''
                chapters_html += '</div>\n'

            # Full book page
            book_html = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <title>{title}</title>
  <link rel="stylesheet" type="text/css" href="styles.css"/>
</head>
<body>
  <div class="book-section">
    <h1 class="book-title">{i}. {title}</h1>
    <p class="book-author">by {author}</p>
    <p class="book-meta">{word_count:,} words | {len(chapters)} chapters</p>

    <div class="book-overview">
      <h2>Book Overview</h2>
      <div class="overview-content">
        {summary_html}
      </div>
    </div>

    {chapters_html}
  </div>
</body>
</html>
'''
            (oebps / filename).write_text(book_html, encoding='utf-8')
            book_files.append((f'book_{i:02d}', filename, book['title'][:60]))

            if i % 10 == 0:
                print(f"  Generated {i}/{len(books)} book pages...")

        # CSS
        css = '''
body { font-family: Georgia, serif; line-height: 1.6; margin: 0 auto; padding: 1em; }
h1, h2, h3 { font-family: -apple-system, "Segoe UI", sans-serif; }

.title-page { text-align: center; margin: 4em 1em; }
.title-page h1 { font-size: 2.2em; color: #2c3e50; margin-bottom: 0.3em; }
.subtitle { font-size: 1.1em; color: #7f8c8d; margin: 0.3em 0; }
.compiled-by { font-style: italic; color: #95a5a6; margin: 2em 0; }
.intro { text-align: left; margin: 2em auto; padding: 1.5em; background: #ecf0f1; border-radius: 6px; }
.intro h2 { margin-top: 0; color: #34495e; }

.book-section { margin: 1em 0; }
.book-title { font-size: 1.8em; color: #2c3e50; margin: 0.5em 0 0.2em 0; }
.book-author { font-size: 1.1em; color: #7f8c8d; margin: 0.2em 0; }
.book-meta { color: #95a5a6; font-size: 0.9em; margin: 0.3em 0 1.5em 0; }

.book-overview { background: #f8f9fa; padding: 1.2em; margin: 1.5em 0; border-left: 4px solid #3498db; border-radius: 4px; }
.book-overview h2 { margin: 0 0 0.8em 0; color: #2c3e50; font-size: 1.2em; }
.overview-content { line-height: 1.7; color: #34495e; }
.overview-content p { margin: 0.5em 0; }

.chapters { margin: 2em 0; }
.chapters > h2 { font-size: 1.3em; color: #2c3e50; border-bottom: 2px solid #ecf0f1; padding-bottom: 0.4em; }

.chapter { margin: 1.2em 0; padding: 0.8em 0; border-bottom: 1px solid #f0f0f0; }
.chapter h3 { color: #34495e; margin: 0 0 0.3em 0; font-size: 1.05em; }
.chapter-meta { color: #95a5a6; font-size: 0.8em; margin: 0.2em 0; }
.chapter-summary { line-height: 1.6; color: #555; margin: 0.5em 0; font-size: 0.95em; }
.chapter-summary p { margin: 0.3em 0; }
.chapter-summary ul { margin: 0.3em 0; padding-left: 1.5em; }
.chapter-summary li { margin: 0.2em 0; }
.stats { margin-top: 1.5em; color: #34495e; }

/* Library Overview */
.overview-page { margin: 1em 0; }
.overview-page h1 { color: #2c3e50; font-size: 1.8em; }
.toc-table { width: 100%; border-collapse: collapse; margin: 1.5em 0; font-size: 0.9em; }
.toc-table th { text-align: left; padding: 0.6em 0.4em; border-bottom: 2px solid #3498db; color: #2c3e50; font-size: 0.85em; }
.toc-table td { padding: 0.5em 0.4em; border-bottom: 1px solid #ecf0f1; vertical-align: top; }
.toc-table tr:hover { background: #f8f9fa; }
.toc-num { width: 2em; color: #95a5a6; }
.toc-title a { color: #2c3e50; text-decoration: none; font-weight: 600; }
.toc-author { color: #95a5a6; font-size: 0.85em; }
.toc-words { text-align: right; color: #7f8c8d; font-size: 0.9em; }
.toc-time { text-align: right; color: #7f8c8d; font-size: 0.9em; }
.toc-size { color: #95a5a6; font-size: 0.85em; }
.legend { margin: 1.5em 0; color: #95a5a6; font-size: 0.85em; }
.legend p { margin: 0.3em 0; }
'''
        (oebps / 'styles.css').write_text(css, encoding='utf-8')

        # OPF manifest with all book files
        manifest_items = [
            '<item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>',
            '<item id="title" href="title.xhtml" media-type="application/xhtml+xml"/>',
            '<item id="overview" href="overview.xhtml" media-type="application/xhtml+xml"/>',
            '<item id="css" href="styles.css" media-type="text/css"/>',
        ]
        spine_items = ['<itemref idref="title"/>', '<itemref idref="overview"/>']

        for file_id, filename, _ in book_files:
            manifest_items.append(f'<item id="{file_id}" href="{filename}" media-type="application/xhtml+xml"/>')
            spine_items.append(f'<itemref idref="{file_id}"/>')

        opf = f'''<?xml version="1.0" encoding="UTF-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="book-id">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Summary: Agrifood Books</dc:title>
    <dc:creator>Compiled by Jarvis AI</dc:creator>
    <dc:language>en</dc:language>
    <dc:date>{datetime.now().strftime('%Y-%m-%d')}</dc:date>
    <dc:description>Summaries of {len(books)} agrifood books</dc:description>
    <dc:identifier id="book-id">agrifood-summaries-v2-{datetime.now().strftime('%Y%m%d')}</dc:identifier>
    <meta property="dcterms:modified">{datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}</meta>
  </metadata>
  <manifest>
    {chr(10)+'    '.join(manifest_items)}
  </manifest>
  <spine>
    {chr(10)+'    '.join(spine_items)}
  </spine>
</package>
'''
        (oebps / 'content.opf').write_text(opf, encoding='utf-8')

        # Navigation (table of contents)
        nav_items = ['      <li><a href="overview.xhtml">Library Overview</a></li>']
        for file_id, filename, title in book_files:
            nav_items.append(f'      <li><a href="{filename}">{escape(title)}</a></li>')

        nav_html = f'''<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:epub="http://www.idpf.org/2007/ops">
<head><title>Table of Contents</title></head>
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
        (oebps / 'nav.xhtml').write_text(nav_html, encoding='utf-8')

        # Package EPUB
        print("\n  Packaging EPUB...")
        with zipfile.ZipFile(OUTPUT_FILE, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(temp_dir / 'mimetype', 'mimetype', compress_type=zipfile.ZIP_STORED)

            for file_path in temp_dir.rglob('*'):
                if file_path.is_file() and file_path.name != 'mimetype':
                    arcname = file_path.relative_to(temp_dir)
                    zf.write(file_path, arcname)

        file_size = OUTPUT_FILE.stat().st_size / 1024
        total_chapters = sum(len(b['chapters']) for b in books)
        books_with_chapters = sum(1 for b in books if b['chapters'])
        books_with_summaries = sum(1 for b in books if b.get('summary') and len(b['summary']) > 20)

        print(f"\n{'=' * 70}")
        print(f"EPUB CREATED SUCCESSFULLY!")
        print(f"{'=' * 70}")
        print(f"  File: {OUTPUT_FILE}")
        print(f"  Size: {file_size:.0f} KB")
        print(f"  Books: {len(books)} ({books_with_summaries} with overviews)")
        print(f"  Chapters: {total_chapters} ({books_with_chapters} books have chapters)")
        print(f"  Structure: 1 XHTML file per book (for highlight tracking)")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    main()
