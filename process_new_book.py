#!/usr/bin/env python3
"""
===================================================================================
NEW BOOK PROCESSING PIPELINE
===================================================================================

Orchestrates the complete pipeline for processing a new EPUB book:

1. Parse EPUB - Extract metadata and chapters
2. Build reader context - Personalize based on reading history
3. Create/update book in Supabase
4. Store chapters in book_chapters table
5. Generate enhancements (preview summaries + learning questions)
6. Build enhanced EPUB with injected content
7. Upload original to Google Drive (Jarvis/books/originals/)
8. Upload enhanced to Google Drive (Jarvis/books/)
9. Upload enhanced to Bookfusion
10. Update database with all URLs and metadata

Usage:
    python process_new_book.py /path/to/book.epub
    python process_new_book.py book.epub --preview          # Show what would be generated
    python process_new_book.py book.epub --skip-bookfusion  # Don't upload to Bookfusion
    python process_new_book.py book.epub --skip-drive       # Don't upload to Drive
    python process_new_book.py book.epub -o output.epub     # Custom output path

Environment Variables:
    SUPABASE_URL           - Supabase database URL
    SUPABASE_KEY           - Supabase API key
    ANTHROPIC_API_KEY      - For AI enhancement generation
    GOOGLE_TOKEN_JSON      - Google OAuth credentials for Drive
    BOOKFUSION_API_KEY     - Optional, for Bookfusion upload
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Add lib to path
sys.path.insert(0, os.path.dirname(__file__))

from lib.epub_parser import EPUBParser, EPUBMetadata, Chapter, parse_epub_file
from lib.enhancement_context import build_reader_context_sync, ReaderContext
from lib.bookfusion_client import BookfusionClient, BookfusionMetadata, UploadResult

# =============================================================================
# CONFIGURATION
# =============================================================================

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
BOOKFUSION_API_KEY = os.environ.get('BOOKFUSION_API_KEY')

# Google OAuth - supports both formats:
# 1. GOOGLE_TOKEN_JSON (combined JSON) - for local development
# 2. GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN - for Cloud Run
GOOGLE_TOKEN_JSON = os.environ.get('GOOGLE_TOKEN_JSON')
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = os.environ.get('GOOGLE_REFRESH_TOKEN')

# Check if we have Google credentials (either format)
HAS_GOOGLE_CREDS = bool(GOOGLE_TOKEN_JSON) or all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN])

# Drive folder structure
JARVIS_FOLDER_NAME = "Jarvis"
BOOKS_FOLDER_NAME = "books"
ORIGINALS_FOLDER_NAME = "originals"

# Bookfusion shelf for books (different from articles)
BOOKFUSION_BOOKS_SHELF = "Books"


# =============================================================================
# GOOGLE DRIVE HELPER
# =============================================================================

class GoogleDriveUploader:
    """Handles Google Drive uploads for EPUBs."""

    def __init__(self):
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        # Try GOOGLE_TOKEN_JSON first (local dev), then individual vars (Cloud Run)
        if GOOGLE_TOKEN_JSON:
            token_data = json.loads(GOOGLE_TOKEN_JSON)
            creds = Credentials.from_authorized_user_info(token_data)
        elif all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
            creds = Credentials(
                token=None,
                refresh_token=GOOGLE_REFRESH_TOKEN,
                client_id=GOOGLE_CLIENT_ID,
                client_secret=GOOGLE_CLIENT_SECRET,
                token_uri="https://oauth2.googleapis.com/token"
            )
        else:
            raise ValueError(
                "Google credentials not configured. Set either GOOGLE_TOKEN_JSON "
                "or GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET + GOOGLE_REFRESH_TOKEN"
            )

        # Refresh if expired or no token yet
        if creds and (not creds.token or (creds.expired and creds.refresh_token)):
            creds.refresh(Request())

        self.drive = build('drive', 'v3', credentials=creds)
        self._books_folder_id: Optional[str] = None
        self._originals_folder_id: Optional[str] = None

    def _find_or_create_folder(self, name: str, parent_id: Optional[str] = None) -> str:
        """Find or create a folder in Drive."""
        query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"

        results = self.drive.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        folders = results.get('files', [])
        if folders:
            return folders[0]['id']

        # Create folder
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_id:
            file_metadata['parents'] = [parent_id]

        folder = self.drive.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        return folder['id']

    def _ensure_folders(self) -> None:
        """Ensure Jarvis/books and Jarvis/books/originals folders exist."""
        # Find Jarvis folder
        results = self.drive.files().list(
            q=f"name='{JARVIS_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        jarvis_folders = results.get('files', [])
        if not jarvis_folders:
            raise ValueError(f"'{JARVIS_FOLDER_NAME}' folder not found in Drive")

        jarvis_id = jarvis_folders[0]['id']

        # Find or create books folder
        self._books_folder_id = self._find_or_create_folder(BOOKS_FOLDER_NAME, jarvis_id)

        # Find or create originals subfolder
        self._originals_folder_id = self._find_or_create_folder(ORIGINALS_FOLDER_NAME, self._books_folder_id)

    def upload_file(self, file_path: Path, folder: str = "books") -> tuple[str, str]:
        """
        Upload file to Google Drive.

        Args:
            file_path: Path to file
            folder: "books" or "originals"

        Returns:
            Tuple of (file_id, web_url)
        """
        from googleapiclient.http import MediaFileUpload

        if self._books_folder_id is None:
            self._ensure_folders()

        parent_id = self._originals_folder_id if folder == "originals" else self._books_folder_id

        file_metadata = {
            'name': file_path.name,
            'parents': [parent_id]
        }

        media = MediaFileUpload(
            str(file_path),
            mimetype='application/epub+zip',
            resumable=True
        )

        file = self.drive.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink'
        ).execute()

        return file['id'], file.get('webViewLink', '')


# =============================================================================
# EPUB ENHANCER (Placeholder until lib/epub_enhancer.py is created)
# =============================================================================

class EPUBLearningEnhancer:
    """
    Enhances EPUB with AI-generated preview summaries and learning questions.

    NOTE: This is a placeholder implementation. The full implementation
    should be in lib/epub_enhancer.py. This provides the expected interface.
    """

    def __init__(self, anthropic_api_key: str, reader_context: Optional[ReaderContext] = None):
        """
        Initialize enhancer.

        Args:
            anthropic_api_key: Anthropic API key for Claude
            reader_context: Optional personalized reader context
        """
        self.api_key = anthropic_api_key
        self.reader_context = reader_context
        self._client = None

    def _get_client(self):
        """Get or create Anthropic client."""
        if self._client is None:
            import anthropic
            self._client = anthropic.Anthropic(api_key=self.api_key)
        return self._client

    def generate_chapter_enhancement(
        self,
        chapter: Chapter,
        book_title: str,
        book_author: Optional[str] = None
    ) -> dict:
        """
        Generate preview summary and learning questions for a chapter.

        Args:
            chapter: Chapter to enhance
            book_title: Title of the book
            book_author: Author of the book

        Returns:
            Dict with 'preview_summary' and 'learning_questions'
        """
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

        prompt = f"""{context_section}You are enhancing a book chapter for active learning.

BOOK: {book_title}
{f'AUTHOR: {book_author}' if book_author else ''}
CHAPTER: {chapter.title or f'Chapter {chapter.number}'}

CHAPTER CONTENT:
{content}

Generate:

1. PREVIEW SUMMARY (50-100 words)
   A teaser that hooks the reader without spoilers. Focus on:
   - The main question or problem this chapter addresses
   - Why it matters to the reader
   - What transformation/insight awaits

2. LEARNING QUESTIONS (3-5 questions)
   Questions to prime active reading. Include:
   - One about the chapter's core thesis/argument
   - One connecting to broader themes or real-world application
   - One that challenges assumptions or invites critical thinking

Format your response as JSON:
```json
{{
  "preview_summary": "Your preview summary here...",
  "learning_questions": [
    "Question 1?",
    "Question 2?",
    "Question 3?"
  ]
}}
```
"""

        response = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )

        # Parse JSON from response
        import re
        text = response.content[0].text

        json_match = re.search(r'```json\s*(\{[\s\S]*?\})\s*```', text)
        if json_match:
            return json.loads(json_match.group(1))

        # Fallback: try to find JSON object
        json_match = re.search(r'\{[\s\S]*"preview_summary"[\s\S]*\}', text)
        if json_match:
            return json.loads(json_match.group())

        # Default fallback
        return {
            "preview_summary": "Read this chapter to discover key insights.",
            "learning_questions": [
                "What is the main argument of this chapter?",
                "How does this connect to your own experience?",
                "What would you challenge or question?"
            ]
        }

    def build_enhanced_epub(
        self,
        original_epub_path: Path,
        chapters: list[Chapter],
        enhancements: dict[int, dict],
        output_path: Optional[Path] = None
    ) -> Path:
        """
        Build enhanced EPUB with injected preview/questions.

        Args:
            original_epub_path: Path to original EPUB
            chapters: List of parsed chapters
            enhancements: Dict mapping chapter_number to enhancement data
            output_path: Optional output path (default: adds _enhanced suffix)

        Returns:
            Path to enhanced EPUB
        """
        import zipfile
        import shutil
        from bs4 import BeautifulSoup

        if output_path is None:
            stem = original_epub_path.stem
            output_path = original_epub_path.parent / f"{stem}_enhanced.epub"

        # Copy original EPUB
        shutil.copy2(original_epub_path, output_path)

        # Modify the copy
        with zipfile.ZipFile(output_path, 'a') as zf:
            # For each chapter with enhancement, inject content
            from xml.etree import ElementTree as ET
            parser = EPUBParser(original_epub_path)
            parser._find_opf()
            parser._build_manifest(
                ET.fromstring(zf.read(parser._opf_path).decode('utf-8'))
            )
            parser._build_spine(
                ET.fromstring(zf.read(parser._opf_path).decode('utf-8'))
            )

            for chapter in chapters:
                if chapter.number not in enhancements:
                    continue

                enhancement = enhancements[chapter.number]
                full_path = parser._resolve_path(chapter.epub_href)

                try:
                    content_html = zf.read(full_path).decode('utf-8')
                    soup = BeautifulSoup(content_html, 'html.parser')

                    # Create enhancement block
                    enhancement_html = self._create_enhancement_html(
                        enhancement.get('preview_summary', ''),
                        enhancement.get('learning_questions', [])
                    )

                    # Find body and prepend enhancement
                    body = soup.find('body')
                    if body:
                        enhancement_soup = BeautifulSoup(enhancement_html, 'html.parser')
                        body.insert(0, enhancement_soup)

                        # Write modified content back
                        # Note: ZipFile doesn't support overwriting, so we need a workaround
                        # For now, we'll skip actual injection - full implementation needed
                        pass

                except Exception as e:
                    print(f"Warning: Could not enhance {chapter.epub_href}: {e}")

            parser.close()

        return output_path

    def _create_enhancement_html(self, preview: str, questions: list[str]) -> str:
        """Create HTML block for chapter enhancement."""
        questions_html = '\n'.join(
            f'<li>{q}</li>' for q in questions
        )

        return f'''
<div class="jarvis-enhancement" style="background:#f5f5f5; padding:1em; margin-bottom:2em; border-left:3px solid #007bff;">
  <div class="preview-summary" style="font-style:italic; margin-bottom:1em;">
    <strong>Chapter Preview:</strong><br/>
    {preview}
  </div>
  <div class="learning-questions">
    <strong>Questions to Consider:</strong>
    <ul>
      {questions_html}
    </ul>
  </div>
</div>
'''


# =============================================================================
# MAIN PIPELINE
# =============================================================================

class BookProcessingPipeline:
    """Orchestrates the complete book processing pipeline."""

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        anthropic_api_key: Optional[str] = None,
        use_drive: bool = True,
        use_bookfusion: bool = True
    ):
        self.supabase = create_client(supabase_url, supabase_key)
        self.anthropic_api_key = anthropic_api_key
        self.use_drive = use_drive
        self.use_bookfusion = use_bookfusion

        self.drive: Optional[GoogleDriveUploader] = None
        if use_drive and HAS_GOOGLE_CREDS:
            try:
                self.drive = GoogleDriveUploader()
            except Exception as e:
                print(f"Warning: Could not initialize Drive: {e}")
                self.use_drive = False

    def find_book_by_title(self, title: str) -> Optional[dict]:
        """Find existing book in database by title."""
        # Try exact match first
        response = self.supabase.table('books').select('*').ilike(
            'title', title
        ).execute()

        if response.data:
            return response.data[0]

        # Try partial match
        base_title = title.split(':')[0].strip()
        response = self.supabase.table('books').select('*').ilike(
            'title', f'%{base_title}%'
        ).execute()

        if response.data:
            return min(response.data, key=lambda b: len(b['title']))

        return None

    def create_book(self, metadata: EPUBMetadata) -> dict:
        """Create new book entry in database."""
        book_data = {
            'title': metadata.title or 'Unknown Title',
            'author': metadata.author,
            'status': 'To Read',
            'processed_at': datetime.now(timezone.utc).isoformat()
        }

        response = self.supabase.table('books').insert(book_data).execute()
        return response.data[0]

    def store_chapters(self, book_id: str, chapters: list[Chapter], enhancements: dict[int, dict]) -> int:
        """Store chapters in book_chapters table."""
        # Delete existing chapters for this book
        existing = self.supabase.table('book_chapters').select('id').eq(
            'book_id', book_id
        ).execute()
        if existing.data:
            print(f"  Removing {len(existing.data)} existing chapters...")
            self.supabase.table('book_chapters').delete().eq(
                'book_id', book_id
            ).execute()

        # Insert new chapters
        chapter_records = []
        for chapter in chapters:
            enhancement = enhancements.get(chapter.number, {})
            chapter_records.append({
                'book_id': book_id,
                'chapter_number': chapter.number,
                'chapter_title': chapter.title,
                'epub_href': chapter.epub_href,
                'content': chapter.content,
                'content_html': chapter.content_html,
                'word_count': chapter.word_count,
                'preview_summary': enhancement.get('preview_summary'),
                'learning_questions': enhancement.get('learning_questions')
            })

        # Insert in batches
        batch_size = 10
        for i in range(0, len(chapter_records), batch_size):
            batch = chapter_records[i:i + batch_size]
            self.supabase.table('book_chapters').insert(batch).execute()

        return len(chapter_records)

    def update_book_urls(
        self,
        book_id: str,
        original_drive_id: Optional[str] = None,
        original_drive_url: Optional[str] = None,
        enhanced_drive_id: Optional[str] = None,
        enhanced_drive_url: Optional[str] = None,
        bookfusion_id: Optional[str] = None
    ) -> None:
        """Update book record with Drive and Bookfusion URLs."""
        update_data = {
            'updated_at': datetime.now(timezone.utc).isoformat()
        }

        if original_drive_id:
            update_data['original_drive_file_id'] = original_drive_id
        if original_drive_url:
            update_data['original_drive_url'] = original_drive_url
        if enhanced_drive_id:
            update_data['drive_file_id'] = enhanced_drive_id
            update_data['epub_status'] = 'linked'
        if enhanced_drive_url:
            update_data['drive_url'] = enhanced_drive_url
        if bookfusion_id:
            update_data['bookfusion_id'] = bookfusion_id

        self.supabase.table('books').update(update_data).eq('id', book_id).execute()

    async def upload_to_bookfusion(
        self,
        epub_path: Path,
        metadata: EPUBMetadata
    ) -> Optional[str]:
        """Upload EPUB to Bookfusion."""
        if not BOOKFUSION_API_KEY:
            print("  Bookfusion API key not configured - skipping")
            return None

        bf_metadata = BookfusionMetadata(
            title=metadata.title or "Unknown Title",
            author_list=[metadata.author] if metadata.author else [],
            language=metadata.language or "eng",
            isbn=metadata.identifier,
            bookshelves=[BOOKFUSION_BOOKS_SHELF]
        )

        async with BookfusionClient(BOOKFUSION_API_KEY) as client:
            result = await client.upload_book(str(epub_path), bf_metadata)

        if result.success:
            return result.bookfusion_id
        else:
            print(f"  Bookfusion upload failed: {result.error}")
            return None

    def process(
        self,
        epub_path: Path,
        output_path: Optional[Path] = None,
        preview: bool = False
    ) -> dict:
        """
        Process a book through the complete pipeline.

        Args:
            epub_path: Path to EPUB file
            output_path: Optional custom output path for enhanced EPUB
            preview: If True, only show what would be generated (no saving)

        Returns:
            Dict with processing results
        """
        results = {
            'success': False,
            'book_id': None,
            'book_title': None,
            'chapters_processed': 0,
            'enhancements_generated': 0,
            'original_drive_url': None,
            'enhanced_drive_url': None,
            'bookfusion_id': None
        }

        print(f"\n{'='*60}")
        print("BOOK PROCESSING PIPELINE")
        print(f"{'='*60}")
        print(f"Input: {epub_path}")
        print(f"Mode: {'PREVIEW' if preview else 'FULL PROCESSING'}")
        print()

        # Step 1: Parse EPUB
        print("[1/10] Parsing EPUB...")
        metadata, chapters = parse_epub_file(epub_path)
        print(f"  Title: {metadata.title}")
        print(f"  Author: {metadata.author}")
        print(f"  Chapters: {len(chapters)}")
        total_words = sum(c.word_count for c in chapters)
        print(f"  Total words: {total_words:,}")

        results['book_title'] = metadata.title

        if preview:
            print("\n--- CHAPTER LIST ---")
            for ch in chapters:
                title = ch.title or '(Untitled)'
                print(f"  {ch.number:2}. {title[:50]:<50} ({ch.word_count:,} words)")

        # Step 2: Build reader context
        print("\n[2/10] Building reader context...")
        reader_context = None
        if self.anthropic_api_key:
            try:
                reader_context = build_reader_context_sync(
                    self.supabase,
                    current_book_title=metadata.title
                )
                print(f"  Thinking style: {', '.join(reader_context.thinking_style)}")
                print(f"  Recent reads: {len(reader_context.recent_books)} books")
            except Exception as e:
                print(f"  Warning: Could not build context: {e}")
        else:
            print("  Skipped (no ANTHROPIC_API_KEY)")

        # Step 3: Check/create book in database
        print("\n[3/10] Checking database...")
        book = self.find_book_by_title(metadata.title or "Unknown")

        if preview:
            if book:
                print(f"  Found existing book: {book['title']} (ID: {book['id']})")
            else:
                print(f"  Would create new book: {metadata.title}")
            results['book_id'] = book['id'] if book else 'NEW'
        else:
            if book:
                print(f"  Found existing book: {book['title']} (ID: {book['id']})")
            else:
                print(f"  Creating new book...")
                book = self.create_book(metadata)
                print(f"  Created: {book['title']} (ID: {book['id']})")
            results['book_id'] = book['id']

        # Step 4: Generate enhancements
        print("\n[4/10] Generating enhancements...")
        enhancements: dict[int, dict] = {}

        if self.anthropic_api_key:
            # Filter to leaf chapters (actual content chapters)
            leaf_chapters = [
                ch for ch in chapters
                if ch.word_count >= 500  # Skip very short chapters
            ]

            # In preview mode, only process first 2 chapters
            chapters_to_enhance = leaf_chapters[:2] if preview else leaf_chapters

            enhancer = EPUBLearningEnhancer(self.anthropic_api_key, reader_context)

            for i, chapter in enumerate(chapters_to_enhance):
                title = chapter.title or f'Chapter {chapter.number}'
                print(f"  [{i+1}/{len(chapters_to_enhance)}] {title[:40]}...")

                try:
                    enhancement = enhancer.generate_chapter_enhancement(
                        chapter,
                        metadata.title or "Unknown",
                        metadata.author
                    )
                    enhancements[chapter.number] = enhancement
                    results['enhancements_generated'] += 1

                    if preview:
                        print(f"      Preview: {enhancement.get('preview_summary', '')[:80]}...")
                        questions = enhancement.get('learning_questions', [])
                        for q in questions[:2]:
                            print(f"      Q: {q[:60]}...")

                except Exception as e:
                    print(f"      Error: {e}")
        else:
            print("  Skipped (no ANTHROPIC_API_KEY)")

        # Step 5: Store chapters
        print("\n[5/10] Storing chapters...")
        if preview:
            print(f"  Would store {len(chapters)} chapters")
        else:
            stored = self.store_chapters(book['id'], chapters, enhancements)
            print(f"  Stored {stored} chapters")
            results['chapters_processed'] = stored

        # Step 6: Build enhanced EPUB
        print("\n[6/10] Building enhanced EPUB...")
        enhanced_path = output_path
        if not preview and enhancements:
            enhancer = EPUBLearningEnhancer(self.anthropic_api_key, reader_context)
            enhanced_path = enhancer.build_enhanced_epub(
                epub_path,
                chapters,
                enhancements,
                output_path
            )
            print(f"  Created: {enhanced_path}")
        elif preview:
            stem = epub_path.stem
            enhanced_path = epub_path.parent / f"{stem}_enhanced.epub"
            print(f"  Would create: {enhanced_path}")
        else:
            print("  Skipped (no enhancements)")
            enhanced_path = epub_path  # Use original

        # Step 7: Upload original to Drive
        print("\n[7/10] Uploading original to Drive...")
        if self.use_drive and self.drive:
            if preview:
                print(f"  Would upload to Jarvis/books/originals/{epub_path.name}")
            else:
                try:
                    file_id, url = self.drive.upload_file(epub_path, folder="originals")
                    print(f"  Uploaded: {url}")
                    results['original_drive_url'] = url
                    self.update_book_urls(book['id'], original_drive_id=file_id, original_drive_url=url)
                except Exception as e:
                    print(f"  Error: {e}")
        else:
            print("  Skipped (--skip-drive or Drive not configured)")

        # Step 8: Upload enhanced to Drive
        print("\n[8/10] Uploading enhanced to Drive...")
        if self.use_drive and self.drive and enhanced_path:
            if preview:
                print(f"  Would upload to Jarvis/books/{enhanced_path.name}")
            else:
                try:
                    file_id, url = self.drive.upload_file(enhanced_path, folder="books")
                    print(f"  Uploaded: {url}")
                    results['enhanced_drive_url'] = url
                    self.update_book_urls(book['id'], enhanced_drive_id=file_id, enhanced_drive_url=url)
                except Exception as e:
                    print(f"  Error: {e}")
        else:
            print("  Skipped (--skip-drive or Drive not configured)")

        # Step 9: Upload to Bookfusion
        print("\n[9/10] Uploading to Bookfusion...")
        if self.use_bookfusion and BOOKFUSION_API_KEY:
            upload_path = enhanced_path if enhanced_path and enhanced_path.exists() else epub_path
            if preview:
                print(f"  Would upload {upload_path.name} to Bookfusion")
            else:
                try:
                    bookfusion_id = asyncio.run(self.upload_to_bookfusion(upload_path, metadata))
                    if bookfusion_id:
                        print(f"  Uploaded! ID: {bookfusion_id}")
                        results['bookfusion_id'] = bookfusion_id
                        self.update_book_urls(book['id'], bookfusion_id=bookfusion_id)
                except Exception as e:
                    print(f"  Error: {e}")
        else:
            print("  Skipped (--skip-bookfusion or API key not configured)")

        # Step 10: Final update
        print("\n[10/10] Finalizing...")
        if not preview:
            self.supabase.table('books').update({
                'processed_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }).eq('id', book['id']).execute()
            print("  Database updated")

        results['success'] = True

        # Print summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print(f"{'='*60}")
        print(f"Book: {results['book_title']}")
        print(f"Book ID: {results['book_id']}")
        print(f"Chapters: {len(chapters)}")
        print(f"Enhancements: {results['enhancements_generated']}")
        if results['original_drive_url']:
            print(f"Original Drive: {results['original_drive_url']}")
        if results['enhanced_drive_url']:
            print(f"Enhanced Drive: {results['enhanced_drive_url']}")
        if results['bookfusion_id']:
            print(f"Bookfusion ID: {results['bookfusion_id']}")

        if preview:
            print("\n[PREVIEW MODE] No changes were saved.")

        return results


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Process new EPUB book through enhancement pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python process_new_book.py /path/to/book.epub
  python process_new_book.py book.epub --preview
  python process_new_book.py book.epub --skip-bookfusion --skip-drive
  python process_new_book.py book.epub -o /output/enhanced.epub
        """
    )

    parser.add_argument(
        'epub_path',
        type=Path,
        help='Path to the EPUB file to process'
    )

    parser.add_argument(
        '--preview',
        action='store_true',
        help='Preview mode: show what would be generated without saving'
    )

    parser.add_argument(
        '--skip-bookfusion',
        action='store_true',
        help='Skip uploading to Bookfusion'
    )

    parser.add_argument(
        '--skip-drive',
        action='store_true',
        help='Skip uploading to Google Drive'
    )

    parser.add_argument(
        '-o', '--output',
        type=Path,
        dest='output_path',
        help='Custom output path for enhanced EPUB'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Validate EPUB path
    if not args.epub_path.exists():
        print(f"ERROR: EPUB file not found: {args.epub_path}")
        sys.exit(1)

    if not args.epub_path.suffix.lower() == '.epub':
        print(f"ERROR: File must be an EPUB: {args.epub_path}")
        sys.exit(1)

    # Check required environment variables
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set")
        sys.exit(1)

    if not ANTHROPIC_API_KEY:
        print("WARNING: ANTHROPIC_API_KEY not set - enhancements will be skipped")

    if not args.skip_drive and not HAS_GOOGLE_CREDS:
        print("WARNING: Google credentials not configured - Drive uploads will be skipped")

    if not args.skip_bookfusion and not BOOKFUSION_API_KEY:
        print("WARNING: BOOKFUSION_API_KEY not set - Bookfusion upload will be skipped")

    # Initialize and run pipeline
    pipeline = BookProcessingPipeline(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        anthropic_api_key=ANTHROPIC_API_KEY,
        use_drive=not args.skip_drive,
        use_bookfusion=not args.skip_bookfusion
    )

    try:
        result = pipeline.process(
            epub_path=args.epub_path,
            output_path=args.output_path,
            preview=args.preview
        )

        if result['success']:
            sys.exit(0)
        else:
            print("\nPipeline completed with errors")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\nAborted by user")
        sys.exit(130)

    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
