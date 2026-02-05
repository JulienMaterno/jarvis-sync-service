#!/usr/bin/env python3
"""
Sync Google Drive EPUBs with Supabase books table.

This script performs bidirectional linking:
- Books with EPUBs in Drive → marked as 'linked' with drive_file_id
- Books without EPUBs → marked as 'missing_epub'
- EPUBs without matching books → logged as 'unlinked_epub'

Additionally, when a new EPUB is linked, chapters are extracted and stored.

Usage:
    python sync_drive_books.py                    # Full sync
    python sync_drive_books.py --dry-run          # Preview without changes
    python sync_drive_books.py --extract-chapters # Also extract chapters for newly linked
"""

import argparse
import os
import sys
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
from supabase import create_client

# Google Drive API
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

load_dotenv()

# Add lib to path for epub parser
sys.path.insert(0, os.path.dirname(__file__))
from lib.epub_parser import parse_epub_file

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Google OAuth - supports both formats:
# 1. GOOGLE_TOKEN_JSON (combined JSON) - for local development
# 2. GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN - for Cloud Run
GOOGLE_TOKEN_JSON = os.environ.get('GOOGLE_TOKEN_JSON')
GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = os.environ.get('GOOGLE_REFRESH_TOKEN')

# Drive folder configuration
JARVIS_FOLDER_NAME = "Jarvis"
BOOKS_FOLDER_NAME = "books"


class DriveBookSync:
    """Syncs Google Drive EPUBs with Supabase books table."""

    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.drive = self._init_drive_service()
        self.books_folder_id = None

    def _init_drive_service(self):
        """Initialize Google Drive API service."""
        # Try GOOGLE_TOKEN_JSON first (local dev), then individual vars (Cloud Run)
        if GOOGLE_TOKEN_JSON:
            import json
            token_data = json.loads(GOOGLE_TOKEN_JSON)
            creds = Credentials.from_authorized_user_info(token_data)
        elif all([GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN]):
            # Build credentials from individual env vars (Cloud Run pattern)
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

        return build('drive', 'v3', credentials=creds)

    def find_books_folder(self) -> str | None:
        """Find the Jarvis/books folder in Drive."""
        # First find Jarvis folder
        results = self.drive.files().list(
            q=f"name='{JARVIS_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        jarvis_folders = results.get('files', [])
        if not jarvis_folders:
            print(f"ERROR: '{JARVIS_FOLDER_NAME}' folder not found in Drive")
            return None

        jarvis_id = jarvis_folders[0]['id']
        print(f"Found Jarvis folder: {jarvis_id}")

        # Find books subfolder
        results = self.drive.files().list(
            q=f"name='{BOOKS_FOLDER_NAME}' and '{jarvis_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        books_folders = results.get('files', [])
        if not books_folders:
            print(f"ERROR: '{BOOKS_FOLDER_NAME}' folder not found in Jarvis")
            return None

        self.books_folder_id = books_folders[0]['id']
        print(f"Found books folder: {self.books_folder_id}")
        return self.books_folder_id

    def list_drive_epubs(self) -> list[dict]:
        """List all EPUB files in the books folder."""
        if not self.books_folder_id:
            self.find_books_folder()

        if not self.books_folder_id:
            return []

        results = self.drive.files().list(
            q=f"'{self.books_folder_id}' in parents and name contains '.epub' and trashed=false",
            spaces='drive',
            fields='files(id, name, webViewLink, size, modifiedTime)'
        ).execute()

        return results.get('files', [])

    def get_all_books(self) -> list[dict]:
        """Get all books from Supabase."""
        response = self.supabase.table('books').select(
            'id, title, author, drive_file_id, epub_status'
        ).is_('deleted_at', 'null').execute()
        return response.data

    def normalize_title(self, title: str) -> str:
        """Normalize a title for matching."""
        # Lowercase
        t = title.lower()
        # Remove common noise patterns
        t = re.sub(r'\s*[-–_]\s*', ' ', t)  # Normalize dashes/underscores to spaces
        t = re.sub(r'\([^)]*\)', '', t)  # Remove parenthetical content
        t = re.sub(r'\[[^\]]*\]', '', t)  # Remove bracketed content
        t = re.sub(r'\d{4}[-_]\d{2}[-_]\d{2}.*', '', t)  # Remove dates like 2022-10-27...
        t = re.sub(r'libgen\.li', '', t)  # Remove libgen suffix
        # Remove publisher names and editions
        t = re.sub(r'(penguin|random house|hachette|harper|simon|schuster|macmillan|wiley|oxford|cambridge|press|books?|publishing|edition|ed\.|1st|2nd|3rd|\d+th).*', '', t, flags=re.IGNORECASE)
        # Remove punctuation except apostrophes
        t = re.sub(r"[^\w\s']", ' ', t)
        # Normalize whitespace
        t = ' '.join(t.split())
        return t.strip()

    def extract_key_words(self, title: str) -> set[str]:
        """Extract significant words from a title."""
        normalized = self.normalize_title(title)
        # Remove common stop words
        stop_words = {'the', 'a', 'an', 'of', 'and', 'or', 'to', 'in', 'on', 'for', 'with', 'by', 'from', 'how', 'why', 'what', 'when', 'is', 'are', 'was', 'were', 'be', 'been', 'your', 'you', 'their', 'its'}
        words = set(normalized.split()) - stop_words
        return words

    def match_epub_to_book(self, epub_name: str, books: list[dict]) -> dict | None:
        """Try to match an EPUB filename to a book in Supabase."""
        # Remove .epub extension
        base_name = epub_name.replace('.epub', '').strip()

        # Normalize the epub filename
        epub_normalized = self.normalize_title(base_name)
        epub_words = self.extract_key_words(base_name)

        best_match = None
        best_score = 0

        for book in books:
            title = book['title']
            title_normalized = self.normalize_title(title)
            title_words = self.extract_key_words(title)

            # Score 1: Exact normalized match
            if epub_normalized == title_normalized:
                return book

            # Score 2: One contains the other
            if title_normalized in epub_normalized or epub_normalized in title_normalized:
                return book

            # Score 3: Word overlap scoring
            if epub_words and title_words:
                overlap = epub_words & title_words
                # Score based on percentage of title words matched
                score = len(overlap) / len(title_words) if title_words else 0

                # Bonus for matching longer words (more significant)
                long_overlap = [w for w in overlap if len(w) > 4]
                score += len(long_overlap) * 0.2

                if score > best_score and len(overlap) >= 2:
                    best_score = score
                    best_match = book

        # Return best match if score is high enough
        if best_score >= 0.5:
            return best_match

        return None

    def rename_epub_in_drive(self, file_id: str, book_title: str, author: str = None) -> str:
        """Rename an EPUB in Drive to 'Title - Author.epub' format."""
        # Clean the title for filename
        clean_title = re.sub(r'[<>:"/\\|?*]', '', book_title)  # Remove invalid chars
        clean_title = clean_title[:50]  # Limit length

        if author:
            clean_author = re.sub(r'[<>:"/\\|?*]', '', author)
            clean_author = clean_author.split(',')[0].strip()[:30]  # First author, limit length
            new_name = f"{clean_title} - {clean_author}.epub"
        else:
            new_name = f"{clean_title}.epub"

        # Rename in Drive
        self.drive.files().update(
            fileId=file_id,
            body={'name': new_name}
        ).execute()

        return new_name

    def download_epub(self, file_id: str, filename: str) -> Path:
        """Download an EPUB from Drive to a temp file."""
        request = self.drive.files().get_media(fileId=file_id)
        temp_path = Path(tempfile.gettempdir()) / filename

        with open(temp_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        return temp_path

    def extract_and_store_chapters(self, book_id: str, epub_path: Path) -> int:
        """Extract chapters from EPUB and store in Supabase."""
        try:
            metadata, chapters = parse_epub_file(epub_path)

            # Delete existing chapters (in case of re-sync)
            self.supabase.table('book_chapters').delete().eq(
                'book_id', book_id
            ).execute()

            # Insert chapters
            chapter_records = []
            for chapter in chapters:
                chapter_records.append({
                    'book_id': book_id,
                    'chapter_number': chapter.number,
                    'chapter_title': chapter.title,
                    'epub_href': chapter.epub_href,
                    'content': chapter.content,
                    'content_html': chapter.content_html,
                    'word_count': chapter.word_count
                })

            # Insert in batches
            batch_size = 10
            for i in range(0, len(chapter_records), batch_size):
                batch = chapter_records[i:i + batch_size]
                self.supabase.table('book_chapters').insert(batch).execute()

            return len(chapters)

        except Exception as e:
            print(f"    ERROR extracting chapters: {e}")
            return 0

    def sync(self, dry_run: bool = False, extract_chapters: bool = False, rename_files: bool = False) -> dict:
        """
        Perform bidirectional sync between Drive and Supabase.

        Args:
            dry_run: Preview changes without applying
            extract_chapters: Extract chapters for newly linked books
            rename_files: Rename EPUB files to 'Title - Author.epub' format

        Returns:
            Dict with sync statistics
        """
        print("=" * 60)
        print("Google Drive ↔ Supabase Book Sync")
        print("=" * 60)

        # Find books folder
        if not self.find_books_folder():
            return {'error': 'Books folder not found'}

        # Get data from both sources
        drive_epubs = self.list_drive_epubs()
        books = self.get_all_books()

        print(f"\nFound {len(drive_epubs)} EPUBs in Drive")
        print(f"Found {len(books)} books in Supabase")

        stats = {
            'linked': 0,
            'missing_epub': 0,
            'unlinked_epub': 0,
            'chapters_extracted': 0,
            'already_linked': 0,
            'renamed': 0
        }

        # Track which books get matched
        matched_book_ids = set()
        unlinked_epubs = []

        # Match EPUBs to books
        print("\n--- Matching EPUBs to Books ---")
        for epub in drive_epubs:
            epub_name = epub['name']
            epub_id = epub['id']
            epub_url = epub.get('webViewLink', '')

            matched_book = self.match_epub_to_book(epub_name, books)

            if matched_book:
                matched_book_ids.add(matched_book['id'])

                # Check if already linked
                if matched_book.get('drive_file_id') == epub_id:
                    # Still rename if requested
                    if rename_files:
                        expected_name = f"{matched_book['title'][:50]}"
                        if matched_book.get('author'):
                            expected_name += f" - {matched_book['author'].split(',')[0][:30]}"
                        expected_name += ".epub"

                        if epub_name != expected_name:
                            if dry_run:
                                print(f"  ✓ Would rename: {epub_name} → {expected_name}")
                            else:
                                new_name = self.rename_epub_in_drive(
                                    epub_id,
                                    matched_book['title'],
                                    matched_book.get('author')
                                )
                                print(f"  ✓ Renamed: {epub_name} → {new_name}")
                                stats['renamed'] += 1
                        else:
                            print(f"  ✓ {epub_name} → Already linked to '{matched_book['title']}'")
                    else:
                        print(f"  ✓ {epub_name} → Already linked to '{matched_book['title']}'")
                    stats['already_linked'] += 1
                    continue

                print(f"  → {epub_name} → '{matched_book['title']}'")

                if not dry_run:
                    # Rename file if requested
                    final_name = epub_name
                    if rename_files:
                        final_name = self.rename_epub_in_drive(
                            epub_id,
                            matched_book['title'],
                            matched_book.get('author')
                        )
                        print(f"    Renamed to: {final_name}")
                        stats['renamed'] += 1

                    # Update book with Drive info
                    self.supabase.table('books').update({
                        'drive_file_id': epub_id,
                        'drive_url': epub_url,
                        'epub_status': 'linked',
                        'epub_filename': final_name,
                        'updated_at': datetime.now(timezone.utc).isoformat()
                    }).eq('id', matched_book['id']).execute()

                    # Extract chapters if requested
                    if extract_chapters:
                        print(f"    Extracting chapters...")
                        temp_path = self.download_epub(epub_id, epub_name)
                        count = self.extract_and_store_chapters(
                            matched_book['id'], temp_path
                        )
                        stats['chapters_extracted'] += count
                        print(f"    Extracted {count} chapters")
                        temp_path.unlink()  # Clean up

                stats['linked'] += 1
            else:
                print(f"  ? {epub_name} → No matching book found")
                unlinked_epubs.append(epub_name)
                stats['unlinked_epub'] += 1

        # Mark books without EPUBs
        print("\n--- Checking for Missing EPUBs ---")
        for book in books:
            if book['id'] not in matched_book_ids:
                # Skip if already marked and still no EPUB
                if book.get('epub_status') == 'missing_epub' and not book.get('drive_file_id'):
                    continue

                if book.get('drive_file_id'):
                    # Had EPUB but now missing from Drive
                    print(f"  ! '{book['title']}' → EPUB removed from Drive")
                else:
                    print(f"  - '{book['title']}' → No EPUB in Drive")

                if not dry_run:
                    self.supabase.table('books').update({
                        'epub_status': 'missing_epub',
                        'updated_at': datetime.now(timezone.utc).isoformat()
                    }).eq('id', book['id']).execute()

                stats['missing_epub'] += 1

        # Summary
        print("\n" + "=" * 60)
        print("SYNC SUMMARY")
        print("=" * 60)
        print(f"  Newly linked:      {stats['linked']}")
        print(f"  Already linked:    {stats['already_linked']}")
        print(f"  Missing EPUB:      {stats['missing_epub']}")
        print(f"  Unlinked EPUBs:    {stats['unlinked_epub']}")
        if rename_files:
            print(f"  Files renamed:     {stats['renamed']}")
        if extract_chapters:
            print(f"  Chapters extracted: {stats['chapters_extracted']}")

        if unlinked_epubs:
            print("\n--- Unlinked EPUBs (add books manually) ---")
            for name in unlinked_epubs:
                print(f"  • {name}")

        if dry_run:
            print("\n[DRY RUN - No changes made]")

        return stats


def main():
    parser = argparse.ArgumentParser(description='Sync Drive EPUBs with Supabase')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without applying')
    parser.add_argument('--extract-chapters', action='store_true',
                        help='Extract and store chapters for newly linked books')
    parser.add_argument('--rename', action='store_true',
                        help='Rename EPUB files to "Title - Author.epub" format')

    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set")
        sys.exit(1)

    if not GOOGLE_TOKEN_JSON:
        print("ERROR: GOOGLE_TOKEN_JSON must be set")
        print("Run scripts/get_drive_token.py to generate a token")
        sys.exit(1)

    syncer = DriveBookSync()
    syncer.sync(
        dry_run=args.dry_run,
        extract_chapters=args.extract_chapters,
        rename_files=args.rename
    )


if __name__ == '__main__':
    main()
