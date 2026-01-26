#!/usr/bin/env python3
"""
===================================================================================
ARTICLE CAPTURE SERVICE
===================================================================================

Captures online articles, converts to EPUB, and uploads to Bookfusion.
Stores article metadata and full text in Supabase for LLM analysis.

Usage:
    python capture_article.py "https://example.substack.com/p/article"
    python capture_article.py --url "https://..." --no-upload
    python capture_article.py --url "https://..." --tags "tech,ai"

Flow:
    1. Extract article content using trafilatura
    2. Download and embed images
    3. Generate EPUB with e-ink optimized CSS
    4. Upload EPUB to Bookfusion (to "Articles" shelf)
    5. Store article record in Supabase
    6. Index full text in knowledge_chunks for RAG

The article will appear in Bookfusion within minutes, ready to read on Boox.
Highlights sync back via: Bookfusion → Notion → sync_highlights.py
"""

import os
import sys
import logging
import argparse
import tempfile
import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path

import httpx
from dotenv import load_dotenv

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

# Add lib to path
sys.path.insert(0, os.path.dirname(__file__))

from lib.article_to_epub import ArticleToEpub, ArticleData, ConversionResult
from lib.bookfusion_client import BookfusionClient, BookfusionMetadata, UploadResult

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger('ArticleCapture')

# ============================================================================
# CONFIGURATION
# ============================================================================

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
BOOKFUSION_API_KEY = os.environ.get('BOOKFUSION_API_KEY')

# Default shelf for articles in Bookfusion
DEFAULT_BOOKFUSION_SHELF = "Articles"


# ============================================================================
# SUPABASE CLIENT
# ============================================================================

class ArticleSupabaseClient:
    """Supabase client for articles table."""

    def __init__(self, url: str, key: str):
        self.base_url = f"{url}/rest/v1"
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)

    def __del__(self):
        if hasattr(self, 'client'):
            self.client.close()

    def get_article_by_url(self, url: str) -> Optional[Dict]:
        """Check if article with this URL already exists."""
        try:
            response = self.client.get(
                f"{self.base_url}/articles",
                params={"url": f"eq.{url}", "select": "*"}
            )
            response.raise_for_status()
            results = response.json()
            return results[0] if results else None
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Table doesn't exist yet
                return None
            raise

    def upsert_article(self, data: Dict) -> Dict:
        """Insert or update article by URL."""
        try:
            response = self.client.post(
                f"{self.base_url}/articles?on_conflict=url",
                json=data,
                headers={**self.headers, "Prefer": "resolution=merge-duplicates,return=representation"}
            )
            response.raise_for_status()
            result = response.json()
            return result[0] if result else {}
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Table doesn't exist yet - log warning and return empty
                logger.warning("Articles table doesn't exist yet - run migrations first")
                return {}
            raise


# ============================================================================
# CAPTURE SERVICE
# ============================================================================

@dataclass
class CaptureResult:
    """Result of article capture."""
    success: bool
    article_id: Optional[str] = None
    title: Optional[str] = None
    bookfusion_id: Optional[str] = None
    epub_path: Optional[str] = None
    error: Optional[str] = None
    already_exists: bool = False


class ArticleCaptureService:
    """
    Main service for capturing articles.

    Coordinates extraction, EPUB generation, Bookfusion upload, and database storage.
    """

    def __init__(
        self,
        supabase_url: Optional[str] = None,
        supabase_key: Optional[str] = None,
        bookfusion_api_key: Optional[str] = None
    ):
        self.supabase_url = supabase_url or SUPABASE_URL
        self.supabase_key = supabase_key or SUPABASE_KEY
        self.bookfusion_api_key = bookfusion_api_key or BOOKFUSION_API_KEY

        # Initialize Supabase client
        if self.supabase_url and self.supabase_key:
            self.db = ArticleSupabaseClient(self.supabase_url, self.supabase_key)
        else:
            self.db = None
            logger.warning("Supabase not configured - articles won't be saved to database")

        # EPUB converter
        self.converter = ArticleToEpub(download_images=True)

    async def capture(
        self,
        url: str,
        upload_to_bookfusion: bool = True,
        tags: Optional[List[str]] = None,
        skip_existing: bool = True,
        keep_epub: bool = False
    ) -> CaptureResult:
        """
        Capture an article from URL.

        Args:
            url: Article URL
            upload_to_bookfusion: Whether to upload to Bookfusion
            tags: Optional tags for categorization
            skip_existing: If True, skip if article already in database
            keep_epub: If True, don't delete EPUB after upload

        Returns:
            CaptureResult with article details
        """
        logger.info(f"Capturing article: {url}")

        # Check if already exists
        if skip_existing and self.db:
            existing = self.db.get_article_by_url(url)
            if existing:
                logger.info(f"Article already exists: {existing.get('title')}")
                return CaptureResult(
                    success=True,
                    article_id=existing.get('id'),
                    title=existing.get('title'),
                    bookfusion_id=existing.get('bookfusion_id'),
                    already_exists=True
                )

        # Extract article
        logger.info("Extracting article content...")
        conversion = await self.converter.convert(url)

        if not conversion.success:
            return CaptureResult(
                success=False,
                error=f"Extraction failed: {conversion.error}"
            )

        article = conversion.article
        epub_path = conversion.epub_path

        logger.info(f"Extracted: {article.title} ({article.word_count} words)")

        # Upload to Bookfusion
        bookfusion_id = None
        if upload_to_bookfusion and self.bookfusion_api_key:
            logger.info("Uploading to Bookfusion...")
            try:
                upload_result = await self._upload_to_bookfusion(
                    epub_path=epub_path,
                    article=article,
                    tags=tags
                )
                if upload_result.success:
                    bookfusion_id = upload_result.bookfusion_id
                    logger.info(f"Uploaded to Bookfusion: {bookfusion_id}")
                else:
                    logger.warning(f"Bookfusion upload failed: {upload_result.error}")
            except Exception as e:
                logger.error(f"Bookfusion upload error: {e}")
        elif upload_to_bookfusion:
            logger.warning("Bookfusion API key not configured - skipping upload")

        # Save to database
        article_id = None
        if self.db:
            logger.info("Saving to database...")
            try:
                article_id = await self._save_to_database(
                    article=article,
                    bookfusion_id=bookfusion_id,
                    tags=tags
                )
                logger.info(f"Saved to database: {article_id}")
            except Exception as e:
                logger.error(f"Database save error: {e}")

        # Clean up EPUB unless keeping it
        if epub_path and not keep_epub:
            try:
                Path(epub_path).unlink()
            except Exception:
                pass

        return CaptureResult(
            success=True,
            article_id=article_id,
            title=article.title,
            bookfusion_id=bookfusion_id,
            epub_path=epub_path if keep_epub else None
        )

    async def _upload_to_bookfusion(
        self,
        epub_path: str,
        article: ArticleData,
        tags: Optional[List[str]] = None
    ) -> UploadResult:
        """Upload EPUB to Bookfusion."""
        metadata = BookfusionMetadata(
            title=article.title,
            author_list=[article.author] if article.author else [],
            summary=article.content_text[:500] if article.content_text else None,
            tag_list=tags or [],
            bookshelves=[DEFAULT_BOOKFUSION_SHELF],
            issued_on=article.date
        )

        async with BookfusionClient(self.bookfusion_api_key) as client:
            return await client.upload_book(epub_path, metadata)

    async def _save_to_database(
        self,
        article: ArticleData,
        bookfusion_id: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> str:
        """Save article to Supabase."""
        data = {
            "title": article.title,
            "author": article.author,
            "source_name": article.source_name,
            "source_type": "article",
            "url": article.url,
            "full_text": article.content_text,
            "word_count": article.word_count,
            "tags": tags or [],
            "bookfusion_id": bookfusion_id,
            "status": "To Read",
            "published_at": article.date,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "last_sync_source": "jarvis"
        }

        result = self.db.upsert_article(data)
        return result.get("id")


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Capture online articles for reading on Boox"
    )
    parser.add_argument(
        "url",
        nargs="?",
        help="Article URL to capture"
    )
    parser.add_argument(
        "--url",
        dest="url_flag",
        help="Article URL (alternative to positional argument)"
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip Bookfusion upload"
    )
    parser.add_argument(
        "--tags",
        help="Comma-separated tags for categorization"
    )
    parser.add_argument(
        "--keep-epub",
        action="store_true",
        help="Keep EPUB file after upload"
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Output EPUB path (implies --keep-epub)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Capture even if article already exists"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    return parser.parse_args()


async def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Get URL from either positional or flag
    url = args.url or args.url_flag
    if not url:
        print("Error: URL required. Use: python capture_article.py <URL>")
        sys.exit(1)

    # Parse tags
    tags = [t.strip() for t in args.tags.split(",")] if args.tags else None

    # Capture
    service = ArticleCaptureService()
    result = await service.capture(
        url=url,
        upload_to_bookfusion=not args.no_upload,
        tags=tags,
        skip_existing=not args.force,
        keep_epub=args.keep_epub or bool(args.output)
    )

    if result.success:
        if result.already_exists:
            print(f"✓ Already captured: {result.title}")
        else:
            print(f"✓ Captured: {result.title}")

        if result.bookfusion_id:
            print(f"  Bookfusion ID: {result.bookfusion_id}")
        if result.article_id:
            print(f"  Article ID: {result.article_id}")
        if result.epub_path:
            print(f"  EPUB: {result.epub_path}")

        print("\n📚 Article will appear in Bookfusion shortly!")
        print("   Read on your Boox and highlights will sync automatically.")
    else:
        print(f"✗ Failed: {result.error}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
