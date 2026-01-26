"""
===================================================================================
BOOKFUSION API CLIENT
===================================================================================

Client for the Bookfusion Calibre API (undocumented but stable).
Used to upload EPUBs to Bookfusion for reading on Boox tablets.

API Flow:
1. POST /calibre-api/v1/uploads/init - Get S3 upload credentials
2. POST to S3 URL - Upload file with pre-signed params
3. POST /calibre-api/v1/uploads/finalize - Complete with metadata

Authentication: HTTP Basic Auth with API key as username, empty password.

Usage:
    client = BookfusionClient(api_key)
    result = await client.upload_book(
        file_path="/path/to/article.epub",
        metadata={"title": "Article Title", "author_list": ["Author Name"]}
    )
"""

import os
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

import httpx

logger = logging.getLogger("BookfusionClient")

# API Configuration
BOOKFUSION_API_BASE = "https://www.bookfusion.com/calibre-api/v1"
BOOKFUSION_DEFAULT_SHELF = "Articles"  # Shelf to separate articles from books


@dataclass
class BookfusionMetadata:
    """Metadata for Bookfusion upload."""
    title: str
    author_list: List[str] = field(default_factory=list)
    summary: Optional[str] = None
    language: Optional[str] = "eng"
    isbn: Optional[str] = None
    issued_on: Optional[str] = None  # YYYY-MM-DD format
    tag_list: List[str] = field(default_factory=list)
    bookshelves: List[str] = field(default_factory=lambda: [BOOKFUSION_DEFAULT_SHELF])
    series: List[Dict[str, Any]] = field(default_factory=list)  # [{"title": "...", "index": 1}]


@dataclass
class UploadResult:
    """Result of a Bookfusion upload."""
    success: bool
    bookfusion_id: Optional[str] = None
    error: Optional[str] = None
    s3_key: Optional[str] = None


class BookfusionClient:
    """Client for Bookfusion Calibre API."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Bookfusion client.

        Args:
            api_key: Bookfusion API key. If not provided, reads from BOOKFUSION_API_KEY env var.
        """
        self.api_key = api_key or os.environ.get("BOOKFUSION_API_KEY")
        if not self.api_key:
            raise ValueError("Bookfusion API key required. Set BOOKFUSION_API_KEY env var or pass api_key.")

        self.api_base = BOOKFUSION_API_BASE
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                auth=(self.api_key, ""),  # Basic auth: api_key as username
                headers={"User-Agent": "jarvis-bookfusion-client/1.0"},
                timeout=60.0
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def _compute_file_digest(self, file_path: Path) -> str:
        """Compute SHA256 digest of file."""
        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                h.update(chunk)
        return h.hexdigest()

    def _compute_metadata_digest(self, metadata: BookfusionMetadata, cover_path: Optional[Path] = None) -> str:
        """
        Compute metadata digest matching Calibre plugin format.

        This is used by Bookfusion to detect metadata changes.
        """
        h = hashlib.sha256()

        def update(val):
            if val is None:
                return
            if isinstance(val, (bytes, bytearray)):
                h.update(bytes(val))
            else:
                h.update(str(val).encode("utf-8"))

        # Scalars (in order)
        update(metadata.title)
        update(metadata.summary)
        update(metadata.language)
        update(metadata.isbn)
        update(metadata.issued_on)

        # Series
        for s in metadata.series:
            update(s.get("title"))
            idx = s.get("index")
            if idx is not None:
                update(str(idx))

        # Authors, Tags
        for a in metadata.author_list:
            update(a)
        for t in metadata.tag_list:
            update(t)

        # Bookshelves
        if metadata.bookshelves is not None:
            for shelf in metadata.bookshelves:
                update(shelf)

        # Cover (if provided)
        if cover_path and cover_path.is_file():
            size = cover_path.stat().st_size
            h.update(bytes(size))  # N zero bytes
            h.update(b"\x00")
            with open(cover_path, "rb") as f:
                while chunk := f.read(65536):
                    h.update(chunk)

        return h.hexdigest()

    async def get_limits(self) -> Dict[str, Any]:
        """Get account limits."""
        client = await self._get_client()
        resp = await client.get(f"{self.api_base}/limits")
        resp.raise_for_status()
        return resp.json()

    async def check_existing(self, digest: str) -> Optional[Dict[str, Any]]:
        """Check if a file with this digest already exists."""
        client = await self._get_client()
        resp = await client.get(f"{self.api_base}/uploads/{digest}")
        if resp.status_code == 200:
            return resp.json()
        return None

    async def _init_upload(self, filename: str, file_digest: str) -> Dict[str, Any]:
        """
        Initialize upload to get S3 credentials.

        Returns dict with:
            - url: S3 bucket URL
            - params: Pre-signed parameters for S3 upload
        """
        client = await self._get_client()

        resp = await client.post(
            f"{self.api_base}/uploads/init",
            files={
                "filename": (None, filename),
                "digest": (None, file_digest)
            }
        )

        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Upload init failed: HTTP {resp.status_code} - {resp.text[:500]}")

        data = resp.json()
        if "url" not in data or "params" not in data:
            raise RuntimeError(f"Upload init response missing fields: {data}")

        return data

    async def _upload_to_s3(self, s3_url: str, s3_params: Dict[str, str], file_path: Path) -> str:
        """
        Upload file to S3 using pre-signed params.

        Returns the S3 key.
        """
        import mimetypes
        mime_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"

        # Build multipart form with params + file
        files = {k: (None, str(v)) for k, v in s3_params.items()}
        files["file"] = (file_path.name, open(file_path, "rb"), mime_type)

        # S3 upload doesn't use auth
        async with httpx.AsyncClient(timeout=300.0) as s3_client:
            resp = await s3_client.post(s3_url, files=files)

        if resp.status_code != 204:
            raise RuntimeError(f"S3 upload failed: HTTP {resp.status_code} - {resp.text[:500]}")

        return s3_params["key"]

    async def _finalize_upload(
        self,
        s3_key: str,
        file_digest: str,
        metadata: BookfusionMetadata,
        metadata_digest: str,
        cover_path: Optional[Path] = None
    ) -> Dict[str, Any]:
        """
        Finalize upload with metadata.

        Returns the upload response with bookfusion_id.
        """
        client = await self._get_client()

        # Build multipart form with Rails-style nested params
        parts = []

        # Required fields
        parts.append(("key", (None, s3_key)))
        parts.append(("digest", (None, file_digest)))

        # Metadata
        parts.append(("metadata[calibre_metadata_digest]", (None, metadata_digest)))
        parts.append(("metadata[title]", (None, metadata.title)))

        if metadata.summary:
            parts.append(("metadata[summary]", (None, metadata.summary)))
        if metadata.language:
            parts.append(("metadata[language]", (None, metadata.language)))
        if metadata.isbn:
            parts.append(("metadata[isbn]", (None, metadata.isbn)))
        if metadata.issued_on:
            parts.append(("metadata[issued_on]", (None, metadata.issued_on)))

        # Series
        for s in metadata.series:
            parts.append(("metadata[series][][title]", (None, s["title"])))
            if s.get("index") is not None:
                parts.append(("metadata[series][][index]", (None, str(s["index"]))))

        # Authors
        for author in metadata.author_list:
            parts.append(("metadata[author_list][]", (None, author)))

        # Tags
        for tag in metadata.tag_list:
            parts.append(("metadata[tag_list][]", (None, tag)))

        # Bookshelves (add empty element first for Rails array semantics)
        if metadata.bookshelves is not None:
            parts.append(("metadata[bookshelves][]", (None, "")))
            for shelf in metadata.bookshelves:
                parts.append(("metadata[bookshelves][]", (None, shelf)))

        # Cover image
        if cover_path and cover_path.is_file():
            import mimetypes
            mime_type = mimetypes.guess_type(str(cover_path))[0] or "image/jpeg"
            parts.append(("metadata[cover]", (cover_path.name, open(cover_path, "rb"), mime_type)))

        resp = await client.post(
            f"{self.api_base}/uploads/finalize",
            files=parts,
            headers={"Accept": "application/json"}
        )

        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Upload finalize failed: HTTP {resp.status_code} - {resp.text[:500]}")

        return resp.json()

    async def upload_book(
        self,
        file_path: str,
        metadata: BookfusionMetadata,
        cover_path: Optional[str] = None,
        skip_existing: bool = True
    ) -> UploadResult:
        """
        Upload an EPUB to Bookfusion.

        Args:
            file_path: Path to EPUB file
            metadata: Book metadata
            cover_path: Optional path to cover image
            skip_existing: If True, skip upload if file already exists

        Returns:
            UploadResult with success status and bookfusion_id
        """
        file_path = Path(file_path)
        cover_path = Path(cover_path) if cover_path else None

        if not file_path.exists():
            return UploadResult(success=False, error=f"File not found: {file_path}")

        try:
            logger.info(f"Uploading {file_path.name} to Bookfusion...")

            # Compute digests
            file_digest = self._compute_file_digest(file_path)
            metadata_digest = self._compute_metadata_digest(metadata, cover_path)

            # Check if already exists
            if skip_existing:
                existing = await self.check_existing(file_digest)
                if existing:
                    logger.info(f"File already exists in Bookfusion: {existing.get('id')}")
                    return UploadResult(
                        success=True,
                        bookfusion_id=existing.get("id"),
                        s3_key=existing.get("key")
                    )

            # Init upload
            init_data = await self._init_upload(file_path.name, file_digest)
            s3_url = init_data["url"]
            s3_params = init_data["params"]

            # Upload to S3
            s3_key = await self._upload_to_s3(s3_url, s3_params, file_path)
            logger.info(f"Uploaded to S3: {s3_key}")

            # Finalize
            result = await self._finalize_upload(
                s3_key, file_digest, metadata, metadata_digest, cover_path
            )

            bookfusion_id = result.get("id") or result.get("bookfusion_id")
            logger.info(f"Upload complete! Bookfusion ID: {bookfusion_id}")

            return UploadResult(
                success=True,
                bookfusion_id=bookfusion_id,
                s3_key=s3_key
            )

        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return UploadResult(success=False, error=str(e))


# Convenience function for simple uploads
async def upload_article_to_bookfusion(
    epub_path: str,
    title: str,
    author: Optional[str] = None,
    tags: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> UploadResult:
    """
    Simple function to upload an article EPUB to Bookfusion.

    Args:
        epub_path: Path to EPUB file
        title: Article title
        author: Article author (optional)
        tags: Tags for categorization (optional)
        api_key: Bookfusion API key (optional, uses env var if not provided)

    Returns:
        UploadResult with success status and bookfusion_id
    """
    metadata = BookfusionMetadata(
        title=title,
        author_list=[author] if author else [],
        tag_list=tags or [],
        bookshelves=[BOOKFUSION_DEFAULT_SHELF]
    )

    async with BookfusionClient(api_key) as client:
        return await client.upload_book(epub_path, metadata)
