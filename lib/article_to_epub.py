"""
===================================================================================
ARTICLE TO EPUB CONVERTER
===================================================================================

Converts web articles to EPUB format for reading on e-ink devices like Boox.

Features:
- Clean article extraction using trafilatura
- Image downloading and embedding
- E-ink optimized CSS (larger fonts, high contrast)
- Proper EPUB metadata

Usage:
    from lib.article_to_epub import ArticleToEpub

    converter = ArticleToEpub()
    result = await converter.convert(
        url="https://example.substack.com/p/article",
        output_path="/path/to/output.epub"
    )
"""

import os
import re
import hashlib
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

# EPUB library
try:
    from ebooklib import epub
except ImportError:
    raise ImportError("ebooklib required. Install with: pip install ebooklib")

# Article extraction
try:
    import trafilatura
    from trafilatura.settings import use_config
except ImportError:
    raise ImportError("trafilatura required. Install with: pip install trafilatura")

logger = logging.getLogger("ArticleToEpub")


# E-ink optimized CSS
EINK_CSS = """
/* E-ink optimized styles for Boox and similar devices */
body {
    font-family: Georgia, "Times New Roman", serif;
    font-size: 1.1em;
    line-height: 1.6;
    margin: 1em;
    color: #000;
    background-color: #fff;
}

h1 {
    font-size: 1.8em;
    margin-top: 1em;
    margin-bottom: 0.5em;
    line-height: 1.2;
}

h2 {
    font-size: 1.4em;
    margin-top: 1em;
    margin-bottom: 0.4em;
}

h3 {
    font-size: 1.2em;
    margin-top: 0.8em;
    margin-bottom: 0.3em;
}

p {
    margin-bottom: 1em;
    text-align: justify;
}

blockquote {
    margin: 1em 2em;
    padding-left: 1em;
    border-left: 3px solid #666;
    font-style: italic;
}

pre, code {
    font-family: "Courier New", monospace;
    font-size: 0.9em;
    background-color: #f5f5f5;
    padding: 0.2em 0.4em;
}

pre {
    padding: 1em;
    overflow-x: auto;
    white-space: pre-wrap;
    word-wrap: break-word;
}

img {
    max-width: 100%;
    height: auto;
    display: block;
    margin: 1em auto;
}

a {
    color: #000;
    text-decoration: underline;
}

ul, ol {
    margin-left: 1.5em;
    margin-bottom: 1em;
}

li {
    margin-bottom: 0.3em;
}

hr {
    border: none;
    border-top: 1px solid #ccc;
    margin: 2em 0;
}

/* Article metadata */
.article-meta {
    font-size: 0.9em;
    color: #666;
    margin-bottom: 2em;
    border-bottom: 1px solid #ccc;
    padding-bottom: 1em;
}

.article-meta .author {
    font-weight: bold;
}

.article-meta .date {
    font-style: italic;
}

/* Source link */
.source-link {
    font-size: 0.8em;
    color: #666;
    margin-top: 2em;
    padding-top: 1em;
    border-top: 1px solid #ccc;
}
"""


@dataclass
class ArticleData:
    """Extracted article data."""
    title: str
    author: Optional[str] = None
    date: Optional[str] = None
    content_html: str = ""
    content_text: str = ""
    url: str = ""
    source_name: Optional[str] = None
    images: Dict[str, bytes] = field(default_factory=dict)  # url -> image data
    word_count: int = 0


@dataclass
class ConversionResult:
    """Result of article to EPUB conversion."""
    success: bool
    epub_path: Optional[str] = None
    article: Optional[ArticleData] = None
    error: Optional[str] = None


class ArticleToEpub:
    """Convert web articles to EPUB format."""

    def __init__(self, download_images: bool = True, max_image_size: int = 5 * 1024 * 1024):
        """
        Initialize converter.

        Args:
            download_images: Whether to download and embed images
            max_image_size: Maximum image size in bytes (default 5MB)
        """
        self.download_images = download_images
        self.max_image_size = max_image_size

    def _extract_source_name(self, url: str) -> str:
        """Extract readable source name from URL."""
        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        # Remove www prefix
        if domain.startswith("www."):
            domain = domain[4:]

        # Known sources with nice names
        source_map = {
            "substack.com": "Substack",
            "medium.com": "Medium",
            "gwern.net": "Gwern",
            "lesswrong.com": "LessWrong",
            "overcomingbias.com": "Overcoming Bias",
            "astralcodexten.substack.com": "Astral Codex Ten",
            "marginalrevolution.com": "Marginal Revolution",
            "paulgraham.com": "Paul Graham",
            "stratechery.com": "Stratechery",
        }

        for pattern, name in source_map.items():
            if pattern in domain:
                return name

        # Default to domain
        return domain.split(".")[0].capitalize()

    async def _fetch_url(self, url: str) -> str:
        """Fetch URL content."""
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=30.0,
            headers={"User-Agent": "Mozilla/5.0 (compatible; JarvisBot/1.0)"}
        ) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text

    async def _download_image(self, url: str, base_url: str) -> Optional[bytes]:
        """Download a single image."""
        try:
            # Make absolute URL
            if not url.startswith(("http://", "https://")):
                url = urljoin(base_url, url)

            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=30.0
            ) as client:
                resp = await client.get(url)
                resp.raise_for_status()

                # Check size
                content = resp.content
                if len(content) > self.max_image_size:
                    logger.warning(f"Image too large ({len(content)} bytes), skipping: {url}")
                    return None

                return content
        except Exception as e:
            logger.warning(f"Failed to download image {url}: {e}")
            return None

    async def _download_images(self, html: str, base_url: str) -> Tuple[str, Dict[str, bytes]]:
        """
        Download images and replace URLs with local references.

        Returns:
            Tuple of (modified HTML, dict of filename -> image data)
        """
        if not self.download_images:
            return html, {}

        soup = BeautifulSoup(html, "html.parser")
        images: Dict[str, bytes] = {}

        for img in soup.find_all("img"):
            src = img.get("src")
            if not src:
                continue

            # Download image
            data = await self._download_image(src, base_url)
            if data is None:
                continue

            # Generate unique filename
            ext = Path(urlparse(src).path).suffix or ".jpg"
            if ext not in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]:
                ext = ".jpg"

            filename = hashlib.md5(src.encode()).hexdigest()[:12] + ext
            images[filename] = data

            # Update src to local reference
            img["src"] = f"images/{filename}"

        return str(soup), images

    def _clean_html(self, html: str) -> str:
        """Clean HTML for EPUB compatibility."""
        soup = BeautifulSoup(html, "html.parser")

        # Remove scripts and styles
        for tag in soup.find_all(["script", "style", "iframe", "noscript"]):
            tag.decompose()

        # Remove unwanted attributes
        for tag in soup.find_all(True):
            # Keep only safe attributes
            safe_attrs = {"href", "src", "alt", "title", "id", "class"}
            attrs_to_remove = [attr for attr in tag.attrs if attr not in safe_attrs]
            for attr in attrs_to_remove:
                del tag[attr]

        return str(soup)

    async def extract_article(self, url: str) -> ArticleData:
        """
        Extract article content from URL.

        Args:
            url: Article URL

        Returns:
            ArticleData with extracted content
        """
        logger.info(f"Extracting article from: {url}")

        # Use trafilatura's fetcher for better JS handling
        # Run in executor to not block async loop
        import asyncio
        loop = asyncio.get_event_loop()
        html = await loop.run_in_executor(None, trafilatura.fetch_url, url)

        if not html:
            # Fallback to our async client
            logger.warning("trafilatura.fetch_url failed, trying httpx...")
            html = await self._fetch_url(url)

        if not html:
            raise ValueError("Failed to fetch article content")

        # Extract with trafilatura
        content_html = trafilatura.extract(
            html,
            include_images=True,
            include_links=True,
            include_tables=True,
            output_format="html"
        )

        content_text = trafilatura.extract(
            html,
            include_images=False,
            include_links=False,
            output_format="txt"
        )

        # Extract metadata
        metadata = trafilatura.extract_metadata(html)

        title = metadata.title if metadata else "Untitled Article"
        author = metadata.author if metadata else None
        date = metadata.date if metadata else None

        # Clean HTML
        if content_html:
            content_html = self._clean_html(content_html)

        # Download images
        images: Dict[str, bytes] = {}
        if content_html and self.download_images:
            content_html, images = await self._download_images(content_html, url)

        # Count words
        word_count = len(content_text.split()) if content_text else 0

        return ArticleData(
            title=title,
            author=author,
            date=date,
            content_html=content_html or "",
            content_text=content_text or "",
            url=url,
            source_name=self._extract_source_name(url),
            images=images,
            word_count=word_count
        )

    def _build_html_content(self, article: ArticleData) -> str:
        """Build HTML body content for EPUB chapter.

        Note: ebooklib wraps this in a proper XHTML document structure,
        so we only return the body contents here.
        """
        meta_parts = []
        if article.author:
            meta_parts.append(f'<span class="author">{article.author}</span>')
        if article.date:
            meta_parts.append(f'<span class="date">{article.date}</span>')
        if article.source_name:
            meta_parts.append(f'<span class="source">{article.source_name}</span>')

        meta_html = " · ".join(meta_parts)

        # Return just the body content - ebooklib handles the XHTML wrapper
        return f"""<h1>{article.title}</h1>
<div class="article-meta">{meta_html}</div>
{article.content_html}
<div class="source-link">
    <p>Original: <a href="{article.url}">{article.url}</a></p>
</div>"""

    def create_epub(self, article: ArticleData, output_path: str) -> str:
        """
        Create EPUB file from article data.

        Args:
            article: Extracted article data
            output_path: Path for output EPUB file

        Returns:
            Path to created EPUB file
        """
        book = epub.EpubBook()

        # Set metadata
        book.set_identifier(hashlib.md5(article.url.encode()).hexdigest())
        book.set_title(article.title)
        book.set_language("en")

        if article.author:
            book.add_author(article.author)

        # Add CSS
        css = epub.EpubItem(
            uid="style",
            file_name="style.css",
            media_type="text/css",
            content=EINK_CSS.encode()
        )
        book.add_item(css)

        # Add images
        image_items = []
        for filename, data in article.images.items():
            # Determine media type
            ext = Path(filename).suffix.lower()
            media_types = {
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".png": "image/png",
                ".gif": "image/gif",
                ".webp": "image/webp",
                ".svg": "image/svg+xml",
            }
            media_type = media_types.get(ext, "image/jpeg")

            img = epub.EpubItem(
                uid=filename,
                file_name=f"images/{filename}",
                media_type=media_type,
                content=data
            )
            book.add_item(img)
            image_items.append(img)

        # Create chapter
        chapter = epub.EpubHtml(
            title=article.title,
            file_name="content.xhtml",
            lang="en"
        )
        chapter.content = self._build_html_content(article)
        chapter.add_item(css)
        book.add_item(chapter)

        # Create spine and TOC
        book.spine = ["nav", chapter]
        book.toc = [epub.Link("content.xhtml", article.title, "content")]

        # Add navigation
        book.add_item(epub.EpubNcx())
        book.add_item(epub.EpubNav())

        # Write EPUB
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        epub.write_epub(str(output_path), book)

        logger.info(f"Created EPUB: {output_path}")
        return str(output_path)

    async def convert(self, url: str, output_path: Optional[str] = None) -> ConversionResult:
        """
        Convert article URL to EPUB.

        Args:
            url: Article URL
            output_path: Optional output path (auto-generated if not provided)

        Returns:
            ConversionResult with success status and paths
        """
        try:
            # Extract article
            article = await self.extract_article(url)

            if not article.content_html:
                return ConversionResult(
                    success=False,
                    error="Failed to extract article content"
                )

            # Generate output path if not provided
            if output_path is None:
                # Sanitize title for filename
                safe_title = re.sub(r'[^\w\s-]', '', article.title)[:50].strip()
                safe_title = re.sub(r'[-\s]+', '-', safe_title)
                output_path = tempfile.mktemp(suffix=".epub", prefix=f"{safe_title}_")

            # Create EPUB
            epub_path = self.create_epub(article, output_path)

            return ConversionResult(
                success=True,
                epub_path=epub_path,
                article=article
            )

        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            return ConversionResult(success=False, error=str(e))


# Convenience function
async def article_to_epub(url: str, output_path: Optional[str] = None) -> ConversionResult:
    """
    Convert article URL to EPUB.

    Args:
        url: Article URL
        output_path: Optional output path

    Returns:
        ConversionResult
    """
    converter = ArticleToEpub()
    return await converter.convert(url, output_path)
