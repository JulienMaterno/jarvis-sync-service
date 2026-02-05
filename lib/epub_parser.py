"""
EPUB Parser Module

Extracts chapters and content from EPUB files for storage in Supabase.
EPUBs are ZIP archives containing XHTML files organized by a spine.
"""

import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Optional
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup


@dataclass
class Chapter:
    """Represents an extracted chapter from an EPUB."""
    number: int
    title: Optional[str]
    content: str  # Plain text or markdown
    content_html: str  # Original HTML
    word_count: int
    epub_href: str  # Original spine reference


@dataclass
class EPUBMetadata:
    """Metadata extracted from EPUB."""
    title: Optional[str] = None
    author: Optional[str] = None
    language: Optional[str] = None
    publisher: Optional[str] = None
    identifier: Optional[str] = None  # ISBN or other ID


class EPUBParser:
    """Parses EPUB files and extracts chapter content."""

    # Namespaces used in EPUB files
    NAMESPACES = {
        'opf': 'http://www.idpf.org/2007/opf',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'ncx': 'http://www.daisy.org/z3986/2005/ncx/',
        'epub': 'http://www.idpf.org/2007/ops',
        'container': 'urn:oasis:names:tc:opendocument:xmlns:container',
    }

    def __init__(self, epub_source: str | Path | BinaryIO | bytes):
        """
        Initialize parser with EPUB source.

        Args:
            epub_source: File path, file object, or bytes of EPUB
        """
        if isinstance(epub_source, (str, Path)):
            self.zip_file = zipfile.ZipFile(epub_source, 'r')
        elif isinstance(epub_source, bytes):
            self.zip_file = zipfile.ZipFile(io.BytesIO(epub_source), 'r')
        else:
            self.zip_file = zipfile.ZipFile(epub_source, 'r')

        self._opf_path: Optional[str] = None
        self._opf_dir: str = ''
        self._manifest: dict[str, dict] = {}
        self._spine: list[str] = []
        self._toc: dict[str, str] = {}  # href -> title mapping

    def parse(self) -> tuple[EPUBMetadata, list[Chapter]]:
        """
        Parse the EPUB and extract all chapters.

        Returns:
            Tuple of (metadata, list of chapters)
        """
        # 1. Find and parse the OPF file (package document)
        self._find_opf()
        opf_content = self.zip_file.read(self._opf_path).decode('utf-8')
        opf_root = ET.fromstring(opf_content)

        # 2. Extract metadata
        metadata = self._extract_metadata(opf_root)

        # 3. Build manifest (id -> href mapping)
        self._build_manifest(opf_root)

        # 4. Build spine (reading order)
        self._build_spine(opf_root)

        # 5. Try to parse TOC for chapter titles
        self._parse_toc(opf_root)

        # 6. Extract chapters from spine
        chapters = self._extract_chapters()

        return metadata, chapters

    def _find_opf(self) -> None:
        """Find the OPF file path from container.xml."""
        container = self.zip_file.read('META-INF/container.xml').decode('utf-8')
        root = ET.fromstring(container)

        rootfile = root.find('.//container:rootfile', self.NAMESPACES)
        if rootfile is None:
            # Try without namespace
            rootfile = root.find('.//{urn:oasis:names:tc:opendocument:xmlns:container}rootfile')

        if rootfile is None:
            raise ValueError("Could not find rootfile in container.xml")

        self._opf_path = rootfile.get('full-path')
        self._opf_dir = str(Path(self._opf_path).parent)
        if self._opf_dir == '.':
            self._opf_dir = ''

    def _extract_metadata(self, opf_root: ET.Element) -> EPUBMetadata:
        """Extract book metadata from OPF."""
        metadata = EPUBMetadata()

        # Find metadata element
        meta_elem = opf_root.find('opf:metadata', self.NAMESPACES)
        if meta_elem is None:
            meta_elem = opf_root.find('{http://www.idpf.org/2007/opf}metadata')

        if meta_elem is not None:
            # Title
            title = meta_elem.find('dc:title', self.NAMESPACES)
            if title is None:
                title = meta_elem.find('{http://purl.org/dc/elements/1.1/}title')
            if title is not None:
                metadata.title = title.text

            # Author (creator)
            creator = meta_elem.find('dc:creator', self.NAMESPACES)
            if creator is None:
                creator = meta_elem.find('{http://purl.org/dc/elements/1.1/}creator')
            if creator is not None:
                metadata.author = creator.text

            # Language
            language = meta_elem.find('dc:language', self.NAMESPACES)
            if language is None:
                language = meta_elem.find('{http://purl.org/dc/elements/1.1/}language')
            if language is not None:
                metadata.language = language.text

            # Publisher
            publisher = meta_elem.find('dc:publisher', self.NAMESPACES)
            if publisher is None:
                publisher = meta_elem.find('{http://purl.org/dc/elements/1.1/}publisher')
            if publisher is not None:
                metadata.publisher = publisher.text

            # Identifier (ISBN)
            identifier = meta_elem.find('dc:identifier', self.NAMESPACES)
            if identifier is None:
                identifier = meta_elem.find('{http://purl.org/dc/elements/1.1/}identifier')
            if identifier is not None:
                metadata.identifier = identifier.text

        return metadata

    def _build_manifest(self, opf_root: ET.Element) -> None:
        """Build manifest mapping from OPF."""
        manifest = opf_root.find('opf:manifest', self.NAMESPACES)
        if manifest is None:
            manifest = opf_root.find('{http://www.idpf.org/2007/opf}manifest')

        if manifest is not None:
            for item in manifest:
                item_id = item.get('id')
                href = item.get('href')
                media_type = item.get('media-type')
                if item_id and href:
                    self._manifest[item_id] = {
                        'href': href,
                        'media_type': media_type
                    }

    def _build_spine(self, opf_root: ET.Element) -> None:
        """Build spine (reading order) from OPF."""
        spine = opf_root.find('opf:spine', self.NAMESPACES)
        if spine is None:
            spine = opf_root.find('{http://www.idpf.org/2007/opf}spine')

        if spine is not None:
            for itemref in spine:
                idref = itemref.get('idref')
                linear = itemref.get('linear', 'yes')
                # Skip non-linear items (like cover)
                if idref and linear != 'no':
                    self._spine.append(idref)

    def _parse_toc(self, opf_root: ET.Element) -> None:
        """Parse table of contents for chapter titles."""
        # Try NCX TOC first (EPUB 2)
        toc_id = None
        spine = opf_root.find('opf:spine', self.NAMESPACES)
        if spine is None:
            spine = opf_root.find('{http://www.idpf.org/2007/opf}spine')
        if spine is not None:
            toc_id = spine.get('toc')

        if toc_id and toc_id in self._manifest:
            toc_href = self._manifest[toc_id]['href']
            toc_path = self._resolve_path(toc_href)
            try:
                toc_content = self.zip_file.read(toc_path).decode('utf-8')
                self._parse_ncx_toc(toc_content)
            except (KeyError, ET.ParseError):
                pass

        # Try nav document (EPUB 3)
        for item_id, item in self._manifest.items():
            if 'nav' in item.get('media_type', '') or item_id == 'nav':
                nav_path = self._resolve_path(item['href'])
                try:
                    nav_content = self.zip_file.read(nav_path).decode('utf-8')
                    self._parse_nav_toc(nav_content)
                except (KeyError, ET.ParseError):
                    pass
                break

    def _parse_ncx_toc(self, ncx_content: str) -> None:
        """Parse NCX table of contents."""
        root = ET.fromstring(ncx_content)
        nav_map = root.find('.//ncx:navMap', self.NAMESPACES)
        if nav_map is None:
            nav_map = root.find('.//{http://www.daisy.org/z3986/2005/ncx/}navMap')

        if nav_map is not None:
            for nav_point in nav_map.iter():
                if 'navPoint' in nav_point.tag:
                    text_elem = nav_point.find('.//ncx:text', self.NAMESPACES)
                    if text_elem is None:
                        text_elem = nav_point.find('.//{http://www.daisy.org/z3986/2005/ncx/}text')
                    content_elem = nav_point.find('.//ncx:content', self.NAMESPACES)
                    if content_elem is None:
                        content_elem = nav_point.find('.//{http://www.daisy.org/z3986/2005/ncx/}content')

                    if text_elem is not None and content_elem is not None:
                        title = text_elem.text
                        src = content_elem.get('src', '')
                        # Remove fragment
                        href = src.split('#')[0]
                        if title and href:
                            self._toc[href] = title

    def _parse_nav_toc(self, nav_content: str) -> None:
        """Parse EPUB 3 nav document for TOC."""
        soup = BeautifulSoup(nav_content, 'html.parser')
        nav = soup.find('nav', {'epub:type': 'toc'}) or soup.find('nav', id='toc')

        if nav:
            for link in nav.find_all('a'):
                href = link.get('href', '')
                title = link.get_text(strip=True)
                # Remove fragment
                href = href.split('#')[0]
                if title and href:
                    self._toc[href] = title

    def _resolve_path(self, href: str) -> str:
        """Resolve href relative to OPF directory."""
        if self._opf_dir:
            return f"{self._opf_dir}/{href}"
        return href

    def _extract_chapters(self) -> list[Chapter]:
        """Extract chapter content from spine items."""
        chapters = []
        chapter_num = 0

        for item_id in self._spine:
            if item_id not in self._manifest:
                continue

            item = self._manifest[item_id]
            href = item['href']
            media_type = item.get('media_type', '')

            # Skip non-content items
            if 'html' not in media_type and 'xml' not in media_type:
                continue

            full_path = self._resolve_path(href)

            try:
                content_bytes = self.zip_file.read(full_path)
                content_html = content_bytes.decode('utf-8')

                # Parse HTML and extract text
                soup = BeautifulSoup(content_html, 'html.parser')

                # Skip very short content (likely cover, title page, etc.)
                text = soup.get_text(separator=' ', strip=True)
                word_count = len(text.split())

                if word_count < 100:
                    continue

                chapter_num += 1

                # Get title from TOC or try to extract from content
                title = self._toc.get(href)
                if not title:
                    # Try to get title from h1/h2
                    heading = soup.find(['h1', 'h2', 'h3'])
                    if heading:
                        title = heading.get_text(strip=True)

                # Convert to clean text/markdown
                content = self._html_to_text(soup)

                chapters.append(Chapter(
                    number=chapter_num,
                    title=title,
                    content=content,
                    content_html=content_html,
                    word_count=word_count,
                    epub_href=href
                ))

            except (KeyError, UnicodeDecodeError) as e:
                print(f"Warning: Could not read {full_path}: {e}")
                continue

        return chapters

    def _html_to_text(self, soup: BeautifulSoup) -> str:
        """Convert HTML to clean text preserving some structure."""
        # Remove scripts and styles
        for element in soup(['script', 'style', 'head']):
            element.decompose()

        # Convert common elements
        text_parts = []
        seen_texts = set()  # Track seen text to avoid duplicates from nested elements

        for element in soup.body.descendants if soup.body else soup.descendants:
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                text = element.get_text(strip=True)
                if text and text not in seen_texts:
                    text_parts.append(f"\n\n## {text}\n\n")
                    seen_texts.add(text)
            elif element.name == 'p':
                text = element.get_text(strip=True)
                if text and text not in seen_texts:
                    text_parts.append(f"{text}\n\n")
                    seen_texts.add(text)
            elif element.name == 'div':
                # Handle div elements that contain direct text (common in some EPUBs)
                # Only get direct text, not nested element text
                direct_text = ''.join(
                    child.strip() for child in element.children
                    if isinstance(child, str) and child.strip()
                )
                if not direct_text:
                    # If no direct text, get full text but only if no nested block elements
                    nested_blocks = element.find(['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                    if not nested_blocks:
                        direct_text = element.get_text(strip=True)
                if direct_text and direct_text not in seen_texts and len(direct_text) > 10:
                    text_parts.append(f"{direct_text}\n\n")
                    seen_texts.add(direct_text)
            elif element.name == 'blockquote':
                text = element.get_text(strip=True)
                if text and text not in seen_texts:
                    text_parts.append(f"> {text}\n\n")
                    seen_texts.add(text)
            elif element.name in ['li']:
                text = element.get_text(strip=True)
                if text and text not in seen_texts:
                    text_parts.append(f"- {text}\n")
                    seen_texts.add(text)

        if text_parts:
            return ''.join(text_parts).strip()

        # Fallback to plain text
        return soup.get_text(separator='\n', strip=True)

    def close(self) -> None:
        """Close the ZIP file."""
        self.zip_file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def parse_epub_file(file_path: str | Path) -> tuple[EPUBMetadata, list[Chapter]]:
    """
    Convenience function to parse an EPUB file.

    Args:
        file_path: Path to the EPUB file

    Returns:
        Tuple of (metadata, list of chapters)
    """
    with EPUBParser(file_path) as parser:
        return parser.parse()


def parse_epub_bytes(epub_bytes: bytes) -> tuple[EPUBMetadata, list[Chapter]]:
    """
    Convenience function to parse EPUB from bytes.

    Args:
        epub_bytes: Raw EPUB file bytes

    Returns:
        Tuple of (metadata, list of chapters)
    """
    with EPUBParser(epub_bytes) as parser:
        return parser.parse()
