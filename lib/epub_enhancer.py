"""
EPUB Learning Enhancer Module

Enhances EPUB files with AI-generated learning aids:
- Chapter previews (2-3 sentences summarizing key concepts)
- Comprehension questions (2-6 per chapter based on density)

Uses Claude to generate personalized content based on reader context.
Injects enhancements directly into EPUB XHTML files.
"""

import json
import logging
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

from lib.epub_parser import EPUBParser, Chapter, EPUBMetadata
from lib.enhancement_context import ReaderContext

logger = logging.getLogger(__name__)


# Question types for variety
QUESTION_TYPES = [
    "elaborative",   # Why does this work? What are the implications?
    "retrieval",     # What is X? Recall key facts
    "application",   # How would you apply this?
    "connection",    # How does this connect to other concepts?
]


@dataclass
class EnhancementResult:
    """Result of EPUB enhancement operation."""
    success: bool
    enhanced_path: Optional[Path] = None
    chapters_enhanced: int = 0
    total_questions: int = 0
    error: Optional[str] = None


@dataclass
class ChapterEnhancements:
    """Enhancements generated for a single chapter."""
    chapter_number: int
    epub_href: str
    preview: str
    questions: list[dict]


# Inline CSS for e-ink compatibility (no external dependencies)
ENHANCEMENT_STYLES = """
<style type="text/css">
.jarvis-preview {
    background-color: #f5f5f5;
    border: 1px solid #ccc;
    border-radius: 4px;
    padding: 1em;
    margin: 1em 0 1.5em 0;
    font-style: italic;
    line-height: 1.5;
}
.jarvis-preview-title {
    font-weight: bold;
    font-style: normal;
    margin-bottom: 0.5em;
    font-size: 0.9em;
    color: #444;
}
.jarvis-questions {
    background-color: #f9f9f9;
    border: 1px solid #ddd;
    border-radius: 4px;
    padding: 1em;
    margin: 2em 0 1em 0;
}
.jarvis-questions-title {
    font-weight: bold;
    margin-bottom: 1em;
    font-size: 1.1em;
    color: #333;
}
.jarvis-question {
    margin-bottom: 1em;
    padding-bottom: 0.5em;
    border-bottom: 1px dotted #ccc;
}
.jarvis-question:last-child {
    border-bottom: none;
    margin-bottom: 0;
    padding-bottom: 0;
}
.jarvis-question-text {
    font-weight: bold;
    margin-bottom: 0.5em;
    line-height: 1.4;
}
.jarvis-question-type {
    font-size: 0.75em;
    color: #666;
    text-transform: uppercase;
    margin-bottom: 0.3em;
}
details.jarvis-answer {
    margin-top: 0.5em;
}
details.jarvis-answer summary {
    cursor: pointer;
    color: #0066cc;
    font-size: 0.9em;
}
details.jarvis-answer .jarvis-answer-content {
    margin-top: 0.5em;
    padding: 0.5em;
    background-color: #fff;
    border-left: 3px solid #0066cc;
    font-size: 0.95em;
    line-height: 1.4;
}
</style>
"""


class EPUBLearningEnhancer:
    """
    Enhances EPUB files with AI-generated learning aids.

    Generates chapter previews and comprehension questions based on
    chapter content and reader context, then injects them into the EPUB.
    """

    def __init__(self, anthropic_client, supabase_client=None):
        """
        Initialize the enhancer.

        Args:
            anthropic_client: Anthropic client for Claude API calls
            supabase_client: Optional Supabase client (for future use)
        """
        self.anthropic = anthropic_client
        self.supabase = supabase_client

    async def enhance_epub(
        self,
        input_path: Path,
        output_path: Path,
        reader_context: ReaderContext,
    ) -> EnhancementResult:
        """
        Enhance an EPUB with AI-generated previews and questions.

        Args:
            input_path: Path to source EPUB file
            output_path: Path for enhanced EPUB output
            reader_context: Personalized reader context for prompt customization

        Returns:
            EnhancementResult with success status and statistics
        """
        try:
            # Parse EPUB to extract chapters
            logger.info(f"Parsing EPUB: {input_path}")
            with EPUBParser(input_path) as parser:
                metadata, chapters = parser.parse()

            if not chapters:
                return EnhancementResult(
                    success=False,
                    error="No chapters found in EPUB"
                )

            book_title = metadata.title or "Unknown Book"
            logger.info(f"Found {len(chapters)} chapters in '{book_title}'")

            # Generate enhancements for each chapter
            all_enhancements: list[ChapterEnhancements] = []
            total_questions = 0

            for chapter in chapters:
                logger.info(f"Enhancing chapter {chapter.number}: {chapter.title or 'Untitled'}")

                try:
                    # Generate preview
                    preview = await self.generate_chapter_preview(
                        chapter, book_title
                    )

                    # Generate questions
                    questions = await self.generate_chapter_questions(
                        chapter, book_title, reader_context
                    )

                    enhancement = ChapterEnhancements(
                        chapter_number=chapter.number,
                        epub_href=chapter.epub_href,
                        preview=preview,
                        questions=questions,
                    )
                    all_enhancements.append(enhancement)
                    total_questions += len(questions)

                    logger.info(
                        f"Chapter {chapter.number}: preview generated, "
                        f"{len(questions)} questions"
                    )

                except Exception as e:
                    logger.warning(
                        f"Failed to enhance chapter {chapter.number}: {e}"
                    )
                    # Continue with other chapters
                    continue

            if not all_enhancements:
                return EnhancementResult(
                    success=False,
                    error="Failed to generate enhancements for any chapter"
                )

            # Inject enhancements into EPUB
            enhancements_dict = {
                e.epub_href: {
                    "preview": e.preview,
                    "questions": e.questions,
                }
                for e in all_enhancements
            }

            enhanced_path = self.inject_enhancements_into_epub(
                input_path, output_path, enhancements_dict
            )

            return EnhancementResult(
                success=True,
                enhanced_path=enhanced_path,
                chapters_enhanced=len(all_enhancements),
                total_questions=total_questions,
            )

        except Exception as e:
            logger.error(f"EPUB enhancement failed: {e}", exc_info=True)
            return EnhancementResult(
                success=False,
                error=str(e)
            )

    async def generate_chapter_preview(
        self,
        chapter: Chapter,
        book_title: str,
    ) -> str:
        """
        Generate a 2-3 sentence preview for a chapter.

        Args:
            chapter: Chapter data with content
            book_title: Title of the book for context

        Returns:
            Preview string (2-3 sentences)
        """
        # Truncate content if very long (keep first ~4000 chars for context)
        content_preview = chapter.content[:4000]
        if len(chapter.content) > 4000:
            content_preview += "\n\n[Content continues...]"

        prompt = f"""Generate a brief chapter preview for an e-reader.

BOOK: {book_title}
CHAPTER: {chapter.title or f"Chapter {chapter.number}"}

CHAPTER CONTENT:
{content_preview}

INSTRUCTIONS:
Write exactly 2-3 sentences that:
1. Introduce the main theme or argument of this chapter
2. Hint at key concepts the reader will encounter
3. Create interest without spoiling insights

Keep it concise and engaging. Write in present tense.
Do NOT start with "This chapter..." - be more creative.

OUTPUT:
Write only the preview sentences, nothing else."""

        response = await self.anthropic.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )

        preview = response.content[0].text.strip()
        return preview

    async def generate_chapter_questions(
        self,
        chapter: Chapter,
        book_title: str,
        reader_context: ReaderContext,
    ) -> list[dict]:
        """
        Generate comprehension questions for a chapter.

        Number of questions scales with content density:
        - Light content (< 1500 words): 2 questions
        - Moderate content (1500-3000 words): 3-4 questions
        - Dense content (> 3000 words): 5-6 questions

        Args:
            chapter: Chapter data with content
            book_title: Title of the book
            reader_context: Reader context for personalization

        Returns:
            List of question dicts with type, question, answer_key
        """
        # Truncate content if very long
        content_for_llm = chapter.content[:6000]
        if len(chapter.content) > 6000:
            content_for_llm += "\n\n[Content continues...]"

        # Get reader context prompt section
        context_section = reader_context.to_prompt_section()

        prompt = f"""Generate comprehension questions for a book chapter.

BOOK: {book_title}
CHAPTER: {chapter.title or f"Chapter {chapter.number}"}
WORD COUNT: {chapter.word_count}

{context_section}

CHAPTER CONTENT:
{content_for_llm}

INSTRUCTIONS:
1. First, assess the chapter's content density:
   - Light (< 1500 words, simple concepts): generate 2 questions
   - Moderate (1500-3000 words, some complexity): generate 3-4 questions
   - Dense (> 3000 words, complex ideas): generate 5-6 questions

2. Generate questions of these types (mix them based on count):
   - "elaborative": Ask WHY something works or what implications follow
   - "retrieval": Ask to recall a key fact, definition, or concept
   - "application": Ask how to apply this in practice
   - "connection": Ask how concepts connect to each other or prior knowledge

3. For each question:
   - Make it specific to this chapter's content
   - Ensure the answer can be found in the chapter
   - Consider the reader's context (interests, thinking style)
   - Keep answers concise (50 words max)

OUTPUT FORMAT (JSON array):
[
  {{
    "type": "elaborative|retrieval|application|connection",
    "question": "The question text",
    "answer_key": "Concise answer (50 words max)"
  }}
]

Return ONLY the JSON array, no other text."""

        response = await self.anthropic.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )

        response_text = response.content[0].text.strip()

        # Parse JSON response
        try:
            # Handle potential markdown code blocks
            if response_text.startswith("```"):
                lines = response_text.split("\n")
                json_lines = []
                in_json = False
                for line in lines:
                    if line.startswith("```") and not in_json:
                        in_json = True
                        continue
                    elif line.startswith("```") and in_json:
                        break
                    elif in_json:
                        json_lines.append(line)
                response_text = "\n".join(json_lines)

            questions = json.loads(response_text)

            # Validate structure
            validated_questions = []
            for q in questions:
                if all(k in q for k in ["type", "question", "answer_key"]):
                    # Normalize type
                    q_type = q["type"].lower()
                    if q_type not in QUESTION_TYPES:
                        q_type = "elaborative"

                    validated_questions.append({
                        "type": q_type,
                        "question": q["question"],
                        "answer_key": q["answer_key"][:250],  # Enforce limit
                    })

            return validated_questions

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse questions JSON: {e}")
            logger.debug(f"Raw response: {response_text}")
            # Return empty list on parse failure
            return []

    def inject_enhancements_into_epub(
        self,
        input_path: Path,
        output_path: Path,
        enhancements: dict,
    ) -> Path:
        """
        Inject enhancements into EPUB by modifying XHTML files.

        Process:
        1. Extract EPUB (ZIP) to temp directory
        2. Find and modify chapter XHTML files
        3. Inject preview after first heading (or at body start)
        4. Inject questions at end of body
        5. Repack as EPUB

        Args:
            input_path: Source EPUB path
            output_path: Output EPUB path
            enhancements: Dict mapping epub_href to {preview, questions}

        Returns:
            Path to enhanced EPUB
        """
        # Create temp directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            extract_path = temp_path / "epub_contents"

            # Extract EPUB
            logger.info(f"Extracting EPUB to {extract_path}")
            with zipfile.ZipFile(input_path, 'r') as zf:
                zf.extractall(extract_path)

            # Find OPF file to get content directory
            opf_path = self._find_opf_path(extract_path)
            opf_dir = opf_path.parent

            # Process each chapter with enhancements
            for href, enhancement_data in enhancements.items():
                # Resolve full path to XHTML file
                xhtml_path = opf_dir / href

                if not xhtml_path.exists():
                    logger.warning(f"XHTML file not found: {xhtml_path}")
                    continue

                logger.info(f"Injecting enhancements into: {href}")

                # Read and modify XHTML
                self._inject_into_xhtml(
                    xhtml_path,
                    enhancement_data["preview"],
                    enhancement_data["questions"],
                )

            # Repack EPUB
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            self._repack_epub(extract_path, output_path)

            logger.info(f"Enhanced EPUB created: {output_path}")
            return output_path

    def _find_opf_path(self, extract_path: Path) -> Path:
        """Find the OPF file path from container.xml."""
        container_path = extract_path / "META-INF" / "container.xml"

        if not container_path.exists():
            raise ValueError("Invalid EPUB: missing META-INF/container.xml")

        container_xml = container_path.read_text(encoding="utf-8")
        root = ET.fromstring(container_xml)

        # Find rootfile element
        namespaces = {"container": "urn:oasis:names:tc:opendocument:xmlns:container"}
        rootfile = root.find(".//container:rootfile", namespaces)

        if rootfile is None:
            # Try without namespace
            rootfile = root.find(
                ".//{urn:oasis:names:tc:opendocument:xmlns:container}rootfile"
            )

        if rootfile is None:
            raise ValueError("Could not find rootfile in container.xml")

        opf_relative = rootfile.get("full-path")
        return extract_path / opf_relative

    def _inject_into_xhtml(
        self,
        xhtml_path: Path,
        preview: str,
        questions: list[dict],
    ) -> None:
        """
        Inject preview and questions into an XHTML file.

        Args:
            xhtml_path: Path to XHTML file
            preview: Preview text to inject
            questions: List of question dicts
        """
        content = xhtml_path.read_text(encoding="utf-8")
        soup = BeautifulSoup(content, "html.parser")

        # Add styles to head
        head = soup.find("head")
        if head:
            style_soup = BeautifulSoup(ENHANCEMENT_STYLES, "html.parser")
            head.append(style_soup)

        # Build preview HTML
        preview_html = f"""
<div class="jarvis-preview">
    <div class="jarvis-preview-title">Chapter Preview</div>
    {preview}
</div>
"""
        preview_element = BeautifulSoup(preview_html, "html.parser")

        # Build questions HTML
        questions_html = self._build_questions_html(questions)
        questions_element = BeautifulSoup(questions_html, "html.parser")

        # Find body
        body = soup.find("body")
        if not body:
            logger.warning(f"No body found in {xhtml_path}")
            return

        # Inject preview after first heading or at start of body
        first_heading = body.find(["h1", "h2", "h3"])
        if first_heading:
            first_heading.insert_after(preview_element)
        else:
            # Insert at beginning of body
            if body.contents:
                body.contents[0].insert_before(preview_element)
            else:
                body.append(preview_element)

        # Inject questions at end of body
        body.append(questions_element)

        # Write modified XHTML
        # Use original declaration if present, otherwise add XHTML doctype
        output = str(soup)

        # Ensure XHTML declaration
        if not output.startswith("<?xml"):
            output = '<?xml version="1.0" encoding="utf-8"?>\n' + output

        xhtml_path.write_text(output, encoding="utf-8")

    def _build_questions_html(self, questions: list[dict]) -> str:
        """Build HTML for questions section with collapsible answers."""
        if not questions:
            return ""

        questions_items = []
        for i, q in enumerate(questions, 1):
            q_type = q.get("type", "elaborative")
            q_text = q.get("question", "")
            q_answer = q.get("answer_key", "")

            # Type display names
            type_display = {
                "elaborative": "Elaboration",
                "retrieval": "Recall",
                "application": "Application",
                "connection": "Connection",
            }.get(q_type, q_type.title())

            question_html = f"""
<div class="jarvis-question">
    <div class="jarvis-question-type">{type_display}</div>
    <div class="jarvis-question-text">{i}. {q_text}</div>
    <details class="jarvis-answer">
        <summary>Show Answer</summary>
        <div class="jarvis-answer-content">{q_answer}</div>
    </details>
</div>
"""
            questions_items.append(question_html)

        questions_inner = "\n".join(questions_items)

        return f"""
<div class="jarvis-questions">
    <div class="jarvis-questions-title">Comprehension Questions</div>
    {questions_inner}
</div>
"""

    def _repack_epub(self, extract_path: Path, output_path: Path) -> None:
        """
        Repack extracted contents into EPUB format.

        EPUB requires specific ZIP structure:
        - mimetype file must be first, uncompressed
        - Other files use standard compression
        """
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            # mimetype must be first and uncompressed
            mimetype_path = extract_path / "mimetype"
            if mimetype_path.exists():
                zf.write(
                    mimetype_path,
                    "mimetype",
                    compress_type=zipfile.ZIP_STORED
                )

            # Add all other files
            for file_path in extract_path.rglob("*"):
                if file_path.is_file() and file_path.name != "mimetype":
                    arcname = file_path.relative_to(extract_path)
                    zf.write(file_path, arcname)


# Convenience function for simple usage
async def enhance_epub_file(
    input_path: str | Path,
    output_path: str | Path,
    anthropic_client,
    reader_context: Optional[ReaderContext] = None,
) -> EnhancementResult:
    """
    Enhance an EPUB file with AI-generated learning aids.

    Args:
        input_path: Path to source EPUB
        output_path: Path for enhanced EPUB output
        anthropic_client: Anthropic client for API calls
        reader_context: Optional reader context (uses defaults if not provided)

    Returns:
        EnhancementResult with success status and statistics
    """
    if reader_context is None:
        reader_context = ReaderContext()

    enhancer = EPUBLearningEnhancer(anthropic_client)
    return await enhancer.enhance_epub(
        Path(input_path),
        Path(output_path),
        reader_context,
    )
