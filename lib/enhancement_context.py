"""
Enhancement Context Module

Builds personalized context for EPUB enhancement by fetching data from:
1. Jarvis user profile (thinking style, learning mode, work context)
2. Recent books (domain interests)
3. Current book metadata

This enables AI-generated questions and previews to be tailored to the reader.
"""

import os
import logging
from typing import Optional
from dataclasses import dataclass, field

import httpx
from supabase import Client

logger = logging.getLogger(__name__)

# Jarvis MCP server URL for profile fetching
JARVIS_MCP_URL = os.environ.get("JARVIS_MCP_URL", "http://localhost:3100")


@dataclass
class ReaderContext:
    """Personalized reader context for enhancement prompts."""

    # Thinking and learning style
    thinking_style: list[str] = field(default_factory=list)
    learning_mode: list[str] = field(default_factory=list)

    # Current work/life context
    work_context: str = ""

    # Domain interests from reading history
    domain_interests: list[str] = field(default_factory=list)

    # Recent book titles for reference
    recent_books: list[str] = field(default_factory=list)

    def to_prompt_section(self) -> str:
        """Format context for LLM prompt injection."""
        sections = []

        if self.thinking_style:
            sections.append(f"- Thinking style: {', '.join(self.thinking_style)}")

        if self.learning_mode:
            sections.append(f"- Learning mode: {', '.join(self.learning_mode)}")

        if self.work_context:
            sections.append(f"- Current work: {self.work_context}")

        if self.domain_interests:
            sections.append(f"- Domain interests: {', '.join(self.domain_interests)}")

        if self.recent_books:
            books_str = ", ".join(self.recent_books[:5])
            sections.append(f"- Recent reads: {books_str}")

        if not sections:
            return "No specific reader context available."

        return "READER CONTEXT:\n" + "\n".join(sections)


async def fetch_user_profile() -> dict:
    """
    Fetch user profile from Jarvis MCP server.

    Returns dict with profile insights organized by category.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Call the Jarvis MCP user_profile_get endpoint
            response = await client.post(
                f"{JARVIS_MCP_URL}/mcp/user_profile_get",
                json={
                    "categories": ["identity", "work_style", "meta_cognition"],
                    "format": "verbose",
                    "min_confidence": 0.7,
                    "include_gaps": False,
                },
            )
            response.raise_for_status()
            return response.json()
    except Exception as e:
        logger.warning(f"Failed to fetch user profile: {e}")
        return {}


def extract_thinking_patterns(profile: dict) -> list[str]:
    """Extract thinking style patterns from profile insights."""
    patterns = []

    # Keywords to look for in insights
    thinking_keywords = {
        "first-principles": "first-principles thinker",
        "systems": "systems-oriented",
        "analytical": "analytical",
        "implementation": "builds to understand",
        "iterative": "iterative learner",
    }

    insights = []
    for category in ["identity", "work_style", "meta_cognition"]:
        insights.extend(profile.get("profile", {}).get(category, []))

    for insight in insights:
        text = insight.get("insight_text", "").lower()
        key = insight.get("insight_key", "").lower()

        for keyword, label in thinking_keywords.items():
            if keyword in text or keyword in key:
                if label not in patterns:
                    patterns.append(label)

    return patterns[:5]  # Limit to top 5


def extract_learning_style(profile: dict) -> list[str]:
    """Extract learning preferences from profile insights."""
    styles = []

    # Keywords to look for
    learning_keywords = {
        "direct experience": "learns through doing",
        "implementation": "learns by building",
        "conversation": "processes through dialogue",
        "reading": "continuous reader",
        "documentation": "systematic documenter",
    }

    insights = []
    for category in ["meta_cognition", "work_style"]:
        insights.extend(profile.get("profile", {}).get(category, []))

    for insight in insights:
        text = insight.get("insight_text", "").lower()

        for keyword, label in learning_keywords.items():
            if keyword in text:
                if label not in styles:
                    styles.append(label)

    return styles[:4]


def extract_work_context(profile: dict) -> str:
    """Extract current work context from profile."""
    work_insights = profile.get("profile", {}).get("work_style", [])

    # Look for specific work context insights
    context_parts = []

    for insight in work_insights:
        text = insight.get("insight_text", "")
        key = insight.get("insight_key", "")

        # Check for specific contexts
        if "jarvis" in key.lower() or "ai" in text.lower():
            if "building AI systems" not in context_parts:
                context_parts.append("building AI systems")

        if "startup" in text.lower() or "founder" in text.lower():
            if "exploring startup opportunities" not in context_parts:
                context_parts.append("exploring startup opportunities")

        if "sea" in text.lower() or "indonesia" in text.lower() or "vietnam" in text.lower():
            if "based in Southeast Asia" not in context_parts:
                context_parts.append("based in Southeast Asia")

    return ", ".join(context_parts) if context_parts else ""


def extract_domains_from_books(books: list[dict]) -> list[str]:
    """
    Extract domain interests from recent reading history.

    Maps book titles/tags to broader domain categories.
    """
    domain_map = {
        # Startup/Business
        "zero to one": "startup strategy",
        "hard thing": "entrepreneurship",
        "lean startup": "startup methodology",

        # Decision Making / Thinking
        "clear thinking": "decision-making",
        "thinking fast": "cognitive biases",
        "ikigai": "life philosophy",
        "subtle art": "personal philosophy",

        # Technology / Future
        "abundance": "technology futures",
        "climate": "climate/sustainability",
        "powering up": "energy systems",

        # History / Geopolitics
        "harari": "macro history",
        "sapiens": "human history",
        "geography": "geopolitics",
        "power of geography": "geopolitics",

        # Science
        "slime": "biology/science",
        "sleep": "health/neuroscience",
        "stolen focus": "attention/focus",

        # Fiction (for completeness)
        "expanse": "science fiction",
        "leviathan": "science fiction",
        "cibola": "science fiction",
    }

    domains = set()

    for book in books:
        title = (book.get("title") or "").lower()
        tags = book.get("tags") or []

        # Check title against domain map
        for keyword, domain in domain_map.items():
            if keyword in title:
                domains.add(domain)

        # Also check tags
        for tag in tags:
            tag_lower = tag.lower()
            if tag_lower in ["non-fiction", "science", "technology"]:
                domains.add(tag_lower)

    return list(domains)[:8]


async def build_reader_context(
    supabase: Client,
    current_book_title: Optional[str] = None,
) -> ReaderContext:
    """
    Build complete reader context for enhancement prompts.

    Args:
        supabase: Supabase client for querying books
        current_book_title: Title of book being enhanced (excluded from recent)

    Returns:
        ReaderContext with all personalization data
    """
    context = ReaderContext()

    # 1. Fetch user profile from Jarvis
    try:
        profile = await fetch_user_profile()
        if profile:
            context.thinking_style = extract_thinking_patterns(profile)
            context.learning_mode = extract_learning_style(profile)
            context.work_context = extract_work_context(profile)
    except Exception as e:
        logger.warning(f"Could not fetch user profile: {e}")

    # 2. Get recent books from Supabase
    try:
        response = supabase.table("books").select(
            "title, tags"
        ).in_(
            "status", ["Finished", "Summarized", "Reading"]
        ).order(
            "updated_at", desc=True
        ).limit(20).execute()

        books = response.data or []

        # Filter out current book if provided
        if current_book_title:
            books = [b for b in books if b.get("title") != current_book_title]

        context.recent_books = [b.get("title") for b in books[:5] if b.get("title")]
        context.domain_interests = extract_domains_from_books(books)

    except Exception as e:
        logger.warning(f"Could not fetch recent books: {e}")

    # 3. Add fallback defaults if context is sparse
    if not context.thinking_style:
        context.thinking_style = ["analytical", "systems-oriented"]

    if not context.learning_mode:
        context.learning_mode = ["learns by doing"]

    return context


def format_enhancement_prompt_context(context: ReaderContext) -> str:
    """
    Format full context section for enhancement LLM prompts.

    Returns a string to be injected into prompts for preview/question generation.
    """
    base_context = context.to_prompt_section()

    instructions = """
ENHANCEMENT GUIDELINES (based on reader context):
- Focus on actionable insights over abstract theory
- Connect concepts to systems thinking and building
- Assume technical/analytical background
- Reference startup/tech contexts where relevant
- Prioritize "how would you apply this" over "what does this mean"
"""

    return f"{base_context}\n{instructions}"


# Convenience function for synchronous contexts
def build_reader_context_sync(supabase: Client, current_book_title: Optional[str] = None) -> ReaderContext:
    """Synchronous wrapper for build_reader_context."""
    import asyncio

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(build_reader_context(supabase, current_book_title))
