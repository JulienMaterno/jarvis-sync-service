#!/usr/bin/env python3
"""
Link highlights to their corresponding articles.

When highlights sync from Bookfusion → Notion → Supabase, they come in with
a book_title but may not be linked to an article (since Bookfusion treats
everything as "books"). This script matches highlights to articles based on title.

Flow:
1. Find highlights where book_id IS NULL (not a real book)
2. Match book_title to articles.title in articles table
3. Set article_id and article_title on matching highlights

Usage:
    python link_highlights_to_articles.py                    # Link all unlinked
    python link_highlights_to_articles.py --article "Title"  # Link for specific article
    python link_highlights_to_articles.py --dry-run          # Preview without changes
"""

import argparse
import os
import sys
from typing import Dict, List, Optional, Tuple

# Fix Windows encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.environ.get('SUPABASE_URL', '').strip()
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


def normalize_title(title: str) -> str:
    """Normalize title for matching (lowercase, strip whitespace)."""
    if not title:
        return ""
    return title.lower().strip()


def find_article_match(
    highlight_title: str,
    articles: List[Dict]
) -> Optional[Dict]:
    """
    Find matching article for a highlight based on title.

    Args:
        highlight_title: The book_title from the highlight
        articles: List of article records

    Returns:
        Matching article dict or None
    """
    if not highlight_title:
        return None

    norm_highlight = normalize_title(highlight_title)

    # Try exact match first
    for article in articles:
        if normalize_title(article.get('title', '')) == norm_highlight:
            return article

    # Try partial match (highlight title contains article title or vice versa)
    for article in articles:
        article_title = normalize_title(article.get('title', ''))
        if article_title and (
            article_title in norm_highlight or
            norm_highlight in article_title
        ):
            return article

    return None


def link_highlights_to_articles(
    article_title_filter: Optional[str] = None,
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Link unlinked highlights to their articles.

    Args:
        article_title_filter: Optional title to filter articles
        dry_run: If True, don't make changes, just report

    Returns:
        Dict with statistics: {'checked': N, 'linked': N, 'already_linked': N}
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set")

    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    stats = {'checked': 0, 'linked': 0, 'already_linked': 0, 'no_match': 0}

    # Get all articles
    articles_query = client.table('articles').select('id, title').is_('deleted_at', 'null')
    if article_title_filter:
        articles_query = articles_query.ilike('title', f'%{article_title_filter}%')

    articles = articles_query.execute().data

    if not articles:
        print("No articles found in database")
        return stats

    print(f"Found {len(articles)} article(s) to match against")

    # Build article title lookup
    article_lookup = {normalize_title(a['title']): a for a in articles}

    # Get highlights that:
    # 1. Have no book_id (not a real book)
    # 2. Have no article_id (not yet linked to article)
    # 3. Have a book_title (something to match on)
    highlights = client.table('highlights').select(
        'id, book_title, book_id, article_id'
    ).is_('book_id', 'null').is_('article_id', 'null').not_.is_('book_title', 'null').execute().data

    if not highlights:
        print("No unlinked highlights found")
        return stats

    print(f"Found {len(highlights)} unlinked highlight(s) to process")

    for highlight in highlights:
        stats['checked'] += 1
        highlight_id = highlight['id']
        book_title = highlight.get('book_title', '')

        # Skip if already linked
        if highlight.get('article_id'):
            stats['already_linked'] += 1
            continue

        # Find matching article
        match = find_article_match(book_title, articles)

        if match:
            article_id = match['id']
            article_title = match['title']

            if dry_run:
                print(f"  Would link: '{book_title}' → Article: '{article_title}'")
            else:
                # Update highlight with article link
                client.table('highlights').update({
                    'article_id': article_id,
                    'article_title': article_title
                }).eq('id', highlight_id).execute()

                print(f"  Linked: '{book_title}' → Article: '{article_title}'")

            stats['linked'] += 1
        else:
            stats['no_match'] += 1
            if dry_run:
                print(f"  No match for: '{book_title}'")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Link highlights to their corresponding articles"
    )
    parser.add_argument(
        "--article",
        help="Filter to specific article title"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without making them"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("LINK HIGHLIGHTS TO ARTICLES")
    print("=" * 60)

    if args.dry_run:
        print("[DRY RUN - No changes will be made]")

    stats = link_highlights_to_articles(
        article_title_filter=args.article,
        dry_run=args.dry_run
    )

    print()
    print("Summary:")
    print(f"  Checked: {stats['checked']}")
    print(f"  Linked: {stats['linked']}")
    print(f"  No match: {stats['no_match']}")
    print(f"  Already linked: {stats['already_linked']}")


if __name__ == "__main__":
    main()
