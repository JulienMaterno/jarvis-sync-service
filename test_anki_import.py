"""
Test script for Anki import with limited cards.

This script tests the Anki import functionality with a small dataset
to ensure proper handling of media-heavy cards.
"""

import asyncio
import os
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

from syncs.anki_sync import run_anki_sync

# Load environment variables
load_dotenv()


async def main():
    """Run test import with limited dataset."""

    # Initialize Supabase client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
        return

    supabase: Client = create_client(supabase_url, supabase_key)

    print("=" * 80)
    print("ANKI IMPORT TEST - Limited Dataset")
    print("=" * 80)
    print()
    print("This will import:")
    print("  - Maximum 2 decks")
    print("  - Maximum 10 cards per deck")
    print("  - HTML and media will be stripped")
    print()

    response = input("Continue? (y/n): ")
    if response.lower() != 'y':
        print("Aborted.")
        return

    print()
    print("Starting test import...")
    print()

    # Run sync in test mode
    result = await run_anki_sync(
        supabase,
        test_mode=True,
        max_test_decks=2,
        max_test_cards=10
    )

    print()
    print("=" * 80)
    print("IMPORT RESULTS")
    print("=" * 80)
    print()
    print(f"Status: {result.get('status')}")
    print(f"Duration: {result.get('duration_seconds', 0):.2f} seconds")
    print()

    if result.get('import_stats'):
        stats = result['import_stats']
        print("Import Statistics:")
        print(f"  Decks imported: {stats.get('decks_imported', 0)}")
        print(f"  Cards imported: {stats.get('cards_imported', 0)}")
        print(f"  Cards skipped: {stats.get('cards_skipped', 0)}")
        if stats.get('errors'):
            print(f"  Errors: {len(stats['errors'])}")
            for error in stats['errors'][:5]:  # Show first 5 errors
                print(f"    - {error}")

    if result.get('cards_stats'):
        stats = result['cards_stats']
        print()
        print("Card Sync Statistics:")
        print(f"  Cards created in Anki: {stats.get('cards_created', 0)}")
        print(f"  Cards updated in Anki: {stats.get('cards_updated', 0)}")

    if result.get('errors'):
        print()
        print(f"WARNING: Total errors: {len(result['errors'])}")

    print()
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Check Supabase anki_decks table for imported decks")
    print("2. Check Supabase anki_cards table for imported cards")
    print("3. Verify that media placeholders ([Audio], [Image]) are used correctly")
    print("4. If everything looks good, remove test_mode to import all decks")
    print()


if __name__ == "__main__":
    asyncio.run(main())
