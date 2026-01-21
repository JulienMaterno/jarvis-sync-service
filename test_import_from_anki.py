"""
Test importing from Anki Desktop to Supabase (TEST MODE).

This tests the initial import with limits (2 decks, 10 cards each).
"""

import asyncio
import os
import sys
from supabase import create_client, Client

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

from syncs.anki_sync import run_anki_sync


async def main():
    """Test import from Anki to Supabase."""

    print("=" * 80)
    print("TEST: Import Anki Desktop → Supabase (TEST MODE)")
    print("=" * 80)
    print()

    # Get credentials
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        print("Error: Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables")
        print()
        print("PowerShell:")
        print('  $env:SUPABASE_URL="your-url"')
        print('  $env:SUPABASE_SERVICE_ROLE_KEY="your-key"')
        print('  python test_import_from_anki.py')
        return

    supabase: Client = create_client(supabase_url, supabase_key)

    print("Step 1: Check Supabase (before import)")
    decks_before = supabase.table("anki_decks").select("id").execute()
    cards_before = supabase.table("anki_cards").select("id").execute()
    print(f"  Decks: {len(decks_before.data)}")
    print(f"  Cards: {len(cards_before.data)}")
    print()

    print("Step 2: Run Import (TEST MODE: 2 decks, 10 cards each)")
    print("  Starting import...")
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
    print(f"Status: {result['status']}")
    print(f"Duration: {result['duration_seconds']:.2f}s")
    print()

    if result.get('import_stats'):
        stats = result['import_stats']
        print("Import Statistics:")
        print(f"  Decks imported: {stats['decks_imported']}")
        print(f"  Cards imported: {stats['cards_imported']}")
        print(f"  Cards skipped: {stats['cards_skipped']}")
        if stats.get('errors'):
            print(f"  Errors: {len(stats['errors'])}")
            for error in stats['errors'][:5]:
                print(f"    - {error}")
        print()

    if result.get('cards_stats'):
        stats = result['cards_stats']
        print("Card Sync Statistics:")
        print(f"  Cards created in Anki: {stats['cards_created']}")
        print(f"  Cards updated in Anki: {stats['cards_updated']}")
        print()

    print("Step 3: Verify in Supabase")
    decks_after = supabase.table("anki_decks").select("id, name, anki_deck_id").execute()
    cards_after = supabase.table("anki_cards").select("id, front, back, anki_note_id").execute()

    print(f"  Decks: {len(decks_after.data)} (+{len(decks_after.data) - len(decks_before.data)})")
    for deck in decks_after.data:
        print(f"    - {deck['name']} [anki_id: {deck.get('anki_deck_id')}]")

    print()
    print(f"  Cards: {len(cards_after.data)} (+{len(cards_after.data) - len(cards_before.data)})")
    for i, card in enumerate(cards_after.data[:5], 1):
        front = card['front'][:50]
        back = card['back'][:50]
        has_media = '[Audio]' in card['front'] or '[Image]' in card['front'] or '[Audio]' in card['back'] or '[Image]' in card['back']
        media_marker = " [has media]" if has_media else ""
        print(f"    {i}. Front: {front}...")
        print(f"       Back: {back}...{media_marker}")
        print(f"       Anki Note ID: {card.get('anki_note_id')}")

    if len(cards_after.data) > 5:
        print(f"    ... and {len(cards_after.data) - 5} more cards")

    print()
    print("=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)
    print()
    print("✓ Check Supabase dashboard to see imported decks and cards")
    print("✓ Verify media is shown as [Audio] or [Image] placeholders")
    print("✓ Verify no HTML tags in card content")
    print()


if __name__ == "__main__":
    asyncio.run(main())
