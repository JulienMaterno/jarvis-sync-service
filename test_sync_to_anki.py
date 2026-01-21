"""
Test syncing from Supabase to Anki Desktop.

This tests that cards created in Supabase appear in Anki Desktop.
"""

import asyncio
import os
import sys
from supabase import create_client, Client

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

from syncs.anki_sync import sync_cards_to_anki
from syncs.anki_client import AnkiConnectClient


async def main():
    """Test sync from Supabase to Anki."""

    print("=" * 80)
    print("TEST: Sync Supabase → Anki Desktop")
    print("=" * 80)
    print()

    # Get credentials from environment
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not supabase_key:
        print("Error: Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables")
        print()
        print("Try:")
        print('  $env:SUPABASE_URL="your-url"')
        print('  $env:SUPABASE_SERVICE_ROLE_KEY="your-key"')
        print('  python test_sync_to_anki.py')
        return

    # Initialize clients
    supabase: Client = create_client(supabase_url, supabase_key)
    anki = AnkiConnectClient("http://localhost:8765")

    # Show what's in Supabase
    print("Step 1: Check Supabase")
    decks = supabase.table("anki_decks").select("id, name").execute()
    print(f"  Decks in Supabase: {len(decks.data)}")
    for deck in decks.data:
        print(f"    - {deck['name']} ({deck['id']})")

    cards = supabase.table("anki_cards").select("id, front, deck_id, anki_note_id").execute()
    print(f"  Cards in Supabase: {len(cards.data)}")
    for card in cards.data:
        status = "✓ synced" if card.get('anki_note_id') else "○ not synced"
        print(f"    - {card['front'][:40]}... [{status}]")
    print()

    # Run sync
    print("Step 2: Sync to Anki Desktop")
    print("  Running sync...")
    result = await sync_cards_to_anki(supabase, anki)
    print(f"  Cards created: {result['cards_created']}")
    print(f"  Cards updated: {result['cards_updated']}")
    print(f"  Cards skipped: {result['cards_skipped']}")
    if result['errors']:
        print(f"  Errors: {len(result['errors'])}")
        for error in result['errors']:
            print(f"    - {error}")
    print()

    # Verify in Anki
    print("Step 3: Verify in Anki Desktop")
    deck_names = await anki.deck_names()
    print(f"  Decks in Anki: {len(deck_names)}")

    if "Jarvis_Test" in deck_names:
        print("  ✓ Found 'Jarvis_Test' deck!")
        note_ids = await anki.find_notes("deck:Jarvis_Test")
        print(f"  Cards in Jarvis_Test: {len(note_ids)}")

        if note_ids:
            notes_info = await anki.notes_info(note_ids)
            for note in notes_info:
                fields = note.get("fields", {})
                front = fields.get("Front", {}).get("value", "")
                print(f"    - {front}")
    else:
        print("  ✗ 'Jarvis_Test' deck not found in Anki")
    print()

    print("=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)
    print()
    print("Next: Check Anki Desktop to see the 'Jarvis_Test' deck with the card!")
    print()


if __name__ == "__main__":
    asyncio.run(main())
