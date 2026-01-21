"""Quick test of Anki import without .env file dependency."""

import asyncio
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(__file__))

from syncs.anki_client import AnkiConnectClient


async def test_anki_connection():
    """Test AnkiConnect and show sample data."""

    print("=" * 80)
    print("ANKI CONNECTION TEST")
    print("=" * 80)
    print()

    try:
        # Initialize AnkiConnect client
        anki = AnkiConnectClient("http://localhost:8765")

        # Test 1: Get version
        print("Test 1: AnkiConnect Version")
        version = await anki.request("version")
        print(f"  Version: {version}")
        print(f"  Status: OK")
        print()

        # Test 2: Get deck names
        print("Test 2: Deck Names")
        deck_names = await anki.deck_names()
        print(f"  Total decks: {len(deck_names)}")
        print(f"  Decks: {', '.join(deck_names[:5])}")
        if len(deck_names) > 5:
            print(f"  ... and {len(deck_names) - 5} more")
        print()

        # Test 3: Sample cards from first deck
        if deck_names:
            test_deck = deck_names[0]
            print(f"Test 3: Sample Cards from '{test_deck}'")

            note_ids = await anki.find_notes(f"deck:{test_deck}")
            print(f"  Total notes in deck: {len(note_ids)}")

            if note_ids:
                # Get first 2 cards as sample
                sample_ids = note_ids[:2]
                notes_info = await anki.notes_info(sample_ids)

                print(f"  Sample cards:")
                for i, note in enumerate(notes_info, 1):
                    fields = note.get("fields", {})
                    front = fields.get("Front", {}).get("value", "")[:50]
                    back = fields.get("Back", {}).get("value", "")[:50]
                    print(f"    Card {i}:")
                    print(f"      Front: {front}...")
                    print(f"      Back: {back}...")
                    print(f"      Type: {note.get('modelName')}")
                    print(f"      Tags: {note.get('tags', [])}")
            print()

        print("=" * 80)
        print("CONNECTION TEST: SUCCESS")
        print("=" * 80)
        print()
        print("Anki is ready for import!")
        print()

    except Exception as e:
        print(f"ERROR: {e}")
        print()
        print("Make sure:")
        print("  1. Anki Desktop is running")
        print("  2. AnkiConnect add-on is installed")
        print("  3. Port 8765 is accessible")
        return False

    return True


if __name__ == "__main__":
    success = asyncio.run(test_anki_connection())
    sys.exit(0 if success else 1)
