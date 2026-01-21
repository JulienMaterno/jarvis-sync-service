"""
================================================================================
ANKI SYNC SERVICE - Bidirectional Supabase ↔ Anki Desktop
================================================================================

Syncs flashcards between Supabase (source of truth) and local Anki Desktop via
AnkiConnect API.

Features:
- Initial import of ALL existing Anki decks and cards
- Bidirectional sync (Supabase → Anki, Anki → Supabase)
- Review statistics tracking
- Automatic deck creation
- Soft deletes support

Usage:
    from syncs.anki_sync import run_anki_sync
    result = await run_anki_sync(supabase_client)
"""

import os
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Any, List
from supabase import Client

from .anki_client import AnkiConnectClient

logger = logging.getLogger(__name__)


def strip_html_and_media(text: str) -> str:
    """
    Strip HTML tags and media references from Anki card content for Supabase storage.

    IMPORTANT: This only affects the TEXT stored in Supabase. The original media files
    in Anki Desktop are NEVER touched or deleted. This is a one-way conversion for
    text indexing purposes only.

    Args:
        text: Raw HTML text from Anki

    Returns:
        Clean text without HTML or media references
    """
    if not text:
        return ""

    # Remove sound/audio tags: [sound:filename.mp3]
    text = re.sub(r'\[sound:[^\]]+\]', '[Audio]', text)

    # Remove image tags: <img src="...">
    text = re.sub(r'<img[^>]*>', '[Image]', text)

    # Remove all HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')

    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()

    return text


async def import_existing_anki_decks(
    supabase: Client,
    anki: AnkiConnectClient,
    max_decks: int = None,
    max_cards_per_deck: int = None
) -> Dict[str, Any]:
    """
    One-time import of all existing Anki decks and cards on first sync.

    Args:
        supabase: Supabase client
        anki: AnkiConnect client
        max_decks: Optional limit on number of decks to import (for testing)
        max_cards_per_deck: Optional limit on cards per deck (for testing)

    Returns:
        Import statistics dictionary
    """
    logger.info("Starting initial import of existing Anki decks...")

    stats = {
        "decks_imported": 0,
        "cards_imported": 0,
        "cards_skipped": 0,
        "errors": []
    }

    try:
        # Get all deck names from Anki
        deck_names = await anki.deck_names()
        logger.info(f"Found {len(deck_names)} decks in Anki Desktop")

        # Apply deck limit if specified
        if max_decks:
            deck_names = deck_names[:max_decks]
            logger.info(f"Limiting import to first {max_decks} decks (test mode)")

        for deck_name in deck_names:
            # Skip default deck if it's a placeholder
            if deck_name == "Default":
                logger.debug("Skipping Default deck")
                continue

            try:
                logger.info(f"Importing deck: {deck_name}")

                # Create deck in Supabase (upsert to handle re-runs)
                deck_response = supabase.table("anki_decks").upsert({
                    "name": deck_name,
                    "description": f"Imported from Anki Desktop",
                }, on_conflict="name").execute()

                deck_id = deck_response.data[0]["id"]
                stats["decks_imported"] += 1

                # Get all note IDs from this deck
                note_ids = await anki.find_notes(f"deck:{deck_name}")
                logger.info(f"  Found {len(note_ids)} notes in deck '{deck_name}'")

                if not note_ids:
                    continue

                # Apply card limit if specified
                if max_cards_per_deck and len(note_ids) > max_cards_per_deck:
                    note_ids = note_ids[:max_cards_per_deck]
                    logger.info(f"  Limiting to first {max_cards_per_deck} cards (test mode)")

                # Get note details in batch (AnkiConnect supports batch operations)
                notes_info = await anki.notes_info(note_ids)

                # Import each card
                for note in notes_info:
                    try:
                        fields = note.get("fields", {})

                        # Extract front and back - try multiple field names
                        front_raw = (
                            fields.get("Front", {}).get("value", "") or
                            fields.get("Word", {}).get("value", "") or
                            fields.get("Question", {}).get("value", "") or
                            fields.get("Text", {}).get("value", "")
                        )
                        back_raw = (
                            fields.get("Back", {}).get("value", "") or
                            fields.get("Definition", {}).get("value", "") or
                            fields.get("Answer", {}).get("value", "") or
                            fields.get("Extra", {}).get("value", "")
                        )

                        # Strip HTML and media references
                        front = strip_html_and_media(front_raw)
                        back = strip_html_and_media(back_raw)

                        # Skip if missing required fields or only contains media
                        if not front or not back:
                            stats["cards_skipped"] += 1
                            logger.debug(f"  Skipped card {note.get('noteId')}: missing fields")
                            continue

                        # Skip cards that only contain media placeholders
                        if front in ["[Audio]", "[Image]"] or back in ["[Audio]", "[Image]"]:
                            stats["cards_skipped"] += 1
                            logger.debug(f"  Skipped card {note.get('noteId')}: media-only")
                            continue

                        # Insert into Supabase (upsert by anki_note_id)
                        supabase.table("anki_cards").upsert({
                            "deck_id": deck_id,
                            "front": front,
                            "back": back,
                            "tags": note.get("tags", []),
                            "note_type": note.get("modelName", "Basic"),
                            "anki_note_id": note["noteId"],
                            "source_type": "anki_import",
                            "last_sync_at": datetime.now(timezone.utc).isoformat()
                        }, on_conflict="anki_note_id").execute()

                        stats["cards_imported"] += 1

                    except Exception as card_error:
                        logger.error(f"  Error importing card {note.get('noteId')}: {card_error}")
                        stats["errors"].append(f"Card {note.get('noteId')}: {str(card_error)}")

            except Exception as deck_error:
                logger.error(f"Error importing deck '{deck_name}': {deck_error}")
                stats["errors"].append(f"Deck '{deck_name}': {str(deck_error)}")

        logger.info(f"Import complete: {stats['decks_imported']} decks, {stats['cards_imported']} cards")

    except Exception as e:
        logger.error(f"Fatal error during import: {e}")
        stats["errors"].append(f"Fatal: {str(e)}")

    return stats


async def sync_cards_to_anki(supabase: Client, anki: AnkiConnectClient) -> Dict[str, Any]:
    """
    Push new/updated cards from Supabase to Anki Desktop.

    Args:
        supabase: Supabase client
        anki: AnkiConnect client

    Returns:
        Sync statistics dictionary
    """
    logger.info("Syncing cards from Supabase → Anki Desktop...")

    stats = {
        "cards_created": 0,
        "cards_updated": 0,
        "cards_skipped": 0,
        "errors": []
    }

    try:
        # Get cards that need syncing (new or updated since last sync)
        response = supabase.table("anki_cards") \
            .select("*, anki_decks!inner(name)") \
            .is_("deleted_at", None) \
            .or_("anki_note_id.is.null,last_sync_at.lt.updated_at") \
            .execute()

        cards_to_sync = response.data
        logger.info(f"Found {len(cards_to_sync)} cards to sync")

        for card in cards_to_sync:
            try:
                deck_name = card["anki_decks"]["name"]

                if card.get("anki_note_id") is None:
                    # Create new note in Anki
                    note_id = await anki.add_note(
                        deck_name=deck_name,
                        front=card["front"],
                        back=card["back"],
                        tags=card.get("tags", [])
                    )

                    # Update Supabase with Anki note ID
                    supabase.table("anki_cards").update({
                        "anki_note_id": note_id,
                        "last_sync_at": datetime.now(timezone.utc).isoformat()
                    }).eq("id", card["id"]).execute()

                    stats["cards_created"] += 1
                    logger.debug(f"Created card in Anki: {note_id}")

                else:
                    # Update existing note
                    await anki.update_note_fields(
                        note_id=card["anki_note_id"],
                        front=card["front"],
                        back=card["back"]
                    )

                    supabase.table("anki_cards").update({
                        "last_sync_at": datetime.now(timezone.utc).isoformat()
                    }).eq("id", card["id"]).execute()

                    stats["cards_updated"] += 1
                    logger.debug(f"Updated card in Anki: {card['anki_note_id']}")

            except Exception as card_error:
                logger.error(f"Error syncing card {card['id']}: {card_error}")
                stats["errors"].append(f"Card {card['id']}: {str(card_error)}")
                stats["cards_skipped"] += 1

        logger.info(f"Sync complete: {stats['cards_created']} created, {stats['cards_updated']} updated")

    except Exception as e:
        logger.error(f"Error syncing cards to Anki: {e}")
        stats["errors"].append(f"Fatal: {str(e)}")

    return stats


async def sync_reviews_from_anki(supabase: Client, anki: AnkiConnectClient) -> Dict[str, Any]:
    """
    Pull review history from Anki to Supabase.

    Args:
        supabase: Supabase client
        anki: AnkiConnect client

    Returns:
        Sync statistics dictionary
    """
    logger.info("Syncing reviews from Anki Desktop → Supabase...")

    stats = {
        "reviews_imported": 0,
        "reviews_skipped": 0,
        "errors": []
    }

    try:
        # Get last sync timestamp
        response = supabase.table("anki_review_logs") \
            .select("reviewed_at") \
            .order("reviewed_at", desc=True) \
            .limit(1) \
            .execute()

        if response.data:
            last_sync_ts = response.data[0]["reviewed_at"]
            last_sync_ms = int(datetime.fromisoformat(last_sync_ts).timestamp() * 1000)
        else:
            last_sync_ms = 0

        logger.info(f"Fetching reviews since: {datetime.fromtimestamp(last_sync_ms / 1000, tz=timezone.utc)}")

        # Get all decks
        decks = supabase.table("anki_decks").select("name").execute().data

        for deck in decks:
            try:
                deck_name = deck["name"]
                logger.debug(f"Checking reviews for deck: {deck_name}")

                reviews = await anki.card_reviews(deck_name, last_sync_ms)

                if not reviews:
                    continue

                logger.info(f"Found {len(reviews)} new reviews in deck '{deck_name}'")

                for review in reviews:
                    try:
                        # Find card by anki_note_id
                        # Note: AnkiConnect review returns card ID, but we store note ID
                        # We'll need to match based on the card's note
                        card_response = supabase.table("anki_cards") \
                            .select("id") \
                            .eq("anki_note_id", review.get("note_id")) \
                            .single() \
                            .execute()

                        if not card_response.data:
                            stats["reviews_skipped"] += 1
                            continue

                        card_id = card_response.data["id"]

                        # Insert review log
                        supabase.table("anki_review_logs").insert({
                            "card_id": card_id,
                            "review_type": "review",
                            "rating": review.get("ease", 3),
                            "time_taken_ms": review.get("time", 0),
                            "reviewed_at": datetime.fromtimestamp(review["id"] / 1000, tz=timezone.utc).isoformat(),
                            "source": "anki_desktop"
                        }).execute()

                        # Update card stats
                        supabase.table("anki_cards").update({
                            "review_count": review.get("reps", 0),
                            "interval_days": review.get("ivl", 0),
                            "last_reviewed_at": datetime.fromtimestamp(review["id"] / 1000, tz=timezone.utc).isoformat()
                        }).eq("id", card_id).execute()

                        stats["reviews_imported"] += 1

                    except Exception as review_error:
                        logger.error(f"Error importing review: {review_error}")
                        stats["errors"].append(f"Review: {str(review_error)}")
                        stats["reviews_skipped"] += 1

            except Exception as deck_error:
                logger.error(f"Error fetching reviews for deck '{deck_name}': {deck_error}")
                stats["errors"].append(f"Deck '{deck_name}': {str(deck_error)}")

        logger.info(f"Reviews sync complete: {stats['reviews_imported']} imported")

    except Exception as e:
        logger.error(f"Error syncing reviews from Anki: {e}")
        stats["errors"].append(f"Fatal: {str(e)}")

    return stats


async def run_anki_sync(
    supabase: Client,
    test_mode: bool = False,
    max_test_decks: int = 2,
    max_test_cards: int = 10
) -> Dict[str, Any]:
    """
    Main sync function (called once daily or manually).

    Performs:
    1. Initial import if first run
    2. Bidirectional sync (Supabase → Anki → Supabase)
    3. Review history sync

    Args:
        supabase: Supabase client
        test_mode: If True, limits import to small dataset for testing
        max_test_decks: Maximum decks to import in test mode
        max_test_cards: Maximum cards per deck in test mode

    Returns:
        Sync result dictionary
    """
    logger.info("=== Starting Anki Sync ===")
    start_time = datetime.now(timezone.utc)

    result = {
        "status": "success",
        "started_at": start_time.isoformat(),
        "import_stats": None,
        "cards_stats": None,
        "reviews_stats": None,
        "errors": []
    }

    try:
        # Initialize AnkiConnect client
        anki_url = os.getenv("ANKI_CONNECT_URL", "http://localhost:8765")
        anki = AnkiConnectClient(anki_url)
        logger.info(f"Connected to AnkiConnect at {anki_url}")

        # Check if this is first run (no decks in Supabase)
        existing_decks = supabase.table("anki_decks").select("id").limit(1).execute()

        if not existing_decks.data:
            if test_mode:
                logger.info(f"First run detected - TEST MODE: importing up to {max_test_decks} decks with {max_test_cards} cards each")
                result["import_stats"] = await import_existing_anki_decks(
                    supabase, anki, max_decks=max_test_decks, max_cards_per_deck=max_test_cards
                )
            else:
                logger.info("First run detected - importing all existing Anki decks")
                result["import_stats"] = await import_existing_anki_decks(supabase, anki)

        # Bidirectional sync
        result["cards_stats"] = await sync_cards_to_anki(supabase, anki)
        result["reviews_stats"] = await sync_reviews_from_anki(supabase, anki)

        # Collect all errors
        for key in ["import_stats", "cards_stats", "reviews_stats"]:
            if result[key] and result[key].get("errors"):
                result["errors"].extend(result[key]["errors"])

        result["completed_at"] = datetime.now(timezone.utc).isoformat()
        result["duration_seconds"] = (datetime.now(timezone.utc) - start_time).total_seconds()

        logger.info(f"=== Anki Sync Complete ({result['duration_seconds']:.1f}s) ===")

    except Exception as e:
        logger.error(f"Fatal error in Anki sync: {e}", exc_info=True)
        result["status"] = "error"
        result["error_message"] = str(e)
        result["errors"].append(f"Fatal: {str(e)}")

    return result
