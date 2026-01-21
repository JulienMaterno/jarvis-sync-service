"""
AnkiConnect Client

HTTP client for AnkiConnect API (port 8765).
Requires AnkiConnect add-on installed in Anki Desktop.

Installation:
1. Open Anki Desktop
2. Tools → Add-ons → Get Add-ons
3. Enter code: 2055492159
4. Restart Anki

Configuration:
- Default: localhost:8765
- Use ANKI_CONNECT_URL env var to override
"""

import aiohttp
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class AnkiConnectClient:
    """Client for AnkiConnect API."""

    def __init__(self, base_url: str = "http://localhost:8765"):
        self.base_url = base_url
        self.version = 6

    async def request(self, action: str, **params) -> Any:
        """
        Make AnkiConnect API request.

        Args:
            action: AnkiConnect action name
            **params: Action parameters

        Returns:
            API response result

        Raises:
            Exception: If AnkiConnect returns an error
        """
        async with aiohttp.ClientSession() as session:
            payload = {
                "action": action,
                "version": self.version,
                "params": params if params else {}
            }

            logger.debug(f"AnkiConnect request: {action}")

            async with session.post(self.base_url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    raise Exception(f"AnkiConnect HTTP error: {resp.status}")

                result = await resp.json()

                if result.get("error"):
                    raise Exception(f"AnkiConnect error: {result['error']}")

                return result.get("result")

    async def add_note(
        self,
        deck_name: str,
        front: str,
        back: str,
        tags: Optional[List[str]] = None,
        model_name: str = "Basic"
    ) -> int:
        """
        Add a note to Anki.

        Args:
            deck_name: Target deck name
            front: Front of card
            back: Back of card
            tags: List of tags
            model_name: Note type (default: Basic)

        Returns:
            Note ID
        """
        return await self.request(
            "addNote",
            note={
                "deckName": deck_name,
                "modelName": model_name,
                "fields": {"Front": front, "Back": back},
                "tags": tags or [],
            }
        )

    async def update_note_fields(self, note_id: int, front: str, back: str):
        """
        Update note fields.

        Args:
            note_id: Note ID to update
            front: New front text
            back: New back text
        """
        await self.request(
            "updateNoteFields",
            note={
                "id": note_id,
                "fields": {"Front": front, "Back": back}
            }
        )

    async def delete_notes(self, note_ids: List[int]):
        """
        Delete notes.

        Args:
            note_ids: List of note IDs to delete
        """
        await self.request("deleteNotes", notes=note_ids)

    async def get_deck_stats(self, deck_name: str) -> Dict:
        """
        Get deck statistics.

        Args:
            deck_name: Deck name

        Returns:
            Deck statistics dictionary
        """
        return await self.request("getDeckStats", decks=[deck_name])

    async def deck_names(self) -> List[str]:
        """
        Get all deck names.

        Returns:
            List of deck names
        """
        return await self.request("deckNames")

    async def find_notes(self, query: str) -> List[int]:
        """
        Search for notes.

        Args:
            query: Anki search query (e.g., "deck:MyDeck")

        Returns:
            List of note IDs
        """
        return await self.request("findNotes", query=query)

    async def notes_info(self, note_ids: List[int]) -> List[Dict]:
        """
        Get detailed note information.

        Args:
            note_ids: List of note IDs

        Returns:
            List of note info dictionaries
        """
        return await self.request("notesInfo", notes=note_ids)

    async def card_reviews(self, deck_name: str, start_id: int = 0) -> List[Dict]:
        """
        Get card review history.

        Args:
            deck_name: Deck name
            start_id: Starting timestamp in milliseconds

        Returns:
            List of review dictionaries
        """
        return await self.request("cardReviews", deck=deck_name, startID=start_id)

    async def sync(self):
        """Trigger Anki sync (uploads to AnkiWeb)."""
        await self.request("sync")
