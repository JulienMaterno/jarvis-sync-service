"""
Beeper → Notion Message Sync
=============================
One-way sync that appends new Beeper messages to Notion pages.

Runs after the main beeper sync (which populates beeper_messages in Supabase).
Reads new messages from beeper_messages table and appends them as blocks to
a continuous conversation page in Notion.

Configuration stored in sync_state table (key: beeper_notion_sync_mappings):
[
  {
    "name": "Victor & Aaron",
    "chat_ids": ["!Ij2uVYZKIXJdZZwX3nlj:beeper.local"],
    "notion_page_id": "308b4bd39e0781f68f9fcf39d3f351da"
  }
]

Falls back to BEEPER_NOTION_SYNC_MAPPINGS env var if not in DB.
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from lib.sync_base import (
    NotionClient,
    ContentBlockBuilder,
    setup_logger,
    NOTION_API_TOKEN,
    MAX_BLOCKS_PER_REQUEST,
)

logger = setup_logger("beeper_notion_sync")

# Fallback env var (DB config takes priority)
BEEPER_NOTION_SYNC_MAPPINGS_ENV = os.environ.get("BEEPER_NOTION_SYNC_MAPPINGS", "")

# Sync state keys
CONFIG_KEY = "beeper_notion_sync_mappings"
CURSOR_KEY_PREFIX = "beeper_notion_sync_cursor"


def _load_mappings(supabase_client) -> List[Dict[str, Any]]:
    """Load chat-to-page mappings from sync_state table, with env var fallback."""
    # Try sync_state table first
    try:
        result = supabase_client.table("sync_state").select("value").eq("key", CONFIG_KEY).execute()
        if result.data and result.data[0].get("value"):
            raw = result.data[0]["value"]
            mappings = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(mappings, list) and mappings:
                logger.info(f"Loaded {len(mappings)} mappings from sync_state")
                return mappings
    except Exception as e:
        logger.warning(f"Could not load mappings from sync_state: {e}")

    # Fallback to env var
    if BEEPER_NOTION_SYNC_MAPPINGS_ENV:
        try:
            mappings = json.loads(BEEPER_NOTION_SYNC_MAPPINGS_ENV)
            if isinstance(mappings, list):
                logger.info(f"Loaded {len(mappings)} mappings from env var")
                return mappings
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in BEEPER_NOTION_SYNC_MAPPINGS env var: {e}")

    return []


def _get_cursor(supabase_client, mapping_name: str) -> Optional[datetime]:
    """Get the last sync cursor for a specific mapping."""
    key = f"{CURSOR_KEY_PREFIX}_{mapping_name}"
    try:
        result = supabase_client.table("sync_state").select("value").eq("key", key).execute()
        if result.data and result.data[0].get("value"):
            return datetime.fromisoformat(result.data[0]["value"].replace("Z", "+00:00"))
    except Exception as e:
        logger.warning(f"Could not get cursor for {mapping_name}: {e}")
    return None


def _set_cursor(supabase_client, mapping_name: str, timestamp: datetime):
    """Update the sync cursor after successful sync."""
    key = f"{CURSOR_KEY_PREFIX}_{mapping_name}"
    try:
        supabase_client.table("sync_state").upsert({
            "key": key,
            "value": timestamp.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        logger.warning(f"Could not update cursor for {mapping_name}: {e}")


def _fetch_new_messages(
    supabase_client,
    chat_ids: List[str],
    since: Optional[datetime],
) -> List[Dict[str, Any]]:
    """Fetch messages newer than the cursor for the given chat IDs."""
    query = (
        supabase_client.table("beeper_messages")
        .select("id,beeper_chat_id,platform,sender_name,is_outgoing,content,content_description,message_type,timestamp")
        .in_("beeper_chat_id", chat_ids)
        .order("timestamp", desc=False)
    )
    if since:
        query = query.gt("timestamp", since.isoformat())

    result = query.execute()
    return result.data or []


def _format_messages_as_blocks(messages: List[Dict[str, Any]]) -> List[Dict]:
    """
    Convert messages into Notion blocks for appending.

    Format:
    - Date header (heading_3) when date changes
    - Each message as a paragraph: **Sender** `HH:MM` content
    """
    blocks: List[Dict] = []
    current_date: Optional[str] = None

    for msg in messages:
        ts_str = msg.get("timestamp")
        if not ts_str:
            continue

        # Parse timestamp
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        date_str = ts.strftime("%Y-%m-%d")
        time_str = ts.strftime("%H:%M")

        # Add date header if date changed
        if date_str != current_date:
            current_date = date_str
            blocks.append(ContentBlockBuilder.heading_3(date_str))

        # Build message line
        sender = msg.get("sender_name", "Unknown")
        content = msg.get("content") or msg.get("content_description") or ""
        content = content.strip()
        if not content:
            # Skip empty messages
            continue

        line = f"**{sender}** `{time_str}` {content}"

        # Truncate if over Notion's block limit
        if len(line) > 1990:
            line = line[:1987] + "..."

        blocks.append(ContentBlockBuilder.paragraph(line))

    return blocks


def _append_blocks_chunked(
    notion: NotionClient,
    page_id: str,
    blocks: List[Dict],
) -> int:
    """Append blocks to a Notion page in chunks of MAX_BLOCKS_PER_REQUEST."""
    total_appended = 0
    for i in range(0, len(blocks), MAX_BLOCKS_PER_REQUEST):
        chunk = blocks[i : i + MAX_BLOCKS_PER_REQUEST]
        notion.append_blocks(page_id, chunk)
        total_appended += len(chunk)
        logger.info(f"Appended {len(chunk)} blocks (total: {total_appended}/{len(blocks)})")
    return total_appended


def run_sync(
    supabase_client,
    full_sync: bool = False,
) -> Dict[str, Any]:
    """
    Run Beeper → Notion message sync.

    Reads new messages from beeper_messages in Supabase and appends them
    to the configured Notion pages.

    Args:
        supabase_client: Supabase client instance.
        full_sync: If True, ignore cursor and resync all messages.

    Returns:
        Dict with sync statistics.
    """
    start_time = time.time()
    mappings = _load_mappings(supabase_client)

    if not mappings:
        logger.info("No beeper→Notion mappings configured, skipping")
        return {"status": "skipped", "reason": "no_mappings"}

    if not NOTION_API_TOKEN:
        logger.error("NOTION_API_TOKEN not set, cannot sync to Notion")
        return {"status": "error", "error": "no_notion_token"}

    notion = NotionClient(NOTION_API_TOKEN)

    total_stats = {
        "mappings_processed": 0,
        "messages_fetched": 0,
        "blocks_appended": 0,
        "errors": [],
    }

    for mapping in mappings:
        name = mapping.get("name", "unknown")
        chat_ids = mapping.get("chat_ids", [])
        page_id = mapping.get("notion_page_id", "")

        if not chat_ids or not page_id:
            logger.warning(f"Skipping mapping '{name}': missing chat_ids or notion_page_id")
            continue

        logger.info(f"Processing mapping '{name}': {len(chat_ids)} chats → page {page_id[:8]}...")

        try:
            # Get cursor (skip if full_sync)
            cursor = None if full_sync else _get_cursor(supabase_client, name)
            if cursor:
                logger.info(f"  Cursor: {cursor.isoformat()}")
            else:
                logger.info("  No cursor (first sync or full sync)")

            # Fetch new messages
            messages = _fetch_new_messages(supabase_client, chat_ids, cursor)
            total_stats["messages_fetched"] += len(messages)

            if not messages:
                logger.info(f"  No new messages for '{name}'")
                total_stats["mappings_processed"] += 1
                continue

            logger.info(f"  Found {len(messages)} new messages")

            # Format as Notion blocks
            blocks = _format_messages_as_blocks(messages)
            if not blocks:
                logger.info(f"  No non-empty messages to append")
                total_stats["mappings_processed"] += 1
                continue

            # Append to Notion page
            appended = _append_blocks_chunked(notion, page_id, blocks)
            total_stats["blocks_appended"] += appended

            # Update cursor to latest message timestamp
            latest_ts = messages[-1].get("timestamp")
            if latest_ts:
                latest_dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                _set_cursor(supabase_client, name, latest_dt)
                logger.info(f"  Updated cursor to {latest_dt.isoformat()}")

            total_stats["mappings_processed"] += 1
            logger.info(f"  Done: appended {appended} blocks for '{name}'")

        except Exception as e:
            error_msg = f"Error syncing '{name}': {e}"
            logger.error(error_msg)
            total_stats["errors"].append(error_msg)

    elapsed = time.time() - start_time
    total_stats["elapsed_seconds"] = round(elapsed, 2)
    logger.info(
        f"Beeper→Notion sync complete: "
        f"{total_stats['messages_fetched']} messages, "
        f"{total_stats['blocks_appended']} blocks appended "
        f"in {elapsed:.1f}s"
    )
    return total_stats
