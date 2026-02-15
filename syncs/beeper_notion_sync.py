"""
Beeper → Notion Message Sync
=============================
One-way sync that appends new Beeper messages to Notion pages.

Newest messages appear at the top (right below the overview section).
Time gap indicators are added when there's a significant pause between messages,
helping the AI understand conversation flow across timezones.

Runs after the main beeper sync (which populates beeper_messages in Supabase).

Configuration stored in sync_state table (key: beeper_notion_sync_mappings):
[
  {
    "name": "Victor & Aaron",
    "chat_ids": ["!Ij2uVYZKIXJdZZwX3nlj:beeper.local"],
    "notion_page_id": "308b4bd39e0781f68f9fcf39d3f351da"
  }
]

Page structure (newest first):
  [Overview / pinned summary]
  [--- anchor divider ---]        ← stored block ID in sync_state
  [newest message]
  [--- 6 hours later ---]
  [older message]
  ...
  [oldest message]

Falls back to BEEPER_NOTION_SYNC_MAPPINGS env var if not in DB.
"""

import os
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

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
ANCHOR_KEY_PREFIX = "beeper_notion_sync_anchor"

# Time gap threshold for adding separators (hours)
TIME_GAP_THRESHOLD_HOURS = 4

# Sender name cleanup: outgoing messages use beeper ID instead of real name
SENDER_NAME_MAP = {
    "@aaronpuetting:beeper.com": "Aaron",
}


def _clean_sender_name(raw_name: str) -> str:
    """Map beeper IDs to friendly display names."""
    return SENDER_NAME_MAP.get(raw_name, raw_name)


def _load_mappings(supabase_client) -> List[Dict[str, Any]]:
    """Load chat-to-page mappings from sync_state table, with env var fallback."""
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

    if BEEPER_NOTION_SYNC_MAPPINGS_ENV:
        try:
            mappings = json.loads(BEEPER_NOTION_SYNC_MAPPINGS_ENV)
            if isinstance(mappings, list):
                logger.info(f"Loaded {len(mappings)} mappings from env var")
                return mappings
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in BEEPER_NOTION_SYNC_MAPPINGS env var: {e}")

    return []


def _get_state(supabase_client, key: str) -> Optional[str]:
    """Get a value from sync_state."""
    try:
        result = supabase_client.table("sync_state").select("value").eq("key", key).execute()
        if result.data and result.data[0].get("value"):
            return result.data[0]["value"]
    except Exception as e:
        logger.warning(f"Could not get state for {key}: {e}")
    return None


def _set_state(supabase_client, key: str, value: str):
    """Set a value in sync_state."""
    try:
        supabase_client.table("sync_state").upsert({
            "key": key,
            "value": value,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        logger.warning(f"Could not set state for {key}: {e}")


def _get_cursor(supabase_client, mapping_name: str) -> Optional[datetime]:
    """Get the last sync cursor for a specific mapping."""
    val = _get_state(supabase_client, f"{CURSOR_KEY_PREFIX}_{mapping_name}")
    if val:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    return None


def _set_cursor(supabase_client, mapping_name: str, timestamp: datetime):
    """Update the sync cursor after successful sync."""
    _set_state(supabase_client, f"{CURSOR_KEY_PREFIX}_{mapping_name}", timestamp.isoformat())


def _get_anchor(supabase_client, mapping_name: str) -> Optional[str]:
    """Get the anchor block ID for inserting new messages."""
    return _get_state(supabase_client, f"{ANCHOR_KEY_PREFIX}_{mapping_name}")


def _set_anchor(supabase_client, mapping_name: str, block_id: str):
    """Store the anchor block ID."""
    _set_state(supabase_client, f"{ANCHOR_KEY_PREFIX}_{mapping_name}", block_id)


def _fetch_messages(
    supabase_client,
    chat_ids: List[str],
    since: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Fetch messages for the given chat IDs, optionally after a timestamp."""
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


def _parse_ts(ts_str: str) -> Optional[datetime]:
    """Parse an ISO timestamp string."""
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _format_time_gap(gap: timedelta) -> str:
    """Format a time gap into a human-readable string."""
    hours = gap.total_seconds() / 3600
    if hours < 24:
        return f"{int(hours)} hours later"
    days = gap.days
    if days == 1:
        return "1 day later"
    return f"{days} days later"


def _format_messages_newest_first(messages: List[Dict[str, Any]]) -> List[Dict]:
    """
    Convert messages into Notion blocks in newest-first order.

    Includes:
    - Date headers (heading_3) when date changes
    - Time gap indicators when >4h between consecutive messages
    - Each message as: **Sender** `HH:MM` content
    """
    if not messages:
        return []

    # Reverse to newest-first
    reversed_msgs = list(reversed(messages))

    blocks: List[Dict] = []
    current_date: Optional[str] = None
    prev_ts: Optional[datetime] = None

    for msg in reversed_msgs:
        ts_str = msg.get("timestamp")
        if not ts_str:
            continue

        ts = _parse_ts(ts_str)
        if not ts:
            continue

        content = (msg.get("content") or msg.get("content_description") or "").strip()
        if not content:
            continue

        date_str = ts.strftime("%Y-%m-%d")
        time_str = ts.strftime("%H:%M")

        # Date header when date changes (going newest → oldest)
        if date_str != current_date:
            current_date = date_str
            blocks.append(ContentBlockBuilder.heading_3(date_str))

        # Time gap indicator (between this message and the previous newer one)
        if prev_ts is not None:
            gap = prev_ts - ts  # prev_ts is newer, ts is older
            if gap >= timedelta(hours=TIME_GAP_THRESHOLD_HOURS):
                gap_text = f"--- {_format_time_gap(gap)} ---"
                blocks.append(ContentBlockBuilder.paragraph(f"*{gap_text}*"))

        # Message line
        sender = _clean_sender_name(msg.get("sender_name", "Unknown"))
        line = f"**{sender}** `{time_str}` {content}"
        if len(line) > 1990:
            line = line[:1987] + "..."

        blocks.append(ContentBlockBuilder.paragraph(line))
        prev_ts = ts

    return blocks


def _insert_blocks_after_anchor(
    notion: NotionClient,
    page_id: str,
    blocks: List[Dict],
    anchor_id: str,
) -> int:
    """Insert blocks after the anchor block in chunks."""
    total = 0
    for i in range(0, len(blocks), MAX_BLOCKS_PER_REQUEST):
        chunk = blocks[i: i + MAX_BLOCKS_PER_REQUEST]
        notion.append_blocks(page_id, chunk, after=anchor_id)
        total += len(chunk)
        logger.info(f"Inserted {len(chunk)} blocks after anchor (total: {total}/{len(blocks)})")
    return total


def _rebuild_page(
    notion: NotionClient,
    supabase_client,
    mapping: Dict[str, Any],
    messages: List[Dict[str, Any]],
) -> Tuple[int, str]:
    """
    Clear the page and rebuild with overview + newest-first messages.

    Returns (blocks_written, anchor_block_id).
    """
    page_id = mapping["notion_page_id"]
    name = mapping.get("name", "unknown")

    # Delete all existing blocks
    existing_blocks = notion.get_all_blocks(page_id)
    for block in existing_blocks:
        try:
            notion.delete_block(block["id"])
        except Exception as e:
            logger.warning(f"Could not delete block {block['id']}: {e}")

    # Build overview section
    overview_blocks = [
        ContentBlockBuilder.heading_2("Overview"),
        ContentBlockBuilder.paragraph(
            f"Continuous conversation log between {name}. "
            f"**Newest messages are at the top.** "
            f"Synced automatically from Beeper (WhatsApp + LinkedIn)."
        ),
    ]

    # Build key topics from all messages
    overview_blocks.append(ContentBlockBuilder.paragraph(
        "Key topics: duckweed fermentation, drying/processing, "
        "product form (powder vs fresh), market strategy, "
        "EF Singapore application, investor outreach, meeting scheduling."
    ))

    # Anchor divider - new messages get inserted right after this
    overview_blocks.append(ContentBlockBuilder.divider())

    # Write overview + anchor
    results = notion.append_blocks(page_id, overview_blocks)

    # The anchor is the divider (last block written)
    anchor_id = results[-1]["id"]
    logger.info(f"Created anchor divider: {anchor_id}")

    # Format all messages newest-first
    message_blocks = _format_messages_newest_first(messages)
    if not message_blocks:
        return len(overview_blocks), anchor_id

    # Append messages after anchor (they go below anchor in newest-first order)
    written = 0
    for i in range(0, len(message_blocks), MAX_BLOCKS_PER_REQUEST):
        chunk = message_blocks[i: i + MAX_BLOCKS_PER_REQUEST]
        notion.append_blocks(page_id, chunk)
        written += len(chunk)
        logger.info(f"Wrote {written}/{len(message_blocks)} message blocks")

    return len(overview_blocks) + written, anchor_id


def run_sync(
    supabase_client,
    full_sync: bool = False,
) -> Dict[str, Any]:
    """
    Run Beeper → Notion message sync.

    On incremental sync: inserts new messages right after the anchor block
    (newest on top). On full_sync: rebuilds the entire page.

    Args:
        supabase_client: Supabase client instance.
        full_sync: If True, clear page and rebuild from all messages.

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
        "blocks_written": 0,
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
            if full_sync:
                # Full rebuild: fetch all messages, clear page, rewrite
                logger.info("  Full sync: fetching all messages...")
                messages = _fetch_messages(supabase_client, chat_ids)
                total_stats["messages_fetched"] += len(messages)

                if not messages:
                    logger.info(f"  No messages for '{name}'")
                    total_stats["mappings_processed"] += 1
                    continue

                logger.info(f"  Rebuilding page with {len(messages)} messages (newest first)...")
                written, anchor_id = _rebuild_page(notion, supabase_client, mapping, messages)
                total_stats["blocks_written"] += written

                # Save anchor and update cursor
                _set_anchor(supabase_client, name, anchor_id)
                latest_ts = messages[-1].get("timestamp")
                if latest_ts:
                    latest_dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                    _set_cursor(supabase_client, name, latest_dt)

                logger.info(f"  Rebuilt: {written} blocks, anchor={anchor_id[:8]}")

            else:
                # Incremental: fetch new messages, insert after anchor
                cursor = _get_cursor(supabase_client, name)
                anchor_id = _get_anchor(supabase_client, name)

                if not anchor_id:
                    logger.info(f"  No anchor for '{name}', triggering full rebuild")
                    messages = _fetch_messages(supabase_client, chat_ids)
                    total_stats["messages_fetched"] += len(messages)

                    if messages:
                        written, anchor_id = _rebuild_page(notion, supabase_client, mapping, messages)
                        total_stats["blocks_written"] += written
                        _set_anchor(supabase_client, name, anchor_id)
                        latest_ts = messages[-1].get("timestamp")
                        if latest_ts:
                            _set_cursor(supabase_client, name,
                                        datetime.fromisoformat(latest_ts.replace("Z", "+00:00")))
                    total_stats["mappings_processed"] += 1
                    continue

                if cursor:
                    logger.info(f"  Cursor: {cursor.isoformat()}")

                messages = _fetch_messages(supabase_client, chat_ids, since=cursor)
                total_stats["messages_fetched"] += len(messages)

                if not messages:
                    logger.info(f"  No new messages for '{name}'")
                    total_stats["mappings_processed"] += 1
                    continue

                logger.info(f"  Found {len(messages)} new messages")

                # Format newest-first and insert after anchor
                blocks = _format_messages_newest_first(messages)
                if blocks:
                    written = _insert_blocks_after_anchor(notion, page_id, blocks, anchor_id)
                    total_stats["blocks_written"] += written

                # Update cursor
                latest_ts = messages[-1].get("timestamp")
                if latest_ts:
                    latest_dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                    _set_cursor(supabase_client, name, latest_dt)
                    logger.info(f"  Updated cursor to {latest_dt.isoformat()}")

            total_stats["mappings_processed"] += 1
            logger.info(f"  Done for '{name}'")

        except Exception as e:
            error_msg = f"Error syncing '{name}': {e}"
            logger.error(error_msg)
            total_stats["errors"].append(error_msg)

    elapsed = time.time() - start_time
    total_stats["elapsed_seconds"] = round(elapsed, 2)
    logger.info(
        f"Beeper→Notion sync complete: "
        f"{total_stats['messages_fetched']} msgs, "
        f"{total_stats['blocks_written']} blocks "
        f"in {elapsed:.1f}s"
    )
    return total_stats
