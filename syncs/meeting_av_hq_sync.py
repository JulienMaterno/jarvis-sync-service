"""
Meeting → AV HQ Sync
=====================
One-way sync that creates meeting pages in the AV HQ teamspace Meeting DB
when meetings involve specific contacts (e.g. Victor).

Flow:
1. Query meetings in Supabase with matching contact_id
2. Query existing pages in AV HQ Meeting DB to get their dates
3. Skip meetings whose date already has a page (avoids duplicating manually created entries)
4. For new meetings, create a page in AV HQ Meeting DB with summary + transcript
5. Store the AV HQ page ID mapping in sync_state

The AV HQ Meeting DB schema is simple: Name (title) + Date (date).
Content is written as page body: summary + transcript.

Dedup strategy: matches by date (YYYY-MM-DD). If a page with the same date
already exists in the DB (whether manually created or auto-synced), it's skipped.

Configuration stored in sync_state table (key: av_hq_meeting_sync_contacts):
[
  {"contact_id": "a380227b-...", "name": "Victor"}
]

Falls back to AV_HQ_CONTACT_IDS constant if not in DB.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Set

from lib.sync_base import (
    NotionClient,
    ContentBlockBuilder,
    setup_logger,
    NOTION_API_TOKEN,
    MAX_BLOCKS_PER_REQUEST,
)

logger = setup_logger("meeting_av_hq_sync")

# AV HQ Meeting DB ID (from the Notion teamspace)
AV_HQ_MEETING_DB_ID = os.environ.get(
    "AV_HQ_MEETING_DB_ID", "630b4bd39e0783ab97908130e4b1d37d"
)

# Default contacts whose meetings get synced to AV HQ
AV_HQ_CONTACT_IDS = {
    "a380227b-7c62-45a8-aa52-ef88296117a2",  # Victor
}

# Sync state keys
SYNC_MAP_KEY = "av_hq_meeting_sync_map"
CONTACTS_CONFIG_KEY = "av_hq_meeting_sync_contacts"


def _get_contact_ids(supabase_client) -> set:
    """Load contact IDs from sync_state, with fallback to constant."""
    try:
        result = (
            supabase_client.table("sync_state")
            .select("value")
            .eq("key", CONTACTS_CONFIG_KEY)
            .execute()
        )
        if result.data and result.data[0].get("value"):
            raw = result.data[0]["value"]
            contacts = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(contacts, list) and contacts:
                ids = {c["contact_id"] for c in contacts if "contact_id" in c}
                if ids:
                    logger.info(f"Loaded {len(ids)} contact IDs from sync_state")
                    return ids
    except Exception as e:
        logger.warning(f"Could not load contact config: {e}")

    return AV_HQ_CONTACT_IDS


def _get_sync_map(supabase_client) -> Dict[str, str]:
    """Get mapping of meeting_id -> av_hq_page_id for already-synced meetings."""
    try:
        result = (
            supabase_client.table("sync_state")
            .select("value")
            .eq("key", SYNC_MAP_KEY)
            .execute()
        )
        if result.data and result.data[0].get("value"):
            raw = result.data[0]["value"]
            return json.loads(raw) if isinstance(raw, str) else raw
    except Exception as e:
        logger.warning(f"Could not load sync map: {e}")
    return {}


def _save_sync_map(supabase_client, sync_map: Dict[str, str]):
    """Save the updated sync map."""
    try:
        supabase_client.table("sync_state").upsert({
            "key": SYNC_MAP_KEY,
            "value": json.dumps(sync_map),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
    except Exception as e:
        logger.warning(f"Could not save sync map: {e}")


def _get_existing_av_hq_dates(notion: NotionClient) -> Set[str]:
    """Query AV HQ Meeting DB and return set of existing date strings (YYYY-MM-DD)."""
    try:
        pages = notion.query_database(AV_HQ_MEETING_DB_ID)
        dates = set()
        for page in pages:
            props = page.get("properties", {})
            date_prop = props.get("Date", {})
            date_obj = date_prop.get("date") or {}
            start = date_obj.get("start", "")
            if start:
                dates.add(start[:10])  # YYYY-MM-DD
        logger.info(f"Found {len(dates)} existing dates in AV HQ Meeting DB")
        return dates
    except Exception as e:
        logger.warning(f"Could not query AV HQ Meeting DB: {e}")
        return set()


def _fetch_meetings_for_contacts(
    supabase_client, contact_ids: set
) -> List[Dict[str, Any]]:
    """Fetch meetings for the given contact IDs."""
    all_meetings = []
    for contact_id in contact_ids:
        result = (
            supabase_client.table("meetings")
            .select(
                "id,title,date,summary,content,contact_name,contact_id,"
                "transcript_id,source_transcript_id,"
                "topics_discussed,follow_up_items"
            )
            .eq("contact_id", contact_id)
            .is_("deleted_at", "null")
            .order("date", desc=True)
            .execute()
        )
        if result.data:
            all_meetings.extend(result.data)
    return all_meetings


def _fetch_transcript_text(supabase_client, transcript_id: str) -> Optional[str]:
    """Fetch transcript full_text from transcripts table."""
    if not transcript_id:
        return None
    try:
        result = (
            supabase_client.table("transcripts")
            .select("full_text")
            .eq("id", transcript_id)
            .execute()
        )
        if result.data and result.data[0].get("full_text"):
            return result.data[0]["full_text"]
    except Exception as e:
        logger.warning(f"Could not fetch transcript {transcript_id}: {e}")
    return None


def _parse_date_utc(date_val: Any) -> Optional[str]:
    """Extract YYYY-MM-DD from a date value, always in UTC.

    Supabase returns timestamps like '2026-02-04T16:00:00+00:00'.
    We want the UTC date, not the local-tz date.
    """
    if not date_val:
        return None
    s = str(date_val)
    # If it's already a bare date (YYYY-MM-DD), use it directly
    if len(s) == 10:
        return s
    # Parse ISO string and extract UTC date
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return s[:10]


def _build_notion_properties(meeting: Dict[str, Any]) -> Dict:
    """Build Notion API properties for the AV HQ Meeting DB page."""
    title = meeting.get("title") or "Untitled Meeting"
    props: Dict[str, Any] = {
        "Name": {"title": [{"text": {"content": title[:2000]}}]},
    }

    date_str = _parse_date_utc(meeting.get("date"))
    if date_str:
        props["Date"] = {"date": {"start": date_str}}

    return props


def _build_page_blocks(
    meeting: Dict[str, Any], transcript_text: Optional[str]
) -> List[Dict]:
    """Build Notion content blocks for the meeting page."""
    blocks: List[Dict] = []

    # Summary
    summary = meeting.get("summary") or meeting.get("content") or ""
    if summary:
        blocks.append(ContentBlockBuilder.heading_2("Summary"))
        blocks.extend(ContentBlockBuilder.chunked_paragraphs(summary))

    # Transcript (behind a divider)
    if transcript_text:
        blocks.append(ContentBlockBuilder.divider())
        blocks.append(ContentBlockBuilder.heading_2("Transcript"))
        blocks.extend(ContentBlockBuilder.chunked_paragraphs(transcript_text))

    return blocks


def run_sync(
    supabase_client,
    full_sync: bool = False,
) -> Dict[str, Any]:
    """
    Sync meetings with AV HQ contacts to the AV HQ Meeting DB.

    On incremental sync: only creates pages for meetings not yet synced.
    On full_sync: re-creates pages for all matching meetings.

    Args:
        supabase_client: Supabase client instance.
        full_sync: If True, ignore existing sync map and re-sync all.

    Returns:
        Dict with sync statistics.
    """
    start_time = time.time()

    if not NOTION_API_TOKEN:
        logger.error("NOTION_API_TOKEN not set")
        return {"status": "error", "error": "no_notion_token"}

    notion = NotionClient(NOTION_API_TOKEN)

    # Load contact IDs and sync map
    contact_ids = _get_contact_ids(supabase_client)
    if not contact_ids:
        logger.info("No AV HQ contacts configured, skipping")
        return {"status": "skipped", "reason": "no_contacts"}

    sync_map = {} if full_sync else _get_sync_map(supabase_client)
    logger.info(f"Sync map has {len(sync_map)} existing entries")

    # Query existing AV HQ DB entries to avoid duplicating manually created pages
    existing_dates = _get_existing_av_hq_dates(notion)

    # Fetch meetings
    meetings = _fetch_meetings_for_contacts(supabase_client, contact_ids)
    logger.info(f"Found {len(meetings)} meetings for {len(contact_ids)} contacts")

    stats: Dict[str, Any] = {
        "meetings_found": len(meetings),
        "already_synced": 0,
        "skipped_date_exists": 0,
        "newly_synced": 0,
        "errors": [],
    }

    for meeting in meetings:
        meeting_id = meeting["id"]

        if meeting_id in sync_map:
            stats["already_synced"] += 1
            continue

        # Check if a page with this date (+/- 1 day for timezone tolerance)
        # already exists in AV HQ DB
        meeting_date = _parse_date_utc(meeting.get("date"))
        if meeting_date:
            try:
                d = datetime.strptime(meeting_date, "%Y-%m-%d").date()
                nearby = {
                    (d - timedelta(days=1)).isoformat(),
                    d.isoformat(),
                    (d + timedelta(days=1)).isoformat(),
                }
            except ValueError:
                nearby = {meeting_date}
            if nearby & existing_dates:
                matched = nearby & existing_dates
                sync_map[meeting_id] = f"existing:{meeting_date}"
                stats["skipped_date_exists"] += 1
                logger.info(
                    f"Skipping '{meeting.get('title')}' — "
                    f"date {meeting_date} matches existing {matched}"
                )
                continue

        title = meeting.get("title", "Untitled")
        try:
            logger.info(f"Syncing '{title}' to AV HQ...")

            # Fetch transcript
            transcript_id = (
                meeting.get("source_transcript_id")
                or meeting.get("transcript_id")
            )
            transcript_text = _fetch_transcript_text(supabase_client, transcript_id)

            # Build content blocks
            blocks = _build_page_blocks(meeting, transcript_text)

            # Create page with first batch of blocks
            properties = _build_notion_properties(meeting)
            first_batch = blocks[:MAX_BLOCKS_PER_REQUEST] if blocks else []

            page = notion.create_page(
                AV_HQ_MEETING_DB_ID, properties, first_batch
            )
            av_hq_page_id = page["id"]
            logger.info(f"Created AV HQ page: {av_hq_page_id}")

            # Append remaining blocks in chunks
            remaining = blocks[MAX_BLOCKS_PER_REQUEST:]
            for i in range(0, len(remaining), MAX_BLOCKS_PER_REQUEST):
                chunk = remaining[i : i + MAX_BLOCKS_PER_REQUEST]
                notion.append_blocks(av_hq_page_id, chunk)
                logger.info(
                    f"  Appended {len(chunk)} blocks "
                    f"({i + len(chunk)}/{len(remaining)} remaining)"
                )

            # Track in sync map and update existing dates
            sync_map[meeting_id] = av_hq_page_id
            _save_sync_map(supabase_client, sync_map)
            if meeting_date:
                existing_dates.add(meeting_date)

            stats["newly_synced"] += 1
            logger.info(f"  Done: '{title}'")

        except Exception as e:
            error_msg = f"Error syncing '{title}': {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)

    # Persist sync map (includes both newly synced and skipped-by-date entries)
    if stats["newly_synced"] > 0 or stats["skipped_date_exists"] > 0:
        _save_sync_map(supabase_client, sync_map)

    elapsed = time.time() - start_time
    stats["elapsed_seconds"] = round(elapsed, 2)
    logger.info(
        f"AV HQ meeting sync complete: "
        f"{stats['newly_synced']} new, {stats['already_synced']} existing, "
        f"{stats['skipped_date_exists']} skipped (date exists) "
        f"in {elapsed:.1f}s"
    )
    return stats
