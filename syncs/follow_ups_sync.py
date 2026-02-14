"""
===================================================================================
FOLLOW-UPS SYNC SERVICE - Bidirectional Notion <-> Supabase
===================================================================================

Syncs email follow-up tracking data between Supabase and Notion.

Notion database displays:
- Properties: Contact Name, Email, Status, Days Waiting, Next Follow-Up, etc.
- Page content: Full email thread (original email + follow-up drafts)

From Notion, only Status (Send Now / Cancel) and interval_days are writable.
"""

import os
import time as time_module
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

from lib.sync_base import (
    TwoWaySyncService,
    NotionPropertyExtractor,
    NotionPropertyBuilder,
    ContentBlockBuilder,
    SyncResult,
    SyncStats,
    create_cli_parser,
    setup_logger,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

NOTION_FOLLOW_UPS_DB_ID = os.environ.get("NOTION_FOLLOW_UPS_DB_ID", "")

# Status mapping
SUPABASE_TO_NOTION_STATUS = {
    "pending": "Pending",
    "draft_created": "Draft Created",
    "send_now": "Send Now",
    "sent": "Sent",
    "replied": "Replied",
    "cancelled": "Cancelled",
}

NOTION_TO_SUPABASE_STATUS = {
    "Pending": "pending",
    "Draft Created": "draft_created",
    "Send Now": "send_now",
    "Sent": "sent",
    "Replied": "replied",
    "Cancelled": "cancelled",
}


# ============================================================================
# FOLLOW-UPS SYNC SERVICE
# ============================================================================

class FollowUpsSyncService(TwoWaySyncService):
    """
    Bidirectional sync for Email Follow-Ups between Notion and Supabase.

    Notion Properties:
    - Name (title): Email subject
    - Contact Name (rich_text): Recipient name
    - Email (email): Recipient email
    - Status (select): Pending / Draft Created / Send Now / Sent / Replied / Cancelled
    - Next Follow-Up (date): When next draft generates
    - Follow-Up Count (number): How many follow-ups sent
    - Original Date (date): When first email was sent
    - Last Sent (date): When last follow-up was sent
    - Interval (number): Days between follow-ups

    Page Content:
    - Original email thread + follow-up drafts
    """

    def __init__(self):
        if not NOTION_FOLLOW_UPS_DB_ID:
            raise ValueError(
                "NOTION_FOLLOW_UPS_DB_ID environment variable not set. "
                "Create a Notion database first."
            )
        super().__init__(
            service_name="FollowUpsSync",
            notion_database_id=NOTION_FOLLOW_UPS_DB_ID,
            supabase_table="email_follow_ups",
        )
        self.logger = setup_logger("FollowUpsSync")

    def convert_from_source(self, notion_record: Dict) -> Dict[str, Any]:
        """
        Notion -> Supabase. Only Status and Interval are writable from Notion.
        """
        props = notion_record.get("properties", {})

        # Extract status
        notion_status = NotionPropertyExtractor.select(props, "Status")
        status = NOTION_TO_SUPABASE_STATUS.get(notion_status)

        result = {}
        if status:
            result["status"] = status

        # Extract interval if changed
        interval = NotionPropertyExtractor.number(props, "Interval")
        if interval and interval > 0:
            result["interval_days"] = int(interval)

        return result

    def convert_to_source(self, supabase_record: Dict) -> Dict[str, Any]:
        """
        Supabase -> Notion properties.
        """
        subject = supabase_record.get("subject", "Untitled")
        status = supabase_record.get("status", "pending")
        recipient_name = supabase_record.get("recipient_name", "")
        recipient_email = supabase_record.get("recipient_email", "")
        next_follow_up = supabase_record.get("next_follow_up_date")
        follow_up_count = supabase_record.get("follow_up_count", 0)
        original_date = supabase_record.get("original_date")
        last_sent_at = supabase_record.get("last_sent_at")
        interval_days = supabase_record.get("interval_days", 7)

        notion_status = SUPABASE_TO_NOTION_STATUS.get(status, "Pending")

        properties = {
            "Name": NotionPropertyBuilder.title((subject or "Untitled")[:100]),
            "Contact Name": NotionPropertyBuilder.rich_text(
                recipient_name or recipient_email or ""
            ),
            "Email": {"email": recipient_email} if recipient_email else {"email": None},
            "Status": NotionPropertyBuilder.select(notion_status),
            "Follow-Up Count": NotionPropertyBuilder.number(follow_up_count),
            "Interval": NotionPropertyBuilder.number(interval_days),
        }

        if next_follow_up:
            date_str = next_follow_up[:10] if isinstance(next_follow_up, str) else next_follow_up.strftime("%Y-%m-%d")
            properties["Next Follow-Up"] = NotionPropertyBuilder.date(date_str)

        if original_date:
            date_str = original_date[:10] if isinstance(original_date, str) else original_date.strftime("%Y-%m-%d")
            properties["Original Date"] = NotionPropertyBuilder.date(date_str)

        if last_sent_at:
            date_str = last_sent_at[:10] if isinstance(last_sent_at, str) else last_sent_at.strftime("%Y-%m-%d")
            properties["Last Sent"] = NotionPropertyBuilder.date(date_str)

        return properties

    def _build_content_blocks(self, record: Dict) -> List[Dict]:
        """Build Notion page content showing the email thread + draft."""
        blocks = []
        builder = ContentBlockBuilder()

        # Original Email section
        blocks.append(builder.heading_2("Original Email"))

        original_date = record.get("original_date", "")
        if original_date:
            date_str = original_date[:10] if isinstance(original_date, str) else str(original_date)
            blocks.append(
                builder.paragraph(
                    f"**To:** {record.get('recipient_name', '')} ({record.get('recipient_email', '')})"
                )
            )
            blocks.append(
                builder.paragraph(f"**Date:** {date_str}")
            )
            blocks.append(
                builder.paragraph(f"**Subject:** {record.get('subject', '')}")
            )

        # Original body
        body_text = record.get("original_body_text", "")
        if body_text:
            blocks.append(builder.divider())
            # Truncate for Notion display
            display_body = body_text[:3000]
            blocks.extend(builder.chunked_paragraphs(display_body))

        # Follow-up draft section (if exists)
        draft_body = record.get("draft_body")
        if draft_body:
            blocks.append(builder.divider())
            blocks.append(builder.heading_2("Follow-Up Draft"))

            follow_up_count = record.get("follow_up_count", 0)
            blocks.append(
                builder.callout(
                    f"Follow-up #{follow_up_count} - Change status to 'Send Now' to send this draft",
                    "📧",
                )
            )
            blocks.extend(builder.chunked_paragraphs(draft_body))

        # Status info
        blocks.append(builder.divider())
        status = record.get("status", "pending")
        count = record.get("follow_up_count", 0)
        max_count = record.get("max_follow_ups", 3)
        interval = record.get("interval_days", 7)
        blocks.append(
            builder.paragraph(
                f"**Status:** {status} | **Follow-ups:** {count}/{max_count} | **Interval:** {interval} days"
            )
        )

        return blocks

    def _sync_supabase_to_notion(
        self, full_sync: bool, since_hours: int, metrics=None
    ) -> SyncResult:
        """Override to include page content blocks."""
        stats = SyncStats()
        start_time = time_module.time()

        try:
            # Get Supabase records to sync
            if full_sync:
                supabase_records = self.supabase.select_all()
            else:
                cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
                supabase_records = self.supabase.select_updated_since(cutoff)

            # Filter out soft-deleted
            supabase_records = [r for r in supabase_records if not r.get("deleted_at")]

            self.logger.info(
                f"Found {len(supabase_records)} follow-up records to sync to Notion"
            )

            # Build lookup for existing Notion pages
            notion_records = self.notion.query_database(self.notion_database_id)
            notion_lookup = {r["id"]: r for r in notion_records}

            for record in supabase_records:
                try:
                    properties = self.convert_to_source(record)
                    notion_page_id = record.get("notion_page_id")
                    content_blocks = self._build_content_blocks(record)

                    if notion_page_id and notion_page_id in notion_lookup:
                        # Update existing page properties
                        updated_page = self.notion.update_page(notion_page_id, properties)

                        # Replace page content
                        existing_blocks = self.notion.get_all_blocks(notion_page_id)
                        for block in existing_blocks:
                            try:
                                self.notion.delete_block(block["id"])
                            except Exception:
                                pass
                        if content_blocks:
                            self.notion.append_blocks(notion_page_id, content_blocks)

                        # Update sync tracking
                        self.supabase.update(
                            record["id"],
                            {
                                "notion_updated_at": updated_page.get("last_edited_time"),
                                "last_sync_source": "notion",
                            },
                        )

                        stats.updated += 1
                    else:
                        # Create new page with content
                        new_page = self.notion.create_page(
                            database_id=self.notion_database_id,
                            properties=properties,
                            children=content_blocks,
                        )

                        # Update Supabase with Notion page ID
                        self.supabase.update(
                            record["id"],
                            {
                                "notion_page_id": new_page["id"],
                                "notion_updated_at": new_page.get("last_edited_time"),
                                "last_sync_source": "notion",
                            },
                        )

                        stats.created += 1
                        self.logger.info(
                            f"Created Notion page for follow-up: {record.get('subject', 'Untitled')}"
                        )

                except Exception as e:
                    self.logger.error(f"Error syncing follow-up to Notion: {e}")
                    stats.errors += 1

            return SyncResult(
                success=True,
                direction="supabase_to_notion",
                stats=stats,
                elapsed_seconds=time_module.time() - start_time,
            )

        except Exception as e:
            return SyncResult(
                success=False,
                direction="supabase_to_notion",
                error_message=str(e),
            )


# ============================================================================
# ENTRY POINT
# ============================================================================

def run_sync(full_sync: bool = False, since_hours: int = 24) -> Dict:
    """Run the follow-ups sync and return results."""
    if not NOTION_FOLLOW_UPS_DB_ID:
        return {
            "success": True,
            "direction": "skipped",
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "skipped": 0,
            "errors": 0,
            "elapsed_seconds": 0,
            "message": "NOTION_FOLLOW_UPS_DB_ID not configured",
        }

    service = FollowUpsSyncService()
    result = service.sync(full_sync=full_sync, since_hours=since_hours)

    return {
        "success": result.success,
        "direction": result.direction,
        "created": result.stats.created,
        "updated": result.stats.updated,
        "deleted": result.stats.deleted,
        "skipped": result.stats.skipped,
        "errors": result.stats.errors,
        "elapsed_seconds": result.elapsed_seconds,
    }


if __name__ == "__main__":
    parser = create_cli_parser("Follow-Ups")
    args = parser.parse_args()

    result = run_sync(full_sync=args.full, since_hours=args.hours)
    print(f"\nResult: {result}")
