"""
Email Follow-Up Sync Module

Tracks outbound emails labeled "Follow-Up" in Gmail, detects replies,
and auto-generates follow-up drafts via the Intelligence Service.

Flow:
1. scan_new_follow_ups() - Find emails with Follow-Up label, create tracking records
2. check_for_replies() - Detect replies in tracked threads (Supabase query only)
3. process_expired_timers() - Generate follow-up drafts when timers expire
4. process_send_now() - Send drafts triggered from Notion (status='send_now')
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from typing import Any, Dict, List, Optional

import httpx

from lib.google_gmail import GmailClient
from lib.supabase_client import supabase, find_contact_by_email
from lib.logging_service import log_sync_event

logger = logging.getLogger("FollowUpSync")

INTELLIGENCE_SERVICE_URL = os.getenv(
    "INTELLIGENCE_SERVICE_URL",
    "https://jarvis-intelligence-service-776871804948.asia-southeast1.run.app"
)

FOLLOW_UP_LABEL_NAME = "Follow-Up"
DEFAULT_INTERVAL_DAYS = 7

# Auto-reply detection patterns (case-insensitive subject checks)
AUTO_REPLY_SUBJECT_PATTERNS = [
    "out of office",
    "automatic reply",
    "auto-reply",
    "autoreply",
    "auto reply",
    "away from office",
    "on leave",
    "out of the office",
    "i am currently out",
    "vacation reply",
    "absence notice",
]

# Sender patterns that indicate auto-generated messages
AUTO_REPLY_SENDER_PATTERNS = [
    "mailer-daemon@",
    "noreply@",
    "no-reply@",
    "postmaster@",
    "donotreply@",
]


def _is_auto_reply(sender: str, subject: str, body_text: str = "") -> bool:
    """Check if an email is an auto-reply (out-of-office, etc.)."""
    sender_lower = sender.lower()
    subject_lower = subject.lower() if subject else ""

    # Check sender patterns
    for pattern in AUTO_REPLY_SENDER_PATTERNS:
        if pattern in sender_lower:
            return True

    # Check subject patterns
    for pattern in AUTO_REPLY_SUBJECT_PATTERNS:
        if pattern in subject_lower:
            return True

    return False
DEFAULT_MAX_FOLLOW_UPS = 3


class FollowUpSync:
    def __init__(self):
        self.gmail_client = GmailClient()
        self._label_id: Optional[str] = None
        self._user_email: Optional[str] = None

    async def _get_user_email(self) -> str:
        """Get the authenticated user's email address from Gmail profile."""
        if self._user_email:
            return self._user_email
        profile = await self.gmail_client.get_profile()
        self._user_email = profile.get("emailAddress", "").lower()
        return self._user_email

    async def _ensure_label(self) -> str:
        """Get or create the Follow-Up label, caching the ID."""
        if self._label_id:
            return self._label_id

        # Check sync_state cache first
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table("sync_state")
                .select("value")
                .eq("key", "gmail_follow_up_label_id")
                .execute()
            )
            if result.data and result.data[0].get("value"):
                self._label_id = result.data[0]["value"]
                return self._label_id
        except Exception as e:
            logger.debug(f"No cached label ID: {e}")

        # Search existing labels
        labels = await self.gmail_client.get_labels()
        for label in labels:
            if label.get("name") == FOLLOW_UP_LABEL_NAME:
                self._label_id = label["id"]
                break

        # Create if not found
        if not self._label_id:
            label = await self.gmail_client.create_label(FOLLOW_UP_LABEL_NAME)
            self._label_id = label["id"]
            logger.info(f"Created Gmail label '{FOLLOW_UP_LABEL_NAME}' with ID: {self._label_id}")

        # Cache in sync_state
        try:
            await asyncio.to_thread(
                lambda: supabase.table("sync_state").upsert({
                    "key": "gmail_follow_up_label_id",
                    "value": self._label_id,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                }).execute()
            )
        except Exception as e:
            logger.warning(f"Failed to cache label ID: {e}")

        return self._label_id

    async def sync_follow_ups(self):
        """Main entry point - called during each sync cycle."""
        label_id = await self._ensure_label()
        user_email = await self._get_user_email()

        stats = {
            "new_tracked": 0,
            "replies_detected": 0,
            "drafts_generated": 0,
            "drafts_sent": 0,
        }

        try:
            stats["new_tracked"] = await self.scan_new_follow_ups(label_id, user_email)
            stats["replies_detected"] = await self.check_for_replies(label_id, user_email)
            stats["drafts_generated"] = await self.process_expired_timers(user_email)
            stats["drafts_sent"] = await self.process_send_now()
        except Exception as e:
            logger.error(f"Follow-up sync failed: {e}")
            await log_sync_event("follow_up_sync", "error", str(e))
            raise

        total = sum(stats.values())
        if total > 0:
            logger.info(f"Follow-up sync: {stats}")
            await log_sync_event("follow_up_sync", "success", f"Processed: {stats}")
        else:
            logger.info("Follow-up sync: no changes")

        return stats

    async def scan_new_follow_ups(self, label_id: str, user_email: str) -> int:
        """Find emails with Follow-Up label that aren't tracked yet."""
        # Query Gmail for labeled messages
        labeled_messages = await self.gmail_client.list_messages(
            query=f"label:{FOLLOW_UP_LABEL_NAME}",
            max_results=50
        )

        if not labeled_messages:
            return 0

        # Get existing tracked thread IDs to skip
        message_ids = [m["id"] for m in labeled_messages]
        existing_threads = set()
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table("email_follow_ups")
                .select("thread_id, google_message_id")
                .is_("deleted_at", "null")
                .execute()
            )
            for row in result.data:
                existing_threads.add(row["thread_id"])
        except Exception as e:
            logger.error(f"Failed to query existing follow-ups: {e}")
            return 0

        new_count = 0
        async with httpx.AsyncClient(timeout=60.0) as client:
            for msg_meta in labeled_messages:
                msg_id = msg_meta["id"]
                thread_id = msg_meta.get("threadId", "")

                # Skip if this thread is already tracked
                if thread_id in existing_threads:
                    continue

                try:
                    # Fetch full message to get sender, recipient, body
                    msg = await self.gmail_client.get_message(msg_id, format="full", client=client)
                    payload = msg.get("payload", {})

                    sender_raw = self.gmail_client.get_header(payload, "From")
                    recipient_raw = self.gmail_client.get_header(payload, "To")
                    subject = self.gmail_client.get_header(payload, "Subject")
                    date_str = self.gmail_client.get_header(payload, "Date")

                    sender_email = parseaddr(sender_raw)[1].lower() if sender_raw else ""
                    recipient_email = parseaddr(recipient_raw)[1].lower() if recipient_raw else ""
                    recipient_name = parseaddr(recipient_raw)[0] if recipient_raw else ""

                    # Only track outbound emails (sent by Aaron)
                    if sender_email != user_email:
                        logger.debug(f"Skipping {msg_id}: not sent by user ({sender_email})")
                        continue

                    # Parse email date
                    email_date = None
                    internal_date = msg.get("internalDate")
                    if date_str:
                        try:
                            from email.utils import parsedate_to_datetime
                            parsed_dt = parsedate_to_datetime(date_str)
                            if parsed_dt.tzinfo is None:
                                parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                            email_date = parsed_dt.astimezone(timezone.utc).isoformat()
                        except Exception:
                            pass
                    if not email_date and internal_date:
                        email_date = datetime.fromtimestamp(
                            int(internal_date) / 1000, timezone.utc
                        ).isoformat()

                    # Get body text for draft generation context
                    body_content = self.gmail_client.parse_message_body(payload)
                    body_text = body_content.get("text", "")

                    # Try to find contact and get name
                    contact_id = None
                    if recipient_email:
                        contact_id = find_contact_by_email(recipient_email)
                        # If no display name from email header, look up from contacts
                        if not recipient_name and contact_id:
                            try:
                                contact_result = await asyncio.to_thread(
                                    lambda cid=contact_id: supabase.table("contacts")
                                    .select("full_name")
                                    .eq("id", cid)
                                    .limit(1)
                                    .execute()
                                )
                                if contact_result.data:
                                    recipient_name = contact_result.data[0].get("full_name", "")
                            except Exception:
                                pass

                    # Look up email_id from emails table
                    email_id = None
                    try:
                        email_result = await asyncio.to_thread(
                            lambda mid=msg_id: supabase.table("emails")
                            .select("id")
                            .eq("google_message_id", mid)
                            .limit(1)
                            .execute()
                        )
                        if email_result.data:
                            email_id = email_result.data[0]["id"]
                    except Exception:
                        pass

                    now = datetime.now(timezone.utc)
                    follow_up_record = {
                        "email_id": email_id,
                        "google_message_id": msg_id,
                        "thread_id": thread_id,
                        "subject": subject,
                        "recipient_email": recipient_email,
                        "recipient_name": recipient_name or None,
                        "original_body_text": body_text[:5000] if body_text else None,
                        "original_date": email_date,
                        "contact_id": contact_id,
                        "status": "pending",
                        "interval_days": DEFAULT_INTERVAL_DAYS,
                        "next_follow_up_date": (now + timedelta(days=DEFAULT_INTERVAL_DAYS)).isoformat(),
                        "follow_up_count": 0,
                        "max_follow_ups": DEFAULT_MAX_FOLLOW_UPS,
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                    }

                    await asyncio.to_thread(
                        lambda rec=follow_up_record: supabase.table("email_follow_ups")
                        .insert(rec)
                        .execute()
                    )

                    existing_threads.add(thread_id)
                    new_count += 1
                    logger.info(
                        f"Tracking follow-up: '{subject}' to {recipient_email} "
                        f"(next: {DEFAULT_INTERVAL_DAYS} days)"
                    )

                except Exception as e:
                    logger.error(f"Failed to process labeled message {msg_id}: {e}")

        return new_count

    async def check_for_replies(self, label_id: str, user_email: str) -> int:
        """
        Detect replies in tracked threads.
        Uses only Supabase queries (zero Gmail API calls).
        """
        # Get active follow-ups
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table("email_follow_ups")
                .select("id, thread_id, original_date, google_message_id, gmail_draft_id, interval_days, status")
                .in_("status", ["pending", "draft_created"])
                .is_("deleted_at", "null")
                .execute()
            )
        except Exception as e:
            logger.error(f"Failed to query active follow-ups: {e}")
            return 0

        if not result.data:
            return 0

        replies_found = 0

        for follow_up in result.data:
            thread_id = follow_up["thread_id"]
            original_date = follow_up["original_date"]
            follow_up_id = follow_up["id"]

            try:
                # Check for newer messages in this thread from the emails table
                thread_emails = await asyncio.to_thread(
                    lambda tid=thread_id, odate=original_date: supabase.table("emails")
                    .select("sender, date, google_message_id")
                    .eq("thread_id", tid)
                    .gt("date", odate)
                    .order("date", desc=False)
                    .execute()
                )

                if not thread_emails.data:
                    continue

                # Check each newer message
                reply_found = False
                aaron_sent_new = False

                for email in thread_emails.data:
                    sender = email.get("sender", "")
                    subject = email.get("subject", "")
                    body_text = email.get("body_text", "")
                    sender_email_addr = parseaddr(sender)[1].lower() if sender else ""

                    if sender_email_addr == user_email:
                        # Aaron sent a new message - reset the timer
                        aaron_sent_new = True
                    elif _is_auto_reply(sender, subject, body_text):
                        # Skip auto-replies (out-of-office, etc.)
                        logger.info(f"Skipping auto-reply in thread {thread_id}: {subject}")
                        continue
                    else:
                        # Real reply from someone else
                        reply_found = True
                        break

                if reply_found:
                    # Mark as replied (keep Gmail label for long-term tracking)
                    now = datetime.now(timezone.utc)
                    await asyncio.to_thread(
                        lambda fid=follow_up_id: supabase.table("email_follow_ups")
                        .update({
                            "status": "replied",
                            "updated_at": now.isoformat(),
                        })
                        .eq("id", fid)
                        .execute()
                    )

                    # Delete draft if exists (no longer needed)
                    if follow_up.get("gmail_draft_id"):
                        try:
                            await self.gmail_client.delete_draft(follow_up["gmail_draft_id"])
                        except Exception as e:
                            logger.debug(f"Could not delete draft {follow_up['gmail_draft_id']}: {e}")

                    replies_found += 1
                    logger.info(f"Reply detected for follow-up {follow_up_id}, status -> replied (label kept)")

                elif aaron_sent_new and follow_up["status"] == "pending":
                    # Aaron sent a new message - reset the timer
                    now = datetime.now(timezone.utc)
                    interval = follow_up.get("interval_days", DEFAULT_INTERVAL_DAYS)
                    await asyncio.to_thread(
                        lambda fid=follow_up_id, intv=interval: supabase.table("email_follow_ups")
                        .update({
                            "next_follow_up_date": (datetime.now(timezone.utc) + timedelta(days=intv)).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        })
                        .eq("id", fid)
                        .execute()
                    )
                    logger.info(f"Timer reset for follow-up {follow_up_id} (Aaron sent new message)")

            except Exception as e:
                logger.error(f"Failed to check replies for follow-up {follow_up_id}: {e}")

        return replies_found

    async def process_expired_timers(self, user_email: str) -> int:
        """Generate follow-up drafts for expired timers."""
        now = datetime.now(timezone.utc)

        try:
            result = await asyncio.to_thread(
                lambda: supabase.table("email_follow_ups")
                .select("*")
                .eq("status", "pending")
                .lte("next_follow_up_date", now.isoformat())
                .is_("deleted_at", "null")
                .execute()
            )
        except Exception as e:
            logger.error(f"Failed to query expired timers: {e}")
            return 0

        if not result.data:
            return 0

        drafts_created = 0

        for follow_up in result.data:
            # Skip if max follow-ups reached
            if follow_up["follow_up_count"] >= follow_up["max_follow_ups"]:
                await asyncio.to_thread(
                    lambda fid=follow_up["id"]: supabase.table("email_follow_ups")
                    .update({
                        "status": "cancelled",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })
                    .eq("id", fid)
                    .execute()
                )
                logger.info(f"Max follow-ups reached for {follow_up['id']}, cancelling")
                continue

            try:
                # Generate draft text via Intelligence Service
                draft_body = await self._generate_draft(follow_up)
                if not draft_body:
                    logger.error(f"Failed to generate draft for {follow_up['id']}")
                    continue

                # Build full email body with quoted original
                subject = follow_up.get("subject", "")
                original_body = follow_up.get("original_body_text", "")
                original_date = follow_up.get("original_date", "")
                recipient_name = follow_up.get("recipient_name", follow_up["recipient_email"])

                # Format quoted original email below the follow-up
                full_body = draft_body
                if original_body:
                    date_display = original_date[:10] if original_date else "unknown date"
                    quoted_original = "\n".join(f"> {line}" for line in original_body.strip().splitlines())
                    full_body += (
                        f"\n\n"
                        f"On {date_display}, Aaron wrote:\n"
                        f"{quoted_original}"
                    )

                # Create Gmail draft as threaded reply
                draft = await self.gmail_client.create_draft(
                    to=follow_up["recipient_email"],
                    subject=f"Re: {subject}" if subject and not subject.lower().startswith("re:") else subject,
                    body=full_body,
                    reply_to_message_id=follow_up["google_message_id"]
                )

                gmail_draft_id = draft.get("id")
                interval = follow_up.get("interval_days", DEFAULT_INTERVAL_DAYS)

                # Update record
                await asyncio.to_thread(
                    lambda fid=follow_up["id"], db=draft_body, gdi=gmail_draft_id, intv=interval: (
                        supabase.table("email_follow_ups")
                        .update({
                            "status": "draft_created",
                            "draft_body": db,
                            "gmail_draft_id": gdi,
                            "follow_up_count": follow_up["follow_up_count"] + 1,
                            "next_follow_up_date": (datetime.now(timezone.utc) + timedelta(days=intv)).isoformat(),
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        })
                        .eq("id", fid)
                        .execute()
                    )
                )

                drafts_created += 1
                logger.info(
                    f"Created follow-up draft #{follow_up['follow_up_count'] + 1} "
                    f"for '{subject}' to {follow_up['recipient_email']}"
                )

            except Exception as e:
                logger.error(f"Failed to create draft for follow-up {follow_up['id']}: {e}")

        return drafts_created

    async def process_send_now(self) -> int:
        """Send drafts triggered from Notion (status='send_now')."""
        try:
            result = await asyncio.to_thread(
                lambda: supabase.table("email_follow_ups")
                .select("*")
                .eq("status", "send_now")
                .is_("deleted_at", "null")
                .execute()
            )
        except Exception as e:
            logger.error(f"Failed to query send_now follow-ups: {e}")
            return 0

        if not result.data:
            return 0

        sent_count = 0

        for follow_up in result.data:
            gmail_draft_id = follow_up.get("gmail_draft_id")
            if not gmail_draft_id:
                logger.warning(f"No draft to send for follow-up {follow_up['id']}")
                # Reset to pending so a new draft can be generated
                await asyncio.to_thread(
                    lambda fid=follow_up["id"]: supabase.table("email_follow_ups")
                    .update({
                        "status": "pending",
                        "next_follow_up_date": datetime.now(timezone.utc).isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })
                    .eq("id", fid)
                    .execute()
                )
                continue

            try:
                # Send the draft
                await self.gmail_client.send_draft(gmail_draft_id)

                now = datetime.now(timezone.utc)
                interval = follow_up.get("interval_days", DEFAULT_INTERVAL_DAYS)

                # Determine next state
                if follow_up["follow_up_count"] >= follow_up["max_follow_ups"]:
                    # Final follow-up sent
                    new_status = "sent"
                    next_date = now.isoformat()
                else:
                    # Reset timer for next follow-up cycle
                    new_status = "pending"
                    next_date = (now + timedelta(days=interval)).isoformat()

                await asyncio.to_thread(
                    lambda fid=follow_up["id"], ns=new_status, nd=next_date: (
                        supabase.table("email_follow_ups")
                        .update({
                            "status": ns,
                            "gmail_draft_id": None,
                            "draft_body": None,
                            "last_sent_at": datetime.now(timezone.utc).isoformat(),
                            "next_follow_up_date": nd,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        })
                        .eq("id", fid)
                        .execute()
                    )
                )

                sent_count += 1
                logger.info(
                    f"Sent follow-up for '{follow_up.get('subject')}' "
                    f"to {follow_up['recipient_email']} (next status: {new_status})"
                )

            except Exception as e:
                logger.error(f"Failed to send draft for follow-up {follow_up['id']}: {e}")
                # Reset to draft_created so user can retry
                await asyncio.to_thread(
                    lambda fid=follow_up["id"]: supabase.table("email_follow_ups")
                    .update({
                        "status": "draft_created",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })
                    .eq("id", fid)
                    .execute()
                )

        return sent_count

    async def _generate_draft(self, follow_up: Dict[str, Any]) -> Optional[str]:
        """Generate a follow-up draft using the Intelligence Service."""
        subject = follow_up.get("subject", "")
        original_body = follow_up.get("original_body_text", "")
        recipient_name = follow_up.get("recipient_name") or follow_up.get("recipient_email", "")
        follow_up_count = follow_up.get("follow_up_count", 0)

        # Calculate days since original email
        days_since = 0
        if follow_up.get("original_date"):
            try:
                original_dt = datetime.fromisoformat(follow_up["original_date"])
                days_since = (datetime.now(timezone.utc) - original_dt).days
            except Exception:
                days_since = follow_up.get("interval_days", DEFAULT_INTERVAL_DAYS)

        payload = {
            "subject": subject,
            "original_body": (original_body or "")[:2000],
            "recipient_name": recipient_name,
            "days_since": days_since,
            "follow_up_count": follow_up_count,
        }

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{INTELLIGENCE_SERVICE_URL}/api/v1/follow-up/generate-draft",
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
                return data.get("draft_body", "")
        except Exception as e:
            logger.error(f"Intelligence Service draft generation failed: {e}")
            return None


async def run_follow_up_sync():
    """Entry point for sync orchestration."""
    syncer = FollowUpSync()
    return await syncer.sync_follow_ups()


if __name__ == "__main__":
    asyncio.run(run_follow_up_sync())
