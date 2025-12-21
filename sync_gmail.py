import asyncio
import logging
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime, parseaddr
from typing import Any, Dict, List, Optional

import httpx

from lib.google_gmail import GmailClient
from lib.supabase_client import supabase, find_contact_by_email
from lib.logging_service import log_sync_event

logger = logging.getLogger("GmailSync")
logging.basicConfig(level=logging.INFO)

class GmailSync:
    def __init__(self):
        self.gmail_client = GmailClient()

    async def get_history_id(self) -> Optional[str]:
        try:
            response = supabase.table("sync_state").select("value").eq("key", "gmail_history_id").execute()
            if response.data:
                return response.data[0]["value"]
            return None
        except Exception:
            return None

    async def save_history_id(self, history_id: str):
        try:
            supabase.table("sync_state").upsert({
                "key": "gmail_history_id",
                "value": history_id,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }).execute()
        except Exception as e:
            logger.warning(f"Failed to save history ID: {e}")

    async def sync_emails(self, days_history: int = 3, max_results: int = 100):
        """
        Syncs emails from Gmail to Supabase.
        Uses historyId for incremental syncs if available.
        """
        try:
            logger.info(f"Starting Gmail sync...")
            
            last_history_id = await self.get_history_id()
            full_sync = False
            messages_to_fetch = []  # List of (id, format)
            new_history_id = None
            existing_email_cache: Dict[str, Dict[str, Any]] = {}

            async def get_existing_email_record(message_id: str) -> Optional[Dict[str, Any]]:
                if message_id in existing_email_cache:
                    cached = existing_email_cache[message_id]
                    return cached or None

                def fetch() -> Dict[str, Any]:
                    result = supabase.table("emails").select(
                        "google_message_id, thread_id, label_ids, snippet"
                    ).eq("google_message_id", message_id).limit(1).execute()
                    return result.data[0] if result.data else None

                record = await asyncio.to_thread(fetch)
                existing_email_cache[message_id] = record or {}
                return record
            
            if last_history_id:
                logger.info(f"Found history ID {last_history_id}, attempting incremental sync")
                try:
                    history_data = await self.gmail_client.list_history(start_history_id=last_history_id)
                    
                    if history_data.get("expired"):
                        logger.info("History ID expired, falling back to full sync")
                        full_sync = True
                    else:
                        new_history_id = history_data.get("historyId")
                        history_records = history_data.get("history", [])
                        
                        if not history_records:
                            logger.info("No changes found in history")
                            # Still update history ID to latest
                            if new_history_id:
                                await self.save_history_id(new_history_id)
                            return {"status": "success", "count": 0}
                            
                        logger.info(f"Found {len(history_records)} history records")
                        
                        # Process history to find changed messages
                        seen_ids = set()
                        
                        for record in history_records:
                            # Messages added
                            for msg in record.get("messagesAdded", []):
                                mid = msg["message"]["id"]
                                if mid not in seen_ids:
                                    messages_to_fetch.append((mid, 'full'))
                                    seen_ids.add(mid)
                                    
                            # Labels added/removed (fetch minimal to update labels)
                            for msg in record.get("labelsAdded", []):
                                mid = msg["message"]["id"]
                                if mid not in seen_ids:
                                    messages_to_fetch.append((mid, 'minimal'))
                                    seen_ids.add(mid)
                                    
                            for msg in record.get("labelsRemoved", []):
                                mid = msg["message"]["id"]
                                if mid not in seen_ids:
                                    messages_to_fetch.append((mid, 'minimal'))
                                    seen_ids.add(mid)
                                    
                except Exception as e:
                    logger.warning(f"Incremental sync failed ({e}), falling back to full sync")
                    full_sync = True
            else:
                full_sync = True
                
            if full_sync:
                logger.info(f"Performing full sync (last {days_history} days)")
                
                # Get current history ID first to start tracking from now
                try:
                    profile = await self.gmail_client.get_profile()
                    new_history_id = profile.get("historyId")
                except Exception as e:
                    logger.warning(f"Failed to get profile/historyId: {e}")
                
                # Calculate date query
                date_query = (datetime.now() - timedelta(days=days_history)).strftime("%Y/%m/%d")
                query = f"after:{date_query}"
                
                # Use a single client for all requests to avoid connection issues
                async with httpx.AsyncClient(timeout=60.0) as client:
                    messages_meta = await self.gmail_client.list_messages(
                        query=query,
                        max_results=max_results,
                        include_spam_trash=True,
                        client=client
                    )
                    
                    if not messages_meta:
                        logger.info("No messages found")
                        if new_history_id:
                            await self.save_history_id(new_history_id)
                        return {"status": "success", "count": 0}

                    logger.info(f"Found {len(messages_meta)} messages in Gmail")
                    
                    # Check which ones exist in Supabase
                    all_ids = [m['id'] for m in messages_meta]
                    
                    # Supabase 'in' query might fail if list is too long, chunk it
                    existing_ids = set()
                    chunk_size = 50  # Reduced chunk size to avoid timeouts
                    for i in range(0, len(all_ids), chunk_size):
                        chunk = all_ids[i:i+chunk_size]
                        try:
                            response = supabase.table("emails").select(
                                "google_message_id, thread_id, label_ids, snippet"
                            ).in_("google_message_id", chunk).execute()
                            for row in response.data:
                                message_id = row.get('google_message_id')
                                if message_id:
                                    existing_ids.add(message_id)
                                    existing_email_cache[message_id] = row
                        except Exception as e:
                            logger.error(f"Error checking existing emails chunk {i}: {e}")
                            continue
                    
                    logger.info(f"Found {len(existing_ids)} existing emails in DB")
                    
                    for msg_meta in messages_meta:
                        msg_id = msg_meta['id']
                        if msg_id in existing_ids:
                            messages_to_fetch.append((msg_id, 'minimal'))
                        else:
                            messages_to_fetch.append((msg_id, 'full'))

            # Process messages
            if not messages_to_fetch:
                logger.info("No messages to process")
                if new_history_id:
                    await self.save_history_id(new_history_id)
                return {"status": "success", "count": 0}
                
            logger.info(f"Processing {len(messages_to_fetch)} messages")

            new_email_records: List[Dict[str, Any]] = []
            update_records: List[Dict[str, Any]] = []

            async with httpx.AsyncClient(timeout=60.0) as client:
                for msg_id, fetch_format in messages_to_fetch:
                    try:
                        if fetch_format == "minimal":
                            msg = await self.gmail_client.get_message(msg_id, format="minimal", client=client)
                            update_candidate = {
                                "google_message_id": msg_id,
                                "thread_id": msg.get("threadId"),
                                "label_ids": msg.get("labelIds", []),
                                "snippet": msg.get("snippet", ""),
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                                "last_sync_at": datetime.now(timezone.utc).isoformat(),
                            }

                            existing_record = await get_existing_email_record(msg_id)
                            if existing_record:
                                existing_labels = existing_record.get("label_ids") or []
                                candidate_labels = update_candidate["label_ids"] or []
                                if (
                                    (existing_record.get("thread_id") or "") == (update_candidate["thread_id"] or "")
                                    and sorted(existing_labels) == sorted(candidate_labels)
                                    and (existing_record.get("snippet") or "") == (update_candidate["snippet"] or "")
                                ):
                                    logger.debug(f"Skipping update for {msg_id}; no changes detected")
                                    continue

                            update_records.append(update_candidate)
                            existing_email_cache[msg_id] = {
                                "google_message_id": msg_id,
                                "thread_id": update_candidate["thread_id"],
                                "label_ids": update_candidate["label_ids"],
                                "snippet": update_candidate["snippet"],
                            }
                            continue

                        # New email path - fetch full payload
                        msg = await self.gmail_client.get_message(msg_id, format="full", client=client)
                        payload = msg.get("payload", {})

                        body_content = self.gmail_client.parse_message_body(payload) or {}
                        subject = self.gmail_client.get_header(payload, "Subject")
                        sender_raw = self.gmail_client.get_header(payload, "From")
                        recipient_raw = self.gmail_client.get_header(payload, "To")
                        date_str = self.gmail_client.get_header(payload, "Date")

                        sender_email = parseaddr(sender_raw)[1] if sender_raw else None
                        recipient_email = parseaddr(recipient_raw)[1] if recipient_raw else None

                        contact_id = None
                        if sender_email:
                            contact_id = find_contact_by_email(sender_email)
                        if not contact_id and recipient_email:
                            contact_id = find_contact_by_email(recipient_email)

                        email_date: Optional[str] = None
                        internal_date = msg.get("internalDate")
                        if date_str:
                            try:
                                parsed_dt = parsedate_to_datetime(date_str)
                                if parsed_dt.tzinfo is None:
                                    parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                                email_date = parsed_dt.astimezone(timezone.utc).isoformat()
                            except Exception:
                                email_date = None
                        if not email_date and internal_date:
                            email_date = datetime.fromtimestamp(int(internal_date) / 1000, timezone.utc).isoformat()

                        new_email_records.append(
                            {
                                "google_message_id": msg_id,
                                "thread_id": msg.get("threadId"),
                                "label_ids": msg.get("labelIds", []),
                                "snippet": msg.get("snippet", ""),
                                "sender": sender_raw,
                                "recipient": recipient_raw,
                                "subject": subject,
                                "date": email_date,
                                "body_text": body_content.get("text"),
                                "body_html": body_content.get("html"),
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                                "last_sync_at": datetime.now(timezone.utc).isoformat(),
                                "contact_id": contact_id,
                            }
                        )

                    except Exception as exc:
                        logger.error(f"Failed to process message {msg_id}: {exc}")

            batch_size = 50

            if new_email_records:
                for i in range(0, len(new_email_records), batch_size):
                    batch = new_email_records[i : i + batch_size]
                    supabase.table("emails").upsert(batch, on_conflict="google_message_id").execute()
                    logger.info(f"Inserted {len(batch)} new emails")

            if update_records:
                sem = asyncio.Semaphore(10)

                async def update_item(item: Dict[str, Any]) -> None:
                    async with sem:
                        await asyncio.to_thread(
                            lambda: supabase.table("emails").update(item).eq("google_message_id", item["google_message_id"]).execute()
                        )

                await asyncio.gather(*(update_item(record) for record in update_records))
                logger.info(f"Updated {len(update_records)} existing emails")

            total_processed = len(new_email_records) + len(update_records)
            if new_history_id:
                await self.save_history_id(new_history_id)
            await log_sync_event("gmail_sync", "success", f"Synced {total_processed} emails")
            return {"status": "success", "count": total_processed}

        except Exception as e:
            logger.error(f"Gmail sync failed: {str(e)}")
            await log_sync_event("gmail_sync", "error", str(e))
            raise e

async def run_gmail_sync():
    syncer = GmailSync()
    return await syncer.sync_emails()

if __name__ == "__main__":
    asyncio.run(run_gmail_sync())
