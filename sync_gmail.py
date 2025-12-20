import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
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
            messages_to_fetch = [] # List of (id, format)
            new_history_id = None
            
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
                            response = supabase.table("emails").select("google_message_id").in_("google_message_id", chunk).execute()
                            for row in response.data:
                                existing_ids.add(row['google_message_id'])
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
            upsert_data = []
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                for msg_id, fetch_format in messages_to_fetch:
                    try:
                        if fetch_format == 'minimal':
                            # Optimization: If exists, fetch MINIMAL format just to update labels/thread_id
                            msg = await self.gmail_client.get_message(msg_id, format='minimal', client=client)
                            
                            record = {
                                "google_message_id": msg_id,
                                "thread_id": msg.get('threadId'),
                                "label_ids": msg.get('labelIds', []),
                                "snippet": msg.get('snippet', ''),
                                "last_sync_at": datetime.now(timezone.utc).isoformat()
                            }
                            upsert_data.append(record)
                        else:
                            # New email: Fetch FULL content
                            msg = await self.gmail_client.get_message(msg_id, format='full', client=client)
                            payload = msg.get('payload', {})
                            
                            # Parse body
                            body_content = self.gmail_client.parse_message_body(payload)
                            
                            # Parse headers
                            subject = self.gmail_client.get_header(payload, 'Subject')
                            sender = self.gmail_client.get_header(payload, 'From')
                            recipient = self.gmail_client.get_header(payload, 'To')
                            date_str = self.gmail_client.get_header(payload, 'Date')
                            
                            # Parse date
                            email_date = None
                            if date_str:
                                try:
                                    from email.utils import parsedate_to_datetime
                                    email_date = parsedate_to_datetime(date_str).isoformat()
                                except:
                                    internal_date = msg.get('internalDate')
                                if internal_date:
                                    email_date = datetime.fromtimestamp(int(internal_date)/1000, timezone.utc).isoformat()

                        record = {
                            "google_message_id": msg_id,
                            "thread_id": msg.get('threadId'),
                            "label_ids": msg.get('labelIds', []),
                            "snippet": msg.get('snippet', ''),
                            "sender": sender,
                            "recipient": recipient,
                            "subject": subject,
                            "date": email_date,
                            "body_text": body_content['text'],
                            "body_html": body_content['html'],
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                            "last_sync_at": datetime.now(timezone.utc).isoformat(),
                            # Auto-link to contact if found
                            "contact_id": find_contact_by_email(sender) or find_contact_by_email(recipient)
                        }
                        upsert_data.append(record)
                    
                except Exception as e:
                    logger.error(f"Failed to process message {msg_id}: {e}")
                    continue

            if upsert_data:
                # Upsert in batches
                batch_size = 50 
                for i in range(0, len(upsert_data), batch_size):
                    batch = upsert_data[i:i+batch_size]
                    # For existing emails, we might be missing required fields if we did a pure insert
                    # But upsert updates existing rows. 
                    # Wait, if we only provide partial data for existing rows, will it nullify others?
                    # Supabase/Postgres UPSERT (INSERT ... ON CONFLICT DO UPDATE) usually updates only provided columns 
                    # IF we construct the query right. The supabase-py client's `upsert` replaces the row by default?
                    # Actually, supabase-py `upsert` usually does a full replace if not specified otherwise.
                    # We need to be careful.
                    # If we want partial update, we should use `update` for existing and `insert` for new.
                    # Or ensure we don't overwrite with nulls.
                    # The `upsert` method in supabase-js has `ignoreDuplicates` option, but not "merge".
                    # Standard Postgres `ON CONFLICT DO UPDATE SET ...` merges.
                    # The Supabase client maps to `INSERT ... ON CONFLICT ...`.
                    # If we send a dict with missing keys, and the row exists, those keys might be set to NULL or default?
                    # No, usually it only updates the columns present in the payload.
                    # Let's verify this assumption or split into insert/update to be safe.
                    
                    # Safer approach: Split into `to_insert` and `to_update`.
                    pass

                # Let's split them
                to_insert = [d for d in upsert_data if "body_text" in d] # New emails have body
                to_update = [d for d in upsert_data if "body_text" not in d] # Existing emails don't
                
                if to_insert:
                    for i in range(0, len(to_insert), batch_size):
                        batch = to_insert[i:i+batch_size]
                        supabase.table("emails").upsert(batch, on_conflict="google_message_id").execute()
                        logger.info(f"Inserted {len(batch)} new emails")
                        
                if to_update:
                    # Parallelize updates to speed up processing
                    # We use a semaphore to limit concurrency to avoid overwhelming Supabase/Network
                    sem = asyncio.Semaphore(10)
                    
                    async def update_item(item):
                        async with sem:
                            # We wrap the blocking Supabase call in a thread
                            await asyncio.to_thread(
                                lambda: supabase.table("emails").update(item).eq("google_message_id", item["google_message_id"]).execute()
                            )

                    tasks = [update_item(item) for item in to_update]
                    await asyncio.gather(*tasks)
                    logger.info(f"Updated {len(to_update)} existing emails")

            await log_sync_event("gmail_sync", "success", f"Synced {len(upsert_data)} emails")
            return {"status": "success", "count": len(upsert_data)}

        except Exception as e:
            logger.error(f"Gmail sync failed: {str(e)}")
            await log_sync_event("gmail_sync", "error", str(e))
            raise e

async def run_gmail_sync():
    syncer = GmailSync()
    return await syncer.sync_emails()

if __name__ == "__main__":
    asyncio.run(run_gmail_sync())
