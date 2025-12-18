import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from lib.google_gmail import GmailClient
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event

logger = logging.getLogger("GmailSync")
logging.basicConfig(level=logging.INFO)

class GmailSync:
    def __init__(self):
        self.gmail_client = GmailClient()

    async def sync_emails(self, days_history: int = 3, max_results: int = 100):
        """
        Syncs emails from Gmail to Supabase.
        Optimized to only fetch full content for new emails.
        """
        try:
            logger.info(f"Starting Gmail sync (last {days_history} days)")
            
            # Calculate date query
            date_query = (datetime.now() - timedelta(days=days_history)).strftime("%Y/%m/%d")
            query = f"after:{date_query}"
            
            # List messages (IDs only)
            messages_meta = await self.gmail_client.list_messages(
                query=query,
                max_results=max_results
            )
            
            if not messages_meta:
                logger.info("No messages found")
                return {"status": "success", "count": 0}

            logger.info(f"Found {len(messages_meta)} messages in Gmail")
            
            # Check which ones exist in Supabase
            all_ids = [m['id'] for m in messages_meta]
            
            # Supabase 'in' query might fail if list is too long, chunk it
            existing_ids = set()
            chunk_size = 100
            for i in range(0, len(all_ids), chunk_size):
                chunk = all_ids[i:i+chunk_size]
                response = supabase.table("emails").select("google_message_id").in_("google_message_id", chunk).execute()
                for row in response.data:
                    existing_ids.add(row['google_message_id'])
            
            logger.info(f"Found {len(existing_ids)} existing emails in DB")
            
            upsert_data = []
            for msg_meta in messages_meta:
                msg_id = msg_meta['id']
                try:
                    if msg_id in existing_ids:
                        # Optimization: If exists, fetch MINIMAL format just to update labels/thread_id
                        # This saves bandwidth and processing time
                        msg = await self.gmail_client.get_message(msg_id, format='minimal')
                        
                        # We only update labels and thread_id for existing emails
                        # We assume body/subject/sender don't change
                        record = {
                            "google_message_id": msg_id,
                            "thread_id": msg.get('threadId'),
                            "label_ids": msg.get('labelIds', []),
                            "snippet": msg.get('snippet', ''),
                            "last_sync_at": datetime.now(timezone.utc).isoformat()
                            # Don't update body/headers
                        }
                        upsert_data.append(record)
                    else:
                        # New email: Fetch FULL content
                        msg = await self.gmail_client.get_message(msg_id, format='full')
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
                            "last_sync_at": datetime.now(timezone.utc).isoformat()
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
                    for i in range(0, len(to_update), batch_size):
                        batch = to_update[i:i+batch_size]
                        # For updates, we can't use upsert easily with partial data if we are unsure of behavior.
                        # But actually, postgrest `upsert` DOES merge if you provide the ID.
                        # However, to be 100% safe and efficient:
                        # We can just upsert them. If I send {"id": 1, "label_ids": [...]}, it should update only label_ids?
                        # No, PostgREST (Supabase) upsert replaces the row unless you use `ignoreDuplicates`.
                        # It does NOT do a partial patch on upsert.
                        # So we must use `.update()` for existing rows.
                        # But `.update()` doesn't support bulk update with different values easily in one request 
                        # (unless we use a custom RPC or complex query).
                        # We have to loop updates? That's slow.
                        # OR: We fetch the existing row, merge in memory, and upsert full row.
                        # That defeats the bandwidth saving purpose (we didn't fetch full body).
                        
                        # Compromise: For now, let's just loop updates for existing emails. 
                        # It's 15 mins sync, likely few emails change labels.
                        for item in batch:
                            supabase.table("emails").update(item).eq("google_message_id", item["google_message_id"]).execute()
                        logger.info(f"Updated {len(batch)} existing emails")

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
