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

    async def sync_emails(self, days_history: int = 7, max_results: int = 100):
        """
        Syncs emails from Gmail to Supabase.
        """
        try:
            logger.info(f"Starting Gmail sync (last {days_history} days)")
            
            # Calculate date query
            date_query = (datetime.now() - timedelta(days=days_history)).strftime("%Y/%m/%d")
            query = f"after:{date_query}"
            
            # List messages
            messages_meta = await self.gmail_client.list_messages(
                query=query,
                max_results=max_results
            )
            
            logger.info(f"Found {len(messages_meta)} messages to sync")
            
            upsert_data = []
            for msg_meta in messages_meta:
                try:
                    # Get full message details
                    msg = await self.gmail_client.get_message(msg_meta['id'])
                    payload = msg.get('payload', {})
                    headers = payload.get('headers', [])
                    
                    # Parse body
                    body_content = self.gmail_client.parse_message_body(payload)
                    
                    # Parse headers
                    subject = self.gmail_client.get_header(payload, 'Subject')
                    sender = self.gmail_client.get_header(payload, 'From')
                    recipient = self.gmail_client.get_header(payload, 'To')
                    date_str = self.gmail_client.get_header(payload, 'Date')
                    
                    # Parse date (this can be tricky with email formats, but let's try basic parsing)
                    # If parsing fails, we might leave it null or use internalDate
                    email_date = None
                    if date_str:
                        try:
                            # Basic attempt, might need python-dateutil for robust parsing
                            from email.utils import parsedate_to_datetime
                            email_date = parsedate_to_datetime(date_str).isoformat()
                        except:
                            # Fallback to internalDate (ms timestamp)
                            internal_date = msg.get('internalDate')
                            if internal_date:
                                email_date = datetime.fromtimestamp(int(internal_date)/1000, timezone.utc).isoformat()

                    record = {
                        "google_message_id": msg['id'],
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
                    logger.error(f"Failed to process message {msg_meta['id']}: {e}")
                    continue

            if upsert_data:
                # Upsert in batches
                batch_size = 50 # Smaller batch size for emails as they are larger
                for i in range(0, len(upsert_data), batch_size):
                    batch = upsert_data[i:i+batch_size]
                    response = supabase.table("emails").upsert(
                        batch, on_conflict="google_message_id"
                    ).execute()
                    logger.info(f"Upserted batch {i//batch_size + 1}: {len(batch)} emails")

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
