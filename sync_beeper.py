"""
Beeper Sync Service
===================
Syncs messages from Beeper Bridge to Supabase with smart incremental sync.

Key features:
- Looks back to last_synced_at per chat (handles offline periods)
- Deduplicates by beeper_event_id (never re-syncs same message)
- Links chats to contacts by phone/LinkedIn/name matching
- Modular design for reuse by other services
"""

import os
import logging
import httpx
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple
from supabase import Client

logger = logging.getLogger(__name__)

# Configuration
BEEPER_BRIDGE_URL = os.getenv("BEEPER_BRIDGE_URL", "http://localhost:8377")
SYNC_LOOKBACK_DAYS = int(os.getenv("BEEPER_SYNC_LOOKBACK_DAYS", "30"))  # Default: 30 days on first sync

# Platforms to completely ignore during sync
IGNORED_PLATFORMS = ["slack", "hungryserv", "matrix"]  # Groups with high noise, no personal DM value


class BeeperSyncService:
    """
    Smart sync service for Beeper messages.
    
    Sync Strategy:
    1. On first sync: Look back SYNC_LOOKBACK_DAYS
    2. On subsequent syncs: Look back to last_synced_at per chat
    3. Deduplicate by beeper_event_id (unique constraint in DB)
    4. Link chats to contacts where possible
    """
    
    def __init__(self, supabase_client: Client):
        self.db = supabase_client
        self.http_client: Optional[httpx.AsyncClient] = None
        self.stats = {
            "chats_synced": 0,
            "chats_created": 0,
            "chats_updated": 0,
            "messages_synced": 0,
            "messages_new": 0,
            "messages_skipped": 0,
            "contacts_linked": 0,
            "errors": []
        }
    
    async def __aenter__(self):
        self.http_client = httpx.AsyncClient(
            base_url=BEEPER_BRIDGE_URL,
            timeout=120.0
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.http_client:
            await self.http_client.aclose()
    
    # =========================================
    # Main Sync Methods
    # =========================================
    
    async def sync_all(self, full_sync: bool = False) -> Dict[str, Any]:
        """
        Sync all chats and messages.
        
        Args:
            full_sync: If True, ignore last_synced and fetch all (up to LOOKBACK_DAYS)
        
        Returns:
            Dict with sync statistics
        """
        self.stats = {
            "chats_synced": 0,
            "chats_created": 0,
            "chats_updated": 0,
            "messages_synced": 0,
            "messages_new": 0,
            "messages_skipped": 0,
            "contacts_linked": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        
        try:
            # 1. Sync all chats (get metadata, create/update records)
            logger.info("Starting Beeper sync - fetching chats...")
            chats = await self._fetch_all_chats()
            logger.info(f"Found {len(chats)} chats to sync")
            
            for chat in chats:
                try:
                    await self._sync_chat(chat, full_sync=full_sync)
                except Exception as e:
                    logger.error(f"Error syncing chat {chat.get('beeper_chat_id')}: {e}")
                    self.stats["errors"].append({
                        "chat_id": chat.get('beeper_chat_id'),
                        "error": str(e)
                    })
            
            self.stats["finished_at"] = datetime.now(timezone.utc).isoformat()
            
            # Log summary
            logger.info(
                f"Beeper sync complete: "
                f"{self.stats['chats_synced']} chats, "
                f"{self.stats['messages_new']} new messages, "
                f"{self.stats['contacts_linked']} contacts linked"
            )
            
            return self.stats
            
        except Exception as e:
            logger.error(f"Beeper sync failed: {e}")
            self.stats["errors"].append({"fatal": str(e)})
            self.stats["finished_at"] = datetime.now(timezone.utc).isoformat()
            raise
    
    async def sync_chat(self, beeper_chat_id: str, full_sync: bool = False) -> Dict[str, Any]:
        """Sync a single chat by ID."""
        # Fetch chat from bridge
        response = await self.http_client.get(f"/chats/{beeper_chat_id}")
        if response.status_code != 200:
            raise Exception(f"Failed to fetch chat: {response.text}")
        
        chat = response.json()
        await self._sync_chat(chat, full_sync=full_sync)
        return self.stats
    
    # =========================================
    # Internal Sync Methods
    # =========================================
    
    async def _fetch_all_chats(self, limit: int = 500) -> List[Dict]:
        """Fetch all chats from bridge, filtering out ignored platforms."""
        response = await self.http_client.get("/chats", params={"limit": limit})
        if response.status_code != 200:
            raise Exception(f"Failed to fetch chats: {response.text}")
        
        data = response.json()
        all_chats = data.get("chats", [])
        
        # Filter out ignored platforms (Slack, Matrix groups)
        filtered_chats = [
            chat for chat in all_chats
            if chat.get("platform") not in IGNORED_PLATFORMS
        ]
        
        ignored_count = len(all_chats) - len(filtered_chats)
        if ignored_count > 0:
            logger.info(f"Filtered out {ignored_count} chats from ignored platforms: {IGNORED_PLATFORMS}")
        
        return filtered_chats
    
    async def _sync_chat(self, chat_data: Dict, full_sync: bool = False):
        """Sync a single chat and its messages."""
        beeper_chat_id = chat_data.get("beeper_chat_id")
        if not beeper_chat_id:
            logger.warning(f"Chat missing beeper_chat_id: {chat_data}")
            return
        
        # 1. Upsert chat record
        db_chat = await self._upsert_chat(chat_data)
        self.stats["chats_synced"] += 1
        
        # 2. Determine sync window
        if full_sync or not db_chat.get("last_synced_at"):
            # First sync or full sync: look back N days
            since = datetime.now(timezone.utc) - timedelta(days=SYNC_LOOKBACK_DAYS)
        else:
            # Incremental: from last sync
            since = datetime.fromisoformat(db_chat["last_synced_at"].replace("Z", "+00:00"))
        
        # 3. Fetch and sync messages
        messages = await self._fetch_messages(beeper_chat_id, since=since)
        
        for msg in messages:
            try:
                was_new = await self._upsert_message(msg, chat_data)
                self.stats["messages_synced"] += 1
                if was_new:
                    self.stats["messages_new"] += 1
                else:
                    self.stats["messages_skipped"] += 1
            except Exception as e:
                if "duplicate key" in str(e).lower():
                    self.stats["messages_skipped"] += 1
                else:
                    logger.error(f"Error syncing message: {e}")
        
        # 4. Update last_synced_at
        await self._update_chat_sync_time(beeper_chat_id)
    
    async def _upsert_chat(self, chat_data: Dict) -> Dict:
        """Create or update a chat record. Returns the DB record."""
        beeper_chat_id = chat_data["beeper_chat_id"]
        
        # Check if exists
        existing = self.db.table("beeper_chats").select("*").eq(
            "beeper_chat_id", beeper_chat_id
        ).execute()
        
        # Prepare record
        record = {
            "beeper_chat_id": beeper_chat_id,
            "account_id": chat_data.get("account_id"),
            "platform": chat_data.get("platform"),
            "chat_type": chat_data.get("chat_type", "dm"),
            "chat_name": chat_data.get("name"),
            "participant_count": chat_data.get("participant_count", 1),
            "remote_user_id": chat_data.get("remote_user_id"),
            "remote_user_name": chat_data.get("remote_user_name"),
            "remote_phone": chat_data.get("remote_phone"),
            "remote_linkedin_id": chat_data.get("remote_linkedin_id"),
            "remote_telegram_username": chat_data.get("remote_telegram_username"),
            "last_message_at": chat_data.get("last_message_at"),
            "last_message_preview": chat_data.get("last_message_preview"),
            "last_message_is_outgoing": chat_data.get("last_message_is_outgoing"),
            "last_message_type": chat_data.get("last_message_type", "text"),
            "is_archived": chat_data.get("is_archived", False),
            "is_muted": chat_data.get("is_muted", False),
            "unread_count": chat_data.get("unread_count", 0),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        
        # Calculate needs_response for inbox-zero workflow:
        # TRUE if: DM + not archived + last message was incoming (not from me)
        is_dm = chat_data.get("chat_type", "dm") == "dm"
        is_archived = chat_data.get("is_archived", False)
        last_was_incoming = not chat_data.get("last_message_is_outgoing", True)
        record["needs_response"] = is_dm and not is_archived and last_was_incoming
        
        if existing.data:
            # Update
            result = self.db.table("beeper_chats").update(record).eq(
                "beeper_chat_id", beeper_chat_id
            ).execute()
            self.stats["chats_updated"] += 1
            db_chat = existing.data[0]
        else:
            # Create
            record["first_synced_at"] = datetime.now(timezone.utc).isoformat()
            result = self.db.table("beeper_chats").insert(record).execute()
            self.stats["chats_created"] += 1
            db_chat = result.data[0] if result.data else {}
            
            # Try to link to contact
            await self._link_chat_to_contact(beeper_chat_id, chat_data)
        
        return db_chat
    
    async def _fetch_messages(
        self, 
        beeper_chat_id: str, 
        since: Optional[datetime] = None,
        limit: int = 500
    ) -> List[Dict]:
        """Fetch messages from bridge, optionally since a timestamp."""
        params = {"limit": limit}
        if since:
            params["since"] = since.isoformat()
        
        # URL encode the chat ID
        import urllib.parse
        encoded_id = urllib.parse.quote(beeper_chat_id, safe='')
        
        response = await self.http_client.get(
            f"/sync/messages/{encoded_id}",
            params=params
        )
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch messages for {beeper_chat_id}: {response.text}")
            return []
        
        data = response.json()
        return data.get("messages", [])
    
    async def _upsert_message(self, msg_data: Dict, chat_data: Dict) -> bool:
        """
        Insert a message if it doesn't exist.
        Returns True if new, False if already existed.
        """
        beeper_event_id = msg_data.get("beeper_event_id")
        if not beeper_event_id:
            logger.warning(f"Message missing beeper_event_id")
            return False
        
        # Check if exists (fast path to avoid insert errors)
        existing = self.db.table("beeper_messages").select("id").eq(
            "beeper_event_id", beeper_event_id
        ).execute()
        
        if existing.data:
            return False  # Already synced
        
        # Get contact_id from chat if linked
        chat_record = self.db.table("beeper_chats").select("contact_id").eq(
            "beeper_chat_id", msg_data.get("beeper_chat_id")
        ).execute()
        contact_id = chat_record.data[0].get("contact_id") if chat_record.data else None
        
        # Generate content_description for media messages
        message_type = msg_data.get("message_type", "text")
        content_description = self._generate_content_description(msg_data)
        
        # Prepare record
        record = {
            "beeper_event_id": beeper_event_id,
            "beeper_chat_id": msg_data.get("beeper_chat_id"),
            "platform": chat_data.get("platform"),
            "sender_id": msg_data.get("sender_id"),
            "sender_name": msg_data.get("sender_name"),
            "is_outgoing": msg_data.get("is_outgoing", False),
            "content": msg_data.get("content"),
            "content_description": content_description,
            "message_type": message_type,
            "timestamp": msg_data.get("timestamp"),
            "is_read": msg_data.get("is_read", False),
            "contact_id": contact_id,
            "has_media": msg_data.get("has_media", False),
            "media_url": msg_data.get("media_url"),
            "media_mime_type": msg_data.get("media_mime_type"),
            "media_filename": msg_data.get("media_filename"),
            "reply_to_event_id": msg_data.get("reply_to_event_id"),
            "reactions": msg_data.get("reactions"),
        }
        
        try:
            self.db.table("beeper_messages").insert(record).execute()
            return True
        except Exception as e:
            if "duplicate key" in str(e).lower():
                return False
            raise
    
    def _generate_content_description(self, msg_data: Dict) -> Optional[str]:
        """
        Generate a human-readable description for media messages.
        E.g., "📷 Photo", "🎤 Voice message", "📄 PDF document"
        """
        message_type = msg_data.get("message_type", "text")
        content = msg_data.get("content")
        
        # If there's text content, no need for description
        if content and message_type == "text":
            return None
        
        # Map message types to descriptions
        type_descriptions = {
            "image": "📷 Photo",
            "photo": "📷 Photo",
            "video": "🎬 Video",
            "audio": "🎵 Audio",
            "voice": "🎤 Voice message",
            "file": "📎 File",
            "document": "📄 Document",
            "sticker": "🏷️ Sticker",
            "location": "📍 Location shared",
            "contact": "👤 Contact shared",
            "gif": "🎞️ GIF",
        }
        
        base_desc = type_descriptions.get(message_type, f"📎 {message_type.title()}")
        
        # Add filename if available
        filename = msg_data.get("media_filename")
        if filename and message_type in ("file", "document"):
            # Get file extension
            ext = filename.split(".")[-1].upper() if "." in filename else ""
            if ext:
                base_desc = f"📄 {ext} document"
        
        # Add caption if there's text with media
        if content and msg_data.get("has_media"):
            base_desc = f"{base_desc} with caption"
        
        return base_desc
    
    async def _update_chat_sync_time(self, beeper_chat_id: str):
        """Update last_synced_at for a chat."""
        self.db.table("beeper_chats").update({
            "last_synced_at": datetime.now(timezone.utc).isoformat()
        }).eq("beeper_chat_id", beeper_chat_id).execute()
    
    # =========================================
    # Contact Linking
    # =========================================
    
    async def _link_chat_to_contact(self, beeper_chat_id: str, chat_data: Dict) -> Optional[str]:
        """
        Try to link a chat to an existing contact.
        
        Matching priority:
        1. Phone number (WhatsApp, Signal, Telegram)
        2. LinkedIn ID (if we have linkedin_url in contacts)
        3. Cross-platform check (same person already linked on another platform)
        4. Name fuzzy match (last resort, uses BOTH chat_name and remote_user_name)
        """
        if chat_data.get("chat_type") != "dm":
            return None  # Only link DMs
        
        contact_id = None
        link_method = None
        confidence = 0.0
        
        # Method 1: Phone number (highest confidence)
        remote_phone = chat_data.get("remote_phone")
        if remote_phone:
            normalized = self._normalize_phone(remote_phone)
            
            result = self.db.table("contacts").select("id, first_name, last_name").or_(
                f"phone.eq.{normalized},phone.eq.{remote_phone}"
            ).execute()
            
            if result.data:
                contact_id = result.data[0]["id"]
                link_method = "phone"
                confidence = 1.0
        
        # Method 2: LinkedIn ID
        if not contact_id and chat_data.get("remote_linkedin_id"):
            linkedin_id = chat_data["remote_linkedin_id"]
            
            result = self.db.table("contacts").select("id, first_name, last_name").ilike(
                "linkedin_url", f"%{linkedin_id}%"
            ).execute()
            
            if result.data:
                contact_id = result.data[0]["id"]
                link_method = "linkedin"
                confidence = 0.95
        
        # Method 3: Cross-platform check
        # If we have a name, check if this person is already linked on another platform
        if not contact_id:
            name_to_check = chat_data.get("remote_user_name") or chat_data.get("name")
            if name_to_check:
                cross_platform_id = await self._find_cross_platform_contact(name_to_check, chat_data.get("platform"))
                if cross_platform_id:
                    contact_id = cross_platform_id
                    link_method = "cross_platform"
                    confidence = 0.9
        
        # Method 4: Name fuzzy match (uses BOTH chat_name and remote_user_name)
        if not contact_id:
            contact_id, confidence, match_source = await self._fuzzy_match_contact_improved(
                chat_name=chat_data.get("name"),
                remote_name=chat_data.get("remote_user_name")
            )
            if contact_id:
                link_method = f"name_{match_source}"
        
        # Update chat with contact link
        if contact_id and confidence >= 0.8:  # Only auto-link if confident
            self.db.table("beeper_chats").update({
                "contact_id": contact_id,
                "contact_link_method": link_method,
                "contact_link_confidence": confidence,
            }).eq("beeper_chat_id", beeper_chat_id).execute()
            
            self.stats["contacts_linked"] += 1
            logger.info(f"Linked chat {beeper_chat_id} to contact {contact_id} via {link_method}")
            return contact_id
        
        return None
    
    async def _find_cross_platform_contact(self, name: str, current_platform: str) -> Optional[str]:
        """
        Check if a person with similar name is already linked on another platform.
        This enables cross-platform linking (e.g., WhatsApp + LinkedIn = same contact).
        """
        name_lower = name.lower().strip()
        
        # Get all linked chats on OTHER platforms
        other_platform_chats = self.db.table("beeper_chats").select(
            "contact_id, chat_name, remote_user_name, platform"
        ).neq("platform", current_platform).not_.is_("contact_id", "null").execute()
        
        if not other_platform_chats.data:
            return None
        
        for chat in other_platform_chats.data:
            # Check both chat_name and remote_user_name
            chat_name = (chat.get("chat_name") or "").lower().strip()
            remote_name = (chat.get("remote_user_name") or "").lower().strip()
            
            # Exact match on either name
            if name_lower == chat_name or name_lower == remote_name:
                logger.info(f"Cross-platform match: '{name}' found on {chat['platform']} -> contact {chat['contact_id']}")
                return chat["contact_id"]
            
            # First name match (e.g., "John" matches "John Smith")
            name_parts = name_lower.split()
            if name_parts:
                first_name = name_parts[0]
                if len(first_name) >= 3:  # Avoid matching single letters
                    if (chat_name.startswith(first_name + " ") or 
                        remote_name.startswith(first_name + " ") or
                        chat_name == first_name or 
                        remote_name == first_name):
                        logger.info(f"Cross-platform partial match: '{name}' -> {chat['platform']} -> contact {chat['contact_id']}")
                        return chat["contact_id"]
        
        return None
    
    async def _fuzzy_match_contact_improved(
        self, 
        chat_name: Optional[str], 
        remote_name: Optional[str]
    ) -> Tuple[Optional[str], float, str]:
        """
        Improved fuzzy matching using BOTH chat_name and remote_user_name.
        Returns (contact_id, confidence, match_source) or (None, 0, "").
        """
        # Get all contacts
        result = self.db.table("contacts").select("id, first_name, last_name").execute()
        
        if not result.data:
            return None, 0.0, ""
        
        # Try both names and take the best match
        best_match = None
        best_score = 0.0
        best_source = ""
        
        names_to_try = []
        if remote_name and len(remote_name.strip()) > 2:
            names_to_try.append(("remote", remote_name.strip()))
        if chat_name and len(chat_name.strip()) > 2:
            # Skip phone-number-looking chat names
            if not chat_name.replace(" ", "").replace("+", "").isdigit():
                names_to_try.append(("chat", chat_name.strip()))
        
        for source, name in names_to_try:
            contact_id, score = await self._fuzzy_match_single_name(name, result.data)
            if score > best_score:
                best_score = score
                best_match = contact_id
                best_source = source
        
        return best_match, best_score, best_source
    
    async def _fuzzy_match_single_name(
        self, 
        name: str, 
        contacts: List[Dict]
    ) -> Tuple[Optional[str], float]:
        """
        Match a single name against contacts list.
        """
        name_lower = name.lower().strip()
        name_parts = name_lower.split()
        best_match = None
        best_score = 0.0
        
        for contact in contacts:
            first = (contact.get("first_name") or "").lower().strip()
            last = (contact.get("last_name") or "").lower().strip()
            full_name = f"{first} {last}".strip()
            
            # Exact full name match
            if name_lower == full_name:
                return contact["id"], 1.0
            
            # First name only exact match (chat name is just first name)
            if name_lower == first and first and len(first) >= 3:
                return contact["id"], 0.95
            
            # Chat name STARTS with contact's first name (e.g., "Yolanda Claudia" starts with "Yolanda")
            # This is a strong signal - if the first name matches, it's likely the same person
            if first and len(first) >= 3 and name_parts and name_parts[0] == first:
                # If contact has no last name, this is very likely a match
                if not last or last == "none":
                    score = 0.85
                else:
                    # Contact has last name - check if it also matches
                    if len(name_parts) > 1 and name_parts[-1] == last:
                        return contact["id"], 0.98
                    score = 0.75  # First name matches but last doesn't
                
                if score > best_score:
                    best_score = score
                    best_match = contact["id"]
                continue
            
            # Full name contained in chat name (e.g., "John Smith MBA" contains "John Smith")
            if full_name and len(full_name) >= 5 and full_name in name_lower:
                score = min(0.90, len(full_name) / len(name_lower) + 0.3)
                if score > best_score:
                    best_score = score
                    best_match = contact["id"]
            
            # First name contained at word boundary (e.g., "Irwan Tjahaja" contains "Irwan")
            if first and len(first) >= 4 and first in name_lower:
                # Check if it's at a word boundary
                import re
                if re.search(rf'\b{re.escape(first)}\b', name_lower):
                    score = 0.80
                    if score > best_score:
                        best_score = score
                        best_match = contact["id"]
        
        return best_match, best_score
    
    async def _fuzzy_match_contact(self, name: str) -> Tuple[Optional[str], float]:
        """
        Legacy fuzzy match method - now uses improved version.
        Returns (contact_id, confidence) or (None, 0).
        """
        contact_id, score, _ = await self._fuzzy_match_contact_improved(
            chat_name=None, 
            remote_name=name
        )
        return contact_id, score
    
    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number for matching."""
        import re
        digits = re.sub(r'[^\d+]', '', phone)
        
        if not digits.startswith('+'):
            digits = '+' + digits
        
        return digits
    
    async def relink_all_unlinked_chats(self) -> Dict[str, int]:
        """
        Re-process all unlinked chats to try contact linking again.
        Useful after adding new contacts or improving matching logic.
        """
        logger.info("Starting relink of all unlinked chats...")
        
        # Get all unlinked DM chats
        unlinked = self.db.table("beeper_chats").select("*").is_(
            "contact_id", "null"
        ).eq("chat_type", "dm").execute()
        
        stats = {
            "total_unlinked": len(unlinked.data),
            "newly_linked": 0,
            "still_unlinked": 0
        }
        
        for chat in unlinked.data:
            chat_data = {
                "chat_type": chat["chat_type"],
                "name": chat["chat_name"],
                "remote_user_name": chat.get("remote_user_name"),
                "remote_phone": chat.get("remote_phone"),
                "remote_linkedin_id": chat.get("remote_linkedin_id"),
                "platform": chat["platform"]
            }
            
            contact_id = await self._link_chat_to_contact(chat["beeper_chat_id"], chat_data)
            
            if contact_id:
                stats["newly_linked"] += 1
            else:
                stats["still_unlinked"] += 1
        
        logger.info(f"Relink complete: {stats['newly_linked']} newly linked, {stats['still_unlinked']} still unlinked")
        return stats


# =========================================
# Standalone sync function for use in main.py
# =========================================

async def run_beeper_sync(
    supabase_client: Client,
    full_sync: bool = False
) -> Dict[str, Any]:
    """
    Run Beeper sync with the given Supabase client.
    
    Args:
        supabase_client: Supabase client instance
        full_sync: If True, resync all messages (up to LOOKBACK_DAYS)
    
    Returns:
        Sync statistics dict
    """
    async with BeeperSyncService(supabase_client) as sync_service:
        return await sync_service.sync_all(full_sync=full_sync)


async def run_beeper_relink(supabase_client: Client) -> Dict[str, int]:
    """
    Re-process all unlinked chats for contact linking.
    """
    async with BeeperSyncService(supabase_client) as sync_service:
        return await sync_service.relink_all_unlinked_chats()
