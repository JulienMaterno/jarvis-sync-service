import httpx
import base64
from typing import List, Dict, Any, Optional
from lib.google_auth import get_access_token
from lib.utils import retry_on_error

GOOGLE_GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"

class GmailClient:
    def __init__(self):
        self.access_token = None

    async def _ensure_token(self):
        if not self.access_token:
            self.access_token = await get_access_token()

    @retry_on_error()
    async def list_messages(self, 
                          query: str = None, 
                          max_results: int = 100,
                          include_spam_trash: bool = False,
                          client: Optional[httpx.AsyncClient] = None) -> List[Dict[str, Any]]:
        """
        List messages matching a query.
        Returns a list of message objects (id, threadId).
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        params = {
            "maxResults": max_results,
            "includeSpamTrash": str(include_spam_trash).lower()
        }
        if query:
            params["q"] = query

        if client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/messages",
                headers=headers,
                params=params
            )
        else:
            async with httpx.AsyncClient(timeout=60.0) as new_client:
                response = await new_client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/messages",
                    headers=headers,
                    params=params
                )
            
        if response.status_code == 401:
            self.access_token = await get_access_token()
            headers["Authorization"] = f"Bearer {self.access_token}"
            # Retry with fresh token
            if client:
                response = await client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/messages",
                    headers=headers,
                    params=params
                )
            else:
                async with httpx.AsyncClient(timeout=60.0) as new_client:
                    response = await new_client.get(
                        f"{GOOGLE_GMAIL_API_BASE}/messages",
                        headers=headers,
                        params=params
                    )
            
        response.raise_for_status()
        data = response.json()
        return data.get("messages", [])

    @retry_on_error()
    async def get_message(self, message_id: str, format: str = 'full', client: Optional[httpx.AsyncClient] = None) -> Dict[str, Any]:
        """
        Get full message details.
        format: 'full', 'metadata', 'minimal', 'raw'
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        params = {"format": format}
        
        if client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/messages/{message_id}",
                headers=headers,
                params=params
            )
        else:
            async with httpx.AsyncClient(timeout=60.0) as new_client:
                response = await new_client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/messages/{message_id}",
                    headers=headers,
                    params=params
                )

        if response.status_code == 401:
            self.access_token = await get_access_token()
            headers["Authorization"] = f"Bearer {self.access_token}"
            if client:
                response = await client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/messages/{message_id}",
                    headers=headers,
                    params=params
                )
            else:
                async with httpx.AsyncClient(timeout=60.0) as new_client:
                    response = await new_client.get(
                        f"{GOOGLE_GMAIL_API_BASE}/messages/{message_id}",
                        headers=headers,
                        params=params
                    )

        response.raise_for_status()
        return response.json()

    async def search_messages(self, query: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Search for messages using Gmail's search syntax.
        This is an alias for list_messages with a query.
        
        Args:
            query: Gmail search query (e.g., 'from:john subject:meeting')
            max_results: Maximum messages to return
            
        Returns:
            List of message objects with id and threadId
        """
        return await self.list_messages(query=query, max_results=max_results)

    @retry_on_error()
    async def get_profile(self) -> Dict[str, Any]:
        """
        Get user profile (useful for historyId).
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/profile",
                headers=headers
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/profile",
                    headers=headers
                )
            
            response.raise_for_status()
            return response.json()

    @retry_on_error()
    async def list_history(self, start_history_id: str, max_results: int = 100) -> Dict[str, Any]:
        """
        List history of changes since start_history_id.
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        params = {
            "startHistoryId": start_history_id,
            "maxResults": max_results
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/history",
                headers=headers,
                params=params
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/history",
                    headers=headers,
                    params=params
                )
            
            # 404 means historyId is too old, caller should handle this
            if response.status_code == 404:
                return {"history": [], "historyId": None, "expired": True}
                
            response.raise_for_status()
            return response.json()

    def parse_message_body(self, payload: Dict[str, Any]) -> Dict[str, str]:
        """
        Extracts plain text and HTML body from message payload.
        """
        body_text = ""
        body_html = ""

        def decode_data(data):
            if not data:
                return ""
            # URL-safe base64 decode
            padding = len(data) % 4
            if padding:
                data += '=' * (4 - padding)
            return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')

        if 'parts' in payload:
            for part in payload['parts']:
                mime_type = part.get('mimeType')
                body_data = part.get('body', {}).get('data', '')
                
                if mime_type == 'text/plain' and not body_text:
                    body_text = decode_data(body_data)
                elif mime_type == 'text/html' and not body_html:
                    body_html = decode_data(body_data)
                elif 'parts' in part: # Nested parts
                    nested = self.parse_message_body(part)
                    if not body_text: body_text = nested['text']
                    if not body_html: body_html = nested['html']
        else:
            # Single part message
            mime_type = payload.get('mimeType')
            body_data = payload.get('body', {}).get('data', '')
            content = decode_data(body_data)
            if mime_type == 'text/plain':
                body_text = content
            elif mime_type == 'text/html':
                body_html = content

        return {"text": body_text, "html": body_html}

    def get_header(self, payload: Dict[str, Any], name: str) -> str:
        headers = payload.get("headers", [])
        for h in headers:
            if h["name"].lower() == name.lower():
                return h["value"]
        return ""

    @retry_on_error()
    async def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        cc: Optional[str] = None,
        bcc: Optional[str] = None,
        reply_to_message_id: Optional[str] = None,
        is_html: bool = False
    ) -> Dict[str, Any]:
        """
        Send an email via Gmail API.
        
        Args:
            to: Recipient email address (can be comma-separated for multiple)
            subject: Email subject line
            body: Email body (plain text or HTML)
            cc: CC recipients (optional, comma-separated)
            bcc: BCC recipients (optional, comma-separated)
            reply_to_message_id: If replying to an email, the original message ID
            is_html: If True, body is treated as HTML
            
        Returns:
            Dict with id, threadId, labelIds of sent message
        """
        import email
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # Build the email message
        if is_html:
            msg = MIMEMultipart('alternative')
            msg.attach(MIMEText(body, 'html'))
        else:
            msg = MIMEText(body)
        
        msg['To'] = to
        msg['Subject'] = subject
        
        if cc:
            msg['Cc'] = cc
        if bcc:
            msg['Bcc'] = bcc
        
        # If this is a reply, add threading headers
        thread_id = None
        if reply_to_message_id:
            # Get the original message to get its headers
            original = await self.get_message(reply_to_message_id, format='metadata')
            original_subject = self.get_header(original.get('payload', {}), 'Subject')
            message_id_header = self.get_header(original.get('payload', {}), 'Message-ID')
            
            if message_id_header:
                msg['In-Reply-To'] = message_id_header
                msg['References'] = message_id_header
            
            thread_id = original.get('threadId')
            
            # Prepend Re: if not already there
            if original_subject and not subject.lower().startswith('re:'):
                msg.replace_header('Subject', f"Re: {original_subject}")
        
        # Encode to base64url
        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')
        
        # Build request body
        request_body = {"raw": raw_message}
        if thread_id:
            request_body["threadId"] = thread_id
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{GOOGLE_GMAIL_API_BASE}/messages/send",
                headers=headers,
                json=request_body
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.post(
                    f"{GOOGLE_GMAIL_API_BASE}/messages/send",
                    headers=headers,
                    json=request_body
                )
            
            response.raise_for_status()
            return response.json()

    # =========================================================================
    # DRAFT MANAGEMENT - Create, read, update, delete, send drafts
    # =========================================================================
    
    def _build_email_message(
        self,
        to: str,
        subject: str,
        body: str,
        cc: Optional[str] = None,
        bcc: Optional[str] = None,
        is_html: bool = False,
        in_reply_to: Optional[str] = None,
        references: Optional[str] = None
    ) -> str:
        """Build a raw RFC 2822 email message encoded as base64url."""
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        if is_html:
            msg = MIMEMultipart('alternative')
            msg.attach(MIMEText(body, 'html'))
        else:
            msg = MIMEText(body)
        
        msg['To'] = to
        msg['Subject'] = subject
        
        if cc:
            msg['Cc'] = cc
        if bcc:
            msg['Bcc'] = bcc
        if in_reply_to:
            msg['In-Reply-To'] = in_reply_to
        if references:
            msg['References'] = references
        
        return base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')

    @retry_on_error()
    async def list_drafts(self, max_results: int = 20) -> List[Dict[str, Any]]:
        """
        List all drafts in the mailbox.
        
        Returns:
            List of draft objects with id, message.id, message.threadId
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/drafts",
                headers=headers,
                params={"maxResults": max_results}
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/drafts",
                    headers=headers,
                    params={"maxResults": max_results}
                )
            
            response.raise_for_status()
            data = response.json()
            return data.get("drafts", [])

    @retry_on_error()
    async def get_draft(self, draft_id: str, format: str = 'full') -> Dict[str, Any]:
        """
        Get a specific draft by ID.
        
        Args:
            draft_id: The draft ID
            format: 'full', 'metadata', 'minimal', 'raw'
            
        Returns:
            Draft object with id, message (containing headers, body, etc.)
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/drafts/{draft_id}",
                headers=headers,
                params={"format": format}
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/drafts/{draft_id}",
                    headers=headers,
                    params={"format": format}
                )
            
            response.raise_for_status()
            return response.json()

    @retry_on_error()
    async def create_draft(
        self,
        to: str,
        subject: str,
        body: str,
        cc: Optional[str] = None,
        bcc: Optional[str] = None,
        is_html: bool = False,
        reply_to_message_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new draft in Gmail.
        
        Args:
            to: Recipient email address
            subject: Email subject
            body: Email body (plain text or HTML)
            cc: CC recipients (optional)
            bcc: BCC recipients (optional)
            is_html: If True, body is HTML
            reply_to_message_id: If replying to an existing message
            
        Returns:
            Created draft object with id, message.id, message.threadId
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        # Handle reply threading
        thread_id = None
        in_reply_to = None
        references = None
        
        if reply_to_message_id:
            original = await self.get_message(reply_to_message_id, format='metadata')
            message_id_header = self.get_header(original.get('payload', {}), 'Message-ID')
            original_subject = self.get_header(original.get('payload', {}), 'Subject')
            
            if message_id_header:
                in_reply_to = message_id_header
                references = message_id_header
            
            thread_id = original.get('threadId')
            
            if original_subject and not subject.lower().startswith('re:'):
                subject = f"Re: {original_subject}"
        
        raw_message = self._build_email_message(
            to=to,
            subject=subject,
            body=body,
            cc=cc,
            bcc=bcc,
            is_html=is_html,
            in_reply_to=in_reply_to,
            references=references
        )
        
        request_body = {
            "message": {"raw": raw_message}
        }
        if thread_id:
            request_body["message"]["threadId"] = thread_id
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{GOOGLE_GMAIL_API_BASE}/drafts",
                headers=headers,
                json=request_body
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.post(
                    f"{GOOGLE_GMAIL_API_BASE}/drafts",
                    headers=headers,
                    json=request_body
                )
            
            response.raise_for_status()
            return response.json()

    @retry_on_error()
    async def update_draft(
        self,
        draft_id: str,
        to: str,
        subject: str,
        body: str,
        cc: Optional[str] = None,
        bcc: Optional[str] = None,
        is_html: bool = False
    ) -> Dict[str, Any]:
        """
        Update an existing draft.
        
        Args:
            draft_id: The draft ID to update
            to: Recipient email address
            subject: Email subject
            body: Email body
            cc: CC recipients (optional)
            bcc: BCC recipients (optional)
            is_html: If True, body is HTML
            
        Returns:
            Updated draft object
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        raw_message = self._build_email_message(
            to=to,
            subject=subject,
            body=body,
            cc=cc,
            bcc=bcc,
            is_html=is_html
        )
        
        request_body = {
            "message": {"raw": raw_message}
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.put(
                f"{GOOGLE_GMAIL_API_BASE}/drafts/{draft_id}",
                headers=headers,
                json=request_body
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.put(
                    f"{GOOGLE_GMAIL_API_BASE}/drafts/{draft_id}",
                    headers=headers,
                    json=request_body
                )
            
            response.raise_for_status()
            return response.json()

    @retry_on_error()
    async def delete_draft(self, draft_id: str) -> bool:
        """
        Delete a draft permanently.
        
        Args:
            draft_id: The draft ID to delete
            
        Returns:
            True if successful
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.delete(
                f"{GOOGLE_GMAIL_API_BASE}/drafts/{draft_id}",
                headers=headers
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.delete(
                    f"{GOOGLE_GMAIL_API_BASE}/drafts/{draft_id}",
                    headers=headers
                )
            
            response.raise_for_status()
            return True

    @retry_on_error()
    async def send_draft(self, draft_id: str) -> Dict[str, Any]:
        """
        Send an existing draft.
        
        Args:
            draft_id: The draft ID to send
            
        Returns:
            Sent message object with id, threadId, labelIds
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{GOOGLE_GMAIL_API_BASE}/drafts/send",
                headers=headers,
                json={"id": draft_id}
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.post(
                    f"{GOOGLE_GMAIL_API_BASE}/drafts/send",
                    headers=headers,
                    json={"id": draft_id}
                )
            
            response.raise_for_status()
            return response.json()