import httpx
import base64
from typing import List, Dict, Any, Optional
from lib.google_auth import get_access_token

GOOGLE_GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"

class GmailClient:
    def __init__(self):
        self.access_token = None

    async def _ensure_token(self):
        if not self.access_token:
            self.access_token = await get_access_token()

    async def list_messages(self, 
                          query: str = None, 
                          max_results: int = 100,
                          include_spam_trash: bool = False) -> List[Dict[str, Any]]:
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

        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/messages",
                headers=headers,
                params=params
            )
            
            if response.status_code == 401:
                self.access_token = await get_access_token()
                headers["Authorization"] = f"Bearer {self.access_token}"
                response = await client.get(
                    f"{GOOGLE_GMAIL_API_BASE}/messages",
                    headers=headers,
                    params=params
                )
                
            response.raise_for_status()
            data = response.json()
            return data.get("messages", [])

    async def get_message(self, message_id: str, format: str = 'full') -> Dict[str, Any]:
        """
        Get full message details.
        format: 'full', 'metadata', 'minimal', 'raw'
        """
        await self._ensure_token()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{GOOGLE_GMAIL_API_BASE}/messages/{message_id}",
                headers=headers,
                params={"format": format}
            )
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
