import os
import httpx
import logging
from dotenv import load_dotenv
from typing import Dict, Any, Optional, List, Generator
from lib.utils import retry_on_error_sync

load_dotenv()

logger = logging.getLogger("NotionClient")

notion_token = os.environ.get("NOTION_API_TOKEN")
notion_database_id = os.environ.get("NOTION_CRM_DATABASE_ID")

# Format DB ID if needed
if notion_database_id and len(notion_database_id) == 32:
    notion_database_id = f"{notion_database_id[:8]}-{notion_database_id[8:12]}-{notion_database_id[12:16]}-{notion_database_id[16:20]}-{notion_database_id[20:]}"

class NotionClient:
    def __init__(self, token: str):
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        self.client = httpx.Client(headers=self.headers, timeout=30.0)

    @retry_on_error_sync()
    def query_database(self, database_id: str, page_size: int = 100, start_cursor: Optional[str] = None, filter: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/databases/{database_id}/query"
        body = {"page_size": page_size}
        if start_cursor:
            body["start_cursor"] = start_cursor
        if filter:
            body["filter"] = filter
            
        response = self.client.post(url, json=body)
        response.raise_for_status()
        return response.json()

    def query_database_all(self, database_id: str, filter: Optional[Dict[str, Any]] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Yields all pages from a database, handling pagination automatically.
        """
        has_more = True
        start_cursor = None
        
        while has_more:
            data = self.query_database(database_id, start_cursor=start_cursor, filter=filter)
            for page in data.get("results", []):
                yield page
            
            has_more = data.get("has_more", False)
            start_cursor = data.get("next_cursor")

    @retry_on_error_sync()
    def create_page(self, parent: Dict[str, Any], properties: Dict[str, Any], children: Optional[List[Dict]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/pages"
        body = {
            "parent": parent,
            "properties": properties
        }
        if children:
            body["children"] = children
            
        response = self.client.post(url, json=body)
        response.raise_for_status()
        return response.json()

    @retry_on_error_sync()
    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/pages/{page_id}"
        body = {
            "properties": properties
        }
        response = self.client.patch(url, json=body)
        response.raise_for_status()
        return response.json()

    def archive_page(self, page_id: str) -> Dict[str, Any]:
        """
        Archive a Notion page. Handles cases where the page is already archived
        or has been deleted.
        
        Returns:
            Dict with 'archived': True on success, or 'already_archived': True if page was already gone
        """
        url = f"{self.base_url}/pages/{page_id}"
        
        # First check if page exists and its current state
        try:
            page = self.retrieve_page(page_id)
            if page.get("archived"):
                logger.info(f"Page {page_id} is already archived")
                return {"id": page_id, "already_archived": True, "archived": True}
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                logger.info(f"Page {page_id} not found (may be deleted)")
                return {"id": page_id, "not_found": True, "archived": True}
            raise
        
        # Archive the page
        body = {"archived": True}
        try:
            response = self.client.patch(url, json=body)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code in (400, 404):
                # Page may have been archived/deleted between check and archive
                logger.info(f"Page {page_id} archive failed (400/404) - treating as archived")
                return {"id": page_id, "already_archived": True, "archived": True}
            raise

    @retry_on_error_sync()
    def retrieve_page(self, page_id: str) -> Dict[str, Any]:
        """Retrieve a single page by ID to check its archived status."""
        url = f"{self.base_url}/pages/{page_id}"
        response = self.client.get(url)
        response.raise_for_status()
        return response.json()

    @retry_on_error_sync()
    def search(self, query: Optional[str] = None, filter: Optional[Dict[str, Any]] = None, sort: Optional[Dict[str, Any]] = None, page_size: int = 100, start_cursor: Optional[str] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/search"
        body = {"page_size": page_size}
        if query:
            body["query"] = query
        if filter:
            body["filter"] = filter
        if sort:
            body["sort"] = sort
        if start_cursor:
            body["start_cursor"] = start_cursor
            
        response = self.client.post(url, json=body)
        response.raise_for_status()
        return response.json()

    @retry_on_error_sync()
    def append_block_children(self, block_id: str, children: List[Dict[str, Any]]) -> Dict[str, Any]:
        url = f"{self.base_url}/blocks/{block_id}/children"
        body = {"children": children}
        response = self.client.patch(url, json=body)
        response.raise_for_status()
        return response.json()

if not notion_token:
    print("Warning: NOTION_API_TOKEN not found in environment variables.")
    notion = None
else:
    notion = NotionClient(notion_token)
