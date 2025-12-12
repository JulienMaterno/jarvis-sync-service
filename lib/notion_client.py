import os
import httpx
from dotenv import load_dotenv
from typing import Dict, Any, Optional

load_dotenv()

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

    def create_page(self, parent: Dict[str, Any], properties: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/pages"
        body = {
            "parent": parent,
            "properties": properties
        }
        response = self.client.post(url, json=body)
        response.raise_for_status()
        return response.json()

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}/pages/{page_id}"
        body = {
            "properties": properties
        }
        response = self.client.patch(url, json=body)
        response.raise_for_status()
        return response.json()

    def archive_page(self, page_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/pages/{page_id}"
        body = {
            "archived": True
        }
        response = self.client.patch(url, json=body)
        response.raise_for_status()
        return response.json()

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

if not notion_token:
    print("Warning: NOTION_API_TOKEN not found in environment variables.")
    notion = None
else:
    notion = NotionClient(notion_token)
