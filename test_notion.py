import os
from dotenv import load_dotenv
from notion_client import Client

load_dotenv()

token = os.environ.get("NOTION_API_TOKEN")
db_id = os.environ.get("NOTION_CRM_DATABASE_ID")
if db_id and len(db_id) == 32:
    db_id = f"{db_id[:8]}-{db_id[8:12]}-{db_id[12:16]}-{db_id[16:20]}-{db_id[20:]}"

print(f"Token present: {bool(token)}")
print(f"DB ID: {db_id}")

import httpx

try:
    print(f"Testing with httpx directly...")
    headers = {
        "Authorization": f"Bearer {token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    print(f"URL: {url}")
    
    response = httpx.post(url, headers=headers, json={"page_size": 1})
    print(f"Status: {response.status_code}")
    if response.status_code == 200:
        print("Success!")
        print(f"Results: {len(response.json().get('results', []))}")
    else:
        print(f"Error: {response.text}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
