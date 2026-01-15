import requests
import os
from dotenv import load_dotenv

load_dotenv()

headers = {
    'Authorization': f'Bearer {os.getenv("NOTION_API_TOKEN")}',
    'Notion-Version': '2022-06-28'
}

db = requests.get('https://api.notion.com/v1/databases/bfb77dff-9721-47b6-9bab-0cd0b315a298', headers=headers).json()
print('Database Title:', db.get('title', [{}])[0].get('plain_text', 'Unknown'))
print('URL:', db.get('url'))
