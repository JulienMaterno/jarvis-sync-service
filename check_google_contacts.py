"""Check Google contacts count."""
import os
import asyncio
from dotenv import load_dotenv
load_dotenv()

from lib.google_contacts import get_access_token, get_all_contacts

async def check():
    token = await get_access_token()
    contacts = await get_all_contacts(token)
    print(f'Google contacts: {len(contacts)}')
    if contacts:
        for c in contacts[:5]:
            names = c.get('names', [{}])
            name = names[0].get('displayName', 'Unknown') if names else 'Unknown'
            print(f'  - {name}')

asyncio.run(check())
