#!/usr/bin/env python3
"""Quick script to check record counts in Supabase."""
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

url = os.environ.get('SUPABASE_URL')
key = os.environ.get('SUPABASE_KEY')

headers = {'apikey': key, 'Authorization': f'Bearer {key}', 'Prefer': 'count=exact'}

tables = ['meetings', 'tasks', 'books', 'highlights', 'journals', 'reflections', 'contacts']

print('Supabase Record Counts:')
print('-' * 40)
for table in tables:
    response = httpx.head(f'{url}/rest/v1/{table}?select=*', headers=headers)
    count = response.headers.get('content-range', '0/*').split('/')[1]
    print(f'  {table}: {count}')
