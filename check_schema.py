"""Check table schema."""
from supabase import create_client
import os
from dotenv import load_dotenv
load_dotenv()

supabase = create_client(os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY'))

# Check reflections
result = supabase.table('reflections').select('*').limit(1).execute()
if result.data:
    print('Reflections columns:', list(result.data[0].keys()))
else:
    print('No reflections data')
