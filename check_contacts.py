"""Quick check of contact status."""
from lib.supabase_client import supabase

# Get counts
total = supabase.table("contacts").select("id", count="exact").execute()
deleted = supabase.table("contacts").select("id", count="exact").not_.is_("deleted_at", "null").execute()
no_google = supabase.table("contacts").select("id", count="exact").is_("google_resource_name", "null").is_("deleted_at", "null").execute()
with_google = supabase.table("contacts").select("id", count="exact").not_.is_("google_resource_name", "null").is_("deleted_at", "null").execute()

print(f"Total contacts: {total.count}")
print(f"Deleted (deleted_at not null): {deleted.count}")
print(f"Active, no google_resource_name: {no_google.count}")
print(f"Active, with google_resource_name: {with_google.count}")

# Sample
sample = supabase.table("contacts").select("first_name, last_name, deleted_at, google_resource_name").limit(10).execute()
print("\nSample:")
for c in sample.data:
    print(f"  {c.get('first_name')} {c.get('last_name')} - deleted: {c.get('deleted_at')}, google: {c.get('google_resource_name')[:30] if c.get('google_resource_name') else 'None'}...")
