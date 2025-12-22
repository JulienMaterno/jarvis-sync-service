"""Remove duplicate meetings from Supabase, keeping the one with most data."""
from lib.supabase_client import supabase
from collections import defaultdict

# Get all meetings with full data
resp = supabase.table('meetings').select('*').execute()
meetings = resp.data

print(f"Total meetings before cleanup: {len(meetings)}")

# Group by title + date
by_title_date = defaultdict(list)
for m in meetings:
    date_key = m.get('date', '')[:10] if m.get('date') else 'no-date'
    key = (m['title'], date_key)
    by_title_date[key].append(m)

# Find duplicates
exact_dupes = {k: v for k, v in by_title_date.items() if len(v) > 1}
print(f"Groups with duplicates: {len(exact_dupes)}")

# Score function - higher score = more data
def score_meeting(m):
    score = 0
    if m.get('summary'): score += len(m.get('summary', ''))
    if m.get('topics_discussed'): score += len(str(m.get('topics_discussed', [])))
    if m.get('action_items'): score += len(str(m.get('action_items', [])))
    if m.get('people_mentioned'): score += len(m.get('people_mentioned', []))
    if m.get('contact_id'): score += 100  # Prefer linked contacts
    if m.get('notion_page_id'): score += 50  # Prefer synced to Notion
    return score

to_delete = []
to_keep = []

for (title, date), records in exact_dupes.items():
    # Score each and keep the best
    scored = [(score_meeting(r), r) for r in records]
    scored.sort(key=lambda x: -x[0])
    
    # Keep the highest scored one
    best = scored[0][1]
    to_keep.append(best['id'])
    
    # Delete the rest
    for _, r in scored[1:]:
        to_delete.append(r['id'])

print(f"Meetings to keep: {len(to_keep)}")
print(f"Meetings to delete: {len(to_delete)}")

if to_delete:
    print(f"\nDeleting {len(to_delete)} duplicate meetings...")
    
    # Delete in batches
    batch_size = 50
    deleted = 0
    for i in range(0, len(to_delete), batch_size):
        batch = to_delete[i:i+batch_size]
        try:
            # Delete using OR filter
            for meeting_id in batch:
                supabase.table('meetings').delete().eq('id', meeting_id).execute()
                deleted += 1
            print(f"  Deleted batch {i//batch_size + 1}: {len(batch)} meetings")
        except Exception as e:
            print(f"  Error in batch {i//batch_size + 1}: {e}")
    
    print(f"\n✅ Deleted {deleted} duplicate meetings")
    
    # Verify
    resp2 = supabase.table('meetings').select('id').execute()
    print(f"Total meetings after cleanup: {len(resp2.data)}")
else:
    print("No duplicates to delete")
