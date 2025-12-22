"""Check for duplicate meetings in Supabase."""
from lib.supabase_client import supabase
from collections import Counter

# Get all meetings
resp = supabase.table('meetings').select('id, title, date, notion_page_id, created_at').order('title').execute()
meetings = resp.data

print(f"Total meetings in Supabase: {len(meetings)}")
print()

# Check for duplicates by title
titles = [m['title'] for m in meetings]
title_counts = Counter(titles)
duplicates = {t: c for t, c in title_counts.items() if c > 1}

if duplicates:
    print(f"⚠️ Found {len(duplicates)} titles with duplicates:")
    print("-" * 50)
    for title, count in sorted(duplicates.items(), key=lambda x: -x[1])[:20]:
        print(f"  '{title[:50]}': {count} copies")
    
    print()
    print("Detailed duplicate check (same title + date):")
    # Group by title+date
    from collections import defaultdict
    by_title_date = defaultdict(list)
    for m in meetings:
        key = (m['title'], m.get('date', '')[:10] if m.get('date') else 'no-date')
        by_title_date[key].append(m)
    
    exact_dupes = {k: v for k, v in by_title_date.items() if len(v) > 1}
    print(f"Exact duplicates (same title + same date): {len(exact_dupes)}")
    for (title, date), records in list(exact_dupes.items())[:10]:
        print(f"  '{title[:40]}' on {date}: {len(records)} copies")
        for r in records:
            print(f"    - ID: {r['id'][:8]}... Notion: {r.get('notion_page_id', 'None')[:8] if r.get('notion_page_id') else 'None'}...")
else:
    print("✅ No duplicate titles found")

# Check meetings without notion_page_id
no_notion = [m for m in meetings if not m.get('notion_page_id')]
print(f"\nMeetings without notion_page_id: {len(no_notion)}")

# Check recent meetings (last 7 days)
from datetime import datetime, timedelta
recent_cutoff = (datetime.now() - timedelta(days=7)).isoformat()
recent = [m for m in meetings if m.get('created_at', '') > recent_cutoff]
print(f"Meetings created in last 7 days: {len(recent)}")
