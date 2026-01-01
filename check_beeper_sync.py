"""Check Beeper sync status."""
from lib.supabase_client import supabase
from datetime import datetime, timezone, timedelta

# Check sync logs for beeper
result = supabase.table('sync_logs').select('*').ilike('event_type', '%beeper%').order('created_at', desc=True).limit(10).execute()

print("=" * 60)
print("Recent Beeper Sync Logs:")
print("=" * 60)
for r in result.data:
    msg = str(r.get("message", ""))[:80] if r.get("message") else "N/A"
    print(f"  {r['created_at'][:19]} - {r['status']:10} - {msg}")

# Check latest messages
print("\n" + "=" * 60)
print("Latest Beeper Messages (last 5):")
print("=" * 60)
msgs = supabase.table('beeper_messages').select('timestamp, platform, content, is_outgoing').order('timestamp', desc=True).limit(5).execute()
for m in msgs.data:
    content = m.get('content', '')[:50] if m.get('content') else '[media]'
    direction = "→" if m.get('is_outgoing') else "←"
    print(f"  {m['timestamp'][:19]} - {m['platform']:10} {direction} {content}")

# Check oldest message
print("\n" + "=" * 60)
print("Oldest Beeper Message:")
print("=" * 60)
oldest = supabase.table('beeper_messages').select('timestamp, platform, content').order('timestamp').limit(1).execute()
if oldest.data:
    m = oldest.data[0]
    print(f"  {m['timestamp'][:19]} - {m['platform']}")

# Check sync coverage
print("\n" + "=" * 60)
print("Sync Coverage Stats:")
print("=" * 60)
chats = supabase.table('beeper_chats').select('id', count='exact').execute()
msgs = supabase.table('beeper_messages').select('id', count='exact').execute()
linked = supabase.table('beeper_chats').select('id', count='exact').not_.is_('contact_id', 'null').execute()
needs_resp = supabase.table('beeper_chats').select('id', count='exact').eq('needs_response', True).execute()

print(f"  Total Chats:       {chats.count}")
print(f"  Total Messages:    {msgs.count}")
print(f"  Linked to Contact: {linked.count}")
print(f"  Needs Response:    {needs_resp.count}")

# Check last successful sync
print("\n" + "=" * 60)
print("Last Successful Sync:")
print("=" * 60)
last_sync = supabase.table('beeper_chats').select('last_synced_at').not_.is_('last_synced_at', 'null').order('last_synced_at', desc=True).limit(1).execute()
if last_sync.data:
    print(f"  {last_sync.data[0]['last_synced_at']}")
else:
    print("  No sync timestamps found")
