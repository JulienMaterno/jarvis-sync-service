"""Full sync health check."""
from lib.supabase_client import supabase
from datetime import datetime, timedelta, timezone

# Check sync logs for each service
services = ['calendar_sync', 'gmail_sync', 'meetings_sync', 'tasks_sync', 'reflections_sync', 'journals_sync', 'google_contacts']
cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

print("=" * 60)
print("SYNC HEALTH CHECK - Last 24 hours")
print("=" * 60)

for service in services:
    resp = supabase.table('sync_logs').select('status, created_at, message').eq('event_type', service).gte('created_at', cutoff).order('created_at', desc=True).limit(5).execute()
    
    if not resp.data:
        print(f"\n❓ {service}: No logs in last 24h")
        continue
    
    success = sum(1 for r in resp.data if r['status'] == 'success')
    error = sum(1 for r in resp.data if r['status'] == 'error')
    latest = resp.data[0]
    
    status_icon = "✅" if latest['status'] == 'success' else "❌"
    print(f"\n{status_icon} {service}:")
    print(f"   Latest: {latest['status']} at {latest['created_at']}")
    print(f"   Last 5: {success} success, {error} errors")
    if latest.get('message'):
        print(f"   Message: {latest['message'][:80]}")

# Check total stats
print("\n" + "=" * 60)
print("TOTAL STATISTICS")
print("=" * 60)

stats_resp = supabase.table('sync_logs').select('status').gte('created_at', cutoff).execute()
if stats_resp.data:
    total = len(stats_resp.data)
    successes = sum(1 for r in stats_resp.data if r['status'] == 'success')
    print(f"Total operations: {total}")
    print(f"Success rate: {successes}/{total} ({100*successes/total:.1f}%)")
