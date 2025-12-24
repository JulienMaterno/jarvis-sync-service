"""Test briefing generation for Robin - even with minimal data"""
import os
import asyncio
from dotenv import load_dotenv
import httpx

load_dotenv()

SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
INTELLIGENCE_URL = os.environ.get('INTELLIGENCE_SERVICE_URL', 'https://jarvis-intelligence-service-qkz4et4n4q-as.a.run.app')

h = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

async def test_robin_briefing():
    print('=' * 60)
    print('TESTING BRIEFING FOR ROBIN (Even with no meeting data)')
    print('=' * 60)
    
    # 1. Find Robin's contact
    print('\n1. Finding Robin contact...')
    r = httpx.get(f'{SUPABASE_URL}/rest/v1/contacts?select=*&first_name=eq.Robin&deleted_at=is.null', headers=h)
    contacts = r.json()
    
    if not contacts:
        print('   ❌ No Robin contact found!')
        return
    
    robin = contacts[0]
    print(f'   ✅ Found: {robin.get("first_name")} {robin.get("last_name")}')
    print(f'   ID: {robin.get("id")}')
    print(f'   Email: {robin.get("email")}')
    print(f'   Notion: {robin.get("notion_page_id")}')
    
    # 2. Check for any meetings with Robin
    print('\n2. Checking for existing meetings with Robin...')
    r = httpx.get(f'{SUPABASE_URL}/rest/v1/meetings?select=id,title,date,summary&contact_id=eq.{robin["id"]}&deleted_at=is.null', headers=h)
    meetings = r.json()
    print(f'   Found {len(meetings)} meetings with Robin')
    for m in meetings[:3]:
        print(f'   - {m.get("date", "?")[:10]}: {m.get("title")}')
    
    # 3. Check for calendar events with Robin
    print('\n3. Checking for calendar events with Robin...')
    r = httpx.get(f'{SUPABASE_URL}/rest/v1/calendar_events?select=id,summary,start_time,contact_id&contact_id=eq.{robin["id"]}', headers=h)
    events = r.json()
    print(f'   Found {len(events)} calendar events with Robin')
    
    # 4. Create a fake/test calendar event for Robin to trigger briefing
    print('\n4. Creating test calendar event with Robin...')
    from datetime import datetime, timedelta
    
    import uuid
    test_event = {
        'google_event_id': f'test_briefing_{uuid.uuid4().hex[:12]}',  # Required field
        'calendar_id': 'primary',  # Required field
        'summary': 'Test Meeting with Robin Böhmer',
        'start_time': (datetime.utcnow() + timedelta(hours=1)).isoformat() + 'Z',
        'end_time': (datetime.utcnow() + timedelta(hours=2)).isoformat() + 'Z',
        'contact_id': robin['id'],
        'status': 'confirmed',
        'attendees': [{'email': robin.get('email', 'robin@test.com'), 'name': f"{robin.get('first_name')} {robin.get('last_name')}".strip()}]
    }
    
    r = httpx.post(f'{SUPABASE_URL}/rest/v1/calendar_events', headers={**h, 'Prefer': 'return=representation'}, json=test_event)
    if r.status_code in [200, 201]:
        result = r.json()
        event_data = result[0] if isinstance(result, list) else result
        event_id = event_data.get('id')
        print(f'   ✅ Created test event: {event_id}')
    else:
        print(f'   ❌ Failed to create event: {r.status_code} {r.text}')
        return
    
    # 5. Call the briefing trigger endpoint
    print('\n5. Triggering briefing generation...')
    print(f'   Calling POST {INTELLIGENCE_URL}/api/v1/briefings/trigger')
    
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f'{INTELLIGENCE_URL}/api/v1/briefings/trigger',
                json={
                    'event_id': event_id,
                    'send_notification': False  # Don't spam Telegram
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f'   ✅ Briefing generated successfully!')
                print(f'\n   Event: {result.get("event_title")}')
                print(f'   Contact: {result.get("contact_name")} ({result.get("contact_company", "No company")})')
                print(f'   Previous meetings: {result.get("previous_meetings_count", 0)}')
                print(f'   Recent emails: {result.get("recent_emails_count", 0)}')
                print(f'   Previous events: {result.get("previous_events_count", 0)}')
                print(f'\n   === BRIEFING TEXT ===')
                print(result.get('briefing_text', 'No briefing text'))
            else:
                print(f'   ❌ Briefing failed: {response.status_code}')
                print(f'   Response: {response.text}')
    except httpx.ConnectError:
        print(f'   ⚠️ Cannot connect to {INTELLIGENCE_URL}')
        print(f'   Make sure the Intelligence Service is running locally or use the deployed URL.')
        print(f'\n   For testing, you can call the deployed service directly:')
        print(f'   curl -X POST https://jarvis-intelligence-service-qkz4et4n4q-as.a.run.app/api/v1/briefings/trigger \\')
        print(f'     -H "Content-Type: application/json" \\')
        print(f'     -d \'{{"event_id": "{event_id}", "send_notification": true}}\'')
    
    # 6. Clean up test event
    print('\n6. Cleaning up test event...')
    r = httpx.delete(f'{SUPABASE_URL}/rest/v1/calendar_events?id=eq.{event_id}', headers=h)
    if r.status_code == 204:
        print('   ✅ Test event deleted')
    else:
        print(f'   ⚠️ Cleanup note: Event {event_id} left in DB')

if __name__ == '__main__':
    asyncio.run(test_robin_briefing())
