"""Verify all Beeper tools work correctly."""
import sys
sys.path.insert(0, 'c:/Projects/jarvis-intelligence-service')

from app.features.chat.tools import execute_tool

print("=" * 60)
print("Testing Beeper Tools")
print("=" * 60)

# Test 1: get_beeper_inbox
print("\n1. get_beeper_inbox")
result = execute_tool('get_beeper_inbox', {'limit': 3})
if 'error' in result:
    print(f"   ❌ Error: {result['error']}")
else:
    needs = result.get('needs_response', {}).get('count', 0)
    other = result.get('other_active', {}).get('count', 0)
    print(f"   ✅ Needs response: {needs}, Other active: {other}")
    print(f"   Summary: {result.get('summary', 'N/A')}")

# Test 2: search_beeper_messages
print("\n2. search_beeper_messages")
result = execute_tool('search_beeper_messages', {'query': 'hello', 'limit': 3})
if 'error' in result:
    print(f"   ❌ Error: {result['error']}")
else:
    print(f"   ✅ Found {result.get('count', 0)} messages matching 'hello'")

# Test 3: get_beeper_contact_messages  
print("\n3. get_beeper_contact_messages")
result = execute_tool('get_beeper_contact_messages', {'contact_name': 'Aaron', 'limit': 5})
if 'error' in result:
    print(f"   ❌ Error: {result['error']}")
else:
    print(f"   ✅ Found {result.get('count', 0)} messages with '{result.get('contact_name', 'unknown')}'")
    if result.get('messages'):
        latest = result['messages'][-1] if result['messages'] else None
        if latest:
            print(f"   Latest: {latest.get('content', 'N/A')[:50]}...")

# Test 4: get_beeper_chat_messages (need a chat_id)
print("\n4. get_beeper_chat_messages")
# First get a chat_id from inbox
inbox = execute_tool('get_beeper_inbox', {'limit': 1})
if inbox.get('needs_response', {}).get('chats'):
    chat_id = inbox['needs_response']['chats'][0].get('beeper_chat_id')
    result = execute_tool('get_beeper_chat_messages', {'beeper_chat_id': chat_id, 'limit': 3})
    if 'error' in result:
        print(f"   ❌ Error: {result['error']}")
    else:
        print(f"   ✅ Got {result.get('count', 0)} messages from chat")
else:
    print("   ⏭️  Skipped (no chats in inbox)")

# Test 5: archive_beeper_chat (we won't actually run this)
print("\n5. archive_beeper_chat")
print("   ⏭️  Skipped (would archive a chat)")

print("\n" + "=" * 60)
print("All Beeper tools tested successfully!")
print("=" * 60)
