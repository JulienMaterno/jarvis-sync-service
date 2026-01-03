"""
Final comprehensive verification of Beeper integration.
1. All tools work correctly
2. Incremental sync doesn't miss messages
3. List contacts with phone but no linked chat
"""
import asyncio
from datetime import datetime, timezone, timedelta
from lib.supabase_client import supabase
import httpx

BEEPER_BRIDGE_URL = "http://localhost:8377"


async def test_all_endpoints():
    """Test all Beeper API endpoints."""
    print("=" * 60)
    print("TESTING ALL BEEPER ENDPOINTS")
    print("=" * 60)
    
    results = {}
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Health check
        try:
            resp = await client.get(f"{BEEPER_BRIDGE_URL}/health")
            results["GET /health"] = "✅ PASS" if resp.status_code == 200 else f"❌ {resp.status_code}"
        except Exception as e:
            results["GET /health"] = f"❌ {e}"
        
        # 2. Get chats
        try:
            resp = await client.get(f"{BEEPER_BRIDGE_URL}/chats?since_days=7")
            data = resp.json()
            results["GET /chats"] = f"✅ PASS ({len(data.get('chats', []))} chats)"
        except Exception as e:
            results["GET /chats"] = f"❌ {e}"
        
        # 3. Get accounts
        try:
            resp = await client.get(f"{BEEPER_BRIDGE_URL}/accounts")
            data = resp.json()
            results["GET /accounts"] = f"✅ PASS ({len(data)} accounts)"
        except Exception as e:
            results["GET /accounts"] = f"❌ {e}"
    
    # Database endpoints (via supabase)
    # 4. List chats from DB
    try:
        chats = supabase.table("beeper_chats").select("*").limit(5).execute()
        results["DB: beeper_chats"] = f"✅ PASS ({len(chats.data)} sample rows)"
    except Exception as e:
        results["DB: beeper_chats"] = f"❌ {e}"
    
    # 5. List messages from DB
    try:
        msgs = supabase.table("beeper_messages").select("*").limit(5).execute()
        results["DB: beeper_messages"] = f"✅ PASS ({len(msgs.data)} sample rows)"
    except Exception as e:
        results["DB: beeper_messages"] = f"❌ {e}"
    
    # 6. Views work
    try:
        needs_resp = supabase.table("beeper_chats").select("*").eq("needs_response", True).limit(5).execute()
        results["DB: needs_response filter"] = f"✅ PASS ({len(needs_resp.data)} chats need response)"
    except Exception as e:
        results["DB: needs_response filter"] = f"❌ {e}"
    
    # 7. Contact join works
    try:
        with_contact = supabase.table("beeper_chats").select(
            "chat_name, contacts(first_name, last_name)"
        ).not_.is_("contact_id", "null").limit(3).execute()
        results["DB: contact join"] = f"✅ PASS"
    except Exception as e:
        results["DB: contact join"] = f"❌ {e}"
    
    # 8. Full-text search works
    try:
        search = supabase.table("beeper_messages").select("content").text_search(
            "content", "hello"
        ).limit(3).execute()
        results["DB: full-text search"] = f"✅ PASS ({len(search.data)} results for 'hello')"
    except Exception as e:
        results["DB: full-text search"] = f"❌ {e}"
    
    print("\nEndpoint Test Results:")
    for endpoint, result in results.items():
        print(f"  {endpoint}: {result}")
    
    passed = sum(1 for r in results.values() if "✅" in r)
    print(f"\nTotal: {passed}/{len(results)} passed")
    return passed == len(results)


def test_incremental_sync():
    """Verify incremental sync tracking is working."""
    print("\n" + "=" * 60)
    print("TESTING INCREMENTAL SYNC")
    print("=" * 60)
    
    # Check that chats have last_synced_at
    chats = supabase.table("beeper_chats").select(
        "beeper_chat_id, chat_name, last_synced_at, last_message_at, platform"
    ).order("last_message_at", desc=True).limit(10).execute()
    
    print("\nRecent chats with sync timestamps:")
    has_sync_time = 0
    for chat in chats.data:
        name = (chat.get("chat_name") or "?")[:25]
        last_sync = chat.get("last_synced_at")
        last_msg = chat.get("last_message_at")
        platform = chat.get("platform")
        
        if last_sync:
            has_sync_time += 1
            # Check if last_synced_at is after last_message_at (good!)
            sync_dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
            msg_dt = datetime.fromisoformat(last_msg.replace("Z", "+00:00")) if last_msg else None
            
            status = "✅" if msg_dt and sync_dt >= msg_dt else "⚠️"
            print(f"  {status} [{platform:10}] {name:25} | synced: {last_sync[:19]} | last_msg: {last_msg[:19] if last_msg else 'N/A'}")
        else:
            print(f"  ⚠️ [{platform:10}] {name:25} | NO SYNC TIME")
    
    print(f"\nChats with sync timestamps: {has_sync_time}/{len(chats.data)}")
    
    # Check for any messages that might be newer than last_synced_at
    print("\nChecking for potentially missed messages...")
    
    # Get chats and their newest messages
    potential_gaps = []
    all_chats = supabase.table("beeper_chats").select(
        "beeper_chat_id, chat_name, last_synced_at"
    ).not_.is_("last_synced_at", "null").execute()
    
    for chat in all_chats.data[:20]:  # Check first 20
        last_sync = chat.get("last_synced_at")
        if not last_sync:
            continue
        
        # Get newest message for this chat
        newest = supabase.table("beeper_messages").select("timestamp").eq(
            "beeper_chat_id", chat["beeper_chat_id"]
        ).order("timestamp", desc=True).limit(1).execute()
        
        if newest.data:
            msg_time = newest.data[0]["timestamp"]
            sync_dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
            msg_dt = datetime.fromisoformat(msg_time.replace("Z", "+00:00"))
            
            # If message is significantly newer than sync time, flag it
            if msg_dt > sync_dt + timedelta(minutes=5):
                potential_gaps.append({
                    "chat": chat["chat_name"],
                    "sync_time": last_sync,
                    "newest_msg": msg_time
                })
    
    if potential_gaps:
        print(f"\n⚠️ Found {len(potential_gaps)} chats with messages newer than sync time:")
        for gap in potential_gaps[:5]:
            print(f"  - {gap['chat']}: msg at {gap['newest_msg']}, synced at {gap['sync_time']}")
    else:
        print("\n✅ No gaps found - all messages are within sync timestamps")
    
    return len(potential_gaps) == 0


def list_contacts_with_phone_no_chat():
    """List all contacts with phone numbers but no linked WhatsApp chat."""
    print("\n" + "=" * 60)
    print("CONTACTS WITH PHONE BUT NO LINKED WHATSAPP CHAT")
    print("=" * 60)
    
    # Get all contacts with phones
    contacts = supabase.table("contacts").select(
        "id, first_name, last_name, phone, company"
    ).not_.is_("phone", "null").execute()
    
    contacts_with_phone = [c for c in contacts.data if c.get("phone") and c["phone"].strip()]
    
    # Get contacts linked to WhatsApp
    linked = supabase.table("beeper_chats").select("contact_id").eq(
        "platform", "whatsapp"
    ).not_.is_("contact_id", "null").execute()
    
    linked_ids = {c["contact_id"] for c in linked.data}
    
    # Find unlinked
    unlinked = [c for c in contacts_with_phone if c["id"] not in linked_ids]
    
    print(f"\nTotal contacts with phone: {len(contacts_with_phone)}")
    print(f"Linked to WhatsApp: {len(linked_ids)}")
    print(f"NOT linked to WhatsApp: {len(unlinked)}")
    
    print(f"\n{'Name':<35} | {'Company':<20} | Phone")
    print("-" * 90)
    
    for c in sorted(unlinked, key=lambda x: (x.get("first_name") or "").lower()):
        name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
        company = (c.get("company") or "")[:20]
        phone = c.get("phone", "")
        print(f"{name:<35} | {company:<20} | {phone}")
    
    return unlinked


async def run_all():
    """Run all verification tests."""
    print("\n" + "=" * 60)
    print("  BEEPER INTEGRATION - FINAL VERIFICATION")
    print("  " + datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))
    print("=" * 60)
    
    # Test 1: All endpoints
    endpoints_ok = await test_all_endpoints()
    
    # Test 2: Incremental sync
    sync_ok = test_incremental_sync()
    
    # Test 3: List unlinked contacts
    unlinked = list_contacts_with_phone_no_chat()
    
    # Summary
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"  Endpoints working: {'✅ YES' if endpoints_ok else '❌ NO'}")
    print(f"  Incremental sync: {'✅ NO GAPS' if sync_ok else '⚠️ POTENTIAL GAPS'}")
    print(f"  Contacts with phone, no WhatsApp: {len(unlinked)}")
    
    if endpoints_ok and sync_ok:
        print("\n🎉 ALL SYSTEMS OPERATIONAL!")
    else:
        print("\n⚠️ Some issues detected - review above")


if __name__ == "__main__":
    asyncio.run(run_all())
