"""Check what chats were synced to the database."""
import os
from supabase import create_client

# Get Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
sb = create_client(supabase_url, supabase_key)

# Query chats
chats = sb.table("beeper_chats").select(
    "platform, chat_name, beeper_chat_id, last_message_at, contact_id, unread_count"
).order("created_at", desc=True).limit(30).execute()

print(f"\n{'='*80}")
print(f"SYNCED CHATS IN DATABASE")
print(f"{'='*80}")
print(f"\nTotal: {len(chats.data)} chats\n")

# Group by platform
from collections import defaultdict
by_platform = defaultdict(list)
for chat in chats.data:
    by_platform[chat["platform"]].append(chat)

for platform, platform_chats in sorted(by_platform.items()):
    print(f"\n{platform.upper()} ({len(platform_chats)} chats):")
    print(f"{'-'*80}")
    for chat in platform_chats:
        contact = "✓ Linked" if chat.get("contact_id") else "✗ No contact"
        chat_name = chat.get("chat_name") or "N/A"
        unread = chat.get("unread_count", 0)
        print(f"  {chat_name[:35]:35} | {contact:12} | Unread: {unread:3} | {chat['beeper_chat_id'][:40]}")

# Check messages
messages = sb.table("beeper_messages").select("platform, beeper_chat_id").limit(1000).execute()
print(f"\n{'='*80}")
print(f"MESSAGES: {len(messages.data)} total")
print(f"{'='*80}")

if messages.data:
    msg_by_platform = defaultdict(int)
    for msg in messages.data:
        msg_by_platform[msg["platform"]] += 1
    
    for platform, count in sorted(msg_by_platform.items()):
        print(f"  {platform:12}: {count:4} messages")
else:
    print("  No messages synced yet!")
