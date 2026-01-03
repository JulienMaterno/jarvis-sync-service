"""Test search coverage - verify all chats are captured by our search chars."""
import httpx
import asyncio

BEEPER_TOKEN = "3d3dad13-3f2d-44bc-8489-c4c805b06458"
BEEPER_API = "http://localhost:23373/v1"

SEARCH_CHARS = ["a", "e", "i", "o", "u", "s", "t", "n", "r", "l", "0", "1", "2", "3", "4", "5", "@", "+"]


async def test_coverage():
    headers = {"Authorization": f"Bearer {BEEPER_TOKEN}"}
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Get chats from main endpoint
        main_resp = await client.get(f"{BEEPER_API}/chats?limit=100", headers=headers)
        main_chats = main_resp.json().get("items", [])
        main_ids = {c["id"] for c in main_chats}
        
        print(f"Main /chats endpoint returns: {len(main_chats)} chats")
        
        # Get all chats from search queries
        search_ids = set()
        for char in SEARCH_CHARS:
            resp = await client.get(f"{BEEPER_API}/chats/search?query={char}&limit=200", headers=headers)
            items = resp.json().get("items", [])
            for item in items:
                search_ids.add(item["id"])
        
        print(f"Search-based approach returns: {len(search_ids)} unique chats")
        
        # Check if any main chats are missing from search
        missing = main_ids - search_ids
        if missing:
            print(f"\n⚠️ WARNING: {len(missing)} chats from /chats NOT found via search!")
            for chat_id in list(missing)[:5]:
                chat = next((c for c in main_chats if c["id"] == chat_id), None)
                if chat:
                    print(f"  - {chat.get('title', '(no title)')}")
        else:
            print("\n✅ All chats from /chats are captured by search queries")
        
        # Extra chats found by search
        extra = search_ids - main_ids
        if extra:
            print(f"\n📈 Search found {len(extra)} EXTRA chats beyond /chats limit")


if __name__ == "__main__":
    asyncio.run(test_coverage())
