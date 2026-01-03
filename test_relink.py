"""Test the Beeper relink function."""
import asyncio
from sync_beeper import run_beeper_relink
from lib.supabase_client import supabase


async def test():
    print("Running Beeper relink...")
    result = await run_beeper_relink(supabase)
    print("\nRelink results:")
    for k, v in result.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    asyncio.run(test())
