"""Test evening journal generation."""
import asyncio
from reports import generate_evening_journal_prompt

async def test():
    result = await generate_evening_journal_prompt()
    print(f'Result status: {result.get("status")}')
    print(f'Highlights: {len(result.get("highlights", []))}')
    print(f'Questions: {len(result.get("questions", []))}')
    print(f'Observations: {len(result.get("observations", []))}')
    print(f'Activity summary: {result.get("activity_summary")}')

if __name__ == "__main__":
    asyncio.run(test())
