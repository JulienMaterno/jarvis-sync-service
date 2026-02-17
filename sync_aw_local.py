"""
Local ActivityWatch sync script.
Run via Windows Task Scheduler every 15 minutes to keep Supabase up to date.

Usage: python sync_aw_local.py
"""
import asyncio
import sys
import logging
from sync_activitywatch import run_activitywatch_sync

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def main():
    try:
        result = await run_activitywatch_sync(hours=2, full=False)
        status = result.get("status", "unknown")
        events = result.get("total_events", 0)
        logger.info(f"AW sync: {status}, {events} events")
    except Exception as e:
        logger.error(f"AW sync failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
