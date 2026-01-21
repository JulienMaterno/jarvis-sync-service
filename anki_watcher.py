"""
Anki Watcher - Detects when Anki Desktop opens/closes and triggers sync.

This runs alongside the sync service and watches for Anki Desktop to start/stop,
triggering syncs automatically when you actually use Anki.

Usage:
    python anki_watcher.py
"""

import asyncio
import aiohttp
import logging
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SYNC_SERVICE_URL = "http://localhost:8000"
ANKI_CONNECT_URL = "http://localhost:8765"
CHECK_INTERVAL = 10  # Check every 10 seconds


async def check_anki_running() -> bool:
    """Check if Anki Desktop is running by testing AnkiConnect."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                ANKI_CONNECT_URL,
                json={"action": "version", "version": 6},
                timeout=aiohttp.ClientTimeout(total=2)
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result.get("error") is None
    except Exception:
        pass
    return False


async def trigger_sync() -> bool:
    """Trigger Anki sync on the sync service."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{SYNC_SERVICE_URL}/sync/anki/manual",
                timeout=aiohttp.ClientTimeout(total=120)  # 2 min timeout
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    status = result.get("status")
                    if status == "success":
                        import_stats = result.get("import_stats", {})
                        cards_stats = result.get("cards_stats", {})
                        reviews_stats = result.get("reviews_stats", {})

                        logger.info(f"✓ Sync completed successfully:")
                        if import_stats:
                            logger.info(f"  - Imported: {import_stats.get('cards_imported', 0)} cards")
                        if cards_stats:
                            logger.info(f"  - Created: {cards_stats.get('cards_created', 0)} cards in Anki")
                        if reviews_stats:
                            logger.info(f"  - Synced: {reviews_stats.get('reviews_imported', 0)} reviews")
                        return True
                    else:
                        logger.error(f"✗ Sync failed: {result.get('error_message', 'Unknown error')}")
                        return False
                else:
                    logger.error(f"✗ Sync request failed: HTTP {resp.status}")
                    return False
    except asyncio.TimeoutError:
        logger.error("✗ Sync timed out (took > 2 minutes)")
        return False
    except Exception as e:
        logger.error(f"✗ Sync error: {e}")
        return False


async def watch_anki():
    """Watch for Anki Desktop to open/close and trigger syncs."""
    logger.info("=" * 80)
    logger.info("ANKI WATCHER - Automatic Sync on Anki Open/Close")
    logger.info("=" * 80)
    logger.info("")
    logger.info("Watching for Anki Desktop...")
    logger.info(f"  - Checks every {CHECK_INTERVAL} seconds")
    logger.info(f"  - Syncs when Anki opens")
    logger.info(f"  - Syncs when Anki closes")
    logger.info("")
    logger.info("Press Ctrl+C to stop")
    logger.info("")

    anki_was_running = False
    last_sync_time = None

    while True:
        try:
            anki_is_running = await check_anki_running()

            # Anki just started
            if anki_is_running and not anki_was_running:
                logger.info("━" * 80)
                logger.info(f"⚡ Anki Desktop OPENED at {datetime.now().strftime('%H:%M:%S')}")
                logger.info("━" * 80)
                logger.info("Triggering sync to import any changes from Anki...")

                success = await trigger_sync()
                if success:
                    last_sync_time = time.time()
                    logger.info("✓ Initial sync complete")
                else:
                    logger.warning("⚠ Initial sync failed, will retry on next check")

                logger.info("")
                logger.info("Watching for Anki to close...")
                logger.info("")

            # Anki just closed
            elif not anki_is_running and anki_was_running:
                logger.info("━" * 80)
                logger.info(f"⏹ Anki Desktop CLOSED at {datetime.now().strftime('%H:%M:%S')}")
                logger.info("━" * 80)
                logger.info("Triggering final sync to capture any reviews...")

                success = await trigger_sync()
                if success:
                    last_sync_time = time.time()
                    logger.info("✓ Final sync complete")
                else:
                    logger.warning("⚠ Final sync failed")

                logger.info("")
                logger.info("Waiting for Anki to open again...")
                logger.info("")

            anki_was_running = anki_is_running

            await asyncio.sleep(CHECK_INTERVAL)

        except KeyboardInterrupt:
            logger.info("")
            logger.info("=" * 80)
            logger.info("Stopping Anki Watcher...")
            logger.info("=" * 80)
            break
        except Exception as e:
            logger.error(f"Error in watch loop: {e}")
            await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(watch_anki())
