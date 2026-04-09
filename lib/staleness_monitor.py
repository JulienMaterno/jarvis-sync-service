"""
Withings data freshness monitor.

Runs at 15:00 SGT via cron. If no sleep data has synced for today,
sends a Telegram reminder to open the Withings app.

Withings compresses raw HR/HRV data after ~24h, so syncing promptly
preserves the detailed epoch data needed for custom sleep staging.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

SGT = timedelta(hours=8)


def check_withings_freshness(supabase_client: Any) -> dict[str, Any]:
    """Check if today's sleep data has arrived. Send reminder if missing.

    Args:
        supabase_client: Supabase client instance.

    Returns:
        Status dict with action taken.
    """
    from lib.telegram_client import send_telegram_message

    now_sgt = datetime.now(timezone.utc) + SGT
    today = now_sgt.strftime("%Y-%m-%d")
    yesterday = (now_sgt - timedelta(days=1)).strftime("%Y-%m-%d")

    # Check for today's sleep data in custom staging
    try:
        resp = supabase_client.table("health_sleep_custom").select(
            "sleep_date,processed_at"
        ).gte("sleep_date", yesterday).order("sleep_date", desc=True).limit(1).execute()
    except Exception as e:
        logger.error(f"Freshness check query failed: {e}")
        return {"status": "error", "detail": str(e)}

    if resp.data:
        latest_date = resp.data[0]["sleep_date"]
        # If the latest sleep_date is today or yesterday, data is fresh
        if str(latest_date)[:10] >= yesterday:
            logger.info(f"Sleep data fresh: latest={latest_date}")
            return {"status": "data_present", "latest_sleep_date": str(latest_date)[:10]}

    # No recent data, send reminder
    msg = (
        "*Sleep Data Missing*\n\n"
        "No sleep data has synced today. "
        "Open your Withings app to trigger a sync before "
        "detailed HR/HRV data gets compressed.\n\n"
        "_Withings compresses raw data after ~24h, "
        "which degrades the custom sleep analysis._"
    )

    logger.info(f"No sleep data for {today}, sending reminder")
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(send_telegram_message(msg, force=True))
    finally:
        loop.close()

    return {"status": "reminder_sent", "date": today}
