"""
Reactive sleep briefing: fires once per night after new sleep data arrives.

Instead of a fixed cron, this runs after Withings sleep staging completes.
Guards against duplicate briefings by checking health_daily_briefings.
Includes retry logic for transient API failures.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

SGT = timedelta(hours=8)
MAX_RETRIES = 2
RETRY_DELAY_S = 30


def run_sleep_briefing_if_needed(supabase_client: Any) -> dict[str, Any] | None:
    """Check if today's briefing was already sent; if not, generate and deliver it.

    Args:
        supabase_client: Supabase client instance.

    Returns:
        Briefing result dict, or None if skipped.
    """
    now_sgt = datetime.now(timezone.utc) + SGT
    today_sgt = now_sgt.strftime("%Y-%m-%d")

    # Only send briefings between 6 AM and 6 PM SGT (covers late data arrival)
    if now_sgt.hour < 6 or now_sgt.hour >= 18:
        logger.debug(f"Outside briefing window (SGT {now_sgt.hour}:00), skipping")
        return None

    # Check if briefing already sent today
    try:
        resp = supabase_client.table("health_daily_briefings").select(
            "id,generated_at"
        ).gte("generated_at", f"{today_sgt}T00:00:00+08:00").limit(1).execute()
        if resp.data:
            logger.debug(f"Briefing already sent today ({today_sgt}), skipping")
            return None
    except Exception as e:
        logger.warning(f"Could not check existing briefings: {e}")
        # Continue anyway; duplicate is better than missing

    # Check that we have fresh sleep data (custom staging processed today)
    try:
        yesterday = (now_sgt - timedelta(days=1)).strftime("%Y-%m-%d")
        resp = supabase_client.table("health_sleep_custom").select(
            "sleep_date,processed_at"
        ).gte("sleep_date", yesterday).order(
            "sleep_date", desc=True
        ).limit(1).execute()

        if not resp.data:
            logger.info("No recent custom sleep data, skipping briefing")
            return None

        last_processed = resp.data[0].get("processed_at", "")
        logger.info(f"Latest sleep staging: date={resp.data[0]['sleep_date']}, processed={last_processed}")
    except Exception as e:
        logger.warning(f"Could not check sleep data: {e}")
        return None

    # Generate and send briefing with retry
    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            from lib.health_insights import generate_daily_briefing, format_daily_telegram
            from lib.telegram_client import send_telegram_message
            import asyncio

            logger.info(f"Generating reactive daily health briefing (attempt {attempt + 1})")
            result = generate_daily_briefing(supabase_client)

            msg = format_daily_telegram(result)
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(send_telegram_message(msg))
            finally:
                loop.close()

            logger.info(f"Daily health briefing delivered: {len(result.get('briefing_text', ''))} chars")

            # Send a trends chart alongside the text briefing
            try:
                from lib.health_charts import generate_multi_night_trends
                from lib.telegram_client import send_telegram_photo

                custom_resp = supabase_client.table("health_sleep_custom").select(
                    "sleep_date,duration_deep_s,duration_light_s,duration_rem_s,"
                    "duration_awake_s,duration_total_s,custom_sleep_score"
                ).order("sleep_date", desc=True).limit(14).execute()

                if custom_resp.data:
                    chart_bytes = generate_multi_night_trends(custom_resp.data, 14)
                    loop2 = asyncio.new_event_loop()
                    try:
                        loop2.run_until_complete(
                            send_telegram_photo(chart_bytes, caption="Sleep trends (14 nights)")
                        )
                    finally:
                        loop2.close()
                    logger.info("Trends chart sent with daily briefing")
            except Exception as chart_err:
                logger.warning(f"Failed to send trends chart (non-fatal): {chart_err}")

            return result

        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                logger.warning(f"Briefing attempt {attempt + 1} failed: {e}. Retrying in {RETRY_DELAY_S}s...")
                time.sleep(RETRY_DELAY_S)
            else:
                logger.error(f"Failed to generate/send daily briefing after {MAX_RETRIES + 1} attempts: {e}", exc_info=True)

    return None
