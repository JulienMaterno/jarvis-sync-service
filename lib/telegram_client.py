import os
import httpx
import logging
import asyncio
import traceback

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_BOT_SERVICE_URL = os.environ.get("TELEGRAM_BOT_SERVICE_URL")

async def send_telegram_message(text: str):
    """
    Sends a message to the configured Telegram chat.
    """
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID not configured. Skipping message.")
        return

    if TELEGRAM_BOT_SERVICE_URL:
        # Use internal bot service
        url = f"{TELEGRAM_BOT_SERVICE_URL}/send_message"
        payload = {
            "chat_id": int(TELEGRAM_CHAT_ID),
            "text": text,
            "parse_mode": "Markdown"
        }
    elif TELEGRAM_BOT_TOKEN:
        # Use direct Telegram API (Legacy)
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown"
        }
    else:
        logger.warning("Telegram credentials not configured. Skipping message.")
        return

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=10.0)
            response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

async def notify_error(context: str, error: str):
    """
    Sends an error alert with traceback if available.
    """
    # Get full traceback
    tb = traceback.format_exc()
    
    # If the error string is just the message, the traceback gives more context.
    # If traceback is "NoneType: None", it means no active exception, so just use the error msg.
    if "NoneType: None" in tb:
        details = error
    else:
        # Truncate traceback to avoid Telegram message limit (4096 chars)
        details = f"{error}\n\nTraceback:\n{tb}"[:3000]

    message = f"🚨 **Error in {context}**\n\n```\n{details}\n```"
    await send_telegram_message(message)
