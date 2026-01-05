import os
import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAYS = [1, 2, 4]  # Exponential backoff in seconds


async def get_access_token() -> str:
    """
    Exchanges the refresh token for a new access token.
    Includes retry logic for transient failures.
    """
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Missing Google OAuth credentials in environment variables.")

    last_error = None
    
    async with httpx.AsyncClient() as client:
        for attempt in range(MAX_RETRIES):
            try:
                response = await client.post(GOOGLE_TOKEN_URL, data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                }, timeout=10.0)
                response.raise_for_status()
                data = response.json()
                
                if attempt > 0:
                    logger.info(f"Google token refresh succeeded on attempt {attempt + 1}")
                    
                return data["access_token"]
                
            except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    logger.warning(f"Google token refresh failed (attempt {attempt + 1}/{MAX_RETRIES}), retrying in {delay}s: {e}")
                    import asyncio
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Google token refresh failed after {MAX_RETRIES} attempts: {e}")
    
    raise last_error or Exception("Google token refresh failed")
