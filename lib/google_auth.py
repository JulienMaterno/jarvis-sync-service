import os
import httpx
from typing import Optional

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

async def get_access_token() -> str:
    """
    Exchanges the refresh token for a new access token.
    """
    client_id = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise ValueError("Missing Google OAuth credentials in environment variables.")

    async with httpx.AsyncClient() as client:
        response = await client.post(GOOGLE_TOKEN_URL, data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        })
        response.raise_for_status()
        data = response.json()
        return data["access_token"]
