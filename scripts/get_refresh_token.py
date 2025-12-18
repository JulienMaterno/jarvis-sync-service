import os
import json
import httpx
import asyncio
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

# Configuration
# You can either set these env vars or paste them when prompted
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")

# Scopes required for the sync service
SCOPES = [
    "https://www.googleapis.com/auth/contacts",          # Read/Write Contacts
    "https://www.googleapis.com/auth/calendar.readonly", # Read Calendar
    "https://www.googleapis.com/auth/gmail.readonly",    # Read Emails
    "https://www.googleapis.com/auth/userinfo.email"     # Identity
]

REDIRECT_URI = "http://localhost"  # Standard for manual copy-paste flow

async def get_refresh_token():
    print("--- Google OAuth Refresh Token Generator ---")
    
    c_id = CLIENT_ID or input("Enter your Google Client ID: ").strip()
    c_secret = CLIENT_SECRET or input("Enter your Google Client Secret: ").strip()
    
    if not c_id or not c_secret:
        print("Error: Client ID and Secret are required.")
        return

    # 1. Generate Authorization URL
    params = {
        "client_id": c_id,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline", # Crucial for getting a refresh token
        "prompt": "consent"       # Force consent screen to ensure refresh token is returned
    }
    
    auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    
    print("\n1. Visit this URL in your browser:")
    print("-" * 80)
    print(auth_url)
    print("-" * 80)
    
    print("\n2. Authorize the app.")
    print("3. You will be redirected to a URL that looks like 'http://localhost/?code=...'")
    print("   (The page might fail to load, that's fine. Just copy the URL from the address bar)")
    
    # 2. Get Code
    redirect_response = input("\nPaste the full redirect URL (or just the code): ").strip()
    
    code = ""
    if "code=" in redirect_response:
        code = redirect_response.split("code=")[1].split("&")[0]
    else:
        code = redirect_response
        
    if not code:
        print("Error: Could not extract authorization code.")
        return

    # 3. Exchange Code for Token
    token_url = "https://oauth2.googleapis.com/token"
    data = {
        "client_id": c_id,
        "client_secret": c_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI
    }
    
    print("\nExchanging code for tokens...")
    async with httpx.AsyncClient() as client:
        response = await client.post(token_url, data=data)
        
        if response.status_code == 200:
            tokens = response.json()
            refresh_token = tokens.get("refresh_token")
            
            print("\n✅ SUCCESS! Here is your new Refresh Token:")
            print("=" * 80)
            print(refresh_token)
            print("=" * 80)
            print("\nUpdate your .env file and Cloud Run environment variable:")
            print(f"GOOGLE_REFRESH_TOKEN={refresh_token}")
        else:
            print("\n❌ Error exchanging token:")
            print(response.text)

if __name__ == "__main__":
    # Install httpx if needed: pip install httpx
    try:
        asyncio.run(get_refresh_token())
    except KeyboardInterrupt:
        print("\nCancelled.")
