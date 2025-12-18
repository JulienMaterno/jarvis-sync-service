import os
import json
import asyncio
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

# Configuration - use environment variables
CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID")
CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET")

if not CLIENT_ID or not CLIENT_SECRET:
    print("Error: Set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET environment variables")
    exit(1)

# Scopes required for Bot and Audio Pipeline
SCOPES = ["https://www.googleapis.com/auth/drive"]

def get_new_token():
    print("--- Google Drive Token Generator ---")
    print("This script will generate a new token.json for the Bot and Audio Pipeline.")
    print(f"Using Client ID: {CLIENT_ID}")
    
    # Create client config dictionary
    client_config = {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": ["http://localhost"]
        }
    }

    # Run the flow
    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0)

    # Convert to JSON
    token_json = creds.to_json()
    
    print("\n✅ SUCCESS! Here is your new GOOGLE_TOKEN_JSON content:")
    print("=" * 80)
    print(token_json)
    print("=" * 80)
    print("\nACTION REQUIRED:")
    print("1. Copy the JSON content above.")
    print("2. Run the following command to update the secret in Google Cloud:")
    print('   gcloud secrets versions add GOOGLE_TOKEN_JSON --data-file=-')
    print("   (Paste the JSON when prompted, then press Ctrl+Z and Enter on Windows)")

if __name__ == "__main__":
    try:
        get_new_token()
    except ImportError:
        print("Please install required packages: pip install google-auth-oauthlib")
