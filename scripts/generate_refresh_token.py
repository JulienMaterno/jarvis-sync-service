#!/usr/bin/env python3
"""
Generate a new Google OAuth refresh token with full read/write scopes.

Run this locally to get a new refresh token, then update the 
GOOGLE_REFRESH_TOKEN secret in Google Cloud Secret Manager.

PREREQUISITES:
1. Go to https://console.cloud.google.com/apis/credentials
2. Find your OAuth 2.0 Client ID (or create one if needed)
3. Download the JSON credentials file
4. Rename it to 'credentials.json' and place in this directory

HOW TO ENABLE WRITE SCOPES:
1. Go to https://console.cloud.google.com/apis/library
2. Enable these APIs if not already enabled:
   - Gmail API
   - Google Calendar API
   - Google People API (for contacts)
   - Google Drive API
3. Go to OAuth consent screen: https://console.cloud.google.com/apis/credentials/consent
4. Make sure your app has the required scopes listed
5. Run this script to generate a token with those scopes

Usage:
    pip install google-auth-oauthlib
    python generate_refresh_token.py
"""

import os
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# SCOPES - These determine what access you have
# Full read/write scopes for all services
SCOPES = [
    # Gmail - full access including send and drafts
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.compose',  # Create/modify drafts
    'https://www.googleapis.com/auth/gmail.modify',   # Modify messages/labels
    
    # Calendar - full access including create/edit
    'https://www.googleapis.com/auth/calendar',
    'https://www.googleapis.com/auth/calendar.events',
    
    # Contacts - for looking up people
    'https://www.googleapis.com/auth/contacts.readonly',
    
    # Google Drive - for audio pipeline
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file',
]

def main():
    creds = None
    token_file = 'token.json'
    creds_file = 'credentials.json'
    
    # Check for existing credentials file
    if not os.path.exists(creds_file):
        print("=" * 60)
        print("ERROR: credentials.json not found!")
        print("=" * 60)
        print("""
To get this file:

1. Go to https://console.cloud.google.com/apis/credentials

2. Look for your OAuth 2.0 Client ID under "OAuth 2.0 Client IDs"
   - If you don't have one, click "+ CREATE CREDENTIALS" → "OAuth client ID"
   - Select "Desktop app" as application type

3. Click on the OAuth client name to open it

4. Click "DOWNLOAD JSON" button at the top

5. Rename the downloaded file to 'credentials.json'
   and place it in this directory:
   {cwd}
""".format(cwd=os.getcwd()))
        return
    
    # Delete existing token to force re-authentication with new scopes
    if os.path.exists(token_file):
        print(f"Removing existing {token_file} to request new scopes...")
        os.remove(token_file)
    
    print("=" * 60)
    print("Starting OAuth flow...")
    print("=" * 60)
    print(f"\nRequesting these scopes:")
    for scope in SCOPES:
        print(f"  • {scope.split('/')[-1]}")
    print()
    
    flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
    creds = flow.run_local_server(port=8080)
    
    # Save for future use
    with open(token_file, 'w') as f:
        f.write(creds.to_json())
    print(f"Token saved to {token_file}")
    
    # Print the tokens
    print("\n" + "=" * 60)
    print("✅ TOKENS GENERATED SUCCESSFULLY")
    print("=" * 60)
    
    print(f"\n📌 REFRESH TOKEN (copy this):\n")
    print("-" * 60)
    print(creds.refresh_token)
    print("-" * 60)
    
    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print("""
1. Copy the REFRESH TOKEN above

2. Update Google Cloud Secret Manager:
   
   Option A - Using gcloud CLI:
   echo "YOUR_REFRESH_TOKEN" | gcloud secrets versions add GOOGLE_REFRESH_TOKEN --data-file=-
   
   Option B - Using Console:
   https://console.cloud.google.com/security/secret-manager
   → Find GOOGLE_REFRESH_TOKEN → "NEW VERSION" → Paste token

3. Redeploy the sync service:
   cd jarvis-sync-service && git commit --allow-empty -m "Trigger redeploy" && git push
   
   Or wait for the next push to trigger Cloud Build.

4. Test the new capabilities:
   
   # Test calendar creation
   curl -X POST "https://jarvis-sync-service-xxx.run.app/calendar/create" \\
     -H "Content-Type: application/json" \\
     -d '{"summary": "Test Event", "start_time": "2025-01-01T10:00:00", "end_time": "2025-01-01T11:00:00"}'
   
   # Test email draft creation
   curl -X POST "https://jarvis-sync-service-xxx.run.app/gmail/drafts" \\
     -H "Content-Type: application/json" \\
     -d '{"to": "test@example.com", "subject": "Test", "body": "Hello!"}'
""")


if __name__ == "__main__":
    main()
