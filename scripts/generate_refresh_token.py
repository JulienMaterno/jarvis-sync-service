#!/usr/bin/env python3
"""
Generate a new Google OAuth refresh token with full read/write scopes.

Run this locally to get a new refresh token, then update the 
GOOGLE_REFRESH_TOKEN secret in Google Cloud Secret Manager.

Usage:
    1. Ensure you have credentials.json (OAuth client) in this directory
    2. Run: python generate_refresh_token.py
    3. Complete OAuth flow in browser
    4. Copy the refresh token and update Secret Manager
"""

import os
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# SCOPES - These determine what access you have
# READ + WRITE scopes for all services
SCOPES = [
    # Gmail - full access including send
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.send',
    'https://www.googleapis.com/auth/gmail.modify',
    
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
        print("ERROR: credentials.json not found!")
        print("\nTo get this file:")
        print("1. Go to https://console.cloud.google.com/apis/credentials")
        print("2. Click on your OAuth 2.0 Client")
        print("3. Download JSON and save as 'credentials.json' in this directory")
        return
    
    # Check if we have existing token
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    
    # If no valid credentials, do OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing expired token...")
            creds.refresh(Request())
        else:
            print("Starting OAuth flow...")
            print(f"Requesting scopes: {SCOPES}")
            flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
            creds = flow.run_local_server(port=8080)
        
        # Save for future use
        with open(token_file, 'w') as f:
            f.write(creds.to_json())
        print(f"\nToken saved to {token_file}")
    
    # Print the tokens
    print("\n" + "="*60)
    print("TOKENS GENERATED SUCCESSFULLY")
    print("="*60)
    
    print(f"\n📌 REFRESH TOKEN (save to Secret Manager):\n")
    print(creds.refresh_token)
    
    print(f"\n📌 ACCESS TOKEN (temporary, expires ~1hr):\n")
    print(creds.token[:50] + "...")
    
    print("\n" + "="*60)
    print("NEXT STEPS:")
    print("="*60)
    print("""
1. Copy the REFRESH TOKEN above

2. Update Secret Manager:
   gcloud secrets versions add GOOGLE_REFRESH_TOKEN --data-file=-
   (then paste the token and press Ctrl+D)
   
   OR use the console: https://console.cloud.google.com/security/secret-manager

3. Redeploy the sync service (or wait for next deploy)

4. Test:
   curl -X POST https://jarvis-sync-service-xxx.run.app/calendar/create \\
     -H "Content-Type: application/json" \\
     -d '{"summary": "Test", "start_time": "2025-01-01T10:00:00", "end_time": "2025-01-01T11:00:00"}'
""")


if __name__ == "__main__":
    main()
