"""
Add last_sync_source column to meetings table

This script adds the missing column that's required for the robust
bidirectional sync to track whether updates came from Notion or Supabase.
"""

from lib.supabase_client import supabase
import sys

def add_last_sync_source_column():
    """Add last_sync_source column if it doesn't exist."""
    
    # Check if column exists by trying to query it
    try:
        result = supabase.table('meetings').select('last_sync_source').limit(1).execute()
        print("✓ Column 'last_sync_source' already exists")
        return True
    except Exception as e:
        if 'column' in str(e).lower() and 'does not exist' in str(e).lower():
            print("Column 'last_sync_source' not found, needs to be added manually")
            print("\nPlease run this SQL in your Supabase SQL Editor:")
            print("-" * 60)
            print("ALTER TABLE meetings ADD COLUMN last_sync_source TEXT;")
            print("COMMENT ON COLUMN meetings.last_sync_source IS 'Tracks sync source: notion or supabase';")
            print("CREATE INDEX idx_meetings_last_sync_source ON meetings(last_sync_source);")
            print("-" * 60)
            return False
        else:
            print(f"Error checking column: {e}")
            return False

if __name__ == '__main__':
    success = add_last_sync_source_column()
    sys.exit(0 if success else 1)
