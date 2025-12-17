-- Add last_sync_source column to meetings table for bidirectional sync tracking
-- This column tracks whether the last update came from 'notion' or 'supabase'
-- to prevent ping-pong updates with proper timestamp buffering

ALTER TABLE meetings 
ADD COLUMN IF NOT EXISTS last_sync_source TEXT;

-- Add comment for documentation
COMMENT ON COLUMN meetings.last_sync_source IS 'Tracks the source of the last sync operation: notion or supabase';

-- Create index for faster filtering during incremental syncs
CREATE INDEX IF NOT EXISTS idx_meetings_last_sync_source ON meetings(last_sync_source);
