-- Migration: Add source_transcript_id to meetings and reflections tables
-- This column links records back to the transcript they were created from
-- Run this in Supabase SQL Editor

-- Add to meetings table
ALTER TABLE meetings ADD COLUMN IF NOT EXISTS source_transcript_id UUID;
CREATE INDEX IF NOT EXISTS idx_meetings_source_transcript ON meetings(source_transcript_id);

-- Add to reflections table
ALTER TABLE reflections ADD COLUMN IF NOT EXISTS source_transcript_id UUID;
CREATE INDEX IF NOT EXISTS idx_reflections_source_transcript ON reflections(source_transcript_id);

-- Add source_file column if missing (for tracking original audio file)
ALTER TABLE meetings ADD COLUMN IF NOT EXISTS source_file TEXT;
ALTER TABLE reflections ADD COLUMN IF NOT EXISTS source_file TEXT;

-- Comments
COMMENT ON COLUMN meetings.source_transcript_id IS 'UUID of the transcript record this meeting was created from';
COMMENT ON COLUMN reflections.source_transcript_id IS 'UUID of the transcript record this reflection was created from';
