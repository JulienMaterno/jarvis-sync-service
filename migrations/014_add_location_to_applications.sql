-- =============================================================================
-- ADD LOCATION FIELD TO APPLICATIONS
-- =============================================================================
-- Adds location field for tracking where applications/programs are based
-- Simple TEXT field - Notion handles select options automatically
-- =============================================================================

-- Add location column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'applications' AND column_name = 'location'
    ) THEN
        ALTER TABLE applications ADD COLUMN location TEXT;
        
        -- Add index for filtering by location
        CREATE INDEX IF NOT EXISTS idx_applications_location ON applications(location);
        
        RAISE NOTICE 'Added location column to applications table';
    ELSE
        RAISE NOTICE 'Location column already exists';
    END IF;
END $$;

-- Drop constraint if it exists (in case migration was run with old version)
ALTER TABLE applications DROP CONSTRAINT IF EXISTS applications_location_check;

-- Update comment on table
COMMENT ON COLUMN applications.location IS 'Where the program is based: Remote, Singapore, USA, Europe, UK, Global, Asia, Other';
