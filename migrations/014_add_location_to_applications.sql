-- =============================================================================
-- ADD LOCATION FIELD TO APPLICATIONS
-- =============================================================================
-- Adds location field for tracking where applications/programs are based
-- Valid values: Remote, Singapore, USA, Europe, UK, Global, Asia, Other
-- =============================================================================

-- Add location column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'applications' AND column_name = 'location'
    ) THEN
        ALTER TABLE applications ADD COLUMN location TEXT;
        
        -- Add check constraint for valid values
        ALTER TABLE applications ADD CONSTRAINT applications_location_check 
            CHECK (location IS NULL OR location IN (
                'Remote', 'Singapore', 'USA', 'Europe', 'UK', 'Global', 'Asia', 'Other'
            ));
        
        -- Add index for filtering by location
        CREATE INDEX IF NOT EXISTS idx_applications_location ON applications(location);
        
        RAISE NOTICE 'Added location column to applications table';
    ELSE
        RAISE NOTICE 'Location column already exists';
    END IF;
END $$;

-- Update comment on table
COMMENT ON COLUMN applications.location IS 'Where the program is based: Remote, Singapore, USA, Europe, UK, Global, Asia, Other';
