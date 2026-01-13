-- Migration: Add contact_type and profile fields to contacts
-- Purpose: 
--   1. Categorize contacts as Friends, Family, Business, or Other
--   2. Add profile_content for accumulated learnings about the person
--   3. Add linkedin_data for structured LinkedIn profile data

-- Add contact_type column (matches Notion "Type" select property)
ALTER TABLE contacts 
ADD COLUMN IF NOT EXISTS contact_type TEXT;

-- Add profile content (accumulated learnings, similar to reflections)
ALTER TABLE contacts 
ADD COLUMN IF NOT EXISTS profile_content TEXT;

-- Add LinkedIn data as structured JSON (from BrightData scraping)
ALTER TABLE contacts 
ADD COLUMN IF NOT EXISTS linkedin_data JSONB;

-- Track when profile was last enriched
ALTER TABLE contacts 
ADD COLUMN IF NOT EXISTS profile_enriched_at TIMESTAMPTZ;

-- Add indexes
CREATE INDEX IF NOT EXISTS idx_contacts_type ON contacts(contact_type);
CREATE INDEX IF NOT EXISTS idx_contacts_profile_enriched ON contacts(profile_enriched_at);

-- Comments
COMMENT ON COLUMN contacts.contact_type IS 'Contact category: Friends, Family, Business, Other (synced with Notion Type property)';
COMMENT ON COLUMN contacts.profile_content IS 'Accumulated learnings about this person from meetings, calls, etc.';
COMMENT ON COLUMN contacts.linkedin_data IS 'Structured LinkedIn profile data from BrightData API scraping';
COMMENT ON COLUMN contacts.profile_enriched_at IS 'When the profile was last enriched with new data';
