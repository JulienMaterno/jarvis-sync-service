-- Migration 022: Add Content Field to Contacts and Meetings
-- Unifies Notion page content extraction across all entities
-- Enables rich biographical details for contacts and meeting notes

-- ============================================================================
-- ADD CONTENT COLUMN TO MEETINGS
-- ============================================================================

-- Add content field for full meeting notes from Notion page body
ALTER TABLE meetings
ADD COLUMN IF NOT EXISTS content TEXT;

COMMENT ON COLUMN meetings.content IS
  'Full meeting content extracted from Notion page body (notes, discussion points, action items, etc.)';

-- ============================================================================
-- RENAME profile_content TO content IN CONTACTS (for consistency)
-- ============================================================================

-- Contacts already have profile_content field (from migration 017)
-- We'll keep it as-is for now to avoid data migration issues
-- But add a comment clarifying it's the same as content in other tables

COMMENT ON COLUMN contacts.profile_content IS
  'Full contact content extracted from Notion page body (personal details, investment info, background notes, etc.).
   Syncs bidirectionally with Google Contacts notes field.
   Equivalent to "content" field in other tables (meetings, reflections, journals, applications).';

-- ============================================================================
-- ADD SECTIONS JSONB FOR STRUCTURED CONTENT (OPTIONAL)
-- ============================================================================

-- Add structured sections storage (matching reflections pattern)
ALTER TABLE contacts
ADD COLUMN IF NOT EXISTS sections JSONB DEFAULT '[]'::jsonb;

ALTER TABLE meetings
ADD COLUMN IF NOT EXISTS sections JSONB DEFAULT '[]'::jsonb;

COMMENT ON COLUMN contacts.sections IS
  'Structured sections extracted from Notion page (array of {heading, content} objects).
   Example: [{"heading": "Investment Details", "content": "Series A investor..."}, ...]';

COMMENT ON COLUMN meetings.sections IS
  'Structured sections extracted from Notion page (array of {heading, content} objects).
   Example: [{"heading": "Discussion", "content": "..."}, {"heading": "Action Items", "content": "..."}]';

-- ============================================================================
-- INDEXES FOR SEARCH
-- ============================================================================

-- Full-text search indexes for content (if needed)
-- Uncomment if you want to enable full-text search on content fields

-- CREATE INDEX IF NOT EXISTS idx_contacts_content_search ON contacts USING gin(to_tsvector('english', profile_content));
-- CREATE INDEX IF NOT EXISTS idx_meetings_content_search ON meetings USING gin(to_tsvector('english', content));

-- ============================================================================
-- VALIDATION
-- ============================================================================

DO $$
DECLARE
  contacts_count INTEGER;
  meetings_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO contacts_count FROM contacts WHERE profile_content IS NOT NULL;
  SELECT COUNT(*) INTO meetings_count FROM meetings WHERE content IS NOT NULL;

  RAISE NOTICE 'Migration 022 complete: Content fields added';
  RAISE NOTICE '  Contacts with profile_content: %', contacts_count;
  RAISE NOTICE '  Meetings with content: %', meetings_count;
  RAISE NOTICE '';
  RAISE NOTICE 'Next steps:';
  RAISE NOTICE '  1. Update sync services to extract Notion page content';
  RAISE NOTICE '  2. Run full sync to populate content fields';
  RAISE NOTICE '  3. Verify bidirectional sync with Google Contacts notes';
END $$;
