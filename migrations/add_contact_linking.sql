-- ============================================================================
-- ADD CONTACT LINKING TO EMAILS & CALENDAR
-- ============================================================================
-- Run in Supabase SQL Editor
-- This adds contact_id linking to emails and calendar_events tables
-- ============================================================================

-- ============================================================================
-- STEP 1: Add contact_id to EMAILS
-- ============================================================================
ALTER TABLE emails 
ADD COLUMN IF NOT EXISTS contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_emails_contact_id 
ON emails(contact_id) 
WHERE contact_id IS NOT NULL;

COMMENT ON COLUMN emails.contact_id IS 'Contact linked to this email (sender for inbound, recipient for outbound)';


-- ============================================================================
-- STEP 2: Add contact_id to CALENDAR_EVENTS
-- ============================================================================
ALTER TABLE calendar_events 
ADD COLUMN IF NOT EXISTS contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_calendar_events_contact_id 
ON calendar_events(contact_id) 
WHERE contact_id IS NOT NULL;

COMMENT ON COLUMN calendar_events.contact_id IS 'Primary contact associated with this calendar event';


-- ============================================================================
-- STEP 3: Add contact_email to MEETINGS (for linking)
-- ============================================================================
ALTER TABLE meetings 
ADD COLUMN IF NOT EXISTS contact_email TEXT;

CREATE INDEX IF NOT EXISTS idx_meetings_contact_email 
ON meetings(contact_email) 
WHERE contact_email IS NOT NULL;


-- ============================================================================
-- STEP 4: Add interaction stats to CONTACTS
-- ============================================================================
ALTER TABLE contacts 
ADD COLUMN IF NOT EXISTS total_interactions INTEGER DEFAULT 0;

ALTER TABLE contacts 
ADD COLUMN IF NOT EXISTS last_interaction_date DATE;


-- ============================================================================
-- STEP 5: Create function to find contact by email
-- ============================================================================
CREATE OR REPLACE FUNCTION find_contact_by_email(email_address TEXT)
RETURNS UUID AS $$
DECLARE
    contact_uuid UUID;
    email_clean TEXT;
BEGIN
    IF email_address IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Clean email: extract just the address if in "Name <email>" format
    email_clean := LOWER(TRIM(email_address));
    IF email_clean LIKE '%<%>%' THEN
        email_clean := SUBSTRING(email_clean FROM '<([^>]+)>');
    END IF;
    
    -- Find contact by email
    SELECT id INTO contact_uuid
    FROM contacts
    WHERE LOWER(email) = email_clean
    AND deleted_at IS NULL
    LIMIT 1;
    
    RETURN contact_uuid;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- STEP 6: Create retroactive linking function
-- ============================================================================
CREATE OR REPLACE FUNCTION link_contact_interactions(contact_uuid UUID, contact_email TEXT)
RETURNS JSONB AS $$
DECLARE
    linked_emails INTEGER := 0;
    linked_meetings INTEGER := 0;
    linked_events INTEGER := 0;
    email_clean TEXT;
BEGIN
    IF contact_email IS NULL THEN
        RETURN jsonb_build_object('status', 'skipped', 'reason', 'no_email');
    END IF;

    -- Clean email
    email_clean := LOWER(TRIM(contact_email));
    IF email_clean LIKE '%<%>%' THEN
        email_clean := SUBSTRING(email_clean FROM '<([^>]+)>');
    END IF;

    -- Link emails where sender matches
    WITH updated AS (
        UPDATE emails
        SET contact_id = contact_uuid
        WHERE contact_id IS NULL
        AND (
            LOWER(sender) LIKE '%' || email_clean || '%'
            OR LOWER(recipient) LIKE '%' || email_clean || '%'
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO linked_emails FROM updated;

    -- Link meetings where contact_email matches
    WITH updated AS (
        UPDATE meetings
        SET contact_id = contact_uuid
        WHERE contact_id IS NULL
        AND LOWER(contact_email) = email_clean
        RETURNING 1
    )
    SELECT COUNT(*) INTO linked_meetings FROM updated;

    -- Link calendar events where organizer matches
    WITH updated AS (
        UPDATE calendar_events
        SET contact_id = contact_uuid
        WHERE contact_id IS NULL
        AND (
            LOWER(organizer->>'email') = email_clean
            OR EXISTS (
                SELECT 1 FROM jsonb_array_elements(attendees) AS att
                WHERE LOWER(att->>'email') = email_clean
            )
        )
        RETURNING 1
    )
    SELECT COUNT(*) INTO linked_events FROM updated;

    -- Update contact stats
    UPDATE contacts
    SET 
        total_interactions = (
            SELECT COUNT(*) FROM emails WHERE contact_id = contact_uuid
        ) + (
            SELECT COUNT(*) FROM meetings WHERE contact_id = contact_uuid
        ) + (
            SELECT COUNT(*) FROM calendar_events WHERE contact_id = contact_uuid
        ),
        last_interaction_date = GREATEST(
            (SELECT MAX(date)::date FROM emails WHERE contact_id = contact_uuid),
            (SELECT MAX(date)::date FROM meetings WHERE contact_id = contact_uuid),
            (SELECT MAX(start_time)::date FROM calendar_events WHERE contact_id = contact_uuid)
        ),
        updated_at = NOW()
    WHERE id = contact_uuid;

    RETURN jsonb_build_object(
        'status', 'success',
        'linked_emails', linked_emails,
        'linked_meetings', linked_meetings,
        'linked_events', linked_events
    );
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- STEP 7: Create auto-link trigger on contacts
-- ============================================================================
CREATE OR REPLACE FUNCTION trigger_auto_link_on_contact()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.email IS NOT NULL THEN
        PERFORM link_contact_interactions(NEW.id, NEW.email);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS auto_link_contact_interactions ON contacts;
CREATE TRIGGER auto_link_contact_interactions
AFTER INSERT OR UPDATE OF email ON contacts
FOR EACH ROW
EXECUTE FUNCTION trigger_auto_link_on_contact();


-- ============================================================================
-- STEP 8: Run retroactive linking for all existing contacts
-- ============================================================================
DO $$
DECLARE
    rec RECORD;
    result JSONB;
    total_emails INTEGER := 0;
    total_meetings INTEGER := 0;
    total_events INTEGER := 0;
BEGIN
    RAISE NOTICE 'Linking existing contacts to their interactions...';
    
    FOR rec IN SELECT id, email, first_name, last_name FROM contacts WHERE email IS NOT NULL AND deleted_at IS NULL
    LOOP
        result := link_contact_interactions(rec.id, rec.email);
        total_emails := total_emails + COALESCE((result->>'linked_emails')::int, 0);
        total_meetings := total_meetings + COALESCE((result->>'linked_meetings')::int, 0);
        total_events := total_events + COALESCE((result->>'linked_events')::int, 0);
    END LOOP;
    
    RAISE NOTICE 'Done! Linked % emails, % meetings, % calendar events', total_emails, total_meetings, total_events;
END $$;


-- ============================================================================
-- STEP 9: Create interaction_log view
-- ============================================================================
DROP VIEW IF EXISTS interaction_log CASCADE;

CREATE VIEW interaction_log AS
SELECT 
    'meeting' AS type,
    m.id,
    m.contact_id,
    c.first_name || ' ' || COALESCE(c.last_name, '') AS contact_name,
    m.title,
    m.summary AS description,
    m.date::timestamp with time zone AS date,
    m.created_at
FROM meetings m
LEFT JOIN contacts c ON m.contact_id = c.id
WHERE m.deleted_at IS NULL AND m.contact_id IS NOT NULL

UNION ALL

SELECT 
    'email' AS type,
    e.id,
    e.contact_id,
    c.first_name || ' ' || COALESCE(c.last_name, '') AS contact_name,
    e.subject AS title,
    e.snippet AS description,
    e.date AS date,
    e.created_at
FROM emails e
LEFT JOIN contacts c ON e.contact_id = c.id
WHERE e.contact_id IS NOT NULL

UNION ALL

SELECT 
    'calendar' AS type,
    ce.id,
    ce.contact_id,
    c.first_name || ' ' || COALESCE(c.last_name, '') AS contact_name,
    ce.summary AS title,
    ce.description,
    ce.start_time AS date,
    ce.created_at
FROM calendar_events ce
LEFT JOIN contacts c ON ce.contact_id = c.id
WHERE ce.contact_id IS NOT NULL

ORDER BY date DESC;

COMMENT ON VIEW interaction_log IS 'Unified view of all interactions (meetings, emails, calendar) linked to contacts';


-- ============================================================================
-- DONE!
-- ============================================================================
