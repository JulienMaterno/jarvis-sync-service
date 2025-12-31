-- Migration: Beeper Integration (WhatsApp, LinkedIn, Telegram, etc.)
-- Date: 2024-12-31
-- Description: Tables for storing Beeper chat and message history

-- ============================================
-- 1. BEEPER CHATS (conversation metadata)
-- ============================================
CREATE TABLE IF NOT EXISTS beeper_chats (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    beeper_chat_id TEXT UNIQUE NOT NULL,        -- Beeper's internal ID (e.g., "!abc123:beeper.local")
    
    -- Origin/Platform
    account_id TEXT NOT NULL,                    -- Beeper account ID (e.g., "whatsapp", "li_urn:12345")
    platform TEXT NOT NULL,                      -- Normalized: 'whatsapp', 'linkedin', 'telegram', 'signal', 'imessage', etc.
    
    -- Chat info
    chat_type TEXT DEFAULT 'dm',                 -- 'dm' (direct message), 'group', 'channel'
    chat_name TEXT,                              -- Display name (person's name or group name)
    participant_count INT DEFAULT 1,
    
    -- For DMs: the other person's info (extracted from Beeper)
    remote_user_id TEXT,                         -- Their Beeper/platform user ID
    remote_user_name TEXT,                       -- Their display name at time of sync
    remote_phone TEXT,                           -- Phone number (WhatsApp/Signal) - normalized E.164 format
    remote_email TEXT,                           -- Email if available
    remote_linkedin_id TEXT,                     -- LinkedIn username (extracted from Beeper ID)
    remote_telegram_username TEXT,               -- Telegram @username if available
    
    -- State
    last_message_at TIMESTAMPTZ,
    last_message_preview TEXT,                   -- First 100 chars of last message
    last_message_is_outgoing BOOLEAN,            -- Did I send the last message?
    last_message_type TEXT DEFAULT 'text',       -- Type of last message (text, image, file, voice, etc.)
    is_archived BOOLEAN DEFAULT FALSE,           -- Archived = "answered" in inbox-zero workflow
    is_muted BOOLEAN DEFAULT FALSE,
    unread_count INT DEFAULT 0,
    
    -- Inbox-zero workflow: Does this chat need a response?
    -- TRUE if: not archived AND last message was incoming (not from me)
    -- This is computed/updated on sync
    needs_response BOOLEAN DEFAULT FALSE,
    archived_at TIMESTAMPTZ,                     -- When was it last archived
    
    -- Contact linking
    contact_id UUID REFERENCES contacts(id),
    contact_link_method TEXT,                    -- 'phone', 'linkedin', 'telegram', 'email', 'name', 'manual', NULL
    contact_link_confidence FLOAT,               -- 0.0-1.0 for fuzzy matches (1.0 = exact match)
    
    -- Sync tracking
    first_synced_at TIMESTAMPTZ DEFAULT NOW(),
    last_synced_at TIMESTAMPTZ,
    sync_cursor TEXT,                            -- For incremental sync (last event ID or timestamp)
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- 2. BEEPER MESSAGES
-- ============================================
CREATE TABLE IF NOT EXISTS beeper_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    beeper_event_id TEXT UNIQUE NOT NULL,        -- Beeper's event ID (for deduplication)
    beeper_chat_id TEXT NOT NULL,                -- References beeper_chats.beeper_chat_id
    
    -- Origin
    platform TEXT NOT NULL,                      -- 'whatsapp', 'linkedin', 'telegram', etc.
    
    -- Sender
    sender_id TEXT,                              -- Beeper sender ID
    sender_name TEXT,                            -- Display name at time of message
    is_outgoing BOOLEAN DEFAULT FALSE,           -- TRUE = I sent this message
    
    -- Content
    content TEXT,                                -- Message text (NULL for media-only messages)
    message_type TEXT DEFAULT 'text',            -- 'text', 'image', 'file', 'audio', 'video', 'voice', 'sticker', 'location', 'contact', 'document'
    
    -- Human-readable description for media (e.g., "Photo", "Voice message (0:45)", "PDF document")
    content_description TEXT,                    -- Generated description when content is NULL
    
    -- Timing
    timestamp TIMESTAMPTZ NOT NULL,              -- When message was sent/received
    
    -- Status
    is_read BOOLEAN DEFAULT FALSE,
    
    -- Contact (denormalized from chat for quick queries)
    contact_id UUID REFERENCES contacts(id),
    
    -- Media (if applicable)
    has_media BOOLEAN DEFAULT FALSE,
    media_url TEXT,                              -- Beeper media URL (may expire)
    media_mime_type TEXT,
    media_filename TEXT,
    media_size_bytes INT,
    
    -- Threading
    reply_to_event_id TEXT,                      -- If this is a reply, the parent message ID
    thread_root_event_id TEXT,                   -- Root of the thread (for threaded conversations)
    
    -- Reactions (stored as JSONB array)
    reactions JSONB DEFAULT '[]'::jsonb,         -- [{"emoji": "👍", "sender_id": "...", "sender_name": "..."}]
    
    -- Metadata
    metadata JSONB,                              -- Extra platform-specific data
    
    created_at TIMESTAMPTZ DEFAULT NOW()         -- When we synced this message
);

-- ============================================
-- 3. INDEXES
-- ============================================

-- Chat indexes
CREATE INDEX IF NOT EXISTS idx_beeper_chats_platform ON beeper_chats(platform);
CREATE INDEX IF NOT EXISTS idx_beeper_chats_contact ON beeper_chats(contact_id);
CREATE INDEX IF NOT EXISTS idx_beeper_chats_last_message ON beeper_chats(last_message_at DESC);
CREATE INDEX IF NOT EXISTS idx_beeper_chats_unread ON beeper_chats(unread_count) WHERE unread_count > 0;
CREATE INDEX IF NOT EXISTS idx_beeper_chats_needs_response ON beeper_chats(needs_response) WHERE needs_response = TRUE;
CREATE INDEX IF NOT EXISTS idx_beeper_chats_type ON beeper_chats(chat_type);
CREATE INDEX IF NOT EXISTS idx_beeper_chats_remote_phone ON beeper_chats(remote_phone) WHERE remote_phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_beeper_chats_remote_linkedin ON beeper_chats(remote_linkedin_id) WHERE remote_linkedin_id IS NOT NULL;

-- Message indexes
CREATE INDEX IF NOT EXISTS idx_beeper_messages_chat ON beeper_messages(beeper_chat_id);
CREATE INDEX IF NOT EXISTS idx_beeper_messages_timestamp ON beeper_messages(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_beeper_messages_platform ON beeper_messages(platform);
CREATE INDEX IF NOT EXISTS idx_beeper_messages_contact ON beeper_messages(contact_id);
CREATE INDEX IF NOT EXISTS idx_beeper_messages_outgoing ON beeper_messages(is_outgoing);
CREATE INDEX IF NOT EXISTS idx_beeper_messages_type ON beeper_messages(message_type);

-- Full-text search on message content
CREATE INDEX IF NOT EXISTS idx_beeper_messages_content_fts 
    ON beeper_messages USING gin(to_tsvector('english', COALESCE(content, '')));

-- ============================================
-- 4. FOREIGN KEY CONSTRAINT (after both tables exist)
-- ============================================
-- Note: We don't enforce FK on beeper_chat_id to allow flexible sync order
-- ALTER TABLE beeper_messages ADD CONSTRAINT fk_beeper_messages_chat 
--     FOREIGN KEY (beeper_chat_id) REFERENCES beeper_chats(beeper_chat_id);

-- ============================================
-- 5. UPDATED_AT TRIGGER FOR CHATS
-- ============================================
CREATE OR REPLACE FUNCTION update_beeper_chats_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_beeper_chats_updated_at ON beeper_chats;
CREATE TRIGGER trigger_beeper_chats_updated_at
    BEFORE UPDATE ON beeper_chats
    FOR EACH ROW
    EXECUTE FUNCTION update_beeper_chats_updated_at();

-- ============================================
-- 6. HELPFUL VIEWS
-- ============================================

-- View: Recent conversations with contact info
CREATE OR REPLACE VIEW v_beeper_recent_chats AS
SELECT 
    bc.id,
    bc.beeper_chat_id,
    bc.platform,
    bc.chat_type,
    bc.chat_name,
    bc.remote_user_name,
    bc.last_message_at,
    bc.last_message_preview,
    bc.last_message_is_outgoing,
    bc.last_message_type,
    bc.unread_count,
    bc.is_archived,
    bc.needs_response,
    bc.contact_id,
    bc.contact_link_method,
    c.first_name || ' ' || COALESCE(c.last_name, '') AS contact_name,
    c.company AS contact_company
FROM beeper_chats bc
LEFT JOIN contacts c ON bc.contact_id = c.id
WHERE bc.is_archived = FALSE
ORDER BY bc.last_message_at DESC NULLS LAST;

-- View: Chats that need a response (inbox-zero style)
-- Only DMs, not archived, last message was from the other person
CREATE OR REPLACE VIEW v_beeper_needs_response AS
SELECT 
    bc.id,
    bc.beeper_chat_id,
    bc.platform,
    bc.chat_name,
    bc.remote_user_name,
    bc.last_message_at,
    bc.last_message_preview,
    bc.last_message_type,
    bc.unread_count,
    bc.contact_id,
    c.first_name || ' ' || COALESCE(c.last_name, '') AS contact_name,
    c.company AS contact_company
FROM beeper_chats bc
LEFT JOIN contacts c ON bc.contact_id = c.id
WHERE bc.chat_type = 'dm'
  AND bc.is_archived = FALSE
  AND (bc.last_message_is_outgoing = FALSE OR bc.needs_response = TRUE)
ORDER BY bc.last_message_at DESC NULLS LAST;

-- View: Group chats (lower priority, just for reference)
CREATE OR REPLACE VIEW v_beeper_groups AS
SELECT 
    bc.id,
    bc.beeper_chat_id,
    bc.platform,
    bc.chat_name,
    bc.participant_count,
    bc.last_message_at,
    bc.last_message_preview,
    bc.unread_count,
    bc.is_muted
FROM beeper_chats bc
WHERE bc.chat_type IN ('group', 'channel')
  AND bc.is_archived = FALSE
ORDER BY bc.last_message_at DESC NULLS LAST;

-- View: Unlinked chats (need manual linking)
CREATE OR REPLACE VIEW v_beeper_unlinked_chats AS
SELECT 
    id,
    beeper_chat_id,
    platform,
    chat_name,
    remote_user_name,
    remote_phone,
    remote_linkedin_id,
    remote_telegram_username,
    last_message_at,
    unread_count
FROM beeper_chats
WHERE contact_id IS NULL 
  AND chat_type = 'dm'
  AND is_archived = FALSE
ORDER BY last_message_at DESC NULLS LAST;

-- ============================================
-- 7. SAMPLE QUERIES (for reference, not executed)
-- ============================================
/*
-- Get recent messages with a contact
SELECT bm.*, bc.chat_name
FROM beeper_messages bm
JOIN beeper_chats bc ON bm.beeper_chat_id = bc.beeper_chat_id
WHERE bm.contact_id = 'contact-uuid-here'
ORDER BY bm.timestamp DESC
LIMIT 50;

-- Full-text search across messages
SELECT bm.*, bc.chat_name, bc.platform
FROM beeper_messages bm
JOIN beeper_chats bc ON bm.beeper_chat_id = bc.beeper_chat_id
WHERE to_tsvector('english', bm.content) @@ plainto_tsquery('english', 'meeting tomorrow')
ORDER BY bm.timestamp DESC
LIMIT 20;

-- Get all WhatsApp conversations
SELECT * FROM beeper_chats WHERE platform = 'whatsapp' ORDER BY last_message_at DESC;

-- Get unread messages
SELECT bm.*, bc.chat_name, bc.platform
FROM beeper_messages bm
JOIN beeper_chats bc ON bm.beeper_chat_id = bc.beeper_chat_id
WHERE bm.is_read = FALSE AND bm.is_outgoing = FALSE
ORDER BY bm.timestamp DESC;
*/
