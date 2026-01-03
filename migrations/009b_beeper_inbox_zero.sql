-- Migration: Beeper Integration - Add inbox-zero workflow fields
-- Date: 2024-12-31
-- Description: Adds needs_response, archived_at, and content_description for inbox-zero workflow
-- Run this AFTER the initial 009_beeper_integration.sql if that was already executed

-- ============================================
-- 1. ADD NEW COLUMNS TO beeper_chats
-- ============================================

-- Add last_message_type to track media in last message
ALTER TABLE beeper_chats 
ADD COLUMN IF NOT EXISTS last_message_type TEXT DEFAULT 'text';

-- Add needs_response for inbox-zero workflow
-- TRUE if: DM + not archived + last message was incoming
ALTER TABLE beeper_chats 
ADD COLUMN IF NOT EXISTS needs_response BOOLEAN DEFAULT FALSE;

-- Add archived_at timestamp
ALTER TABLE beeper_chats 
ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;

-- ============================================
-- 2. ADD NEW COLUMNS TO beeper_messages  
-- ============================================

-- Add content_description for media messages
ALTER TABLE beeper_messages 
ADD COLUMN IF NOT EXISTS content_description TEXT;

-- ============================================
-- 3. ADD NEW INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_beeper_chats_needs_response 
ON beeper_chats(needs_response) WHERE needs_response = TRUE;

CREATE INDEX IF NOT EXISTS idx_beeper_chats_type 
ON beeper_chats(chat_type);

-- ============================================
-- 4. CREATE NEW VIEWS
-- ============================================

-- View: Chats that need a response (inbox-zero style)
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

-- View: Group chats (lower priority)
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

-- Update recent chats view to include new fields
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

-- ============================================
-- 5. UPDATE EXISTING CHATS (set needs_response)
-- ============================================

-- Set needs_response for existing DM chats where last message was incoming
UPDATE beeper_chats
SET needs_response = TRUE
WHERE chat_type = 'dm'
  AND is_archived = FALSE
  AND last_message_is_outgoing = FALSE;

-- Set needs_response to FALSE for groups and archived chats
UPDATE beeper_chats
SET needs_response = FALSE
WHERE chat_type != 'dm' OR is_archived = TRUE;
