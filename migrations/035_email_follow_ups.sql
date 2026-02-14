-- ============================================================================
-- EMAIL FOLLOW-UP TRACKING TABLE
-- Tracks outbound emails needing follow-up. Integrates with Gmail labels,
-- auto-generates follow-up drafts via Intelligence Service, syncs to Notion.
-- ============================================================================

CREATE TABLE IF NOT EXISTS email_follow_ups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Link to original email/thread
    email_id UUID REFERENCES emails(id) ON DELETE SET NULL,
    google_message_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,

    -- Email context (denormalized for draft generation)
    subject TEXT,
    recipient_email TEXT NOT NULL,
    recipient_name TEXT,
    original_body_text TEXT,
    original_date TIMESTAMP WITH TIME ZONE,
    contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL,

    -- Status tracking
    -- pending: waiting for reply or timer expiry
    -- draft_created: follow-up draft generated in Gmail
    -- send_now: user triggered send from Notion
    -- sent: final follow-up sent (max reached)
    -- replied: recipient replied (auto-resolved)
    -- cancelled: manually cancelled
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'draft_created', 'send_now', 'sent', 'replied', 'cancelled')),

    -- Timer fields
    interval_days INTEGER NOT NULL DEFAULT 7,
    next_follow_up_date TIMESTAMP WITH TIME ZONE NOT NULL,
    follow_up_count INTEGER NOT NULL DEFAULT 0,
    max_follow_ups INTEGER NOT NULL DEFAULT 3,

    -- Draft content
    draft_body TEXT,
    gmail_draft_id TEXT,
    last_sent_at TIMESTAMP WITH TIME ZONE,

    -- Notion sync fields
    notion_page_id TEXT UNIQUE,
    notion_updated_at TIMESTAMP WITH TIME ZONE,
    last_sync_source TEXT DEFAULT 'supabase',

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Index for querying active follow-ups by status
CREATE INDEX IF NOT EXISTS idx_follow_ups_status ON email_follow_ups(status);

-- Index for finding follow-ups with expired timers
CREATE INDEX IF NOT EXISTS idx_follow_ups_pending_date
    ON email_follow_ups(next_follow_up_date)
    WHERE status = 'pending';

-- Index for reply detection (lookup by thread)
CREATE INDEX IF NOT EXISTS idx_follow_ups_thread ON email_follow_ups(thread_id);

-- Index for processing send_now requests from Notion
CREATE INDEX IF NOT EXISTS idx_follow_ups_send_now
    ON email_follow_ups(status)
    WHERE status = 'send_now';

-- Index for Notion sync lookups
CREATE INDEX IF NOT EXISTS idx_follow_ups_notion_page
    ON email_follow_ups(notion_page_id)
    WHERE notion_page_id IS NOT NULL;
