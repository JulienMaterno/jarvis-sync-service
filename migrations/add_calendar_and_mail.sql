-- Calendar Events Table
CREATE TABLE calendar_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    google_event_id TEXT UNIQUE NOT NULL,
    calendar_id TEXT NOT NULL, -- usually 'primary'
    summary TEXT,
    description TEXT,
    start_time TIMESTAMP WITH TIME ZONE,
    end_time TIMESTAMP WITH TIME ZONE,
    location TEXT,
    status TEXT, -- 'confirmed', 'tentative', 'cancelled'
    html_link TEXT,
    attendees JSONB DEFAULT '[]', -- List of attendees with response status
    creator JSONB,
    organizer JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    last_sync_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Emails Table
CREATE TABLE emails (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    google_message_id TEXT UNIQUE NOT NULL,
    thread_id TEXT,
    label_ids JSONB DEFAULT '[]', -- e.g. ["INBOX", "UNREAD", "IMPORTANT", "CATEGORY_PERSONAL"]
    snippet TEXT,
    sender TEXT, -- Parsed 'From' header
    recipient TEXT, -- Parsed 'To' header
    subject TEXT,
    date TIMESTAMP WITH TIME ZONE,
    body_text TEXT,
    body_html TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    last_sync_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Indexes for faster querying
CREATE INDEX idx_calendar_start_time ON calendar_events(start_time);
CREATE INDEX idx_emails_date ON emails(date);
CREATE INDEX idx_emails_thread_id ON emails(thread_id);
CREATE INDEX idx_emails_google_id ON emails(google_message_id);
