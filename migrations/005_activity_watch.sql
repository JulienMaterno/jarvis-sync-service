-- ActivityWatch Integration Schema
-- Stores activity data from ActivityWatch for productivity tracking

-- Activity events table - stores raw events from ActivityWatch
CREATE TABLE IF NOT EXISTS activity_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Source identification
    bucket_id TEXT NOT NULL,                    -- e.g., 'aw-watcher-window_Laptop'
    bucket_type TEXT NOT NULL,                  -- 'currentwindow', 'afkstatus', 'web.tab.current'
    hostname TEXT,                              -- Device hostname
    
    -- Event data
    event_id BIGINT,                            -- Original AW event ID (for deduplication)
    timestamp TIMESTAMPTZ NOT NULL,             -- When the event occurred
    duration FLOAT NOT NULL DEFAULT 0,          -- Duration in seconds
    
    -- Application tracking (for window events)
    app_name TEXT,                              -- Application name (e.g., 'Code', 'Chrome')
    window_title TEXT,                          -- Window title
    
    -- Web tracking (for browser events)
    url TEXT,                                   -- Full URL
    site_domain TEXT,                           -- Extracted domain (e.g., 'github.com')
    tab_title TEXT,                             -- Browser tab title
    
    -- AFK tracking
    afk_status TEXT,                            -- 'afk' or 'not-afk'
    
    -- Raw data storage for future analysis
    raw_data JSONB,                             -- Original event data from AW
    
    -- Metadata
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Constraints
    UNIQUE(bucket_id, event_id)                 -- Prevent duplicate events
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_activity_events_timestamp ON activity_events(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_activity_events_bucket ON activity_events(bucket_id);
CREATE INDEX IF NOT EXISTS idx_activity_events_app ON activity_events(app_name);
CREATE INDEX IF NOT EXISTS idx_activity_events_domain ON activity_events(site_domain);

-- Activity summaries table - pre-aggregated daily summaries for fast querying
CREATE TABLE IF NOT EXISTS activity_summaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    date DATE NOT NULL,
    hostname TEXT NOT NULL,
    
    -- Time tracking (in seconds)
    total_active_time FLOAT DEFAULT 0,          -- Total non-AFK time
    total_afk_time FLOAT DEFAULT 0,             -- Total AFK time
    
    -- Top applications (JSONB array)
    top_apps JSONB DEFAULT '[]'::jsonb,         -- [{app: "Code", duration: 3600, percentage: 40}, ...]
    
    -- Top websites (JSONB array)
    top_sites JSONB DEFAULT '[]'::jsonb,        -- [{domain: "github.com", duration: 1800, percentage: 20}, ...]
    
    -- Productivity categories
    productive_time FLOAT DEFAULT 0,            -- Time on productive apps/sites
    neutral_time FLOAT DEFAULT 0,               -- Uncategorized time
    distracting_time FLOAT DEFAULT 0,           -- Time on distracting apps/sites
    
    -- Detailed breakdown
    hourly_breakdown JSONB DEFAULT '[]'::jsonb, -- [{hour: 9, active: 3200, afk: 400}, ...]
    
    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(date, hostname)
);

CREATE INDEX IF NOT EXISTS idx_activity_summaries_date ON activity_summaries(date DESC);

-- Sync state for ActivityWatch
-- Track last sync timestamp per bucket
INSERT INTO sync_state (key, value)
VALUES ('activitywatch_last_sync', '{}')
ON CONFLICT (key) DO NOTHING;

-- View for easy querying of activity by app
-- Note: Filter by date in your query: WHERE date = CURRENT_DATE
CREATE OR REPLACE VIEW activity_by_app AS
SELECT 
    timestamp::date as date,
    app_name,
    SUM(duration) as total_seconds,
    COUNT(*) as event_count,
    ROUND((SUM(duration) / 3600.0)::numeric, 2) as hours
FROM activity_events
WHERE bucket_type = 'currentwindow'
    AND afk_status IS NULL
GROUP BY timestamp::date, app_name
ORDER BY date DESC, total_seconds DESC;

-- Comment on tables
COMMENT ON TABLE activity_events IS 'Raw activity events from ActivityWatch - window, AFK, and browser tracking';
COMMENT ON TABLE activity_summaries IS 'Pre-aggregated daily summaries for dashboard and journal prompts';
