-- =============================================================================
-- Withings Health Integration Schema
-- =============================================================================
-- Stores health data from Withings ScanWatch 2 Nova
-- Data types: body measurements, activity, sleep, heart rate, ECG
-- OAuth tokens stored in sync_state table (key: withings_oauth_tokens)
-- =============================================================================

-- 1. Body Measurements (weight, BP, SpO2, temp, body comp)
CREATE TABLE IF NOT EXISTS health_measurements (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    withings_grpid BIGINT UNIQUE NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL,
    category INTEGER NOT NULL DEFAULT 1,
    device_id TEXT,

    weight_kg NUMERIC(5,2),
    fat_ratio_pct NUMERIC(5,2),
    fat_mass_kg NUMERIC(5,2),
    fat_free_mass_kg NUMERIC(5,2),
    muscle_mass_kg NUMERIC(5,2),
    bone_mass_kg NUMERIC(5,2),
    hydration_pct NUMERIC(5,2),
    systolic_bp INTEGER,
    diastolic_bp INTEGER,
    heart_pulse INTEGER,
    spo2_pct NUMERIC(5,2),
    body_temp_c NUMERIC(4,2),
    skin_temp_c NUMERIC(4,2),
    pulse_wave_velocity NUMERIC(5,2),
    vo2max NUMERIC(5,2),

    raw_measures JSONB,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_health_meas_at ON health_measurements(measured_at DESC);

-- 2. Daily Activity Summaries
CREATE TABLE IF NOT EXISTS health_activity (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE UNIQUE NOT NULL,

    steps INTEGER DEFAULT 0,
    distance_m NUMERIC(10,2) DEFAULT 0,
    calories_total NUMERIC(8,2) DEFAULT 0,
    calories_active NUMERIC(8,2) DEFAULT 0,
    elevation_m NUMERIC(6,2),
    soft_activity_seconds INTEGER DEFAULT 0,
    moderate_activity_seconds INTEGER DEFAULT 0,
    intense_activity_seconds INTEGER DEFAULT 0,
    hr_average INTEGER,
    hr_min INTEGER,
    hr_max INTEGER,

    raw_data JSONB,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_health_activity_date ON health_activity(date DESC);

-- 3. Sleep Summaries (one per night)
CREATE TABLE IF NOT EXISTS health_sleep (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    date DATE NOT NULL,
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,

    duration_total_s INTEGER,
    duration_light_s INTEGER,
    duration_deep_s INTEGER,
    duration_rem_s INTEGER,
    duration_awake_s INTEGER,
    wakeup_count INTEGER,

    sleep_score INTEGER,
    sleep_efficiency_pct NUMERIC(5,2),

    hr_average INTEGER,
    hr_min INTEGER,
    hr_max INTEGER,
    rr_average INTEGER,
    rr_min INTEGER,
    rr_max INTEGER,
    spo2_average NUMERIC(5,2),
    snoring_seconds INTEGER,
    snoring_episode_count INTEGER,

    raw_data JSONB,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(date, start_at)
);

CREATE INDEX IF NOT EXISTS idx_health_sleep_date ON health_sleep(date DESC);

-- 4. ECG Recordings
CREATE TABLE IF NOT EXISTS health_ecg (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    withings_ecg_id BIGINT UNIQUE NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,

    heart_rate INTEGER,
    classification TEXT,
    afib_ppg_classification TEXT,
    signal_quality TEXT,
    signal_data JSONB,

    raw_data JSONB,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_health_ecg_at ON health_ecg(recorded_at DESC);

-- 5. Intraday Heart Rate (~10min granularity)
CREATE TABLE IF NOT EXISTS health_heart_rate (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMPTZ NOT NULL,
    heart_rate INTEGER NOT NULL,
    source TEXT DEFAULT 'watch',

    UNIQUE(timestamp, source)
);

CREATE INDEX IF NOT EXISTS idx_health_hr_ts ON health_heart_rate(timestamp DESC);

-- Seed sync state entries
INSERT INTO sync_state (key, value)
VALUES
    ('withings_last_sync', '{}'),
    ('withings_oauth_tokens', '{}')
ON CONFLICT (key) DO NOTHING;

COMMENT ON TABLE health_measurements IS 'Withings body measurements: weight, BP, SpO2, temperature, body composition';
COMMENT ON TABLE health_activity IS 'Withings daily activity: steps, distance, calories, HR';
COMMENT ON TABLE health_sleep IS 'Withings sleep: duration, stages, scores, vitals';
COMMENT ON TABLE health_ecg IS 'Withings ECG recordings: classification, signal data';
COMMENT ON TABLE health_heart_rate IS 'Intraday heart rate at ~10min granularity';
