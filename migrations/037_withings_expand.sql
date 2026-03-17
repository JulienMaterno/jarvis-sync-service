-- =============================================================================
-- Withings Health Integration Expansion
-- =============================================================================
-- Adds: workouts table, expanded measurement types, expanded sleep fields,
--       high-frequency sleep details
-- =============================================================================

-- 1. New columns on health_measurements for additional Withings measure types
ALTER TABLE health_measurements
    ADD COLUMN IF NOT EXISTS height_m NUMERIC(4,3),
    ADD COLUMN IF NOT EXISTS afib_result INTEGER,
    ADD COLUMN IF NOT EXISTS qrs_interval_ms NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS pr_interval_ms NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS qt_interval_ms NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS qtc_interval_ms NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS afib_ppg_result INTEGER,
    ADD COLUMN IF NOT EXISTS vascular_age INTEGER,
    ADD COLUMN IF NOT EXISTS nerve_health_conductance NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS extracellular_water_kg NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS intracellular_water_kg NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS visceral_fat_index NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS fat_mass_segments JSONB,
    ADD COLUMN IF NOT EXISTS muscle_mass_segments JSONB,
    ADD COLUMN IF NOT EXISTS basal_metabolic_rate NUMERIC(8,2),
    ADD COLUMN IF NOT EXISTS electrodermal_activity NUMERIC(8,2);

-- 2. New columns on health_sleep for additional sleep summary fields
ALTER TABLE health_sleep
    ADD COLUMN IF NOT EXISTS sleep_latency_s INTEGER,
    ADD COLUMN IF NOT EXISTS wakeup_latency_s INTEGER,
    ADD COLUMN IF NOT EXISTS waso_s INTEGER,
    ADD COLUMN IF NOT EXISTS total_timeinbed_s INTEGER,
    ADD COLUMN IF NOT EXISTS nb_rem_episodes INTEGER,
    ADD COLUMN IF NOT EXISTS breathing_disturbances_intensity NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS skin_temp_avg_c NUMERIC(4,2);

-- 3. Workouts table
CREATE TABLE IF NOT EXISTS health_workouts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Withings fields
    category INTEGER NOT NULL,
    category_name TEXT,
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    date DATE NOT NULL,
    device_id TEXT,

    -- Metrics
    duration_s INTEGER,
    calories NUMERIC(8,2),
    intensity INTEGER,
    steps INTEGER,
    distance_m NUMERIC(10,2),
    elevation_m NUMERIC(6,2),

    -- Heart rate
    hr_average INTEGER,
    hr_min INTEGER,
    hr_max INTEGER,
    hr_zone_0_s INTEGER,
    hr_zone_1_s INTEGER,
    hr_zone_2_s INTEGER,
    hr_zone_3_s INTEGER,

    -- SpO2
    spo2_average NUMERIC(5,2),

    -- Manual entries
    manual_distance_m NUMERIC(10,2),
    manual_calories NUMERIC(8,2),
    pause_duration_s INTEGER,
    algo_pause_duration_s INTEGER,

    -- Swimming
    pool_laps INTEGER,
    strokes INTEGER,
    pool_length_m NUMERIC(6,2),

    raw_data JSONB,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(category, start_at)
);

CREATE INDEX IF NOT EXISTS idx_health_workouts_date ON health_workouts(date DESC);
CREATE INDEX IF NOT EXISTS idx_health_workouts_start ON health_workouts(start_at DESC);

-- 4. High-frequency sleep details (per-timestamp HR, RR, snoring, HRV)
CREATE TABLE IF NOT EXISTS health_sleep_details (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sleep_date DATE NOT NULL,
    start_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    state INTEGER,

    -- Time-series data stored as JSONB {unix_ts: value}
    hr JSONB,
    rr JSONB,
    snoring JSONB,
    sdnn_1 JSONB,
    rmssd JSONB,
    mvt_score JSONB,

    raw_data JSONB,
    synced_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(sleep_date, start_at, state)
);

CREATE INDEX IF NOT EXISTS idx_health_sleep_details_date ON health_sleep_details(sleep_date DESC);

COMMENT ON TABLE health_workouts IS 'Withings workouts: running, cycling, swimming, etc. with HR zones, calories, distance';
COMMENT ON TABLE health_sleep_details IS 'Withings high-frequency sleep data: per-timestamp HR, RR, snoring, HRV (SDNN, RMSSD)';
