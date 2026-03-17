-- =============================================================================
-- Health Insights (AI-generated analysis via Claude)
-- =============================================================================
-- Stores periodic AI health analysis: scored categories, findings,
-- correlations, recommendations, and raw LLM response.
-- =============================================================================

CREATE TABLE IF NOT EXISTS health_insights (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Period this analysis covers
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    period_label TEXT NOT NULL,           -- e.g. "7d", "30d", "weekly"

    -- Overall composite score (0-100)
    overall_score INTEGER,

    -- Category scores (0-100 each)
    recovery_score INTEGER,
    sleep_score INTEGER,
    cardiovascular_score INTEGER,
    fitness_score INTEGER,
    body_composition_score INTEGER,
    stress_score INTEGER,

    -- AI-generated content
    summary TEXT NOT NULL,                -- 2-3 sentence executive summary
    findings JSONB NOT NULL DEFAULT '[]', -- [{category, title, detail, severity, metric_refs}]
    correlations JSONB NOT NULL DEFAULT '[]', -- [{metrics, direction, interpretation}]
    recommendations JSONB NOT NULL DEFAULT '[]', -- [{category, action, rationale, priority}]
    alerts JSONB NOT NULL DEFAULT '[]',   -- [{type, metric, value, threshold, message}]

    -- Data snapshot used for analysis (for reproducibility)
    data_snapshot JSONB,

    -- LLM metadata
    model_used TEXT DEFAULT 'claude-sonnet-4-5-20250514',
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    generation_time_ms INTEGER,

    -- Timestamps
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(period_start, period_end, period_label)
);

CREATE INDEX IF NOT EXISTS idx_health_insights_generated ON health_insights(generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_health_insights_period ON health_insights(period_end DESC, period_label);

COMMENT ON TABLE health_insights IS 'AI-generated health analysis from Claude, with scored categories and evidence-based recommendations';
