"""
Custom sleep staging pipeline for Withings ScanWatch 2 Nova.

Ported from the research script at C:/Projects/sleep-optimization/analysis/sleep_staging.py.
Runs as part of the Withings sync pipeline, stores results in health_sleep_custom.

Algorithm: physiological rule-based classification using:
- HR level relative to nightly baseline (deep = lowest, REM = variable/elevated)
- HRV (RMSSD) level (deep = highest parasympathetic, REM = lowest during sleep)
- HR variability within windows (REM has highest beat-to-beat variability)
- Temporal position (deep concentrates early, REM concentrates late)
- Ultradian cycle detection (~90 min NREM-REM cycles)
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

EPOCH_SECONDS = 30
ALGORITHM_VERSION = "heuristic_v4"
STAGE_NAMES = {0: "Wake", 1: "Light", 2: "Deep", 3: "REM"}


# ---------------------------------------------------------------------------
# Time series extraction & resampling
# ---------------------------------------------------------------------------

def jsonb_to_series(jsonb_data: dict | None) -> pd.Series:
    """Convert Withings JSONB {unix_ts: value} to a pandas Series with datetime index."""
    if not jsonb_data:
        return pd.Series(dtype=float)
    timestamps = []
    values = []
    for ts_str, val in jsonb_data.items():
        try:
            ts = datetime.fromtimestamp(int(ts_str), tz=timezone.utc)
            v = float(val)
            if v > 0:
                timestamps.append(ts)
                values.append(v)
        except (ValueError, TypeError):
            continue
    if not timestamps:
        return pd.Series(dtype=float)
    return pd.Series(values, index=pd.DatetimeIndex(timestamps)).sort_index()


def extract_timeseries(epochs: list[dict]) -> tuple[pd.Series, pd.Series, pd.Series, pd.DataFrame]:
    """Extract HR, RMSSD, SDNN time series and Withings staging from epochs."""
    hr_parts = []
    rmssd_parts = []
    sdnn_parts = []
    stage_rows = []

    for epoch in epochs:
        hr_parts.append(jsonb_to_series(epoch.get("hr")))
        rmssd_parts.append(jsonb_to_series(epoch.get("rmssd")))
        sdnn_parts.append(jsonb_to_series(epoch.get("sdnn_1")))
        stage_rows.append({
            "start": pd.Timestamp(epoch["start_at"]),
            "end": pd.Timestamp(epoch["end_at"]),
            "state": epoch["state"],
        })

    hr_nonempty = [s for s in hr_parts if len(s) > 0]
    rmssd_nonempty = [s for s in rmssd_parts if len(s) > 0]
    sdnn_nonempty = [s for s in sdnn_parts if len(s) > 0]
    hr = pd.concat(hr_nonempty).sort_index() if hr_nonempty else pd.Series(dtype=float)
    rmssd = pd.concat(rmssd_nonempty).sort_index() if rmssd_nonempty else pd.Series(dtype=float)
    sdnn = pd.concat(sdnn_nonempty).sort_index() if sdnn_nonempty else pd.Series(dtype=float)

    hr = hr[~hr.index.duplicated(keep="first")]
    rmssd = rmssd[~rmssd.index.duplicated(keep="first")]
    sdnn = sdnn[~sdnn.index.duplicated(keep="first")]

    stages_df = pd.DataFrame(stage_rows)
    return hr, rmssd, sdnn, stages_df


def resample_to_epochs(
    hr: pd.Series, rmssd: pd.Series, sdnn: pd.Series,
    sleep_start: pd.Timestamp, sleep_end: pd.Timestamp,
    epoch_sec: int = EPOCH_SECONDS,
) -> pd.DataFrame:
    """Resample irregular time series to uniform epoch grid."""
    epoch_starts = pd.date_range(start=sleep_start, end=sleep_end, freq=f"{epoch_sec}s")

    # Guard against empty series with non-datetime index (causes comparison errors)
    has_rmssd = len(rmssd) > 0 and isinstance(rmssd.index, pd.DatetimeIndex)
    has_sdnn = len(sdnn) > 0 and isinstance(sdnn.index, pd.DatetimeIndex)

    rows = []
    for i in range(len(epoch_starts) - 1):
        t0 = epoch_starts[i]
        t1 = epoch_starts[i + 1]

        hr_epoch = hr[(hr.index >= t0) & (hr.index < t1)]
        rmssd_epoch = rmssd[(rmssd.index >= t0) & (rmssd.index < t1)] if has_rmssd else pd.Series(dtype=float)
        sdnn_epoch = sdnn[(sdnn.index >= t0) & (sdnn.index < t1)] if has_sdnn else pd.Series(dtype=float)

        row = {
            "epoch_start": t0,
            "epoch_end": t1,
            "epoch_idx": i,
            "hr_mean": hr_epoch.mean() if len(hr_epoch) > 0 else np.nan,
            "hr_std": hr_epoch.std() if len(hr_epoch) > 1 else 0.0,
            "hr_min": hr_epoch.min() if len(hr_epoch) > 0 else np.nan,
            "hr_max": hr_epoch.max() if len(hr_epoch) > 0 else np.nan,
            "hr_range": (hr_epoch.max() - hr_epoch.min()) if len(hr_epoch) > 1 else 0.0,
            "hr_samples": len(hr_epoch),
            "rmssd_mean": rmssd_epoch.mean() if len(rmssd_epoch) > 0 else np.nan,
            "sdnn_mean": sdnn_epoch.mean() if len(sdnn_epoch) > 0 else np.nan,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    for col in ["hr_mean", "hr_min", "hr_max", "rmssd_mean", "sdnn_mean"]:
        df[col] = df[col].ffill().bfill()
    df["hr_std"] = df["hr_std"].fillna(0)
    df["hr_range"] = df["hr_range"].fillna(0)

    return df


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived features for sleep staging (27 features per epoch)."""
    n = len(df)

    # Temporal features
    df["time_fraction"] = df["epoch_idx"] / max(n - 1, 1)
    cycle_period = 90 * 60 / EPOCH_SECONDS
    df["ultradian_sin"] = np.sin(2 * np.pi * df["epoch_idx"] / cycle_period)
    df["ultradian_cos"] = np.cos(2 * np.pi * df["epoch_idx"] / cycle_period)

    # HR normalization
    hr_night_mean = df["hr_mean"].mean()
    hr_night_std = df["hr_mean"].std()
    df["hr_z"] = (df["hr_mean"] - hr_night_mean) / max(hr_night_std, 0.1)

    # Rolling window features (5 min = 10 epochs, 10 min = 20 epochs)
    for window in [10, 20]:
        suffix = f"_{window}"
        df[f"hr_roll_mean{suffix}"] = df["hr_mean"].rolling(window, center=True, min_periods=3).mean()
        df[f"hr_roll_std{suffix}"] = df["hr_mean"].rolling(window, center=True, min_periods=3).std()
        df[f"rmssd_roll_mean{suffix}"] = df["rmssd_mean"].rolling(window, center=True, min_periods=3).mean()

    for col in df.columns:
        if "roll" in col:
            df[col] = df[col].ffill().bfill()

    # Delta features
    df["hr_delta"] = df["hr_mean"].diff().fillna(0)
    df["hr_delta_abs"] = df["hr_delta"].abs()
    df["rmssd_delta"] = df["rmssd_mean"].diff().fillna(0)

    # Acceleration
    df["hr_accel"] = df["hr_delta"].diff().fillna(0)

    # Local variability
    df["hr_cv_10"] = df["hr_roll_std_10"] / df["hr_roll_mean_10"].clip(lower=1)

    # RMSSD normalization
    rmssd_night_mean = df["rmssd_mean"].mean()
    rmssd_night_std = df["rmssd_mean"].std()
    df["rmssd_z"] = (df["rmssd_mean"] - rmssd_night_mean) / max(rmssd_night_std, 0.1)

    # SDNN/RMSSD ratio
    df["sdnn_rmssd_ratio"] = df["sdnn_mean"] / df["rmssd_mean"].clip(lower=1)

    # Percentile rank within night
    df["hr_percentile"] = df["hr_mean"].rank(pct=True)
    df["rmssd_percentile"] = df["rmssd_mean"].rank(pct=True)

    return df


# ---------------------------------------------------------------------------
# Rule-based sleep staging
# ---------------------------------------------------------------------------

def stage_sleep_heuristic(df: pd.DataFrame) -> pd.Series:
    """
    Rule-based sleep staging using physiological heuristics.

    Priority: Wake > Deep > REM > Light
    Post-processing: minimum bout length (2 min), no REM in first 60 min,
    no Deep in first 5 min.
    """
    n = len(df)
    stages = np.ones(n, dtype=int)  # default light

    # Dimension scores
    deep_hr = 1.0 - df["hr_percentile"]
    deep_rmssd = df["rmssd_percentile"]
    deep_stability = 1.0 - df["hr_cv_10"].rank(pct=True)
    deep_temporal = 1.0 - df["time_fraction"]
    deep_score = 0.35 * deep_hr + 0.30 * deep_rmssd + 0.20 * deep_stability + 0.15 * deep_temporal

    rem_hr_var = df["hr_cv_10"].rank(pct=True)
    rem_rmssd_low = 1.0 - df["rmssd_percentile"]
    rem_temporal = df["time_fraction"]
    rem_hr_delta = df["hr_delta_abs"].rank(pct=True)
    rem_score = 0.30 * rem_hr_var + 0.25 * rem_rmssd_low + 0.25 * rem_temporal + 0.20 * rem_hr_delta

    wake_hr = df["hr_percentile"]
    wake_rmssd_low = 1.0 - df["rmssd_percentile"]
    wake_hr_high = (df["hr_z"] > 1.0).astype(float)
    wake_score = 0.40 * wake_hr + 0.30 * wake_rmssd_low + 0.30 * wake_hr_high

    # Thresholds
    deep_threshold = np.percentile(deep_score, 70)
    rem_threshold = np.percentile(rem_score, 65)
    wake_threshold = np.percentile(wake_score, 85)

    for i in range(n):
        if wake_score.iloc[i] >= wake_threshold and df["hr_z"].iloc[i] > 0.5:
            stages[i] = 0
        elif deep_score.iloc[i] >= deep_threshold and df["rmssd_z"].iloc[i] > -0.3:
            stages[i] = 2
        elif rem_score.iloc[i] >= rem_threshold and df["hr_cv_10"].iloc[i] > df["hr_cv_10"].median():
            stages[i] = 3
        else:
            stages[i] = 1

    # Hysteresis: minimum bout length (2 min = 4 epochs)
    stages = _smooth_stages(stages, min_bout=4)

    # Physiological constraints: no REM in first 60 min
    first_60_min = int(60 * 60 / EPOCH_SECONDS)
    for i in range(min(first_60_min, n)):
        if stages[i] == 3:
            stages[i] = 1

    # No deep in first 5 min
    for i in range(min(10, n)):
        if stages[i] == 2:
            stages[i] = 1

    return pd.Series(stages, index=df.index, name="custom_stage")


def _smooth_stages(stages: np.ndarray, min_bout: int) -> np.ndarray:
    """Remove stage bouts shorter than min_bout epochs by replacing with neighbors."""
    n = len(stages)
    result = stages.copy()
    i = 0
    while i < n:
        j = i + 1
        while j < n and stages[j] == stages[i]:
            j += 1
        bout_len = j - i
        if bout_len < min_bout:
            left_stage = stages[max(0, i - 1)] if i > 0 else stages[i]
            right_stage = stages[min(n - 1, j)] if j < n else stages[j - 1]
            replacement = left_stage if left_stage != stages[i] else right_stage
            result[i:j] = replacement
        i = j
    return result


# ---------------------------------------------------------------------------
# Supabase integration
# ---------------------------------------------------------------------------

def run_custom_staging(supabase_client: Any, sleep_date: str) -> dict[str, Any] | None:
    """Run custom staging for a single night and upsert results to health_sleep_custom.

    Args:
        supabase_client: Supabase client instance.
        sleep_date: Date string YYYY-MM-DD (matching health_sleep_details.sleep_date).

    Returns:
        Summary dict or None if insufficient data.
    """
    # Fetch epoch data
    response = supabase_client.table("health_sleep_details").select(
        "sleep_date,state,start_at,end_at,hr,rmssd,sdnn_1"
    ).eq("sleep_date", sleep_date).order("start_at").execute()

    epochs = response.data
    if not epochs or len(epochs) < 10:
        logger.debug(f"Insufficient data for {sleep_date}: {len(epochs) if epochs else 0} epochs")
        return None

    # Check if already processed with current algorithm version
    existing = supabase_client.table("health_sleep_custom").select(
        "id,algorithm_version,rmssd_mean"
    ).eq("sleep_date", sleep_date).execute()
    if existing.data and existing.data[0].get("algorithm_version") == ALGORITHM_VERSION:
        # Allow reprocessing if the previous run had no RMSSD data
        # (staging was degraded, better data may have arrived since)
        prev_rmssd = existing.data[0].get("rmssd_mean")
        if prev_rmssd is not None and float(prev_rmssd) > 0:
            logger.debug(f"Already processed {sleep_date} with {ALGORITHM_VERSION} (has RMSSD)")
            return None
        else:
            logger.info(f"Reprocessing {sleep_date}: previous run had no RMSSD, checking for new data")

    try:
        # Extract time series
        hr, rmssd, sdnn, withings_stages_df = extract_timeseries(epochs)
        if len(hr) < 10:
            logger.debug(f"Insufficient HR data for {sleep_date}: {len(hr)} samples")
            return None

        # Don't process if RMSSD data is missing: staging quality is too degraded
        # without HRV data (can't distinguish deep from REM). Wait for next sync
        # cycle when Withings may have uploaded the detailed data.
        if len(rmssd) < 10:
            logger.info(
                f"Skipping {sleep_date}: only {len(rmssd)} RMSSD samples "
                f"(need 10+). Will retry on next sync when more data may be available."
            )
            return None

        # Split into sessions: if there's a gap >3h between consecutive epochs,
        # they belong to different sleep sessions. Keep only the longest session
        # (the main overnight sleep).
        withings_stages_df = withings_stages_df.sort_values("start").reset_index(drop=True)
        session_ids = [0]
        for i in range(1, len(withings_stages_df)):
            gap = (withings_stages_df.loc[i, "start"] - withings_stages_df.loc[i - 1, "end"]).total_seconds()
            if gap > 3 * 3600:
                session_ids.append(session_ids[-1] + 1)
            else:
                session_ids.append(session_ids[-1])
        withings_stages_df["session"] = session_ids

        if withings_stages_df["session"].nunique() > 1:
            # Pick the longest session by total duration
            session_durations = {}
            for sid, grp in withings_stages_df.groupby("session"):
                dur = (grp["end"].max() - grp["start"].min()).total_seconds()
                session_durations[sid] = dur
            main_session = max(session_durations, key=session_durations.get)
            dropped = len(withings_stages_df) - (withings_stages_df["session"] == main_session).sum()
            withings_stages_df = withings_stages_df[withings_stages_df["session"] == main_session].reset_index(drop=True)
            logger.info(
                f"Split {sleep_date}: {len(session_durations)} sessions detected, "
                f"kept session {main_session} ({session_durations[main_session] / 3600:.1f}h), "
                f"dropped {dropped} epochs from other sessions"
            )
            # Re-extract time series for the main session only
            main_epochs = [e for i, e in enumerate(epochs) if session_ids[i] == main_session]
            hr, rmssd, sdnn, _ = extract_timeseries(main_epochs)
            if len(hr) < 10:
                logger.debug(f"Insufficient HR data after session split for {sleep_date}")
                return None

        sleep_start = withings_stages_df["start"].min()
        sleep_end = withings_stages_df["end"].max()

        # Resample to uniform epochs
        epoch_df = resample_to_epochs(hr, rmssd, sdnn, sleep_start, sleep_end)
        if len(epoch_df) < 20:
            logger.debug(f"Too few resampled epochs for {sleep_date}: {len(epoch_df)}")
            return None

        # Engineer features and run staging
        epoch_df = engineer_features(epoch_df)
        custom_stages = stage_sleep_heuristic(epoch_df)

        # Compute durations
        stage_counts = custom_stages.value_counts()
        total_epochs = len(custom_stages)

        duration_deep_s = int(stage_counts.get(2, 0) * EPOCH_SECONDS)
        duration_light_s = int(stage_counts.get(1, 0) * EPOCH_SECONDS)
        duration_rem_s = int(stage_counts.get(3, 0) * EPOCH_SECONDS)
        duration_awake_s = int(stage_counts.get(0, 0) * EPOCH_SECONDS)
        duration_total_s = total_epochs * EPOCH_SECONDS

        # Compute custom sleep score (0-100)
        # Structure (60%): efficiency + deep + REM proportions
        # Autonomic health (40%): HR, RMSSD, parasympathetic compared to baselines
        sleep_pct = (duration_total_s - duration_awake_s) / max(duration_total_s, 1)
        deep_pct = duration_deep_s / max(duration_total_s - duration_awake_s, 1)
        rem_pct = duration_rem_s / max(duration_total_s - duration_awake_s, 1)

        # Structure sub-score (0-60)
        eff_sub = min(sleep_pct / 0.85, 1.0) * 24
        deep_sub = min(deep_pct / 0.20, 1.0) * 18
        rem_sub = min(rem_pct / 0.22, 1.0) * 18
        structure_score = eff_sub + deep_sub + rem_sub

        # Autonomic sub-score (0-40): penalize elevated HR, low RMSSD, low para%
        # Baselines: HR ~43 bpm, RMSSD ~90, parasympathetic ~55%
        # These are Aaron's personal baselines from healthy nights.
        ans_score = 40.0
        hr_vals_for_score = epoch_df["hr_mean"].dropna()
        rmssd_vals_for_score = epoch_df["rmssd_mean"].dropna()

        if len(hr_vals_for_score) > 0:
            hr_mean = float(hr_vals_for_score.mean())
            # Penalty: each bpm above 50 costs 1 point (max 20 penalty)
            # Normal nights ~43 bpm = 0 penalty. Fever at 67 bpm = 17 penalty.
            hr_penalty = min(max(hr_mean - 50, 0) * 1.0, 20)
            ans_score -= hr_penalty

        if len(rmssd_vals_for_score) > 0:
            rmssd_mean = float(rmssd_vals_for_score.mean())
            # Penalty: each ms below 70 costs 0.5 points (max 15 penalty)
            # Normal nights ~90 = 0 penalty. Fever at 40 = 15 penalty.
            rmssd_penalty = min(max(70 - rmssd_mean, 0) * 0.5, 15)
            ans_score -= rmssd_penalty

        ans_score = max(ans_score, 0)
        custom_sleep_score = int(min(100, structure_score + ans_score))

        # Build epochs JSONB array for dashboard hypnograms
        epoch_records = []
        for idx, row in epoch_df.iterrows():
            epoch_records.append({
                "ts": row["epoch_start"].isoformat(),
                "stage": int(custom_stages.iloc[idx]),
                "hr": round(row["hr_mean"], 1) if pd.notna(row["hr_mean"]) else None,
                "rmssd": round(row["rmssd_mean"], 1) if pd.notna(row["rmssd_mean"]) else None,
            })

        # --- Compute ANS metrics for pattern recognition ---
        hr_vals = epoch_df["hr_mean"].dropna()
        rmssd_vals = epoch_df["rmssd_mean"].dropna()
        sdnn_vals = epoch_df.get("sdnn_mean", pd.Series(dtype=float)).dropna()

        # SVB ratio (sympathovagal balance): SDNN/RMSSD as LF/HF proxy
        if len(sdnn_vals) > 0 and len(rmssd_vals) > 0:
            svb_ratio = round(float(sdnn_vals.mean() / max(rmssd_vals.mean(), 1)), 2)
        else:
            svb_ratio = None

        # Parasympathetic dominance: % of epochs where RMSSD > SDNN
        if len(sdnn_vals) > 0 and len(rmssd_vals) > 0:
            aligned = pd.DataFrame({"rmssd": epoch_df["rmssd_mean"], "sdnn": epoch_df.get("sdnn_mean", pd.Series(dtype=float))}).dropna()
            if len(aligned) > 0:
                para_pct = round(float((aligned["rmssd"] > aligned["sdnn"]).mean() * 100), 1)
            else:
                para_pct = None
        else:
            para_pct = None

        # RMSSD trend slope (positive = improving recovery through the night)
        if len(rmssd_vals) > 10:
            x = np.arange(len(rmssd_vals))
            slope = float(np.polyfit(x, rmssd_vals.values, 1)[0])
            rmssd_trend = round(slope, 4)
        else:
            rmssd_trend = None

        # HR stats
        hr_mean_val = round(float(hr_vals.mean()), 1) if len(hr_vals) > 0 else None
        hr_min_val = round(float(hr_vals.min()), 1) if len(hr_vals) > 0 else None
        hr_max_val = round(float(hr_vals.max()), 1) if len(hr_vals) > 0 else None

        # Micro-awakenings: HR spikes > mean + 3*std during sleep
        micro_awakenings = []
        if len(hr_vals) > 20:
            hr_threshold = hr_vals.mean() + 3 * hr_vals.std()
            for idx_ma, row_ma in epoch_df.iterrows():
                if pd.notna(row_ma["hr_mean"]) and row_ma["hr_mean"] > hr_threshold:
                    if custom_stages.iloc[idx_ma] != 0:  # Not already wake
                        micro_awakenings.append({
                            "ts": row_ma["epoch_start"].isoformat(),
                            "hr": round(row_ma["hr_mean"], 1),
                        })
        micro_count = len(micro_awakenings)

        # Sleep efficiency
        actual_sleep_s = duration_total_s - duration_awake_s
        sleep_eff = round(actual_sleep_s / max(duration_total_s, 1) * 100, 1)

        # Deep in first 3 hours
        first_3h_epochs = min(int(3 * 3600 / EPOCH_SECONDS), len(custom_stages))
        first_3h_deep = int((custom_stages.iloc[:first_3h_epochs] == 2).sum())
        deep_first_3h = round(first_3h_deep / max(first_3h_epochs, 1) * 100, 1)

        # Per-stage ANS metrics
        per_stage_ans = {}
        for stage_code, stage_name in STAGE_NAMES.items():
            mask = custom_stages == stage_code
            if mask.sum() > 0:
                stage_hr = epoch_df.loc[mask, "hr_mean"].dropna()
                stage_rmssd = epoch_df.loc[mask, "rmssd_mean"].dropna()
                stage_sdnn = epoch_df.loc[mask, "sdnn_mean"].dropna() if "sdnn_mean" in epoch_df else pd.Series(dtype=float)
                stage_svb = round(float(stage_sdnn.mean() / max(stage_rmssd.mean(), 1)), 2) if len(stage_sdnn) > 0 and len(stage_rmssd) > 0 else None
                per_stage_ans[stage_name] = {
                    "hr": round(float(stage_hr.mean()), 1) if len(stage_hr) > 0 else None,
                    "rmssd": round(float(stage_rmssd.mean()), 1) if len(stage_rmssd) > 0 else None,
                    "svb": stage_svb,
                }

        logger.info(
            f"ANS metrics {sleep_date}: SVB={svb_ratio}, Para={para_pct}%, "
            f"HR={hr_mean_val}/{hr_min_val}-{hr_max_val}, "
            f"micro-awakenings={micro_count}, eff={sleep_eff}%"
        )

        # Upsert to health_sleep_custom
        record = {
            "sleep_date": sleep_date,
            "duration_deep_s": duration_deep_s,
            "duration_light_s": duration_light_s,
            "duration_rem_s": duration_rem_s,
            "duration_awake_s": duration_awake_s,
            "duration_total_s": duration_total_s,
            "custom_sleep_score": custom_sleep_score,
            "epochs": epoch_records,
            "epoch_count": len(epoch_records),
            "algorithm_version": ALGORITHM_VERSION,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "svb_ratio": svb_ratio,
            "parasympathetic_pct": para_pct,
            "rmssd_mean": round(float(rmssd_vals.mean()), 1) if len(rmssd_vals) > 0 else None,
            "rmssd_trend_slope": rmssd_trend,
            "hr_mean": hr_mean_val,
            "hr_min": hr_min_val,
            "hr_max": hr_max_val,
            "micro_awakening_count": micro_count,
            "micro_awakenings": micro_awakenings if micro_awakenings else None,
            "cycle_count": None,  # Computed by analytics, not staging
            "deep_first_3h_pct": deep_first_3h,
            "sleep_efficiency_pct": sleep_eff,
            "per_stage_ans": per_stage_ans if per_stage_ans else None,
        }

        supabase_client.table("health_sleep_custom").upsert(
            record, on_conflict="sleep_date"
        ).execute()

        logger.info(
            f"Staged {sleep_date}: {len(epoch_records)} epochs, "
            f"deep={duration_deep_s // 60}m, rem={duration_rem_s // 60}m, "
            f"light={duration_light_s // 60}m, wake={duration_awake_s // 60}m, "
            f"score={custom_sleep_score}"
        )

        return {
            "sleep_date": sleep_date,
            "epoch_count": len(epoch_records),
            "duration_deep_s": duration_deep_s,
            "duration_rem_s": duration_rem_s,
            "duration_light_s": duration_light_s,
            "duration_awake_s": duration_awake_s,
            "custom_sleep_score": custom_sleep_score,
        }

    except Exception as e:
        logger.error(f"Sleep staging failed for {sleep_date}: {e}", exc_info=True)
        return None


def run_post_withings_staging(
    supabase_client: Any = None,
    days_back: int = 3,
    **kwargs: Any,
) -> dict[str, Any]:
    """Process custom staging for any recent nights missing it.

    Called as a post-processing step after Withings sync.
    """
    from lib.supabase_client import supabase as sb
    client = supabase_client if supabase_client else sb

    results: dict[str, Any] = {"processed": 0, "skipped": 0, "nights": []}
    today = datetime.now(timezone.utc).date()

    for i in range(days_back):
        d = (today - timedelta(days=i)).isoformat()
        result = run_custom_staging(client, d)
        if result:
            results["processed"] += 1
            results["nights"].append(result)
        else:
            results["skipped"] += 1

    logger.info(f"Sleep staging: processed={results['processed']}, skipped={results['skipped']}")
    return results
