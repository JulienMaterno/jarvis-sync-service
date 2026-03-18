"""
===================================================================================
HEALTH INSIGHTS ENGINE
===================================================================================

AI-powered health data analysis using Claude. Aggregates data from all health
tables, computes trends and baselines, scores 6 health categories, detects
correlations, and generates evidence-based recommendations.

Categories scored (0-100):
  1. Recovery & Readiness  (sleep quality + HRV + resting HR + recovery)
  2. Sleep Quality          (duration + efficiency + deep sleep + REM)
  3. Cardiovascular Health  (resting HR trend + HRV + ECG status + SpO2)
  4. Fitness & Activity     (steps + active minutes + workout frequency)
  5. Body Composition       (weight trend + fat % + muscle mass)
  6. Stress & Balance       (HRV trend + RHR elevation + sleep disruption)

Overall = Recovery*0.25 + Sleep*0.25 + Cardiovascular*0.20 + Fitness*0.15
        + BodyComp*0.05 + Stress*0.10
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import anthropic
import numpy as np
from supabase import Client

try:
    import neurokit2 as nk
    HAS_NEUROKIT = True
except ImportError:
    HAS_NEUROKIT = False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ANALYSIS_MODEL = "claude-sonnet-4-6"

CATEGORY_WEIGHTS = {
    "recovery": 0.25,
    "sleep": 0.25,
    "cardiovascular": 0.20,
    "fitness": 0.15,
    "body_composition": 0.05,
    "stress": 0.10,
}

# Thresholds for alerts (sustained changes, not single-day)
ALERT_THRESHOLDS = {
    "hrv_drop_pct": 20,         # 20% drop from 30-day baseline
    "rhr_rise_bpm": 5,          # 5 bpm rise from baseline
    "sleep_efficiency_low": 80, # below 80% for 3+ nights
    "spo2_low": 93,             # below 93% during sleep
    "weight_change_kg": 2,      # 2kg change in 7 days
}

# Keywords for searching personal context relevant to health
HEALTH_CONTEXT_KEYWORDS = [
    "injury", "pain", "hurt", "sore", "strain", "sprain",
    "workout", "exercise", "gym", "run", "swim", "cycling", "training",
    "sleep", "insomnia", "tired", "fatigue", "exhausted", "energy",
    "stress", "anxiety", "burnout", "mental health", "meditation",
    "diet", "nutrition", "weight", "fasting",
    "doctor", "physio", "physiotherapy", "medication", "supplement",
    "recovery", "rehab", "rehabilitation",
    "goal", "target", "fitness goal",
]


# ---------------------------------------------------------------------------
# Data aggregation
# ---------------------------------------------------------------------------

def _fetch_health_data(supabase: Client, days: int = 7) -> dict[str, Any]:
    """Fetch all health data for the analysis period plus baseline (30d extra)."""
    now = datetime.now(timezone.utc)
    analysis_start = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    baseline_start = (now - timedelta(days=days + 30)).strftime("%Y-%m-%d")
    hr_since = (now - timedelta(days=days)).isoformat()
    baseline_hr_since = (now - timedelta(days=days + 30)).isoformat()

    data = {}

    try:
        # Sleep data (analysis period + baseline)
        sleep_resp = supabase.table("health_sleep").select("*").gte(
            "date", baseline_start
        ).order("date", desc=True).execute()
        data["sleep"] = sleep_resp.data or []

        # Activity data
        activity_resp = supabase.table("health_activity").select("*").gte(
            "date", baseline_start
        ).order("date", desc=True).execute()
        data["activity"] = activity_resp.data or []

        # Measurements (weight, body composition, vitals)
        meas_resp = supabase.table("health_measurements").select("*").gte(
            "measured_at", baseline_start
        ).order("measured_at", desc=True).execute()
        data["measurements"] = meas_resp.data or []

        # Heart rate (analysis period only, can be large)
        hr_resp = supabase.table("health_heart_rate").select("*").gte(
            "timestamp", hr_since
        ).order("timestamp", desc=True).limit(5000).execute()
        data["heart_rate"] = hr_resp.data or []

        # Workouts
        workouts_resp = supabase.table("health_workouts").select("*").gte(
            "date", baseline_start
        ).order("date", desc=True).execute()
        data["workouts"] = workouts_resp.data or []

        # Sleep details (HRV, snoring, etc.)
        sleep_det_resp = supabase.table("health_sleep_details").select("*").gte(
            "sleep_date", analysis_start
        ).order("sleep_date", desc=True).execute()
        data["sleep_details"] = sleep_det_resp.data or []

        # ECG recordings
        ecg_resp = supabase.table("health_ecg").select("*").gte(
            "recorded_at", baseline_start
        ).order("recorded_at", desc=True).limit(20).execute()
        data["ecg"] = ecg_resp.data or []

        # Meetings per day (for correlation with sleep/stress)
        meeting_resp = supabase.table("meetings").select("date").gte(
            "date", analysis_start
        ).is_("deleted_at", "null").execute()
        meeting_counts: dict[str, int] = {}
        for m in (meeting_resp.data or []):
            d = (m.get("date") or "")[:10]
            if d:
                meeting_counts[d] = meeting_counts.get(d, 0) + 1
        data["meetings_by_date"] = meeting_counts

        # Calendar events per day
        cal_resp = supabase.table("calendar_events").select(
            "start_time,summary"
        ).gte("start_time", analysis_start).execute()
        cal_by_date: dict[str, list[str]] = {}
        for e in (cal_resp.data or []):
            d = (e.get("start_time") or "")[:10]
            if d:
                cal_by_date.setdefault(d, []).append(e.get("summary", ""))
        data["calendar_by_date"] = cal_by_date

    except Exception as e:
        logger.error(f"Failed to fetch health data for insights: {e}")
        raise

    return data


def _compute_metrics(raw: dict[str, Any], days: int) -> dict[str, Any]:
    """Compute aggregated metrics, trends, and baselines from raw data."""
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    cutoff_ts = (now - timedelta(days=days)).isoformat()

    metrics: dict[str, Any] = {
        "period_days": days,
        "analysis_start": cutoff,
        "analysis_end": now.strftime("%Y-%m-%d"),
    }

    # --- Sleep metrics ---
    sleep_all = raw.get("sleep", [])
    sleep_period = [s for s in sleep_all if s.get("date", "") >= cutoff]
    sleep_baseline = [s for s in sleep_all if s.get("date", "") < cutoff]

    if sleep_period:
        scores = [s["sleep_score"] for s in sleep_period if s.get("sleep_score")]
        durations = [s["duration_total_s"] / 3600 for s in sleep_period if s.get("duration_total_s")]
        deep = [s["duration_deep_s"] / 3600 for s in sleep_period if s.get("duration_deep_s")]
        rem = [s["duration_rem_s"] / 3600 for s in sleep_period if s.get("duration_rem_s")]
        light = [s["duration_light_s"] / 3600 for s in sleep_period if s.get("duration_light_s")]
        efficiencies = []
        for s in sleep_period:
            total = s.get("duration_total_s")
            inbed = s.get("total_timeinbed_s") or total
            if total and inbed and inbed > 0:
                efficiencies.append(round(total / inbed * 100, 1))
        latencies = [s["sleep_latency_s"] / 60 for s in sleep_period if s.get("sleep_latency_s")]
        waso = [s["waso_s"] / 60 for s in sleep_period if s.get("waso_s")]

        metrics["sleep"] = {
            "avg_score": round(_avg(scores), 1) if scores else None,
            "avg_duration_hrs": round(_avg(durations), 1) if durations else None,
            "avg_deep_hrs": round(_avg(deep), 2) if deep else None,
            "avg_rem_hrs": round(_avg(rem), 2) if rem else None,
            "avg_light_hrs": round(_avg(light), 2) if light else None,
            "avg_efficiency_pct": round(_avg(efficiencies), 1) if efficiencies else None,
            "avg_latency_min": round(_avg(latencies), 1) if latencies else None,
            "avg_waso_min": round(_avg(waso), 1) if waso else None,
            "nights_count": len(sleep_period),
            "nights_below_7h": sum(1 for d in durations if d < 7),
            "nights_below_80_efficiency": sum(1 for e in efficiencies if e < 80),
        }

        # Baseline comparison
        if sleep_baseline:
            baseline_scores = [s["sleep_score"] for s in sleep_baseline if s.get("sleep_score")]
            if baseline_scores and scores:
                metrics["sleep"]["score_vs_baseline"] = round(_avg(scores) - _avg(baseline_scores), 1)
    else:
        metrics["sleep"] = None

    # --- Heart rate metrics ---
    hr_data = raw.get("heart_rate", [])
    if hr_data:
        hr_values = [h["heart_rate"] for h in hr_data if h.get("heart_rate")]
        # Estimate resting HR (lowest 10th percentile)
        sorted_hr = sorted(hr_values)
        resting_count = max(1, len(sorted_hr) // 10)
        resting_hrs = sorted_hr[:resting_count]

        metrics["heart_rate"] = {
            "avg_hr": round(_avg(hr_values)),
            "min_hr": min(hr_values),
            "max_hr": max(hr_values),
            "resting_hr_estimate": round(_avg(resting_hrs)),
            "readings_count": len(hr_values),
        }
    else:
        metrics["heart_rate"] = None

    # --- HRV metrics (from sleep details) ---
    sleep_details = raw.get("sleep_details", [])
    if sleep_details:
        rmssd_values = []
        sdnn_values = []
        for sd in sleep_details:
            if sd.get("rmssd"):
                rmssd_avg = _avg_from_jsonb(sd["rmssd"])
                if rmssd_avg:
                    rmssd_values.append(rmssd_avg)
            if sd.get("sdnn_1"):
                sdnn_avg = _avg_from_jsonb(sd["sdnn_1"])
                if sdnn_avg:
                    sdnn_values.append(sdnn_avg)

        metrics["hrv"] = {
            "avg_rmssd": round(_avg(rmssd_values), 1) if rmssd_values else None,
            "avg_sdnn": round(_avg(sdnn_values), 1) if sdnn_values else None,
            "min_rmssd": round(min(rmssd_values), 1) if rmssd_values else None,
            "max_rmssd": round(max(rmssd_values), 1) if rmssd_values else None,
            "nights_with_data": len(set(sd.get("sleep_date") for sd in sleep_details if sd.get("rmssd"))),
        }
    else:
        metrics["hrv"] = None

    # --- Activity metrics ---
    activity_all = raw.get("activity", [])
    activity_period = [a for a in activity_all if a.get("date", "") >= cutoff]
    activity_baseline = [a for a in activity_all if a.get("date", "") < cutoff]

    if activity_period:
        steps = [a["steps"] for a in activity_period if a.get("steps")]
        calories = [a["calories_total"] for a in activity_period if a.get("calories_total")]
        active_cal = [a["calories_active"] for a in activity_period if a.get("calories_active")]
        distances = [a["distance_m"] for a in activity_period if a.get("distance_m")]
        elevations = [a["elevation_m"] for a in activity_period if a.get("elevation_m")]
        active_soft = [a["soft_activity_s"] / 60 for a in activity_period if a.get("soft_activity_s")]
        active_moderate = [a["moderate_activity_s"] / 60 for a in activity_period if a.get("moderate_activity_s")]
        active_intense = [a["intense_activity_s"] / 60 for a in activity_period if a.get("intense_activity_s")]

        metrics["activity"] = {
            "avg_steps": round(_avg(steps)) if steps else None,
            "total_steps": sum(steps) if steps else None,
            "max_steps": max(steps) if steps else None,
            "min_steps": min(steps) if steps else None,
            "avg_calories_total": round(_avg(calories)) if calories else None,
            "avg_calories_active": round(_avg(active_cal)) if active_cal else None,
            "avg_distance_m": round(_avg(distances)) if distances else None,
            "avg_elevation_m": round(_avg(elevations), 1) if elevations else None,
            "avg_soft_activity_min": round(_avg(active_soft), 1) if active_soft else None,
            "avg_moderate_activity_min": round(_avg(active_moderate), 1) if active_moderate else None,
            "avg_intense_activity_min": round(_avg(active_intense), 1) if active_intense else None,
            "days_above_10k": sum(1 for s in steps if s >= 10000) if steps else 0,
            "days_count": len(activity_period),
        }

        # Baseline comparison
        if activity_baseline:
            baseline_steps = [a["steps"] for a in activity_baseline if a.get("steps")]
            if baseline_steps and steps:
                metrics["activity"]["steps_vs_baseline_pct"] = round(
                    (_avg(steps) - _avg(baseline_steps)) / _avg(baseline_steps) * 100, 1
                ) if _avg(baseline_steps) > 0 else None
    else:
        metrics["activity"] = None

    # --- Body composition metrics ---
    meas_all = raw.get("measurements", [])
    meas_period = [m for m in meas_all if (m.get("measured_at", "") or "")[:10] >= cutoff]
    meas_baseline = [m for m in meas_all if (m.get("measured_at", "") or "")[:10] < cutoff]

    weights_period = [float(m["weight_kg"]) for m in meas_period if m.get("weight_kg")]
    weights_baseline = [float(m["weight_kg"]) for m in meas_baseline if m.get("weight_kg")]
    fat_pcts = [float(m["fat_ratio_pct"]) for m in meas_period if m.get("fat_ratio_pct")]
    muscle = [float(m["muscle_mass_kg"]) for m in meas_period if m.get("muscle_mass_kg")]
    bone = [float(m["bone_mass_kg"]) for m in meas_period if m.get("bone_mass_kg")]
    hydration = [float(m["hydration_pct"]) for m in meas_period if m.get("hydration_pct")]
    vo2max = [float(m["vo2max"]) for m in meas_all if m.get("vo2max")]
    skin_temps = [float(m["skin_temp_c"]) for m in meas_period if m.get("skin_temp_c")]
    body_temps = [float(m["body_temp_c"]) for m in meas_period if m.get("body_temp_c")]
    spo2_vals = [float(m["spo2_pct"]) for m in meas_period if m.get("spo2_pct")]
    visceral = [float(m["visceral_fat_index"]) for m in meas_period if m.get("visceral_fat_index")]

    metrics["body"] = {
        "latest_weight_kg": round(weights_period[0], 1) if weights_period else None,
        "avg_weight_kg": round(_avg(weights_period), 1) if weights_period else None,
        "weight_change_kg": round(weights_period[0] - weights_baseline[0], 1) if weights_period and weights_baseline else None,
        "latest_fat_pct": round(fat_pcts[0], 1) if fat_pcts else None,
        "avg_fat_pct": round(_avg(fat_pcts), 1) if fat_pcts else None,
        "latest_muscle_kg": round(muscle[0], 1) if muscle else None,
        "latest_bone_kg": round(bone[0], 1) if bone else None,
        "avg_hydration_pct": round(_avg(hydration), 1) if hydration else None,
        "latest_vo2max": round(vo2max[0], 1) if vo2max else None,
        "avg_skin_temp_c": round(_avg(skin_temps), 1) if skin_temps else None,
        "avg_body_temp_c": round(_avg(body_temps), 1) if body_temps else None,
        "avg_spo2_pct": round(_avg(spo2_vals), 1) if spo2_vals else None,
        "min_spo2_pct": round(min(spo2_vals), 1) if spo2_vals else None,
        "visceral_fat_index": round(visceral[0], 1) if visceral else None,
    }

    # --- Workout metrics ---
    workouts_all = raw.get("workouts", [])
    workouts_period = [w for w in workouts_all if w.get("date", "") >= cutoff]

    if workouts_period:
        workout_durations = [w["duration_s"] / 60 for w in workouts_period if w.get("duration_s")]
        workout_cals = [float(w["calories"]) for w in workouts_period if w.get("calories")]
        workout_hrs = [w["hr_average"] for w in workouts_period if w.get("hr_average")]
        categories = {}
        for w in workouts_period:
            cat = w.get("category_name") or f"type_{w.get('category', '?')}"
            categories[cat] = categories.get(cat, 0) + 1

        metrics["workouts"] = {
            "count": len(workouts_period),
            "total_duration_min": round(sum(workout_durations), 1) if workout_durations else None,
            "avg_duration_min": round(_avg(workout_durations), 1) if workout_durations else None,
            "total_calories": round(sum(workout_cals)) if workout_cals else None,
            "avg_hr": round(_avg(workout_hrs)) if workout_hrs else None,
            "categories": categories,
        }
    else:
        metrics["workouts"] = {"count": 0}

    # --- ECG metrics ---
    ecg_data = raw.get("ecg", [])
    if ecg_data:
        afib_count = sum(1 for e in ecg_data if e.get("afib_classification") and e["afib_classification"] > 0)
        metrics["ecg"] = {
            "recordings_count": len(ecg_data),
            "afib_detected_count": afib_count,
            "latest_classification": ecg_data[0].get("afib_classification") if ecg_data else None,
        }
    else:
        metrics["ecg"] = None

    return metrics


# ---------------------------------------------------------------------------
# Category scoring (rule-based, 0-100)
# ---------------------------------------------------------------------------

def _score_categories(metrics: dict[str, Any]) -> dict[str, Optional[int]]:
    """Score each health category 0-100 based on computed metrics."""
    scores: dict[str, Optional[int]] = {}

    # --- Recovery & Readiness ---
    recovery_components = []
    sleep_m = metrics.get("sleep")
    if sleep_m and sleep_m.get("avg_score"):
        recovery_components.append(("sleep_score", sleep_m["avg_score"], 0.40))
    hrv_m = metrics.get("hrv")
    if hrv_m and hrv_m.get("avg_rmssd"):
        # RMSSD 20-80ms range, map to 0-100
        hrv_score = min(100, max(0, (hrv_m["avg_rmssd"] - 20) / 60 * 100))
        recovery_components.append(("hrv", hrv_score, 0.30))
    hr_m = metrics.get("heart_rate")
    if hr_m and hr_m.get("resting_hr_estimate"):
        # RHR 40-80 bpm, lower is better
        rhr = hr_m["resting_hr_estimate"]
        rhr_score = min(100, max(0, (80 - rhr) / 40 * 100))
        recovery_components.append(("rhr", rhr_score, 0.20))
    if sleep_m and sleep_m.get("avg_efficiency_pct"):
        eff_score = min(100, max(0, (sleep_m["avg_efficiency_pct"] - 70) / 25 * 100))
        recovery_components.append(("efficiency", eff_score, 0.10))

    scores["recovery"] = _weighted_score(recovery_components)

    # --- Sleep Quality ---
    sleep_components = []
    if sleep_m:
        if sleep_m.get("avg_duration_hrs"):
            # Optimal 7-9h, penalize outside
            dur = sleep_m["avg_duration_hrs"]
            if 7 <= dur <= 9:
                dur_score = 100
            elif dur < 7:
                dur_score = max(0, dur / 7 * 100)
            else:
                dur_score = max(0, 100 - (dur - 9) * 20)
            sleep_components.append(("duration", dur_score, 0.30))
        if sleep_m.get("avg_efficiency_pct"):
            eff_score = min(100, max(0, (sleep_m["avg_efficiency_pct"] - 70) / 25 * 100))
            sleep_components.append(("efficiency", eff_score, 0.35))
        if sleep_m.get("avg_deep_hrs"):
            # Optimal deep: 1.5-2h
            deep = sleep_m["avg_deep_hrs"]
            deep_score = min(100, deep / 1.5 * 100)
            sleep_components.append(("deep_sleep", deep_score, 0.20))
        if sleep_m.get("avg_rem_hrs"):
            # Optimal REM: 1.5-2h
            rem = sleep_m["avg_rem_hrs"]
            rem_score = min(100, rem / 1.5 * 100)
            sleep_components.append(("rem_sleep", rem_score, 0.15))

    scores["sleep"] = _weighted_score(sleep_components)

    # --- Cardiovascular Health ---
    cardio_components = []
    if hr_m and hr_m.get("resting_hr_estimate"):
        rhr = hr_m["resting_hr_estimate"]
        rhr_score = min(100, max(0, (80 - rhr) / 40 * 100))
        cardio_components.append(("rhr", rhr_score, 0.35))
    if hrv_m and hrv_m.get("avg_rmssd"):
        hrv_score = min(100, max(0, (hrv_m["avg_rmssd"] - 20) / 60 * 100))
        cardio_components.append(("hrv", hrv_score, 0.30))
    body_m = metrics.get("body")
    if body_m and body_m.get("avg_spo2_pct"):
        spo2 = body_m["avg_spo2_pct"]
        spo2_score = min(100, max(0, (spo2 - 90) / 8 * 100))
        cardio_components.append(("spo2", spo2_score, 0.15))
    if body_m and body_m.get("latest_vo2max"):
        # VO2max 20-60 range
        vo2 = body_m["latest_vo2max"]
        vo2_score = min(100, max(0, (vo2 - 20) / 40 * 100))
        cardio_components.append(("vo2max", vo2_score, 0.20))

    scores["cardiovascular"] = _weighted_score(cardio_components)

    # --- Fitness & Activity ---
    fitness_components = []
    act_m = metrics.get("activity")
    if act_m:
        if act_m.get("avg_steps"):
            # 10k steps = 100, 0 = 0
            steps_score = min(100, act_m["avg_steps"] / 10000 * 100)
            fitness_components.append(("steps", steps_score, 0.35))
        moderate = act_m.get("avg_moderate_activity_min") or 0
        intense = act_m.get("avg_intense_activity_min") or 0
        active_min = moderate + intense
        if active_min > 0:
            # WHO: 150 min moderate/week = ~21 min/day
            active_score = min(100, active_min / 30 * 100)
            fitness_components.append(("active_minutes", active_score, 0.30))
    work_m = metrics.get("workouts")
    if work_m and work_m.get("count", 0) > 0:
        # 4+ workouts/week in period = 100
        days = metrics.get("period_days", 7)
        weekly_workouts = work_m["count"] / max(1, days / 7)
        workout_score = min(100, weekly_workouts / 4 * 100)
        fitness_components.append(("workout_freq", workout_score, 0.20))
    if act_m and act_m.get("avg_elevation_m"):
        elev_score = min(100, act_m["avg_elevation_m"] / 30 * 100)
        fitness_components.append(("elevation", elev_score, 0.15))

    scores["fitness"] = _weighted_score(fitness_components)

    # --- Body Composition ---
    body_components = []
    if body_m:
        if body_m.get("latest_fat_pct"):
            fat = body_m["latest_fat_pct"]
            # Men: 10-20% optimal. Score drops outside
            if 10 <= fat <= 20:
                fat_score = 100
            elif fat < 10:
                fat_score = max(50, 100 - (10 - fat) * 10)
            else:
                fat_score = max(0, 100 - (fat - 20) * 5)
            body_components.append(("fat_pct", fat_score, 0.35))
        if body_m.get("weight_change_kg") is not None:
            change = abs(body_m["weight_change_kg"])
            stability_score = max(0, 100 - change * 20)
            body_components.append(("weight_stability", stability_score, 0.25))
        if body_m.get("avg_hydration_pct"):
            # Optimal: 50-65%
            hyd = body_m["avg_hydration_pct"]
            hyd_score = min(100, max(0, (hyd - 40) / 20 * 100))
            body_components.append(("hydration", hyd_score, 0.20))
        if body_m.get("visceral_fat_index"):
            visc = body_m["visceral_fat_index"]
            visc_score = max(0, 100 - visc * 7)  # <10 = healthy
            body_components.append(("visceral_fat", visc_score, 0.20))

    scores["body_composition"] = _weighted_score(body_components)

    # --- Stress & Autonomic Balance ---
    stress_components = []
    if hrv_m and hrv_m.get("avg_rmssd"):
        hrv_score = min(100, max(0, (hrv_m["avg_rmssd"] - 20) / 60 * 100))
        stress_components.append(("hrv", hrv_score, 0.40))
    if hr_m and hr_m.get("resting_hr_estimate"):
        rhr = hr_m["resting_hr_estimate"]
        rhr_stress = min(100, max(0, (80 - rhr) / 40 * 100))
        stress_components.append(("rhr_stress", rhr_stress, 0.30))
    if sleep_m and sleep_m.get("avg_efficiency_pct"):
        eff = sleep_m["avg_efficiency_pct"]
        sleep_stress = min(100, max(0, (eff - 70) / 25 * 100))
        stress_components.append(("sleep_disruption", sleep_stress, 0.20))
    if sleep_m and sleep_m.get("avg_latency_min"):
        lat = sleep_m["avg_latency_min"]
        # <10 min = 100, >30 min = 0
        lat_score = max(0, min(100, (30 - lat) / 20 * 100))
        stress_components.append(("latency", lat_score, 0.10))

    scores["stress"] = _weighted_score(stress_components)

    # --- Overall composite ---
    overall_components = []
    for cat, weight in CATEGORY_WEIGHTS.items():
        if scores.get(cat) is not None:
            overall_components.append((cat, scores[cat], weight))
    scores["overall"] = _weighted_score(overall_components)

    return scores


# ---------------------------------------------------------------------------
# Alert detection
# ---------------------------------------------------------------------------

def _detect_alerts(metrics: dict[str, Any]) -> list[dict[str, Any]]:
    """Detect health alerts based on thresholds and sustained changes."""
    alerts = []

    body_m = metrics.get("body", {}) or {}
    sleep_m = metrics.get("sleep", {}) or {}
    hrv_m = metrics.get("hrv", {}) or {}

    # SpO2 low
    if body_m.get("min_spo2_pct") and body_m["min_spo2_pct"] < ALERT_THRESHOLDS["spo2_low"]:
        alerts.append({
            "type": "warning",
            "metric": "spo2",
            "value": body_m["min_spo2_pct"],
            "threshold": ALERT_THRESHOLDS["spo2_low"],
            "message": f"SpO2 dropped to {body_m['min_spo2_pct']}% (below {ALERT_THRESHOLDS['spo2_low']}% threshold). Consider consulting a physician if this persists.",
        })

    # Sleep efficiency consistently low
    if sleep_m.get("nights_below_80_efficiency") and sleep_m["nights_below_80_efficiency"] >= 3:
        alerts.append({
            "type": "attention",
            "metric": "sleep_efficiency",
            "value": sleep_m["avg_efficiency_pct"],
            "threshold": 80,
            "message": f"{sleep_m['nights_below_80_efficiency']} nights with sleep efficiency below 80%. This pattern suggests sleep quality issues worth investigating.",
        })

    # Weight rapid change
    if body_m.get("weight_change_kg") and abs(body_m["weight_change_kg"]) > ALERT_THRESHOLDS["weight_change_kg"]:
        direction = "gained" if body_m["weight_change_kg"] > 0 else "lost"
        alerts.append({
            "type": "info",
            "metric": "weight",
            "value": body_m["weight_change_kg"],
            "threshold": ALERT_THRESHOLDS["weight_change_kg"],
            "message": f"Weight {direction} {abs(body_m['weight_change_kg'])}kg vs previous period. Rapid changes may indicate fluid shifts or dietary changes.",
        })

    # HRV significant drop vs baseline
    if hrv_m.get("avg_rmssd") and hrv_m.get("min_rmssd"):
        if hrv_m["min_rmssd"] < hrv_m["avg_rmssd"] * 0.7:
            alerts.append({
                "type": "attention",
                "metric": "hrv",
                "value": hrv_m["min_rmssd"],
                "threshold": round(hrv_m["avg_rmssd"] * 0.7, 1),
                "message": f"HRV (RMSSD) dropped to {hrv_m['min_rmssd']}ms on some nights, well below your average of {hrv_m['avg_rmssd']}ms. Low HRV can indicate stress, overtraining, or illness onset.",
            })

    return alerts


# ---------------------------------------------------------------------------
# Advanced HRV analysis (NeuroKit2)
# ---------------------------------------------------------------------------

def _compute_advanced_hrv(sleep_details: list[dict]) -> Optional[dict[str, Any]]:
    """
    Compute frequency-domain and nonlinear HRV metrics using NeuroKit2.

    Extracts RR interval time series from sleep_details JSONB, then runs
    NeuroKit2's hrv_time, hrv_frequency, and hrv_nonlinear analyses.

    Returns dict with LF/HF power, LF/HF ratio (sympathovagal balance),
    SD1/SD2 (Poincare), and sample entropy.
    """
    if not HAS_NEUROKIT:
        logger.info("NeuroKit2 not installed, skipping advanced HRV analysis")
        return None

    # Collect all RR intervals from sleep details
    all_rr_intervals = []
    for sd in sleep_details:
        rr_data = sd.get("rr")
        if not rr_data:
            continue
        if isinstance(rr_data, str):
            try:
                rr_data = json.loads(rr_data)
            except (json.JSONDecodeError, TypeError):
                continue
        if isinstance(rr_data, dict):
            # RR values are in breaths/min or ms depending on Withings version
            # Withings stores RR as respirations per minute, but we need the
            # actual RR interval time series. Use HR data to derive RR intervals.
            pass

    # Better approach: derive RR intervals from HR time series
    # HR (bpm) -> RR interval (ms) = 60000 / HR
    all_rr_ms = []
    for sd in sleep_details:
        hr_data = sd.get("hr")
        if not hr_data:
            continue
        if isinstance(hr_data, str):
            try:
                hr_data = json.loads(hr_data)
            except (json.JSONDecodeError, TypeError):
                continue
        if isinstance(hr_data, dict):
            # Sort by timestamp and convert HR->RR
            sorted_entries = sorted(hr_data.items(), key=lambda x: int(x[0]))
            for _, hr_val in sorted_entries:
                if hr_val and isinstance(hr_val, (int, float)) and 30 < hr_val < 220:
                    rr_ms = 60000.0 / hr_val
                    all_rr_ms.append(rr_ms)

    if len(all_rr_ms) < 30:
        logger.info(f"Not enough RR intervals for advanced HRV ({len(all_rr_ms)} points)")
        return None

    try:
        rr_array = np.array(all_rr_ms)

        # Create synthetic peaks from RR intervals (cumulative sum)
        peaks_ms = np.cumsum(rr_array)
        # Convert to samples at 1000Hz (1ms resolution)
        peaks_samples = (peaks_ms).astype(int)

        result = {}

        # Time domain (validates our basic calculations)
        try:
            hrv_time = nk.hrv_time({"ECG_R_Peaks": peaks_samples}, sampling_rate=1000)
            result["time_domain"] = {
                "rmssd": round(float(hrv_time["HRV_RMSSD"].iloc[0]), 2),
                "sdnn": round(float(hrv_time["HRV_SDNN"].iloc[0]), 2),
                "mean_nn": round(float(hrv_time["HRV_MeanNN"].iloc[0]), 2),
                "pnn50": round(float(hrv_time["HRV_pNN50"].iloc[0]), 2),
                "cv": round(float(hrv_time["HRV_CVNN"].iloc[0]), 4),
            }
        except Exception as e:
            logger.warning(f"NeuroKit2 time domain failed: {e}")

        # Frequency domain (the real value-add)
        try:
            hrv_freq = nk.hrv_frequency(
                {"ECG_R_Peaks": peaks_samples},
                sampling_rate=1000,
                normalize=True,
            )
            lf = float(hrv_freq["HRV_LF"].iloc[0])
            hf = float(hrv_freq["HRV_HF"].iloc[0])
            result["frequency_domain"] = {
                "lf_power": round(lf, 4),
                "hf_power": round(hf, 4),
                "lf_hf_ratio": round(lf / hf, 3) if hf > 0 else None,
                "lf_normalized": round(float(hrv_freq["HRV_LFHF"].iloc[0]), 3) if "HRV_LFHF" in hrv_freq else None,
                "vlf_power": round(float(hrv_freq["HRV_VLF"].iloc[0]), 4) if "HRV_VLF" in hrv_freq else None,
            }
            # Interpretation
            ratio = result["frequency_domain"]["lf_hf_ratio"]
            if ratio is not None:
                if ratio < 1.0:
                    result["frequency_domain"]["balance"] = "parasympathetic_dominant"
                    result["frequency_domain"]["interpretation"] = "Parasympathetic dominance indicates good recovery and relaxation state"
                elif ratio < 2.0:
                    result["frequency_domain"]["balance"] = "balanced"
                    result["frequency_domain"]["interpretation"] = "Balanced autonomic nervous system activity"
                else:
                    result["frequency_domain"]["balance"] = "sympathetic_dominant"
                    result["frequency_domain"]["interpretation"] = "Sympathetic dominance may indicate stress, insufficient recovery, or high training load"
        except Exception as e:
            logger.warning(f"NeuroKit2 frequency domain failed: {e}")

        # Nonlinear domain (Poincare plot metrics)
        try:
            hrv_nl = nk.hrv_nonlinear({"ECG_R_Peaks": peaks_samples}, sampling_rate=1000)
            sd1 = float(hrv_nl["HRV_SD1"].iloc[0])
            sd2 = float(hrv_nl["HRV_SD2"].iloc[0])
            result["nonlinear"] = {
                "sd1": round(sd1, 2),
                "sd2": round(sd2, 2),
                "sd1_sd2_ratio": round(sd1 / sd2, 3) if sd2 > 0 else None,
                "interpretation": (
                    "SD1 reflects short-term (parasympathetic) variability, "
                    "SD2 reflects long-term (mixed) variability. "
                    f"SD1/SD2 ratio of {round(sd1/sd2, 2) if sd2 > 0 else 'N/A'} "
                    f"{'suggests good parasympathetic tone' if sd2 > 0 and sd1/sd2 > 0.5 else 'suggests reduced short-term variability'}."
                ),
            }
        except Exception as e:
            logger.warning(f"NeuroKit2 nonlinear analysis failed: {e}")

        # Sample entropy (complexity measure)
        try:
            sampen = nk.entropy_sample(rr_array[:2000])  # Cap at 2000 for speed
            result["complexity"] = {
                "sample_entropy": round(float(sampen), 4),
                "interpretation": (
                    f"Sample entropy of {round(float(sampen), 3)}: "
                    f"{'healthy complexity (good adaptability)' if sampen > 1.0 else 'reduced complexity (may indicate fatigue or stress)' if sampen > 0.5 else 'low complexity (warrants attention)'}"
                ),
            }
        except Exception as e:
            logger.warning(f"NeuroKit2 entropy calculation failed: {e}")

        if result:
            result["data_points_used"] = len(all_rr_ms)
            logger.info(f"Advanced HRV analysis complete: {len(all_rr_ms)} RR intervals, "
                        f"LF/HF={result.get('frequency_domain', {}).get('lf_hf_ratio', 'N/A')}")
            return result

    except Exception as e:
        logger.error(f"NeuroKit2 HRV analysis failed: {e}")

    return None


# ---------------------------------------------------------------------------
# Personal context enrichment
# ---------------------------------------------------------------------------


def _fetch_personal_context(supabase: Client, days: int = 30) -> str:
    """
    Fetch personal context from Jarvis knowledge base that's relevant to health.

    Searches journals, reflections, and transcripts for health-related mentions
    (injuries, fitness goals, lifestyle changes, etc.) to give Claude personalized
    context when generating insights.
    """
    context_items: list[str] = []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")

    # 1. Recent journals mentioning health topics
    try:
        journal_resp = supabase.table("journals").select(
            "date,content"
        ).gte("date", cutoff).order("date", desc=True).limit(30).execute()

        if journal_resp.data:
            for j in journal_resp.data:
                content_lower = (j.get("content") or "").lower()
                if any(kw in content_lower for kw in HEALTH_CONTEXT_KEYWORDS):
                    # Truncate long entries
                    text = (j.get("content") or "")[:500]
                    context_items.append(f"[Journal {j.get('date', '?')}] {text}")
    except Exception as e:
        logger.debug(f"Could not fetch journals for health context: {e}")

    # 2. Reflections about health/fitness/body
    try:
        reflection_resp = supabase.table("reflections").select(
            "topic_key,content,created_at"
        ).gte("created_at", cutoff).order("created_at", desc=True).limit(30).execute()

        if reflection_resp.data:
            for r in reflection_resp.data:
                content_lower = (r.get("content") or "").lower()
                topic = (r.get("topic_key") or "").lower()
                if any(kw in content_lower or kw in topic for kw in HEALTH_CONTEXT_KEYWORDS):
                    text = (r.get("content") or "")[:500]
                    date = (r.get("created_at") or "")[:10]
                    context_items.append(f"[Reflection {date}] {text}")
    except Exception as e:
        logger.debug(f"Could not fetch reflections for health context: {e}")

    # 3. Recent transcripts (voice memos) mentioning health
    try:
        transcript_resp = supabase.table("transcripts").select(
            "content,created_at"
        ).gte("created_at", cutoff).order("created_at", desc=True).limit(20).execute()

        if transcript_resp.data:
            for t in transcript_resp.data:
                content_lower = (t.get("content") or "").lower()
                if any(kw in content_lower for kw in HEALTH_CONTEXT_KEYWORDS):
                    text = (t.get("content") or "")[:500]
                    date = (t.get("created_at") or "")[:10]
                    context_items.append(f"[Voice memo {date}] {text}")
    except Exception as e:
        logger.debug(f"Could not fetch transcripts for health context: {e}")

    # 4. User profile insights (health-related traits/patterns)
    try:
        profile_resp = supabase.table("user_profile_insights").select(
            "key,trait,context,confidence"
        ).gte("confidence", 0.5).order("confidence", desc=True).limit(50).execute()

        if profile_resp.data:
            health_insights = []
            for p in profile_resp.data:
                trait_lower = (p.get("trait") or "").lower()
                key_lower = (p.get("key") or "").lower()
                if any(kw in trait_lower or kw in key_lower for kw in HEALTH_CONTEXT_KEYWORDS):
                    health_insights.append(
                        f"- {p.get('trait', '')} (confidence: {p.get('confidence', 0):.0%})"
                    )
            if health_insights:
                context_items.append(
                    "[User Profile - Health Patterns]\n" + "\n".join(health_insights[:10])
                )
    except Exception as e:
        logger.debug(f"Could not fetch user profile for health context: {e}")

    # 5. Previous insights summary (for trend tracking)
    try:
        prev_resp = supabase.table("health_insights").select(
            "period_label,overall_score,recovery_score,sleep_score,"
            "cardiovascular_score,fitness_score,stress_score,weekly_focus,generated_at"
        ).order("generated_at", desc=True).limit(4).execute()

        if prev_resp.data and len(prev_resp.data) > 1:
            # Skip the most recent (that's what we're replacing), show previous
            history = prev_resp.data[1:]
            history_lines = []
            for h in history:
                date = (h.get("generated_at") or "")[:10]
                history_lines.append(
                    f"- {date}: Overall {h.get('overall_score', '?')}, "
                    f"Recovery {h.get('recovery_score', '?')}, "
                    f"Sleep {h.get('sleep_score', '?')}, "
                    f"Cardio {h.get('cardiovascular_score', '?')}, "
                    f"Fitness {h.get('fitness_score', '?')}, "
                    f"Stress {h.get('stress_score', '?')} "
                    f"| Focus: {h.get('weekly_focus', 'N/A')}"
                )
            context_items.append(
                "[Previous Insights History]\n" + "\n".join(history_lines)
            )
    except Exception as e:
        logger.debug(f"Could not fetch previous insights: {e}")

    # 6. Active health protocols (structured interventions the user is following)
    active_protocol = _fetch_active_health_protocol(supabase)
    if active_protocol:
        context_items.append(active_protocol)

    if not context_items:
        return ""

    # Cap total context to ~4000 chars to keep prompt efficient (increased for protocol)
    combined = "\n\n".join(context_items)
    if len(combined) > 4000:
        combined = combined[:4000] + "\n... (truncated)"

    return combined


def _fetch_active_health_protocol(supabase: Client) -> str:
    """
    Fetch active health protocol from the health_protocols table.

    Returns formatted context string if an active protocol exists, empty string otherwise.
    The protocol provides structured context about what the user is actively doing
    to optimize their health, so insights can be tailored accordingly.
    """
    try:
        resp = supabase.table("health_protocols").select(
            "name,phase,started_at,phase_started_at,target_metrics,protocol_context"
        ).eq("status", "active").order("started_at", desc=True).limit(1).execute()

        if not resp.data:
            return ""

        p = resp.data[0]
        started = (p.get("started_at") or "")[:10]
        phase_started = (p.get("phase_started_at") or "")[:10]

        lines = [
            f"[ACTIVE HEALTH PROTOCOL: {p.get('name', 'Unknown')}]",
            f"- Started: {started}",
            f"- Current phase: {p.get('phase', 'unknown')} (since {phase_started})",
        ]

        targets = p.get("target_metrics")
        if targets:
            if isinstance(targets, str):
                targets = json.loads(targets)
            lines.append("- Target metrics:")
            for k, v in targets.items():
                lines.append(f"  * {k}: {v}")

        context = p.get("protocol_context") or ""
        if context:
            lines.append(f"- Protocol details: {context[:800]}")

        lines.append(
            "IMPORTANT: Evaluate the user's data in the context of this active protocol. "
            "Note progress toward protocol targets. Recommendations should align with "
            "the protocol phase (don't contradict it). Flag if data suggests the protocol "
            "needs adjustment."
        )

        return "\n".join(lines)

    except Exception as e:
        logger.debug(f"Could not fetch active health protocol: {e}")
        return ""


# ---------------------------------------------------------------------------
# Claude AI analysis
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert health analyst and personal wellness advisor. You analyze wearable health data from a Withings ScanWatch 2 Nova smartwatch, combined with personal context from the user's life (journals, voice memos, reflections, and previous insights).

Your role:
- Provide evidence-based health insights, not medical diagnoses
- Identify meaningful patterns and correlations across metrics
- Score health categories and explain the reasoning
- Give specific, actionable recommendations tailored to the user's actual life context
- Flag concerning trends that warrant attention
- Connect biometric data with lifestyle context (injuries, travel, work stress, training goals)
- Track progress against previous insights and recommendations
- Be direct, personal, and clear. You know this person. Address them as "you"

You have access to advanced HRV analysis from NeuroKit2, including:
- Frequency-domain metrics: LF/HF power ratio indicates sympathovagal balance
  (LF/HF < 1.0 = parasympathetic/recovery, > 2.0 = sympathetic/stress)
- Nonlinear metrics: SD1 (short-term parasympathetic) and SD2 (long-term mixed ANS)
  from Poincare plot analysis
- Complexity: Sample entropy indicates heart rate complexity and adaptability
  (higher = healthier, more adaptable; lower = reduced complexity, possible stress/fatigue)

Important guidelines:
- These are wellness metrics from a consumer device, not clinical measurements
- Never diagnose medical conditions
- For concerning patterns (ECG anomalies, persistent SpO2 drops), recommend professional consultation
- Focus on trends over days/weeks, not single-day fluctuations
- Consider correlations: HRV <-> sleep quality, activity <-> sleep, RHR <-> recovery
- Use the LF/HF ratio and SD1/SD2 to provide deeper autonomic nervous system insights
- Be specific: cite actual numbers, percentages, and date ranges
- When personal context mentions injuries, adapt exercise recommendations accordingly
- When previous insights exist, note improvements or regressions from prior periods"""


def _build_analysis_prompt(
    metrics: dict[str, Any],
    scores: dict[str, Optional[int]],
    alerts: list[dict],
    advanced_hrv: Optional[dict[str, Any]] = None,
    personal_context: str = "",
    raw_data: Optional[dict[str, Any]] = None,
    long_term_patterns: Optional[list[dict[str, Any]]] = None,
    previous_insights: Optional[dict[str, Any]] = None,
) -> str:
    """Build the analysis prompt with all health data and personal context."""

    prompt = f"""Analyze this health data for the past {metrics['period_days']} days ({metrics['analysis_start']} to {metrics['analysis_end']}).

## Pre-computed Category Scores (0-100)
| Category | Score |
|----------|-------|
| Overall | {scores.get('overall', 'N/A')} |
| Recovery & Readiness | {scores.get('recovery', 'N/A')} |
| Sleep Quality | {scores.get('sleep', 'N/A')} |
| Cardiovascular Health | {scores.get('cardiovascular', 'N/A')} |
| Fitness & Activity | {scores.get('fitness', 'N/A')} |
| Body Composition | {scores.get('body_composition', 'N/A')} |
| Stress & Balance | {scores.get('stress', 'N/A')} |

## Sleep Data
"""
    if metrics.get("sleep"):
        s = metrics["sleep"]
        prompt += f"""- Average sleep score: {s.get('avg_score', 'N/A')}
- Average duration: {s.get('avg_duration_hrs', 'N/A')} hrs
- Average efficiency: {s.get('avg_efficiency_pct', 'N/A')}%
- Average deep sleep: {s.get('avg_deep_hrs', 'N/A')} hrs
- Average REM: {s.get('avg_rem_hrs', 'N/A')} hrs
- Average light sleep: {s.get('avg_light_hrs', 'N/A')} hrs
- Average sleep latency: {s.get('avg_latency_min', 'N/A')} min
- Average WASO: {s.get('avg_waso_min', 'N/A')} min
- Nights tracked: {s.get('nights_count', 'N/A')}
- Nights below 7h: {s.get('nights_below_7h', 'N/A')}
- Score vs 30-day baseline: {s.get('score_vs_baseline', 'N/A')}
"""
    else:
        prompt += "No sleep data available.\n"

    prompt += "\n## Heart Rate\n"
    if metrics.get("heart_rate"):
        h = metrics["heart_rate"]
        prompt += f"""- Average HR: {h.get('avg_hr', 'N/A')} bpm
- Resting HR estimate (10th percentile): {h.get('resting_hr_estimate', 'N/A')} bpm
- Min HR: {h.get('min_hr', 'N/A')} bpm
- Max HR: {h.get('max_hr', 'N/A')} bpm
- Data points: {h.get('readings_count', 'N/A')}
"""
    else:
        prompt += "No heart rate data available.\n"

    prompt += "\n## HRV (Heart Rate Variability)\n"
    if metrics.get("hrv"):
        v = metrics["hrv"]
        prompt += f"""- Average RMSSD: {v.get('avg_rmssd', 'N/A')} ms
- Average SDNN: {v.get('avg_sdnn', 'N/A')} ms
- Min RMSSD: {v.get('min_rmssd', 'N/A')} ms
- Max RMSSD: {v.get('max_rmssd', 'N/A')} ms
- Nights with HRV data: {v.get('nights_with_data', 'N/A')}
"""
    else:
        prompt += "No HRV data available.\n"

    prompt += "\n## Daily Activity\n"
    if metrics.get("activity"):
        a = metrics["activity"]
        prompt += f"""- Average steps: {a.get('avg_steps', 'N/A')}
- Total steps: {a.get('total_steps', 'N/A')}
- Max steps day: {a.get('max_steps', 'N/A')}
- Min steps day: {a.get('min_steps', 'N/A')}
- Days above 10k: {a.get('days_above_10k', 'N/A')}/{a.get('days_count', 'N/A')}
- Average calories (total): {a.get('avg_calories_total', 'N/A')}
- Average active calories: {a.get('avg_calories_active', 'N/A')}
- Average distance: {a.get('avg_distance_m', 'N/A')} m
- Average elevation: {a.get('avg_elevation_m', 'N/A')} m
- Avg soft activity: {a.get('avg_soft_activity_min', 'N/A')} min/day
- Avg moderate activity: {a.get('avg_moderate_activity_min', 'N/A')} min/day
- Avg intense activity: {a.get('avg_intense_activity_min', 'N/A')} min/day
- Steps vs 30-day baseline: {a.get('steps_vs_baseline_pct', 'N/A')}%
"""
    else:
        prompt += "No activity data available.\n"

    prompt += "\n## Body Composition & Vitals\n"
    if metrics.get("body"):
        b = metrics["body"]
        prompt += f"""- Latest weight: {b.get('latest_weight_kg', 'N/A')} kg
- Weight change vs baseline: {b.get('weight_change_kg', 'N/A')} kg
- Body fat: {b.get('latest_fat_pct', 'N/A')}%
- Muscle mass: {b.get('latest_muscle_kg', 'N/A')} kg
- Bone mass: {b.get('latest_bone_kg', 'N/A')} kg
- Hydration: {b.get('avg_hydration_pct', 'N/A')}%
- VO2 Max: {b.get('latest_vo2max', 'N/A')} ml/min/kg
- Skin temperature: {b.get('avg_skin_temp_c', 'N/A')} C
- Body temperature: {b.get('avg_body_temp_c', 'N/A')} C
- SpO2 average: {b.get('avg_spo2_pct', 'N/A')}%
- SpO2 minimum: {b.get('min_spo2_pct', 'N/A')}%
- Visceral fat index: {b.get('visceral_fat_index', 'N/A')}
"""
    else:
        prompt += "No body composition data available.\n"

    prompt += "\n## Workouts\n"
    work_m = metrics.get("workouts", {})
    if work_m and work_m.get("count", 0) > 0:
        prompt += f"""- Total workouts: {work_m.get('count', 0)}
- Total duration: {work_m.get('total_duration_min', 'N/A')} min
- Average duration: {work_m.get('avg_duration_min', 'N/A')} min
- Total calories burned: {work_m.get('total_calories', 'N/A')}
- Average workout HR: {work_m.get('avg_hr', 'N/A')} bpm
- Workout types: {json.dumps(work_m.get('categories', {}))}
"""
    else:
        prompt += "No workouts recorded in this period.\n"

    prompt += "\n## ECG\n"
    if metrics.get("ecg"):
        e = metrics["ecg"]
        prompt += f"""- ECG recordings: {e.get('recordings_count', 'N/A')}
- AFib detections: {e.get('afib_detected_count', 'N/A')}
"""
    else:
        prompt += "No ECG data available.\n"

    prompt += "\n## Advanced HRV Analysis (NeuroKit2)\n"
    if advanced_hrv:
        if advanced_hrv.get("time_domain"):
            td = advanced_hrv["time_domain"]
            prompt += f"""### Time Domain (validated)
- RMSSD: {td.get('rmssd', 'N/A')} ms
- SDNN: {td.get('sdnn', 'N/A')} ms
- Mean NN interval: {td.get('mean_nn', 'N/A')} ms
- pNN50: {td.get('pnn50', 'N/A')}%
- Coefficient of variation: {td.get('cv', 'N/A')}
"""
        if advanced_hrv.get("frequency_domain"):
            fd = advanced_hrv["frequency_domain"]
            prompt += f"""### Frequency Domain (sympathovagal balance)
- LF power (normalized): {fd.get('lf_power', 'N/A')}
- HF power (normalized): {fd.get('hf_power', 'N/A')}
- LF/HF ratio: {fd.get('lf_hf_ratio', 'N/A')}
- VLF power: {fd.get('vlf_power', 'N/A')}
- Autonomic balance: {fd.get('balance', 'N/A')}
- Interpretation: {fd.get('interpretation', 'N/A')}
NOTE: LF/HF ratio < 1.0 = parasympathetic dominant (recovery), 1.0-2.0 = balanced, > 2.0 = sympathetic dominant (stress/exertion)
"""
        if advanced_hrv.get("nonlinear"):
            nl = advanced_hrv["nonlinear"]
            prompt += f"""### Nonlinear Analysis (Poincare)
- SD1 (short-term, parasympathetic): {nl.get('sd1', 'N/A')} ms
- SD2 (long-term, mixed ANS): {nl.get('sd2', 'N/A')} ms
- SD1/SD2 ratio: {nl.get('sd1_sd2_ratio', 'N/A')}
- {nl.get('interpretation', '')}
"""
        if advanced_hrv.get("complexity"):
            cx = advanced_hrv["complexity"]
            prompt += f"""### Complexity
- Sample entropy: {cx.get('sample_entropy', 'N/A')}
- {cx.get('interpretation', '')}
"""
        prompt += f"- Data points analyzed: {advanced_hrv.get('data_points_used', 'N/A')}\n"
    else:
        prompt += "NeuroKit2 analysis not available (insufficient data or library not installed).\n"

    if alerts:
        prompt += "\n## Automated Alerts\n"
        for alert in alerts:
            prompt += f"- [{alert['type'].upper()}] {alert['message']}\n"

    # Day-by-day breakdown (so Claude can see within-week patterns)
    if raw_data:
        cutoff = metrics.get("analysis_start", "")
        prompt += "\n## Day-by-Day Breakdown\n"

        sleep_all = raw_data.get("sleep", [])
        activity_all = raw_data.get("activity", [])
        meetings = raw_data.get("meetings_by_date", {})
        calendar = raw_data.get("calendar_by_date", {})

        # Collect all dates in the analysis period
        dates = set()
        for s in sleep_all:
            d = (s.get("date") or "")[:10]
            if d >= cutoff:
                dates.add(d)
        for a in activity_all:
            d = (a.get("date") or "")[:10]
            if d >= cutoff:
                dates.add(d)
        dates.update(k for k in meetings.keys() if k >= cutoff)
        dates.update(k for k in calendar.keys() if k >= cutoff)

        sleep_by_date = {(s.get("date") or "")[:10]: s for s in sleep_all}
        activity_by_date = {(a.get("date") or "")[:10]: a for a in activity_all}

        for d in sorted(dates):
            parts = [d]
            s = sleep_by_date.get(d)
            if s:
                dur = round(s["duration_total_s"] / 3600, 1) if s.get("duration_total_s") else "?"
                parts.append(f"sleep:{s.get('sleep_score', '?')}/100 {dur}h")
            a = activity_by_date.get(d)
            if a:
                parts.append(f"steps:{a.get('steps', '?')}")
            mc = meetings.get(d, 0)
            cc = len(calendar.get(d, []))
            if mc or cc:
                parts.append(f"meetings:{mc} events:{cc}")
            cal_names = calendar.get(d, [])
            if cal_names:
                parts.append(f"[{', '.join(n for n in cal_names[:5] if n)}]")
            prompt += "- " + " | ".join(parts) + "\n"

    # Long-term patterns (90-day correlations)
    if long_term_patterns:
        prompt += "\n## Long-Term Patterns (90 days)\n"
        prompt += "These are statistically detected correlations from your historical data:\n"
        for p in long_term_patterns:
            conf = p.get("confidence", 0)
            prompt += f"- {p.get('description', '')} (n={p.get('sample_size', '?')}, confidence={conf:.0%})\n"

    # Previous week's insights (so Claude doesn't repeat itself)
    if previous_insights:
        prompt += "\n## Previous Analysis (DO NOT repeat these, note changes instead)\n"
        prompt += f"- Previous overall: {previous_insights.get('overall_score', '?')}/100\n"
        prompt += f"- Previous summary: {(previous_insights.get('summary') or '')[:300]}\n"
        prev_focus = previous_insights.get("weekly_focus")
        if prev_focus:
            prompt += f"- Previous focus: {prev_focus}\n"
        prev_recs = previous_insights.get("recommendations")
        if prev_recs:
            if isinstance(prev_recs, str):
                prev_recs = json.loads(prev_recs)
            high_recs = [r for r in prev_recs if r.get("priority") == "high"]
            if high_recs:
                prompt += "- Previous high-priority recommendations:\n"
                for r in high_recs[:3]:
                    prompt += f"  * {r.get('action', '')[:150]}\n"
        prompt += "\nNote any progress or regression compared to last analysis. Did the user follow through on recommendations?\n"

    if personal_context:
        prompt += f"""
## Personal Context
The following is extracted from the user's journals, voice memos, reflections, and profile.
Use this to personalize your analysis (e.g., if they mention an injury, adapt exercise recommendations;
if they're traveling, consider jet lag effects on sleep; if they have fitness goals, track progress).

{personal_context}
"""

    prompt += """
## Your Task

Respond with a JSON object (no markdown code fences, just raw JSON) with this exact structure:

{
  "summary": "2-3 sentence executive summary of overall health status, highlighting the most important findings.",
  "findings": [
    {
      "category": "sleep|cardiovascular|activity|body_composition|recovery|stress",
      "title": "Short finding title (5-8 words)",
      "detail": "1-2 sentence explanation with specific numbers from the data.",
      "severity": "positive|neutral|attention|warning",
      "metric_refs": ["metric_name_1", "metric_name_2"]
    }
  ],
  "correlations": [
    {
      "metrics": ["metric_a", "metric_b"],
      "direction": "positive|negative|divergent",
      "interpretation": "1-2 sentence explanation of the relationship observed."
    }
  ],
  "recommendations": [
    {
      "category": "sleep|cardiovascular|activity|body_composition|recovery|stress|general",
      "action": "Specific, actionable recommendation.",
      "rationale": "Why this matters, with reference to the data.",
      "priority": "high|medium|low"
    }
  ],
  "score_adjustments": {
    "recovery": null,
    "sleep": null,
    "cardiovascular": null,
    "fitness": null,
    "body_composition": null,
    "stress": null,
    "overall": null
  },
  "weekly_focus": "One sentence describing what to focus on this week."
}

Generate 4-8 findings, 2-4 correlations, and 3-6 recommendations. For score_adjustments, provide a value only if you believe the rule-based score should be adjusted (e.g., {"sleep": -5} to lower sleep score by 5 points due to context the rules missed). Use null for no adjustment.

Be specific, reference actual numbers, and prioritize actionable insights."""

    return prompt


def generate_health_insights(
    supabase: Client,
    days: int = 7,
    force: bool = False,
) -> dict[str, Any]:
    """
    Generate comprehensive health insights using Claude.

    Args:
        supabase: Supabase client
        days: Number of days to analyze
        force: If True, regenerate even if recent insights exist

    Returns:
        Complete insights response with scores, findings, and recommendations.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    # Check for recent insights (don't regenerate within 6 hours unless forced)
    if not force:
        period_label = f"{days}d"
        existing = supabase.table("health_insights").select("*").eq(
            "period_label", period_label
        ).order("generated_at", desc=True).limit(1).execute()

        if existing.data:
            last_generated = existing.data[0].get("generated_at", "")
            if last_generated:
                try:
                    last_dt = datetime.fromisoformat(last_generated.replace("Z", "+00:00"))
                    if datetime.now(timezone.utc) - last_dt < timedelta(hours=6):
                        logger.info("Recent insights exist, returning cached version")
                        return existing.data[0]
                except (ValueError, TypeError):
                    pass

    start_time = time.time()

    # Step 1: Fetch all health data
    logger.info(f"Generating health insights for {days} days")
    raw_data = _fetch_health_data(supabase, days)

    # Step 2: Compute metrics
    metrics = _compute_metrics(raw_data, days)

    # Step 3: Score categories
    scores = _score_categories(metrics)

    # Step 4: Detect alerts
    alerts = _detect_alerts(metrics)

    # Step 4b: Advanced HRV analysis (NeuroKit2)
    advanced_hrv = _compute_advanced_hrv(raw_data.get("sleep_details", []))
    if advanced_hrv:
        metrics["advanced_hrv"] = advanced_hrv

    # Step 5: Fetch personal context
    personal_context = _fetch_personal_context(supabase, days=max(days, 30))

    # Step 5b: Detect and persist long-term patterns
    long_term_patterns = _detect_long_term_patterns(supabase)
    _persist_patterns(supabase, long_term_patterns)

    # Step 5c: Fetch previous insights (so Claude knows what it already said)
    previous_insights = None
    try:
        prev_resp = supabase.table("health_insights").select(
            "overall_score,summary,weekly_focus,recommendations"
        ).order("generated_at", desc=True).limit(1).execute()
        if prev_resp.data:
            previous_insights = prev_resp.data[0]
    except Exception:
        pass

    # Step 6: Build prompt and call Claude
    prompt = _build_analysis_prompt(
        metrics, scores, alerts, advanced_hrv, personal_context,
        raw_data=raw_data,
        long_term_patterns=long_term_patterns,
        previous_insights=previous_insights,
    )

    client = anthropic.Anthropic(api_key=api_key)
    # Try primary model, fall back to alternatives
    models_to_try = [ANALYSIS_MODEL, "claude-sonnet-4-5-20250514", "claude-3-7-sonnet-20250219", "claude-3-5-sonnet-20241022"]
    response = None
    used_model = ANALYSIS_MODEL
    for model_id in models_to_try:
        try:
            response = client.messages.create(
                model=model_id,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            used_model = model_id
            break
        except anthropic.NotFoundError:
            logger.warning(f"Model {model_id} not available, trying next")
            continue
    if response is None:
        raise ValueError("No available Claude model found")

    generation_time = int((time.time() - start_time) * 1000)

    # Step 6: Parse Claude's response
    raw_text = response.content[0].text.strip()
    # Strip markdown code fences if present
    if raw_text.startswith("```"):
        lines = raw_text.split("\n")
        raw_text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    try:
        ai_analysis = json.loads(raw_text)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Claude response as JSON: {e}")
        logger.debug(f"Raw response: {raw_text[:500]}")
        ai_analysis = {
            "summary": raw_text[:500],
            "findings": [],
            "correlations": [],
            "recommendations": [],
            "score_adjustments": {},
            "weekly_focus": "Unable to parse structured insights.",
        }

    # Step 7: Apply score adjustments from Claude
    final_scores = dict(scores)
    adjustments = ai_analysis.get("score_adjustments", {})
    for cat, adj in adjustments.items():
        if adj is not None and cat in final_scores and final_scores[cat] is not None:
            final_scores[cat] = max(0, min(100, final_scores[cat] + adj))

    # Recompute overall if any adjustment
    if any(v is not None for v in adjustments.values()):
        overall_components = []
        for cat, weight in CATEGORY_WEIGHTS.items():
            if final_scores.get(cat) is not None:
                overall_components.append((cat, final_scores[cat], weight))
        final_scores["overall"] = _weighted_score(overall_components)

    # Step 8: Build result
    now = datetime.now(timezone.utc)
    period_start = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    period_end = now.strftime("%Y-%m-%d")

    result = {
        "period_start": period_start,
        "period_end": period_end,
        "period_label": f"{days}d",
        "overall_score": final_scores.get("overall"),
        "recovery_score": final_scores.get("recovery"),
        "sleep_score": final_scores.get("sleep"),
        "cardiovascular_score": final_scores.get("cardiovascular"),
        "fitness_score": final_scores.get("fitness"),
        "body_composition_score": final_scores.get("body_composition"),
        "stress_score": final_scores.get("stress"),
        "summary": ai_analysis.get("summary", ""),
        "findings": ai_analysis.get("findings", []),
        "correlations": ai_analysis.get("correlations", []),
        "recommendations": ai_analysis.get("recommendations", []),
        "alerts": alerts,
        "weekly_focus": ai_analysis.get("weekly_focus", ""),
        "data_snapshot": metrics,
        "model_used": used_model,
        "prompt_tokens": response.usage.input_tokens,
        "completion_tokens": response.usage.output_tokens,
        "generation_time_ms": generation_time,
        "generated_at": now.isoformat(),
    }

    # Step 9: Persist to Supabase
    try:
        db_record = {
            "period_start": period_start,
            "period_end": period_end,
            "period_label": f"{days}d",
            "overall_score": final_scores.get("overall"),
            "recovery_score": final_scores.get("recovery"),
            "sleep_score": final_scores.get("sleep"),
            "cardiovascular_score": final_scores.get("cardiovascular"),
            "fitness_score": final_scores.get("fitness"),
            "body_composition_score": final_scores.get("body_composition"),
            "stress_score": final_scores.get("stress"),
            "summary": ai_analysis.get("summary", ""),
            "weekly_focus": ai_analysis.get("weekly_focus", ""),
            "findings": json.dumps(ai_analysis.get("findings", [])),
            "correlations": json.dumps(ai_analysis.get("correlations", [])),
            "recommendations": json.dumps(ai_analysis.get("recommendations", [])),
            "alerts": json.dumps(alerts),
            "data_snapshot": json.dumps(metrics),
            "model_used": used_model,
            "prompt_tokens": response.usage.input_tokens,
            "completion_tokens": response.usage.output_tokens,
            "generation_time_ms": generation_time,
        }
        supabase.table("health_insights").upsert(
            db_record, on_conflict="period_start,period_end,period_label"
        ).execute()
        logger.info(f"Health insights saved: overall={final_scores.get('overall')}, "
                     f"tokens={response.usage.input_tokens}+{response.usage.output_tokens}, "
                     f"time={generation_time}ms")
    except Exception as e:
        logger.error(f"Failed to persist health insights: {e}")
        # Don't fail the response, just log

    return result


# ---------------------------------------------------------------------------
# Telegram formatting
# ---------------------------------------------------------------------------

# Score emojis for Telegram
_SCORE_EMOJI = {
    range(0, 40): "🔴",
    range(40, 60): "🟠",
    range(60, 80): "🟡",
    range(80, 101): "🟢",
}


def _score_dot(score: Optional[int]) -> str:
    """Return colored dot emoji for a score."""
    if score is None:
        return "⚪"
    for rng, emoji in _SCORE_EMOJI.items():
        if score in rng:
            return emoji
    return "⚪"


def _severity_emoji(severity: str) -> str:
    """Return emoji for finding severity."""
    return {
        "positive": "✅",
        "neutral": "ℹ️",
        "attention": "⚠️",
        "warning": "🚨",
    }.get(severity, "")


def format_telegram_briefing(insights: dict[str, Any]) -> str:
    """
    Format health insights into a concise Telegram message.

    Designed for the morning health briefing: shows overall score,
    category breakdown, top findings, and the weekly focus.
    """
    overall = insights.get("overall_score")
    summary = insights.get("summary", "")
    weekly_focus = insights.get("weekly_focus", "")
    findings = insights.get("findings", [])
    recommendations = insights.get("recommendations", [])
    alerts = insights.get("alerts", [])

    # Parse JSON strings if needed
    if isinstance(findings, str):
        findings = json.loads(findings)
    if isinstance(recommendations, str):
        recommendations = json.loads(recommendations)
    if isinstance(alerts, str):
        alerts = json.loads(alerts)

    lines = []
    lines.append(f"🏥 *Health Insights* | Overall: {_score_dot(overall)} *{overall or '--'}/100*")
    lines.append("")

    # Category scores
    categories = [
        ("Recovery", insights.get("recovery_score")),
        ("Sleep", insights.get("sleep_score")),
        ("Cardio", insights.get("cardiovascular_score")),
        ("Fitness", insights.get("fitness_score")),
        ("Body", insights.get("body_composition_score")),
        ("Stress", insights.get("stress_score")),
    ]
    score_line = " | ".join(
        f"{_score_dot(s)} {name} {s or '--'}" for name, s in categories
    )
    lines.append(score_line)
    lines.append("")

    # Summary
    if summary:
        lines.append(f"_{summary}_")
        lines.append("")

    # Alerts (urgent first)
    if alerts:
        for alert in alerts[:3]:
            msg = alert.get("message", "") if isinstance(alert, dict) else str(alert)
            lines.append(f"🚨 {msg}")
        lines.append("")

    # Top findings (max 4)
    if findings:
        lines.append("*Key Findings:*")
        for f in findings[:4]:
            sev = _severity_emoji(f.get("severity", "neutral"))
            lines.append(f"{sev} {f.get('title', '')}")
        lines.append("")

    # Top recommendations (max 3, high priority first)
    high_recs = [r for r in recommendations if r.get("priority") == "high"]
    other_recs = [r for r in recommendations if r.get("priority") != "high"]
    top_recs = (high_recs + other_recs)[:3]
    if top_recs:
        lines.append("*Recommendations:*")
        for r in top_recs:
            priority = "❗" if r.get("priority") == "high" else "➡️"
            lines.append(f"{priority} {r.get('action', '')}")
        lines.append("")

    # Weekly focus
    if weekly_focus:
        lines.append(f"🎯 *Focus:* {weekly_focus}")

    # Compact footer
    lines.append("")
    lines.append("_View full analysis in Health Dashboard_")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _avg(values: list) -> float:
    """Safe average of a list of numbers."""
    if not values:
        return 0
    return sum(values) / len(values)


def _weighted_score(components: list[tuple[str, float, float]]) -> Optional[int]:
    """Compute weighted score from (name, score, weight) tuples."""
    if not components:
        return None
    total_weight = sum(w for _, _, w in components)
    if total_weight == 0:
        return None
    weighted_sum = sum(score * weight for _, score, weight in components)
    return round(weighted_sum / total_weight)


def _avg_from_jsonb(ts_data: Any) -> Optional[float]:
    """Extract average from a JSONB time-series {unix_ts: value}."""
    if not ts_data:
        return None
    if isinstance(ts_data, str):
        try:
            ts_data = json.loads(ts_data)
        except (json.JSONDecodeError, TypeError):
            return None
    if isinstance(ts_data, dict):
        values = [v for v in ts_data.values() if v is not None and isinstance(v, (int, float))]
        return _avg(values) if values else None
    return None


# ---------------------------------------------------------------------------
# Daily micro-briefing
# ---------------------------------------------------------------------------

DAILY_MODEL = "claude-haiku-4-5-20251001"

DAILY_SYSTEM_PROMPT = """You are a concise health analyst for a personal health dashboard.
You generate a short daily morning briefing based on last night's sleep and yesterday's activity,
compared against the user's 30-day baselines.

Rules:
- Be EXTREMELY concise. 2-5 short lines max.
- Only mention what's NOTEWORTHY. If everything is normal, say so in one line.
- Use specific numbers (e.g., "7.2h sleep, score 82").
- Compare to baselines with arrows or deltas (e.g., "+12% vs avg", "below your 65 avg").
- If data is missing, say "no data" for that metric, don't speculate.
- Never repeat what you said yesterday. The previous briefing is provided, avoid redundancy.
- Address the user as "you". No greetings, no sign-offs.
- If there are long-term patterns detected, mention the most relevant one briefly.
- Include a one-line readiness assessment at the end (e.g., "Good day to train" or "Consider rest today").
- No markdown headers. Just clean lines with emoji sparingly."""


def _fetch_daily_data(supabase: Client) -> dict[str, Any]:
    """Fetch last night's sleep, yesterday's activity, and 30-day baselines."""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    baseline_start = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    hr_since = (now - timedelta(days=1)).isoformat()

    data: dict[str, Any] = {}

    try:
        # Last night's sleep (most recent)
        sleep_resp = supabase.table("health_sleep").select("*").order(
            "date", desc=True
        ).limit(1).execute()
        data["last_sleep"] = sleep_resp.data[0] if sleep_resp.data else None

        # 30-day sleep baseline
        sleep_baseline = supabase.table("health_sleep").select(
            "sleep_score,duration_total_s,duration_deep_s,duration_rem_s"
        ).gte("date", baseline_start).execute()
        data["sleep_baseline"] = sleep_baseline.data or []

        # Yesterday's activity
        activity_resp = supabase.table("health_activity").select("*").order(
            "date", desc=True
        ).limit(1).execute()
        data["last_activity"] = activity_resp.data[0] if activity_resp.data else None

        # 30-day activity baseline
        activity_baseline = supabase.table("health_activity").select(
            "steps,calories_active"
        ).gte("date", baseline_start).execute()
        data["activity_baseline"] = activity_baseline.data or []

        # Latest HR (last 24h, for resting HR estimate)
        hr_resp = supabase.table("health_heart_rate").select(
            "heart_rate"
        ).gte("timestamp", hr_since).order("timestamp", desc=True).limit(500).execute()
        hr_values = [h["heart_rate"] for h in (hr_resp.data or []) if h.get("heart_rate") and h["heart_rate"] > 0]
        if hr_values:
            sorted_hr = sorted(hr_values)
            resting_count = max(1, len(sorted_hr) // 10)
            data["resting_hr"] = round(_avg(sorted_hr[:resting_count]))
            data["avg_hr"] = round(_avg(hr_values))
        else:
            data["resting_hr"] = None
            data["avg_hr"] = None

        # Latest HRV from sleep details
        hrv_resp = supabase.table("health_sleep_details").select(
            "rmssd,sdnn_1"
        ).order("sleep_date", desc=True).limit(1).execute()
        if hrv_resp.data and hrv_resp.data[0].get("rmssd"):
            data["last_rmssd"] = _avg_from_jsonb(hrv_resp.data[0]["rmssd"])
        else:
            data["last_rmssd"] = None

        # 30-day HRV baseline
        hrv_baseline = supabase.table("health_sleep_details").select(
            "rmssd"
        ).gte("sleep_date", baseline_start).execute()
        rmssd_vals = []
        for sd in (hrv_baseline.data or []):
            v = _avg_from_jsonb(sd.get("rmssd"))
            if v:
                rmssd_vals.append(v)
        data["rmssd_baseline_avg"] = round(_avg(rmssd_vals), 1) if rmssd_vals else None

        # Today's calendar events (meetings)
        cal_resp = supabase.table("calendar_events").select(
            "summary,start_time,end_time"
        ).gte("start_time", today).lte(
            "start_time", (now + timedelta(days=1)).strftime("%Y-%m-%d")
        ).order("start_time").execute()
        data["today_events"] = cal_resp.data or []

    except Exception as e:
        logger.error(f"Failed to fetch daily health data: {e}")
        raise

    return data


def _fetch_previous_briefing(supabase: Client) -> Optional[str]:
    """Fetch yesterday's daily briefing so we don't repeat ourselves."""
    try:
        resp = supabase.table("health_daily_briefings").select(
            "briefing_text"
        ).order("generated_at", desc=True).limit(1).execute()
        if resp.data:
            return resp.data[0].get("briefing_text", "")
    except Exception:
        pass
    return None


def _fetch_long_term_patterns(supabase: Client) -> list[dict[str, Any]]:
    """Fetch stored long-term patterns from health_patterns table."""
    try:
        resp = supabase.table("health_patterns").select("*").eq(
            "active", True
        ).order("confidence", desc=True).limit(10).execute()
        return resp.data or []
    except Exception:
        return []


def _detect_long_term_patterns(supabase: Client) -> list[dict[str, Any]]:
    """
    Detect long-term correlations across 90 days of data.

    Looks for patterns like:
    - Meeting-heavy days -> worse sleep that night
    - High activity days -> better HRV next morning
    - Late calendar events -> longer sleep latency
    - Workout days -> lower resting HR next day
    """
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=90)).strftime("%Y-%m-%d")
    patterns: list[dict[str, Any]] = []

    try:
        # Get sleep data with dates
        sleep_resp = supabase.table("health_sleep").select(
            "date,sleep_score,duration_total_s,duration_deep_s,duration_rem_s,"
            "sleep_latency_s,waso_s,total_timeinbed_s"
        ).gte("date", start).order("date").execute()
        sleep_by_date = {s["date"][:10]: s for s in (sleep_resp.data or [])}

        # Get activity data
        activity_resp = supabase.table("health_activity").select(
            "date,steps,calories_active,intense_activity_seconds,moderate_activity_seconds"
        ).gte("date", start).order("date").execute()
        activity_by_date = {a["date"][:10]: a for a in (activity_resp.data or [])}

        # Get meeting counts per day
        meeting_resp = supabase.table("meetings").select(
            "date"
        ).gte("date", start).is_("deleted_at", "null").execute()
        meeting_counts: dict[str, int] = {}
        for m in (meeting_resp.data or []):
            d = (m.get("date") or "")[:10]
            if d:
                meeting_counts[d] = meeting_counts.get(d, 0) + 1

        # Get calendar events per day (more complete than meetings table)
        cal_resp = supabase.table("calendar_events").select(
            "start_time,end_time"
        ).gte("start_time", start).execute()
        cal_counts: dict[str, int] = {}
        late_event_dates: set[str] = set()
        for e in (cal_resp.data or []):
            st = e.get("start_time", "")
            if st:
                d = st[:10]
                cal_counts[d] = cal_counts.get(d, 0) + 1
                # Check if event is after 18:00 UTC (could be late meeting)
                hour = int(st[11:13]) if len(st) > 13 else 0
                if hour >= 12:  # 12 UTC = 20:00 SGT
                    late_event_dates.add(d)

        # Get workout days
        workout_resp = supabase.table("health_workouts").select(
            "date"
        ).gte("date", start).execute()
        workout_dates = set()
        for w in (workout_resp.data or []):
            d = (w.get("date") or "")[:10]
            if d:
                workout_dates.add(d)

        # --- Pattern 1: Meeting-heavy days -> sleep quality ---
        high_meeting_sleep = []
        low_meeting_sleep = []
        for date_str, sleep in sleep_by_date.items():
            score = sleep.get("sleep_score")
            if not score:
                continue
            # Check previous day's meetings (meetings affect that night's sleep)
            prev_day = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            mc = meeting_counts.get(prev_day, 0) + cal_counts.get(prev_day, 0)
            if mc >= 3:
                high_meeting_sleep.append(score)
            elif mc == 0:
                low_meeting_sleep.append(score)

        if len(high_meeting_sleep) >= 3 and len(low_meeting_sleep) >= 3:
            avg_high = round(_avg(high_meeting_sleep), 1)
            avg_low = round(_avg(low_meeting_sleep), 1)
            diff = round(avg_low - avg_high, 1)
            if abs(diff) > 3:
                patterns.append({
                    "pattern_key": "meetings_vs_sleep",
                    "description": f"Days with 3+ meetings/events correlate with sleep score {avg_high} vs {avg_low} on quiet days (delta: {diff:+.1f})",
                    "direction": "negative" if diff > 0 else "positive",
                    "strength": min(abs(diff) / 10, 1.0),
                    "confidence": min(len(high_meeting_sleep) + len(low_meeting_sleep), 30) / 30,
                    "sample_size": len(high_meeting_sleep) + len(low_meeting_sleep),
                    "data": {"high_meeting_avg": avg_high, "low_meeting_avg": avg_low},
                })

        # --- Pattern 2: Active days -> next night HRV ---
        # (Using activity calories as proxy for activity intensity)
        active_day_next_sleep = []
        inactive_day_next_sleep = []
        for date_str, activity in activity_by_date.items():
            cal_active = activity.get("calories_active") or 0
            steps = activity.get("steps") or 0
            next_day = (datetime.strptime(date_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            next_sleep = sleep_by_date.get(next_day)
            if not next_sleep or not next_sleep.get("sleep_score"):
                continue
            if steps > 8000 or cal_active > 300:
                active_day_next_sleep.append(next_sleep["sleep_score"])
            elif steps < 3000:
                inactive_day_next_sleep.append(next_sleep["sleep_score"])

        if len(active_day_next_sleep) >= 3 and len(inactive_day_next_sleep) >= 3:
            avg_active = round(_avg(active_day_next_sleep), 1)
            avg_inactive = round(_avg(inactive_day_next_sleep), 1)
            diff = round(avg_active - avg_inactive, 1)
            if abs(diff) > 3:
                patterns.append({
                    "pattern_key": "activity_vs_sleep",
                    "description": f"Active days (8k+ steps) followed by sleep score {avg_active} vs {avg_inactive} after sedentary days (delta: {diff:+.1f})",
                    "direction": "positive" if diff > 0 else "negative",
                    "strength": min(abs(diff) / 10, 1.0),
                    "confidence": min(len(active_day_next_sleep) + len(inactive_day_next_sleep), 30) / 30,
                    "sample_size": len(active_day_next_sleep) + len(inactive_day_next_sleep),
                    "data": {"active_avg": avg_active, "inactive_avg": avg_inactive},
                })

        # --- Pattern 3: Workout days -> next day resting HR ---
        # We don't have daily RHR easily, so use sleep deep % as recovery proxy
        workout_next_deep = []
        no_workout_next_deep = []
        for date_str, sleep in sleep_by_date.items():
            deep = sleep.get("duration_deep_s")
            total = sleep.get("duration_total_s")
            if not deep or not total or total == 0:
                continue
            deep_pct = deep / total * 100
            prev_day = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            if prev_day in workout_dates:
                workout_next_deep.append(deep_pct)
            else:
                no_workout_next_deep.append(deep_pct)

        if len(workout_next_deep) >= 3 and len(no_workout_next_deep) >= 3:
            avg_workout = round(_avg(workout_next_deep), 1)
            avg_no = round(_avg(no_workout_next_deep), 1)
            diff = round(avg_workout - avg_no, 1)
            if abs(diff) > 2:
                patterns.append({
                    "pattern_key": "workout_vs_deep_sleep",
                    "description": f"Workout days: {avg_workout:.0f}% deep sleep vs {avg_no:.0f}% on rest days (delta: {diff:+.1f}%)",
                    "direction": "positive" if diff > 0 else "negative",
                    "strength": min(abs(diff) / 8, 1.0),
                    "confidence": min(len(workout_next_deep) + len(no_workout_next_deep), 30) / 30,
                    "sample_size": len(workout_next_deep) + len(no_workout_next_deep),
                    "data": {"workout_avg": avg_workout, "no_workout_avg": avg_no},
                })

        # --- Pattern 4: Late events -> sleep latency ---
        late_event_latency = []
        no_late_latency = []
        for date_str, sleep in sleep_by_date.items():
            latency = sleep.get("sleep_latency_s")
            if not latency:
                continue
            prev_day = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            if prev_day in late_event_dates:
                late_event_latency.append(latency / 60)
            else:
                no_late_latency.append(latency / 60)

        if len(late_event_latency) >= 3 and len(no_late_latency) >= 3:
            avg_late = round(_avg(late_event_latency), 1)
            avg_no = round(_avg(no_late_latency), 1)
            diff = round(avg_late - avg_no, 1)
            if abs(diff) > 2:
                patterns.append({
                    "pattern_key": "late_events_vs_latency",
                    "description": f"Late events (after 8pm): {avg_late:.0f}min sleep latency vs {avg_no:.0f}min normally (delta: {diff:+.1f}min)",
                    "direction": "negative" if diff > 0 else "positive",
                    "strength": min(abs(diff) / 15, 1.0),
                    "confidence": min(len(late_event_latency) + len(no_late_latency), 20) / 20,
                    "sample_size": len(late_event_latency) + len(no_late_latency),
                    "data": {"late_avg": avg_late, "no_late_avg": avg_no},
                })

    except Exception as e:
        logger.error(f"Long-term pattern detection failed: {e}")

    return patterns


def _persist_patterns(supabase: Client, patterns: list[dict[str, Any]]) -> None:
    """Upsert detected patterns to health_patterns table."""
    if not patterns:
        return
    now = datetime.now(timezone.utc).isoformat()
    for p in patterns:
        try:
            record = {
                "pattern_key": p["pattern_key"],
                "description": p["description"],
                "direction": p.get("direction"),
                "strength": round(p.get("strength", 0), 3),
                "confidence": round(p.get("confidence", 0), 3),
                "sample_size": p.get("sample_size", 0),
                "data": json.dumps(p.get("data", {})),
                "active": True,
                "last_detected_at": now,
            }
            supabase.table("health_patterns").upsert(
                record, on_conflict="pattern_key"
            ).execute()
        except Exception as e:
            logger.debug(f"Failed to persist pattern {p['pattern_key']}: {e}")


def generate_daily_briefing(supabase: Client) -> dict[str, Any]:
    """
    Generate a concise daily morning health briefing.

    Uses Haiku for cost efficiency. Compares last night's sleep and
    yesterday's activity against 30-day baselines. Includes long-term
    patterns and avoids repeating yesterday's briefing.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    start_time = time.time()

    # Fetch data
    daily = _fetch_daily_data(supabase)
    prev_briefing = _fetch_previous_briefing(supabase)
    patterns = _fetch_long_term_patterns(supabase)

    # Also re-detect patterns (updates stored patterns)
    fresh_patterns = _detect_long_term_patterns(supabase)
    _persist_patterns(supabase, fresh_patterns)
    # Use fresh if available, fall back to stored
    if fresh_patterns:
        patterns = fresh_patterns

    # Build compact prompt
    prompt_parts = []

    # Last night's sleep
    s = daily.get("last_sleep")
    if s:
        sleep_date = (s.get("date") or "")[:10]
        dur_h = round(s["duration_total_s"] / 3600, 1) if s.get("duration_total_s") else None
        deep_h = round(s["duration_deep_s"] / 3600, 2) if s.get("duration_deep_s") else None
        rem_h = round(s["duration_rem_s"] / 3600, 2) if s.get("duration_rem_s") else None
        prompt_parts.append(f"LAST NIGHT ({sleep_date}): score={s.get('sleep_score')}, "
                           f"duration={dur_h}h, deep={deep_h}h, REM={rem_h}h, "
                           f"latency={round(s['sleep_latency_s']/60, 1) if s.get('sleep_latency_s') else '?'}min, "
                           f"waso={round(s['waso_s']/60, 1) if s.get('waso_s') else '?'}min")
    else:
        prompt_parts.append("LAST NIGHT: no sleep data")

    # Baselines
    sb = daily.get("sleep_baseline", [])
    if sb:
        scores = [x["sleep_score"] for x in sb if x.get("sleep_score")]
        durs = [x["duration_total_s"] / 3600 for x in sb if x.get("duration_total_s")]
        prompt_parts.append(f"30-DAY SLEEP BASELINE: avg score={round(_avg(scores), 1) if scores else '?'}, "
                           f"avg duration={round(_avg(durs), 1) if durs else '?'}h, "
                           f"nights tracked={len(scores)}")

    # Yesterday's activity
    a = daily.get("last_activity")
    if a:
        act_date = (a.get("date") or "")[:10]
        prompt_parts.append(f"YESTERDAY ACTIVITY ({act_date}): steps={a.get('steps')}, "
                           f"active cal={a.get('calories_active')}, "
                           f"hr avg={a.get('hr_average')}")
    else:
        prompt_parts.append("YESTERDAY ACTIVITY: no data")

    # Activity baseline
    ab = daily.get("activity_baseline", [])
    if ab:
        steps_bl = [x["steps"] for x in ab if x.get("steps")]
        prompt_parts.append(f"30-DAY ACTIVITY BASELINE: avg steps={round(_avg(steps_bl)) if steps_bl else '?'}, "
                           f"days tracked={len(steps_bl)}")

    # Heart rate
    if daily.get("resting_hr"):
        prompt_parts.append(f"RESTING HR: {daily['resting_hr']} bpm")
    if daily.get("last_rmssd"):
        bl = daily.get("rmssd_baseline_avg")
        prompt_parts.append(f"HRV (RMSSD): {round(daily['last_rmssd'], 1)}ms"
                           f"{f' (baseline: {bl}ms)' if bl else ''}")

    # Today's schedule
    events = daily.get("today_events", [])
    if events:
        meeting_count = len([e for e in events if not any(
            kw in (e.get("summary") or "").lower()
            for kw in ["birthday", "gym", "swim", "run", "workout", "mobility"]
        )])
        workout_events = [e for e in events if any(
            kw in (e.get("summary") or "").lower()
            for kw in ["gym", "swim", "run", "workout", "mobility", "sprint"]
        )]
        prompt_parts.append(f"TODAY: {meeting_count} meetings/events, "
                           f"{len(workout_events)} planned workouts"
                           f"{' (' + ', '.join(e.get('summary','') for e in workout_events) + ')' if workout_events else ''}")

    # Long-term patterns
    if patterns:
        prompt_parts.append("LONG-TERM PATTERNS (90 days):")
        for p in patterns[:4]:
            prompt_parts.append(f"  - {p.get('description', p.get('pattern_key', ''))}"
                               f" (confidence: {p.get('confidence', 0):.0%})")

    # Previous briefing (to avoid repetition)
    if prev_briefing:
        prompt_parts.append(f"YESTERDAY'S BRIEFING (do NOT repeat this): {prev_briefing[:300]}")

    prompt = "\n".join(prompt_parts)

    # Call Haiku
    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model=DAILY_MODEL,
            max_tokens=500,
            system=DAILY_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.NotFoundError:
        # Fallback to Sonnet if Haiku not available
        response = client.messages.create(
            model=ANALYSIS_MODEL,
            max_tokens=500,
            system=DAILY_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )

    briefing_text = response.content[0].text.strip()
    generation_time = int((time.time() - start_time) * 1000)

    # Persist
    result = {
        "briefing_text": briefing_text,
        "sleep_date": (daily.get("last_sleep", {}) or {}).get("date", "")[:10],
        "activity_date": (daily.get("last_activity", {}) or {}).get("date", "")[:10],
        "model_used": DAILY_MODEL,
        "prompt_tokens": response.usage.input_tokens,
        "completion_tokens": response.usage.output_tokens,
        "generation_time_ms": generation_time,
    }

    try:
        supabase.table("health_daily_briefings").insert({
            "briefing_text": briefing_text,
            "sleep_date": result["sleep_date"] or None,
            "activity_date": result["activity_date"] or None,
            "model_used": result["model_used"],
            "prompt_tokens": result["prompt_tokens"],
            "completion_tokens": result["completion_tokens"],
            "generation_time_ms": generation_time,
        }).execute()
    except Exception as e:
        logger.error(f"Failed to persist daily briefing: {e}")

    logger.info(f"Daily briefing generated: {len(briefing_text)} chars, "
                f"tokens={response.usage.input_tokens}+{response.usage.output_tokens}, "
                f"time={generation_time}ms")

    return result


def format_daily_telegram(result: dict[str, Any]) -> str:
    """Format daily briefing for Telegram."""
    text = result.get("briefing_text", "No briefing generated.")
    return f"☀️ *Morning Health Check*\n\n{text}"
