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
# Claude AI analysis
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert health analyst with deep knowledge of sleep science, cardiovascular physiology, exercise science, and body composition. You analyze wearable health data from a Withings ScanWatch 2 Nova smartwatch.

Your role:
- Provide evidence-based health insights, not medical diagnoses
- Identify meaningful patterns and correlations across metrics
- Score health categories and explain the reasoning
- Give specific, actionable recommendations
- Flag concerning trends that warrant attention
- Be direct and clear, not vague or overly cautious
- Reference specific numbers from the data

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
- Be specific: cite actual numbers, percentages, and date ranges"""


def _build_analysis_prompt(
    metrics: dict[str, Any],
    scores: dict[str, Optional[int]],
    alerts: list[dict],
    advanced_hrv: Optional[dict[str, Any]] = None,
) -> str:
    """Build the analysis prompt with all health data context."""

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

    # Step 5: Build prompt and call Claude
    prompt = _build_analysis_prompt(metrics, scores, alerts, advanced_hrv)

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
