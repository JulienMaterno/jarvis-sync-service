"""
Health chart generation for Telegram delivery.

Generates PNG images using matplotlib (Agg backend for headless rendering).
All functions return raw PNG bytes, never write to disk.
Memory-safe: figures are always closed after rendering.

Charts:
- Hypnogram: single-night sleep stages with HR overlay
- Multi-night trends: stacked bar chart of sleep stages
- HR/HRV overlay: dual-axis HR and RMSSD during sleep
- Weekly dashboard: 2x2 composite overview
"""

import io
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

logger = logging.getLogger(__name__)

# Prevent memory warnings on constrained systems
matplotlib.rcParams["figure.max_open_warning"] = 5

# Color scheme
STAGE_COLORS = {
    0: "#E74C3C",  # Wake - red
    1: "#3498DB",  # Light - blue
    2: "#2C3E50",  # Deep - dark blue
    3: "#27AE60",  # REM - green
}
STAGE_LABELS = {0: "Wake", 1: "Light", 2: "Deep", 3: "REM"}
SGT = timedelta(hours=8)


def _to_sgt(ts_str: str) -> datetime:
    """Parse ISO timestamp and convert to SGT."""
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt + SGT


def _fig_to_bytes(fig) -> bytes:
    """Render figure to PNG bytes and close it."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor="white", dpi=100)
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def generate_hypnogram(epochs: list[dict]) -> bytes:
    """Render a hypnogram (sleep stages over time) with HR overlay.

    Args:
        epochs: List of dicts with keys: ts, stage, hr, rmssd
                (from health_sleep_custom.epochs)

    Returns:
        PNG bytes.
    """
    if not epochs:
        return _empty_chart("No epoch data available")

    times = [_to_sgt(e["ts"]) for e in epochs]
    stages = [e["stage"] for e in epochs]
    hrs = [e.get("hr") for e in epochs]

    fig, ax1 = plt.subplots(figsize=(12, 4))

    # Stage bands (filled rectangles)
    stage_y_map = {0: 3, 3: 2, 1: 1, 2: 0}  # Wake top, Deep bottom
    for i in range(len(times) - 1):
        y = stage_y_map.get(stages[i], 1)
        color = STAGE_COLORS.get(stages[i], "#95A5A6")
        ax1.fill_between(
            [times[i], times[i + 1]], y - 0.4, y + 0.4,
            color=color, alpha=0.7, linewidth=0,
        )

    ax1.set_yticks([0, 1, 2, 3])
    ax1.set_yticklabels(["Deep", "Light", "REM", "Wake"])
    ax1.set_ylim(-0.6, 3.6)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.set_xlabel("Time (SGT)")
    ax1.set_title("Sleep Stages", fontsize=12, fontweight="bold")
    ax1.grid(axis="x", alpha=0.3)

    # HR overlay on secondary axis
    valid_hr = [(t, h) for t, h in zip(times, hrs) if h is not None and h > 0]
    if valid_hr:
        ax2 = ax1.twinx()
        hr_times, hr_vals = zip(*valid_hr)
        ax2.plot(hr_times, hr_vals, color="#E67E22", alpha=0.5, linewidth=0.8, label="HR")
        ax2.set_ylabel("Heart Rate (bpm)", color="#E67E22")
        ax2.tick_params(axis="y", labelcolor="#E67E22")

    fig.tight_layout()
    return _fig_to_bytes(fig)


def generate_multi_night_trends(nights: list[dict], days: int = 14) -> bytes:
    """Stacked bar chart of sleep stages per night with score line.

    Args:
        nights: List of dicts from health_sleep_custom, sorted by sleep_date.
                Needs: sleep_date, duration_deep_s, duration_light_s,
                       duration_rem_s, duration_awake_s, custom_sleep_score
        days: Number of nights to show.

    Returns:
        PNG bytes.
    """
    if not nights:
        return _empty_chart("No sleep data available")

    # Take last N nights
    nights = sorted(nights, key=lambda x: x["sleep_date"])[-days:]

    dates = [n["sleep_date"][:10] for n in nights]
    deep = [int(n.get("duration_deep_s") or 0) / 3600 for n in nights]
    light = [int(n.get("duration_light_s") or 0) / 3600 for n in nights]
    rem = [int(n.get("duration_rem_s") or 0) / 3600 for n in nights]
    awake = [int(n.get("duration_awake_s") or 0) / 3600 for n in nights]
    scores = [n.get("custom_sleep_score") for n in nights]

    x = np.arange(len(dates))
    width = 0.7

    fig, ax1 = plt.subplots(figsize=(12, 5))

    # Stacked bars
    ax1.bar(x, deep, width, label="Deep", color=STAGE_COLORS[2])
    ax1.bar(x, rem, width, bottom=deep, label="REM", color=STAGE_COLORS[3])
    bottom2 = [d + r for d, r in zip(deep, rem)]
    ax1.bar(x, light, width, bottom=bottom2, label="Light", color=STAGE_COLORS[1])
    bottom3 = [b + l for b, l in zip(bottom2, light)]
    ax1.bar(x, awake, width, bottom=bottom3, label="Wake", color=STAGE_COLORS[0], alpha=0.5)

    ax1.set_ylabel("Hours")
    ax1.set_xticks(x)
    date_labels = [d[5:] for d in dates]  # MM-DD
    ax1.set_xticklabels(date_labels, rotation=45, ha="right", fontsize=8)
    ax1.legend(loc="upper left", fontsize=8)
    ax1.set_title("Sleep Stages & Score (last {} nights)".format(len(nights)), fontsize=12, fontweight="bold")

    # Score line on secondary axis
    valid_scores = [(i, s) for i, s in enumerate(scores) if s is not None]
    if valid_scores:
        ax2 = ax1.twinx()
        si, sv = zip(*valid_scores)
        ax2.plot(si, sv, color="#E74C3C", marker="o", markersize=4, linewidth=1.5, label="Score")
        ax2.set_ylabel("Sleep Score", color="#E74C3C")
        ax2.set_ylim(0, 105)
        ax2.tick_params(axis="y", labelcolor="#E74C3C")

    fig.tight_layout()
    return _fig_to_bytes(fig)


def generate_hr_hrv_overlay(epochs: list[dict]) -> bytes:
    """Dual-axis HR and RMSSD chart with sleep stage background.

    Args:
        epochs: List of dicts with keys: ts, stage, hr, rmssd

    Returns:
        PNG bytes.
    """
    if not epochs:
        return _empty_chart("No epoch data available")

    times = [_to_sgt(e["ts"]) for e in epochs]
    hrs = [e.get("hr") for e in epochs]
    rmssds = [e.get("rmssd") for e in epochs]
    stages = [e["stage"] for e in epochs]

    fig, ax1 = plt.subplots(figsize=(12, 4))

    # Stage background bands
    for i in range(len(times) - 1):
        color = STAGE_COLORS.get(stages[i], "#95A5A6")
        ax1.axvspan(times[i], times[i + 1], color=color, alpha=0.1)

    # HR line
    valid_hr = [(t, h) for t, h in zip(times, hrs) if h is not None]
    if valid_hr:
        ht, hv = zip(*valid_hr)
        ax1.plot(ht, hv, color="#E74C3C", linewidth=1, alpha=0.8, label="HR")
    ax1.set_ylabel("Heart Rate (bpm)", color="#E74C3C")
    ax1.tick_params(axis="y", labelcolor="#E74C3C")

    # RMSSD on secondary axis
    ax2 = ax1.twinx()
    valid_rmssd = [(t, r) for t, r in zip(times, rmssds) if r is not None]
    if valid_rmssd:
        rt, rv = zip(*valid_rmssd)
        ax2.plot(rt, rv, color="#3498DB", linewidth=1, alpha=0.8, label="RMSSD")
    ax2.set_ylabel("RMSSD (ms)", color="#3498DB")
    ax2.tick_params(axis="y", labelcolor="#3498DB")

    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax1.set_xlabel("Time (SGT)")
    ax1.set_title("Heart Rate & HRV During Sleep", fontsize=12, fontweight="bold")
    ax1.grid(axis="x", alpha=0.3)

    fig.tight_layout()
    return _fig_to_bytes(fig)


def generate_weekly_dashboard(
    sleep_data: list[dict],
    activity_data: list[dict],
    temp_data: list[dict],
) -> bytes:
    """2x2 composite dashboard: sleep duration, HRV, steps, temperature.

    Args:
        sleep_data: From health_sleep_custom (last 14 days)
        activity_data: From health_activity (last 14 days)
        temp_data: From health_temperature (last 14 days)

    Returns:
        PNG bytes.
    """
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    # Top-left: Sleep duration trend
    ax = axes[0, 0]
    if sleep_data:
        nights = sorted(sleep_data, key=lambda x: x["sleep_date"])
        dates = [n["sleep_date"][:10] for n in nights]
        durations = [int(n.get("duration_total_s") or 0) / 3600 for n in nights]
        awake = [int(n.get("duration_awake_s") or 0) / 3600 for n in nights]
        actual = [d - a for d, a in zip(durations, awake)]
        ax.bar(range(len(dates)), actual, color=STAGE_COLORS[2], alpha=0.7)
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[5:] for d in dates], rotation=45, fontsize=7)
        ax.axhline(y=8, color="green", linestyle="--", alpha=0.5, label="8h target")
        ax.legend(fontsize=7)
    ax.set_title("Sleep Duration (h)", fontsize=10, fontweight="bold")
    ax.set_ylabel("Hours")

    # Top-right: HRV (RMSSD) trend
    ax = axes[0, 1]
    if sleep_data:
        nights = sorted(sleep_data, key=lambda x: x["sleep_date"])
        dates = [n["sleep_date"][:10] for n in nights]
        rmssd = [float(n.get("rmssd_mean") or 0) for n in nights]
        colors = ["#E74C3C" if r < 60 else "#27AE60" for r in rmssd]
        ax.bar(range(len(dates)), rmssd, color=colors, alpha=0.7)
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[5:] for d in dates], rotation=45, fontsize=7)
        ax.axhline(y=80, color="green", linestyle="--", alpha=0.5, label="Healthy baseline")
        ax.legend(fontsize=7)
    ax.set_title("HRV (RMSSD mean)", fontsize=10, fontweight="bold")
    ax.set_ylabel("RMSSD (ms)")

    # Bottom-left: Steps
    ax = axes[1, 0]
    if activity_data:
        acts = sorted(activity_data, key=lambda x: x["date"])
        dates = [a["date"][:10] for a in acts]
        steps = [int(a.get("steps") or 0) for a in acts]
        ax.bar(range(len(dates)), steps, color="#3498DB", alpha=0.7)
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[5:] for d in dates], rotation=45, fontsize=7)
    ax.set_title("Daily Steps", fontsize=10, fontweight="bold")
    ax.set_ylabel("Steps")

    # Bottom-right: Body temperature
    ax = axes[1, 1]
    if temp_data:
        temps = sorted(temp_data, key=lambda x: x["date"])
        dates = [t["date"][:10] for t in temps]
        avg_temps = [float(t.get("temp_avg_c") or 0) for t in temps]
        colors = ["#E74C3C" if t > 37.5 else "#3498DB" for t in avg_temps]
        ax.bar(range(len(dates)), avg_temps, color=colors, alpha=0.7)
        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels([d[5:] for d in dates], rotation=45, fontsize=7)
        ax.axhline(y=37.5, color="red", linestyle="--", alpha=0.5, label="Fever threshold")
        ax.legend(fontsize=7)
    ax.set_title("Body Temperature (C)", fontsize=10, fontweight="bold")
    ax.set_ylabel("Temp (C)")

    fig.suptitle("Health Dashboard", fontsize=14, fontweight="bold")
    fig.tight_layout()
    return _fig_to_bytes(fig)


def _empty_chart(message: str) -> bytes:
    """Generate a placeholder chart with an error message."""
    fig, ax = plt.subplots(figsize=(8, 3))
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=14, color="#95A5A6")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    return _fig_to_bytes(fig)
