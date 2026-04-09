"""
Withings Health Data Sync Service

Syncs health data from Withings Cloud API to Supabase tables:
- Body measurements (weight, body composition, blood pressure, etc.)
- Daily activity summaries (steps, calories, elevation)
- Sleep summaries (duration, stages, scores, vitals)
- Intraday heart rate
- ECG recordings

Uses cursor-based incremental sync with fallback to time-window defaults.
"""

import json
import logging
import time
from datetime import datetime, date, timedelta, timezone
from typing import Any

from lib.withings_client import WithingsClient
from lib.supabase_client import supabase
from lib.logging_service import log_sync_event_sync

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Withings measure type → column name mapping
MEASTYPE_MAP: dict[int, str] = {
    1: "weight_kg",
    4: "height_m",
    5: "fat_free_mass_kg",
    6: "fat_ratio_pct",
    8: "fat_mass_kg",
    9: "diastolic_bp",
    10: "systolic_bp",
    11: "heart_pulse",
    12: "body_temp_c",
    54: "spo2_pct",
    71: "body_temp_c",
    73: "skin_temp_c",
    76: "muscle_mass_kg",
    77: "hydration_pct",
    88: "bone_mass_kg",
    91: "pulse_wave_velocity",
    123: "vo2max",
    130: "afib_result",
    135: "qrs_interval_ms",
    136: "pr_interval_ms",
    137: "qt_interval_ms",
    138: "qtc_interval_ms",
    139: "afib_ppg_result",
    155: "vascular_age",
    167: "nerve_health_conductance",
    168: "extracellular_water_kg",
    169: "intracellular_water_kg",
    170: "visceral_fat_index",
    174: "fat_mass_segments",
    175: "muscle_mass_segments",
    196: "electrodermal_activity",
    226: "basal_metabolic_rate",
}

WORKOUT_CATEGORIES: dict[int, str] = {
    1: "Walking", 2: "Running", 3: "Hiking", 4: "Skating", 5: "BMX",
    6: "Cycling", 7: "Swimming", 8: "Surfing", 9: "Kitesurfing",
    10: "Windsurfing", 11: "Bodyboard", 12: "Tennis", 13: "Ping Pong",
    14: "Squash", 15: "Badminton", 16: "Weightlifting", 17: "Crunches",
    18: "Push-ups", 19: "Pull-ups", 20: "Dips", 30: "Yoga",
    31: "Dancing", 32: "Volleyball", 33: "Water Polo", 34: "Skiing",
    35: "Snowboarding", 36: "Rowing", 37: "Zumba", 38: "Baseball",
    39: "Handball", 40: "Hockey", 41: "Ice Hockey", 42: "Climbing",
    43: "Multi-sport", 44: "Indoor Running", 187: "Rugby",
    188: "Football", 190: "Pilates", 191: "Basketball", 192: "Soccer",
    193: "Martial Arts", 195: "Trail Running", 196: "Strength Training",
    272: "Elliptical", 307: "Indoor Cycling", 550: "HIIT",
}

SYNC_STATE_KEY = "withings_last_sync"

# If a data type hasn't synced new data in this many days, expand lookback
STALENESS_THRESHOLD_DAYS = 2
STALENESS_LOOKBACK_DAYS = 14


class WithingsSync:
    """Syncs Withings health data to Supabase."""

    BATCH_SIZE = 500

    def __init__(self):
        self.client = WithingsClient()
        self._cursors: dict[str, str] = {}

    def _get_effective_start(
        self, cursor_key: str, default_start: datetime, now: datetime
    ) -> datetime:
        """Get effective start time for a data type, with staleness recovery.

        If the cursor exists but is stale (no data found for STALENESS_THRESHOLD_DAYS),
        expand the lookback to STALENESS_LOOKBACK_DAYS to catch up on missed data.

        Args:
            cursor_key: Key in self._cursors (e.g. "heart_rate").
            default_start: Fallback start when no cursor exists.
            now: Current timestamp.

        Returns:
            Effective start datetime for the sync query.
        """
        cursor_val = self._cursors.get(cursor_key)
        if not cursor_val:
            return default_start

        cursor_dt = datetime.fromisoformat(cursor_val.replace("Z", "+00:00"))
        days_since_cursor = (now - cursor_dt).total_seconds() / 86400

        # If cursor is recent, use it normally
        if days_since_cursor <= STALENESS_THRESHOLD_DAYS:
            return cursor_dt

        # Cursor is stale: data hasn't been found in a while.
        # Expand lookback to catch delayed uploads or missed data.
        expanded_start = now - timedelta(days=STALENESS_LOOKBACK_DAYS)
        logger.warning(
            f"Staleness recovery for {cursor_key}: cursor is {days_since_cursor:.1f} days old, "
            f"expanding lookback to {STALENESS_LOOKBACK_DAYS} days"
        )
        return expanded_start

    def _load_cursors(self) -> dict[str, str]:
        """Load last sync timestamps from sync_state table.

        Returns:
            Dict with keys like 'measurements', 'activity', etc.
            and ISO timestamp or date string values.
        """
        try:
            result = (
                supabase.table("sync_state")
                .select("value")
                .eq("key", SYNC_STATE_KEY)
                .execute()
            )
            if result.data and result.data[0].get("value"):
                self._cursors = json.loads(result.data[0]["value"])
                return self._cursors
        except Exception as e:
            logger.warning(f"Failed to load Withings sync cursors: {e}")

        self._cursors = {}
        return self._cursors

    def _save_cursors(self) -> None:
        """Save updated cursors to sync_state table."""
        try:
            supabase.table("sync_state").upsert(
                {
                    "key": SYNC_STATE_KEY,
                    "value": json.dumps(self._cursors),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ).execute()
        except Exception as e:
            logger.error(f"Failed to save Withings sync cursors: {e}")

    def sync_all(self, days: int = 7, full_sync: bool = False, data_type: str | None = None) -> dict[str, Any]:
        """Sync all Withings data types to Supabase.

        Args:
            days: Default lookback window in days (used when no cursor exists).
            full_sync: If True, ignore cursors and go back 365 days.
            data_type: Optional filter to sync only one data type
                (measurements, activity, sleep, heart_rate, ecg).

        Returns:
            Summary dict with status, per-type results, and duration.
        """
        start_time = time.time()
        now = datetime.now(timezone.utc)
        self._load_cursors()

        lookback_days = 365 if full_sync else days
        default_start = now - timedelta(days=lookback_days)

        results: dict[str, Any] = {}
        errors: list[str] = []

        # --- Measurements (uses Unix timestamps) ---
        if data_type is None or data_type == "measurements":
            try:
                if full_sync:
                    meas_start = default_start
                else:
                    meas_start = self._get_effective_start("measurements", default_start, now)
                start_ts = int(meas_start.timestamp())
                end_ts = int(now.timestamp())
                results["measurements"] = self._sync_measurements(start_ts, end_ts)
                if results["measurements"].get("created", 0) > 0:
                    self._cursors["measurements"] = now.isoformat()
            except Exception as e:
                logger.error(f"Measurements sync failed: {e}")
                errors.append(f"measurements: {e}")
                results["measurements"] = {"created": 0, "updated": 0, "error": str(e)}

        # --- Activity (uses YYYY-MM-DD strings) ---
        if data_type is None or data_type == "activity":
            try:
                act_cursor = self._cursors.get("activity")
                if full_sync or not act_cursor:
                    act_start = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                else:
                    # Always look back at least STALENESS_LOOKBACK_DAYS for delayed data
                    min_start = (now - timedelta(days=STALENESS_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
                    act_start = min(act_cursor, min_start)
                act_end = now.strftime("%Y-%m-%d")
                results["activity"] = self._sync_activity(act_start, act_end)
                if results["activity"].get("created", 0) > 0:
                    self._cursors["activity"] = act_end
            except Exception as e:
                logger.error(f"Activity sync failed: {e}")
                errors.append(f"activity: {e}")
                results["activity"] = {"created": 0, "updated": 0, "error": str(e)}

        # --- Sleep (uses YYYY-MM-DD strings) ---
        if data_type is None or data_type == "sleep":
            try:
                sleep_cursor = self._cursors.get("sleep")
                if full_sync or not sleep_cursor:
                    sleep_start = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                else:
                    # Always look back at least STALENESS_LOOKBACK_DAYS to catch
                    # delayed watch uploads and backdated sleep data
                    cursor_date = datetime.fromisoformat(sleep_cursor)
                    min_start = (now - timedelta(days=STALENESS_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
                    sleep_start = min(sleep_cursor, min_start)
                sleep_end = now.strftime("%Y-%m-%d")
                results["sleep"] = self._sync_sleep(sleep_start, sleep_end)
                # Only advance cursor when data was found, so delayed uploads aren't skipped
                if results["sleep"].get("created", 0) > 0:
                    self._cursors["sleep"] = sleep_end
            except Exception as e:
                logger.error(f"Sleep sync failed: {e}")
                errors.append(f"sleep: {e}")
                results["sleep"] = {"created": 0, "updated": 0, "error": str(e)}

        # --- Heart Rate (uses Unix timestamps, max 24h per request) ---
        if data_type is None or data_type == "heart_rate":
            try:
                if full_sync:
                    hr_start = default_start
                else:
                    hr_start = self._get_effective_start("heart_rate", default_start, now)
                results["heart_rate"] = self._sync_heart_rate(
                    int(hr_start.timestamp()), int(now.timestamp())
                )
                # Only advance cursor if records were actually synced,
                # so late-arriving data (delayed watch upload) isn't skipped
                if results["heart_rate"].get("created", 0) > 0:
                    self._cursors["heart_rate"] = now.isoformat()
            except Exception as e:
                logger.error(f"Heart rate sync failed: {e}")
                errors.append(f"heart_rate: {e}")
                results["heart_rate"] = {"created": 0, "updated": 0, "error": str(e)}

        # --- ECG (uses Unix timestamps) ---
        if data_type is None or data_type == "ecg":
            try:
                if full_sync:
                    ecg_start = default_start
                else:
                    ecg_start = self._get_effective_start("ecg", default_start, now)
                results["ecg"] = self._sync_ecg(
                    int(ecg_start.timestamp()), int(now.timestamp())
                )
                # Only advance cursor if records were actually synced
                if results["ecg"].get("created", 0) > 0:
                    self._cursors["ecg"] = now.isoformat()
            except Exception as e:
                logger.error(f"ECG sync failed: {e}")
                errors.append(f"ecg: {e}")
                results["ecg"] = {"created": 0, "updated": 0, "error": str(e)}

        # --- Workouts (uses YYYY-MM-DD, same cursor pattern as activity) ---
        if data_type is None or data_type in ("activity", "workouts"):
            try:
                wk_cursor = self._cursors.get("workouts")
                if full_sync or not wk_cursor:
                    wk_start = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                else:
                    wk_start = wk_cursor
                wk_end = now.strftime("%Y-%m-%d")
                results["workouts"] = self._sync_workouts(wk_start, wk_end)
                self._cursors["workouts"] = wk_end
            except Exception as e:
                logger.error(f"Workouts sync failed: {e}")
                errors.append(f"workouts: {e}")
                results["workouts"] = {"created": 0, "updated": 0, "error": str(e)}

        # --- Sleep Details (uses Unix timestamps, max 24h per request) ---
        # Always look back at least 3 days for sleep_details, regardless of cursor.
        # Withings uploads HRV data (RMSSD, SDNN) in a delayed second phase,
        # sometimes hours after the initial stage/structure data. Without this,
        # the cursor advances past the sleep period and the HRV data is never fetched.
        # The upsert handles deduplication, so re-fetching is safe.
        if data_type is None or data_type in ("sleep", "sleep_details"):
            try:
                sd_start = now - timedelta(days=3)
                results["sleep_details"] = self._sync_sleep_details(
                    int(sd_start.timestamp()), int(now.timestamp())
                )
                if results["sleep_details"].get("created", 0) > 0:
                    self._cursors["sleep_details"] = now.isoformat()
            except Exception as e:
                logger.error(f"Sleep details sync failed: {e}")
                errors.append(f"sleep_details: {e}")
                results["sleep_details"] = {"created": 0, "updated": 0, "error": str(e)}

        # Save cursors and log
        self._save_cursors()
        duration = round(time.time() - start_time, 2)

        status = "success" if not errors else ("partial" if len(errors) < 5 else "error")
        total_created = sum(r.get("created", 0) for r in results.values())
        total_updated = sum(r.get("updated", 0) for r in results.values())

        log_sync_event_sync(
            "withings_sync",
            status,
            f"Synced {total_created} created, {total_updated} updated in {duration}s",
            details={
                "results": results,
                "errors": errors,
                "full_sync": full_sync,
                "duration_seconds": duration,
            },
        )

        return {
            "status": status,
            "measurements": results.get("measurements", {}),
            "activity": results.get("activity", {}),
            "sleep": results.get("sleep", {}),
            "heart_rate": results.get("heart_rate", {}),
            "ecg": results.get("ecg", {}),
            "workouts": results.get("workouts", {}),
            "sleep_details": results.get("sleep_details", {}),
            "duration_seconds": duration,
        }

    def _sync_measurements(self, start_ts: int, end_ts: int) -> dict[str, int]:
        """Sync body measurements from Withings.

        Fetches measure groups, parses each with _parse_measure_group(),
        and batch upserts to health_measurements on withings_grpid.

        Args:
            start_ts: Start of range (Unix seconds).
            end_ts: End of range (Unix seconds).

        Returns:
            Dict with 'created' and 'updated' counts.
        """
        measure_groups = self.client.get_measurements(start_ts, end_ts)

        if not measure_groups:
            logger.info("No new measurements from Withings")
            return {"created": 0, "updated": 0}

        records = []
        for grp in measure_groups:
            parsed = self._parse_measure_group(grp)
            if parsed:
                records.append(parsed)

        persisted = 0
        if records:
            persisted = self._upsert_batch("health_measurements", records, "withings_grpid")

        logger.info(f"Synced {persisted} measurement groups")
        return {"created": persisted, "updated": 0}

    def _sync_activity(self, start_ymd: str, end_ymd: str) -> dict[str, int]:
        """Sync daily activity summaries from Withings.

        Args:
            start_ymd: Start date as YYYY-MM-DD.
            end_ymd: End date as YYYY-MM-DD.

        Returns:
            Dict with 'created' and 'updated' counts.
        """
        activities = self.client.get_activity(start_ymd, end_ymd)

        if not activities:
            logger.info("No new activity data from Withings")
            return {"created": 0, "updated": 0}

        now_iso = datetime.now(timezone.utc).isoformat()
        records = []
        for act in activities:
            record = {
                "date": act.get("date"),
                "steps": act.get("steps"),
                "distance_m": act.get("distance"),
                "calories_total": act.get("totalcalories"),
                "calories_active": act.get("calories"),
                "elevation_m": act.get("elevation"),
                "soft_activity_seconds": act.get("soft"),
                "moderate_activity_seconds": act.get("moderate"),
                "intense_activity_seconds": act.get("intense"),
                "hr_average": act.get("hr_average"),
                "hr_min": act.get("hr_min"),
                "hr_max": act.get("hr_max"),
                "raw_data": act,
                "synced_at": now_iso,
            }
            records.append(record)

        if records:
            self._upsert_batch("health_activity", records, "date")

        logger.info(f"Synced {len(records)} activity days")
        return {"created": len(records), "updated": 0}

    def _sync_sleep(self, start_ymd: str, end_ymd: str) -> dict[str, int]:
        """Sync sleep summaries from Withings.

        Args:
            start_ymd: Start date as YYYY-MM-DD.
            end_ymd: End date as YYYY-MM-DD.

        Returns:
            Dict with 'created' and 'updated' counts.
        """
        series = self.client.get_sleep_summary(start_ymd, end_ymd)

        if not series:
            logger.info("No new sleep data from Withings")
            return {"created": 0, "updated": 0}

        now_iso = datetime.now(timezone.utc).isoformat()
        records = []
        for entry in series:
            data = entry.get("data", {})
            record = {
                "date": entry.get("date"),
                "start_at": (
                    datetime.fromtimestamp(
                        entry["startdate"], tz=timezone.utc
                    ).isoformat()
                    if entry.get("startdate")
                    else None
                ),
                "end_at": (
                    datetime.fromtimestamp(
                        entry["enddate"], tz=timezone.utc
                    ).isoformat()
                    if entry.get("enddate")
                    else None
                ),
                "duration_total_s": data.get("total_sleep_time"),
                "duration_light_s": data.get("lightsleepduration"),
                "duration_deep_s": data.get("deepsleepduration"),
                "duration_rem_s": data.get("remsleepduration"),
                "duration_awake_s": data.get("wakeupduration"),
                "wakeup_count": data.get("wakeupcount"),
                "sleep_score": data.get("sleep_score"),
                "sleep_efficiency_pct": data.get("sleep_efficiency"),
                "hr_average": data.get("hr_average"),
                "hr_min": data.get("hr_min"),
                "hr_max": data.get("hr_max"),
                "rr_average": data.get("rr_average"),
                "rr_min": data.get("rr_min"),
                "rr_max": data.get("rr_max"),
                "spo2_average": data.get("spo2_avg"),
                "snoring_seconds": data.get("snoring"),
                "snoring_episode_count": data.get("snoringepisodecount"),
                "sleep_latency_s": data.get("sleep_latency"),
                "wakeup_latency_s": data.get("wakeup_latency"),
                "waso_s": data.get("waso"),
                "total_timeinbed_s": data.get("total_timeinbed"),
                "nb_rem_episodes": data.get("nb_rem_episodes"),
                "breathing_disturbances_intensity": data.get("breathing_disturbances_intensity"),
                "raw_data": entry,
                "synced_at": now_iso,
            }
            records.append(record)

        if records:
            self._upsert_batch("health_sleep", records, "date,start_at")

        logger.info(f"Synced {len(records)} sleep records")
        return {"created": len(records), "updated": 0}

    def _sync_heart_rate(self, start_ts: int, end_ts: int) -> dict[str, int]:
        """Sync intraday heart rate data from Withings.

        Withings returns max 24h per request, so this loops day-by-day
        for ranges longer than one day.

        Args:
            start_ts: Start of range (Unix seconds).
            end_ts: End of range (Unix seconds).

        Returns:
            Dict with 'created' and 'updated' counts.
        """
        total_records = 0
        day_seconds = 86400
        current_start = start_ts

        while current_start < end_ts:
            current_end = min(current_start + day_seconds, end_ts)

            try:
                series = self.client.get_intraday_activity(
                    current_start, current_end
                )

                if not series:
                    current_start = current_end
                    continue

                records = []
                for ts_str, entry in series.items():
                    hr_value = entry.get("heart_rate")
                    if hr_value is None:
                        continue
                    ts_dt = datetime.fromtimestamp(
                        int(ts_str), tz=timezone.utc
                    )
                    records.append(
                        {
                            "timestamp": ts_dt.isoformat(),
                            "heart_rate": hr_value,
                            "source": "withings",
                        }
                    )

                if records:
                    persisted = self._upsert_batch(
                        "health_heart_rate", records, "timestamp,source"
                    )
                    total_records += persisted
                    if persisted < len(records):
                        logger.warning(
                            f"Heart rate: {len(records) - persisted}/{len(records)} "
                            f"records failed to persist for {current_start}-{current_end}"
                        )

            except Exception as e:
                logger.warning(
                    f"Heart rate sync failed for range "
                    f"{current_start}-{current_end}: {e}"
                )

            current_start = current_end

        logger.info(f"Synced {total_records} heart rate data points")
        return {"created": total_records, "updated": 0}

    def _sync_ecg(self, start_ts: int, end_ts: int) -> dict[str, int]:
        """Sync ECG recordings from Withings.

        Args:
            start_ts: Start of range (Unix seconds).
            end_ts: End of range (Unix seconds).

        Returns:
            Dict with 'created' and 'updated' counts.
        """
        ecg_list = self.client.get_ecg_list(start_ts, end_ts)

        if not ecg_list:
            logger.info("No new ECG data from Withings")
            return {"created": 0, "updated": 0}

        now_iso = datetime.now(timezone.utc).isoformat()
        records = []
        for ecg in ecg_list:
            record = {
                "withings_ecg_id": ecg.get("ecg", {}).get("signalid") or ecg.get("signalid"),
                "recorded_at": (
                    datetime.fromtimestamp(
                        ecg["timestamp"], tz=timezone.utc
                    ).isoformat()
                    if ecg.get("timestamp")
                    else None
                ),
                "heart_rate": ecg.get("heart_rate") or ecg.get("ecg", {}).get("heart_rate"),
                "classification": ecg.get("ecg", {}).get("afib"),
                "afib_ppg_classification": ecg.get("ecg", {}).get("afib_ppg"),
                "signal_quality": ecg.get("ecg", {}).get("quality"),
                "signal_data": ecg.get("ecg", {}).get("signaldata"),
                "raw_data": ecg,
                "synced_at": now_iso,
            }
            # Only include records with a valid ID
            if record["withings_ecg_id"] is not None:
                records.append(record)

        persisted = 0
        if records:
            persisted = self._upsert_batch("health_ecg", records, "withings_ecg_id")

        logger.info(f"Synced {persisted} ECG recordings")
        return {"created": persisted, "updated": 0}

    def _sync_workouts(self, start_ymd: str, end_ymd: str) -> dict[str, int]:
        """Sync workout data from Withings."""
        workouts = self.client.get_workouts(start_ymd, end_ymd)
        if not workouts:
            logger.info("No new workout data from Withings")
            return {"created": 0, "updated": 0}

        now_iso = datetime.now(timezone.utc).isoformat()
        records = []
        for w in workouts:
            data = w.get("data", {})
            start_ts = w.get("startdate")
            end_ts = w.get("enddate")
            cat = w.get("category", 0)
            record = {
                "category": cat,
                "category_name": WORKOUT_CATEGORIES.get(cat, f"Unknown ({cat})"),
                "start_at": datetime.fromtimestamp(start_ts, tz=timezone.utc).isoformat() if start_ts else None,
                "end_at": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat() if end_ts else None,
                "date": w.get("date"),
                "device_id": w.get("deviceid"),
                "duration_s": (end_ts - start_ts) if start_ts and end_ts else None,
                "calories": data.get("calories"),
                "intensity": data.get("intensity"),
                "steps": data.get("steps"),
                "distance_m": data.get("distance"),
                "elevation_m": data.get("elevation"),
                "hr_average": data.get("hr_average"),
                "hr_min": data.get("hr_min"),
                "hr_max": data.get("hr_max"),
                "hr_zone_0_s": data.get("hr_zone_0"),
                "hr_zone_1_s": data.get("hr_zone_1"),
                "hr_zone_2_s": data.get("hr_zone_2"),
                "hr_zone_3_s": data.get("hr_zone_3"),
                "spo2_average": data.get("spo2_average"),
                "manual_distance_m": data.get("manual_distance"),
                "manual_calories": data.get("manual_calories"),
                "pause_duration_s": data.get("pause_duration"),
                "algo_pause_duration_s": data.get("algo_pause_duration"),
                "pool_laps": data.get("pool_laps"),
                "strokes": data.get("strokes"),
                "pool_length_m": data.get("pool_length"),
                "raw_data": w,
                "synced_at": now_iso,
            }
            records.append(record)

        if records:
            self._upsert_batch("health_workouts", records, "category,start_at")

        logger.info(f"Synced {len(records)} workouts")
        return {"created": len(records), "updated": 0}

    def _sync_sleep_details(self, start_ts: int, end_ts: int) -> dict[str, int]:
        """Sync high-frequency sleep data (HR, RR, snoring, HRV per timestamp)."""
        total_records = 0
        day_seconds = 86400
        current_start = start_ts

        while current_start < end_ts:
            current_end = min(current_start + day_seconds, end_ts)

            try:
                series = self.client.get_sleep_details(current_start, current_end)

                if not series:
                    current_start = current_end
                    continue

                now_iso = datetime.now(timezone.utc).isoformat()
                records = []
                for entry in series:
                    s_start = entry.get("startdate")
                    s_end = entry.get("enddate")
                    if not s_start or not s_end:
                        continue

                    start_dt = datetime.fromtimestamp(s_start, tz=timezone.utc)
                    # Assign sleep_date based on local "sleep night":
                    # Convert to local time (UTC+8 for Asia/Singapore).
                    # Epochs before noon local belong to the previous calendar
                    # date's sleep night (e.g. 9:30 AM SGT Apr 6 = night of Apr 5).
                    local_dt = start_dt + timedelta(hours=8)
                    if local_dt.hour < 12:
                        sleep_night = (local_dt - timedelta(days=1)).strftime("%Y-%m-%d")
                    else:
                        sleep_night = local_dt.strftime("%Y-%m-%d")
                    record = {
                        "sleep_date": sleep_night,
                        "start_at": start_dt.isoformat(),
                        "end_at": datetime.fromtimestamp(s_end, tz=timezone.utc).isoformat(),
                        "state": entry.get("state"),
                        "hr": entry.get("hr"),
                        "rr": entry.get("rr"),
                        "snoring": entry.get("snoring"),
                        "sdnn_1": entry.get("sdnn_1"),
                        "rmssd": entry.get("rmssd"),
                        "mvt_score": entry.get("mvt_score"),
                        "raw_data": entry,
                        "synced_at": now_iso,
                    }
                    records.append(record)

                if records:
                    persisted = self._upsert_batch("health_sleep_details", records, "sleep_date,start_at,state")
                    total_records += persisted
                    if persisted < len(records):
                        logger.warning(
                            f"Sleep details: {len(records) - persisted}/{len(records)} "
                            f"records failed to persist for {current_start}-{current_end}"
                        )

            except Exception as e:
                logger.warning(f"Sleep details sync failed for range {current_start}-{current_end}: {e}")

            current_start = current_end

        logger.info(f"Synced {total_records} sleep detail records")
        return {"created": total_records, "updated": 0}

    @staticmethod
    def _parse_measure_group(grp: dict) -> dict[str, Any] | None:
        """Parse a Withings measure group into a flat record.

        A measure group contains multiple measures (e.g., weight + fat ratio
        from the same weigh-in). Each measure has:
        - value: integer raw value
        - type: measure type code (see MEASTYPE_MAP)
        - unit: power of 10 to apply (actual = value * 10^unit)

        Args:
            grp: Raw measure group dict from Withings API.

        Returns:
            Flat dict ready for health_measurements upsert, or None on error.
        """
        try:
            grpid = grp.get("grpid")
            if grpid is None:
                return None

            measured_ts = grp.get("date")
            measured_at = (
                datetime.fromtimestamp(measured_ts, tz=timezone.utc).isoformat()
                if measured_ts
                else None
            )

            record: dict[str, Any] = {
                "withings_grpid": grpid,
                "measured_at": measured_at,
                "category": grp.get("category"),
                "device_id": grp.get("deviceid"),
                "raw_measures": grp.get("measures", []),
                "synced_at": datetime.now(timezone.utc).isoformat(),
            }

            for measure in grp.get("measures", []):
                mtype = measure.get("type")
                column = MEASTYPE_MAP.get(mtype)
                if column is None:
                    continue
                raw_val = measure.get("value", 0)
                unit = measure.get("unit", 0)
                actual = raw_val * (10 ** unit)
                record[column] = actual

            return record

        except Exception as e:
            logger.warning(f"Failed to parse measure group: {e}")
            return None

    def _upsert_batch(
        self, table: str, records: list[dict], conflict_key: str
    ) -> int:
        """Batch upsert records to Supabase.

        Splits records into chunks of BATCH_SIZE and upserts each chunk.

        Args:
            table: Supabase table name.
            records: List of record dicts to upsert.
            conflict_key: Comma-separated column names for ON CONFLICT.

        Returns:
            Number of records successfully upserted.
        """
        success_count = 0
        for i in range(0, len(records), self.BATCH_SIZE):
            batch = records[i : i + self.BATCH_SIZE]
            try:
                supabase.table(table).upsert(
                    batch, on_conflict=conflict_key
                ).execute()
                success_count += len(batch)
            except Exception as e:
                logger.error(
                    f"Failed to upsert batch {i // self.BATCH_SIZE} "
                    f"({len(batch)} records) to {table}: {e}"
                )
        return success_count


def run_sync(
    supabase_client: Any = None,
    days: int = 7,
    full_sync: bool = False,
    data_type: str | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """Entry point called by main.py sync orchestrator.

    Args:
        supabase_client: Ignored (module uses the singleton).
        days: Default lookback window in days.
        full_sync: If True, sync last 365 days regardless of cursors.
        data_type: Optional data type to sync (measurements, activity, sleep, heart_rate, ecg).
        **kwargs: Absorbed for compatibility with orchestrator interface.

    Returns:
        Summary dict with status and per-type results.
    """
    sync = WithingsSync()
    return sync.sync_all(days=days, full_sync=full_sync, data_type=data_type)


# CLI for testing
if __name__ == "__main__":
    import sys

    result = run_sync(
        days=int(sys.argv[1]) if len(sys.argv) > 1 else 7,
        full_sync="--full" in sys.argv,
    )
    print(json.dumps(result, indent=2))
