"""Hevy workout sync module.

Fetches workout data from Hevy API and stores in Supabase tables:
- health_workout_sessions
- health_workout_exercises
- health_workout_sets
"""

import os
import re
import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

HEVY_API_BASE = "https://api.hevyapp.com/v1"

# Map workout title keywords to session types
SESSION_TYPE_MAP = {
    "push": "push",
    "pull": "pull",
    "legs": "legs",
    "leg": "legs",
    "rehab": "rehab",
    "pool": "swim",
    "swim": "swim",
    "sprint": "sprint",
    "sunday": "mobility",
    "mobility": "mobility",
}

# UUID pattern for detecting custom exercises (rehab exercises)
_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _get_api_key() -> str:
    """Read HEVY_API_KEY from environment."""
    key = os.getenv("HEVY_API_KEY")
    if not key:
        raise ValueError("HEVY_API_KEY environment variable not set")
    return key


def _hevy_headers() -> dict[str, str]:
    """Return headers for Hevy API requests."""
    return {
        "api-key": _get_api_key(),
        "Accept": "application/json",
    }


def _infer_session_type(title: str) -> str:
    """Infer session type from workout title.

    Args:
        title: Workout title from Hevy (e.g. "Push Day", "Legs + Core").

    Returns:
        Session type string. Defaults to "strength" if no keyword matches.
    """
    title_lower = title.lower()
    for keyword, session_type in SESSION_TYPE_MAP.items():
        if keyword in title_lower:
            return session_type
    return "strength"


def _is_custom_exercise(template_id: str) -> bool:
    """Check if an exercise template ID is a UUID (custom/rehab exercise).

    Built-in Hevy exercises have short hex IDs like "79D0BB3A".
    Custom exercises have UUID format IDs like "af1b05d9-...".
    """
    return bool(_UUID_PATTERN.match(template_id))


def sync_hevy_workout(supabase_client, workout_id: str) -> dict:
    """Fetch a single workout from Hevy and upsert into Supabase.

    Args:
        supabase_client: Supabase client instance.
        workout_id: Hevy workout ID.

    Returns:
        Dict with sync results (status, session_id, exercise_count, set_count).
    """
    try:
        # Fetch workout from Hevy API
        with httpx.Client(timeout=30) as client:
            resp = client.get(
                f"{HEVY_API_BASE}/workouts/{workout_id}",
                headers=_hevy_headers(),
            )
            resp.raise_for_status()

        workout = resp.json()

        # The Hevy API v1 returns the workout directly or nested under a key
        # Handle both cases
        if "workout" in workout:
            workout = workout["workout"]

        title = workout.get("title", "Untitled Workout")
        session_type = _infer_session_type(title)

        # Parse timestamps
        started_at = workout.get("start_time") or workout.get("created_at")
        ended_at = workout.get("end_time") or workout.get("updated_at")

        # Calculate duration if both timestamps exist
        duration_seconds = None
        if started_at and ended_at:
            try:
                start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
                duration_seconds = int((end_dt - start_dt).total_seconds())
            except (ValueError, TypeError):
                pass

        # Upsert session
        session_record = {
            "hevy_workout_id": workout_id,
            "title": title,
            "session_type": session_type,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": duration_seconds,
            "notes": workout.get("description") or workout.get("notes"),
            "source": "hevy",
        }

        session_result = supabase_client.table("health_workout_sessions").upsert(
            session_record, on_conflict="hevy_workout_id"
        ).execute()

        session_id = session_result.data[0]["id"]
        logger.info(f"Hevy: upserted session {session_id} for workout {workout_id}")

        # Delete existing exercises and sets for clean re-sync
        # First get existing exercise IDs to cascade-delete sets
        existing_exercises = supabase_client.table(
            "health_workout_exercises"
        ).select("id").eq("session_id", session_id).execute()

        if existing_exercises.data:
            exercise_ids = [e["id"] for e in existing_exercises.data]
            supabase_client.table("health_workout_sets").delete().in_(
                "exercise_id", exercise_ids
            ).execute()

        supabase_client.table("health_workout_exercises").delete().eq(
            "session_id", session_id
        ).execute()

        # Insert exercises and sets
        exercises = workout.get("exercises", [])
        total_sets = 0

        for idx, exercise in enumerate(exercises):
            template_id = (
                exercise.get("exercise_template_id")
                or exercise.get("template_id")
                or ""
            )

            exercise_record = {
                "session_id": session_id,
                "exercise_name": exercise.get("title") or exercise.get("name", ""),
                "hevy_exercise_template_id": template_id,
                "exercise_order": idx,
                "is_rehab": _is_custom_exercise(template_id),
                "muscle_group": exercise.get("muscle_group") or exercise.get("primary_muscle_group"),
                "notes": exercise.get("notes"),
            }

            ex_result = supabase_client.table("health_workout_exercises").insert(
                exercise_record
            ).execute()
            exercise_id = ex_result.data[0]["id"]

            # Insert sets
            sets = exercise.get("sets", [])
            for set_idx, s in enumerate(sets):
                set_record = {
                    "exercise_id": exercise_id,
                    "set_order": set_idx,
                    "set_type": s.get("type", s.get("set_type", "normal")),
                    "weight_kg": s.get("weight_kg"),
                    "reps": s.get("reps"),
                    "duration_seconds": s.get("duration_seconds"),
                    "distance_meters": s.get("distance_meters"),
                    "rpe": s.get("rpe"),
                    "completed": s.get("completed", True),
                }
                supabase_client.table("health_workout_sets").insert(
                    set_record
                ).execute()
                total_sets += 1

        logger.info(
            f"Hevy: workout {workout_id} synced with "
            f"{len(exercises)} exercises, {total_sets} sets"
        )

        return {
            "status": "success",
            "session_id": session_id,
            "exercise_count": len(exercises),
            "set_count": total_sets,
        }

    except httpx.HTTPStatusError as e:
        logger.error(f"Hevy API error for workout {workout_id}: {e.response.status_code}")
        return {"status": "error", "error": f"Hevy API {e.response.status_code}"}
    except Exception as e:
        logger.error(f"Hevy sync failed for workout {workout_id}: {e}")
        return {"status": "error", "error": str(e)}


def sync_all_hevy_workouts(supabase_client) -> dict:
    """Fetch all workouts from Hevy and sync those not yet in Supabase.

    Used for initial backfill. Paginates through the Hevy API and skips
    workouts already stored (matched by hevy_workout_id).

    Args:
        supabase_client: Supabase client instance.

    Returns:
        Dict with sync stats (status, synced, skipped, errors, total_fetched).
    """
    start_time = datetime.now(timezone.utc)

    if not os.getenv("HEVY_API_KEY"):
        return {"status": "skipped", "reason": "HEVY_API_KEY not set"}

    try:
        # Get existing workout IDs from Supabase
        existing_resp = supabase_client.table(
            "health_workout_sessions"
        ).select("hevy_workout_id").eq("source", "hevy").execute()
        existing_ids = {r["hevy_workout_id"] for r in existing_resp.data}
        logger.info(f"Hevy: {len(existing_ids)} workouts already in Supabase")

        # Paginate through all Hevy workouts
        all_workouts = []
        page = 1
        page_size = 10  # Hevy API default/max per page

        with httpx.Client(timeout=30) as client:
            while True:
                resp = client.get(
                    f"{HEVY_API_BASE}/workouts",
                    headers=_hevy_headers(),
                    params={"page": page, "pageSize": page_size},
                )
                resp.raise_for_status()
                data = resp.json()

                # Handle response structure
                workouts = data.get("workouts", data.get("data", []))
                if not workouts:
                    break

                all_workouts.extend(workouts)
                logger.info(f"Hevy: fetched page {page} ({len(workouts)} workouts)")

                # Check pagination
                page_count = data.get("page_count", data.get("totalPages"))
                if page_count and page >= page_count:
                    break
                if len(workouts) < page_size:
                    break

                page += 1

        logger.info(f"Hevy: {len(all_workouts)} total workouts fetched from API")

        # Sync workouts not yet in Supabase
        synced = 0
        skipped = 0
        errors = 0

        for workout in all_workouts:
            workout_id = workout.get("id", "")
            if not workout_id:
                continue

            if workout_id in existing_ids:
                skipped += 1
                continue

            result = sync_hevy_workout(supabase_client, workout_id)
            if result["status"] == "success":
                synced += 1
            else:
                errors += 1
                logger.warning(
                    f"Hevy: failed to sync workout {workout_id}: {result.get('error')}"
                )

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            f"Hevy backfill complete: {synced} synced, {skipped} skipped, "
            f"{errors} errors ({len(all_workouts)} total) in {duration:.1f}s"
        )

        return {
            "status": "success",
            "synced": synced,
            "skipped": skipped,
            "errors": errors,
            "total_fetched": len(all_workouts),
            "duration_seconds": round(duration, 1),
        }

    except Exception as e:
        logger.error(f"Hevy backfill failed: {e}")
        return {"status": "error", "error": str(e)}


def run_sync(supabase_client, **kwargs) -> dict:
    """Entry point for the sync scheduler.

    Runs a full backfill (skipping already-synced workouts).

    Args:
        supabase_client: Supabase client instance.

    Returns:
        Dict with sync stats.
    """
    return sync_all_hevy_workouts(supabase_client)
