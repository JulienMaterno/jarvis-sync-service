"""
Insight Timer → Supabase sync.

Fetches meditation sessions from Insight Timer's Firebase/Firestore backend
and upserts them into the meditation_sessions table in Supabase.

Auth: Firebase REST API (email/password → ID token → Firestore query).
"""

import os
import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Firebase config for Insight Timer
FIREBASE_API_KEY = "AIzaSyBKvddqJ42iznuQskyBfarrhBh_rZmtCpQ"
FIREBASE_AUTH_URL = "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword"
FIRESTORE_BASE = "https://firestore.googleapis.com/v1/projects/insight-timer-a1ac7/databases/(default)/documents"


def _get_firebase_token() -> tuple[str, str]:
    """Authenticate with Firebase and return (id_token, user_id)."""
    email = os.getenv("INSIGHT_TIMER_EMAIL")
    password = os.getenv("INSIGHT_TIMER_PASSWORD")

    if not email or not password:
        raise ValueError("INSIGHT_TIMER_EMAIL and INSIGHT_TIMER_PASSWORD must be set")

    resp = requests.post(
        f"{FIREBASE_AUTH_URL}?key={FIREBASE_API_KEY}",
        json={
            "email": email,
            "password": password,
            "returnSecureToken": True,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if "idToken" not in data:
        raise ValueError(f"Firebase auth failed: {data.get('error', {}).get('message', 'unknown')}")

    return data["idToken"], data["localId"]


def _fetch_sessions(token: str, uid: str) -> list[dict]:
    """Fetch all meditation sessions from Firestore."""
    url = f"{FIRESTORE_BASE}/users/{uid}/sessions?pageSize=300"
    all_docs = []
    next_page = None

    while True:
        req_url = url
        if next_page:
            req_url += f"&pageToken={next_page}"

        resp = requests.get(req_url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        resp.raise_for_status()
        result = resp.json()

        docs = result.get("documents", [])
        all_docs.extend(docs)

        next_page = result.get("nextPageToken")
        if not next_page or not docs:
            break

    return all_docs


def _parse_sessions(docs: list[dict]) -> list[dict]:
    """Parse Firestore documents into clean records for Supabase."""
    records = []
    for doc in docs:
        fields = doc.get("fields", {})

        if fields.get("is_deleted", {}).get("booleanValue", False):
            continue

        session_id = fields.get("id", {}).get("stringValue", "")
        date = fields.get("local_calendar_day", {}).get("stringValue", "")
        if not date or not session_id:
            continue

        records.append({
            "insight_timer_id": session_id,
            "date": date,
            "duration_seconds": int(fields.get("duration_in_seconds", {}).get("integerValue", 0)),
            "practice_type": fields.get("practice_type", {}).get("stringValue", ""),
            "session_type": fields.get("type", {}).get("stringValue", ""),
            "started_at_epoch": int(fields.get("started_at", {}).get("integerValue", 0)),
        })

    return records


def run_sync(supabase_client, **kwargs) -> dict:
    """
    Sync meditation sessions from Insight Timer to Supabase.

    Returns dict with sync stats.
    """
    start_time = datetime.now(timezone.utc)

    # Check if credentials are configured
    if not os.getenv("INSIGHT_TIMER_EMAIL"):
        return {"status": "skipped", "reason": "INSIGHT_TIMER_EMAIL not set"}

    try:
        # Authenticate with Firebase
        token, uid = _get_firebase_token()
        logger.info(f"Insight Timer: authenticated as {uid}")

        # Fetch sessions from Firestore
        docs = _fetch_sessions(token, uid)
        logger.info(f"Insight Timer: fetched {len(docs)} session documents")

        # Parse into clean records
        records = _parse_sessions(docs)
        logger.info(f"Insight Timer: {len(records)} active sessions after filtering")

        if not records:
            return {"status": "success", "created": 0, "updated": 0, "total": 0}

        # Upsert into Supabase in batches
        batch_size = 50
        created = 0
        updated = 0

        # Get existing session IDs to distinguish creates vs updates
        existing_resp = supabase_client.table("meditation_sessions").select(
            "insight_timer_id"
        ).execute()
        existing_ids = {r["insight_timer_id"] for r in existing_resp.data}

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            result = supabase_client.table("meditation_sessions").upsert(
                batch, on_conflict="insight_timer_id"
            ).execute()

            for r in batch:
                if r["insight_timer_id"] in existing_ids:
                    updated += 1
                else:
                    created += 1
                    existing_ids.add(r["insight_timer_id"])

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            f"Insight Timer sync complete: {created} created, {updated} updated "
            f"({len(records)} total) in {duration:.1f}s"
        )

        return {
            "status": "success",
            "created": created,
            "updated": updated,
            "total": len(records),
            "duration_seconds": round(duration, 1),
        }

    except Exception as e:
        logger.error(f"Insight Timer sync failed: {e}")
        return {"status": "error", "error": str(e)}
