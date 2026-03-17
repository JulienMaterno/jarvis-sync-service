"""
===================================================================================
WITHINGS HEALTH API CLIENT
===================================================================================

Client for the Withings Health API (https://developer.withings.com/).
Handles OAuth2 token management and all data endpoints for health metrics.

Supported data:
- Body measurements (weight, body fat, blood pressure, SpO2, temperature)
- Daily activity (steps, distance, calories, active minutes)
- Intraday activity (heart rate, steps at sub-daily granularity)
- Sleep summaries (duration, phases, HR, RR, sleep score)
- ECG recordings

Token lifecycle:
- Access tokens expire every 3 hours
- Tokens stored in Supabase sync_state table (key: withings_oauth_tokens)
- Auto-refresh when within 5 minutes of expiry

All Withings API data calls use POST method. Values are encoded as
actual_value = value * 10^unit (e.g., weight 72.5kg = {value: 72500, type: 1, unit: -3}).

Usage:
    client = WithingsClient()
    measurements = client.get_measurements(
        startdate=int(datetime(2026, 3, 1).timestamp()),
        enddate=int(datetime(2026, 3, 17).timestamp()),
    )
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import urlencode

import httpx

from lib.supabase_client import supabase

logger = logging.getLogger(__name__)

# Withings notification appli codes
APPLI_WEIGHT = 1
APPLI_BLOOD_PRESSURE_SPO2 = 4
APPLI_ACTIVITY = 16
APPLI_SLEEP = 44
APPLI_ECG = 54
APPLI_HRV = 62

DEFAULT_APPLI_LIST = [
    APPLI_WEIGHT,
    APPLI_BLOOD_PRESSURE_SPO2,
    APPLI_ACTIVITY,
    APPLI_SLEEP,
    APPLI_ECG,
    APPLI_HRV,
]

# How many minutes before expiry to trigger a refresh
TOKEN_REFRESH_BUFFER_MINUTES = 5

# Sync state key for token storage
SYNC_STATE_KEY = "withings_oauth_tokens"


class WithingsError(Exception):
    """Raised when the Withings API returns a non-zero status."""

    def __init__(self, status: int, message: str = ""):
        self.status = status
        super().__init__(f"Withings API error (status={status}): {message}")


class WithingsAuthError(WithingsError):
    """Raised on authentication/token errors (status 401)."""
    pass


class WithingsClient:
    """Client for the Withings Health API with automatic token management."""

    BASE_URL = "https://wbsapi.withings.net"
    OAUTH_URL = "https://account.withings.com/oauth2_user/authorize2"

    def __init__(self):
        """Initialize the Withings client.

        Reads client credentials from environment variables:
        - WITHINGS_CLIENT_ID
        - WITHINGS_CLIENT_SECRET
        - WITHINGS_CALLBACK_URL (optional, has default)
        """
        self.client_id = os.getenv("WITHINGS_CLIENT_ID", "")
        self.client_secret = os.getenv("WITHINGS_CLIENT_SECRET", "")
        self.callback_url = os.getenv(
            "WITHINGS_CALLBACK_URL",
            "https://sync.new-world-project.com/webhooks/withings/callback",
        )

        if not self.client_id or not self.client_secret:
            logger.warning(
                "WITHINGS_CLIENT_ID or WITHINGS_CLIENT_SECRET not set. "
                "API calls will fail until credentials are configured."
            )

        self._http = httpx.Client(timeout=30.0)

    # -------------------------------------------------------------------------
    # Token Management
    # -------------------------------------------------------------------------

    def _load_tokens(self) -> dict | None:
        """Load OAuth tokens from the sync_state table.

        Returns:
            Token dict with access_token, refresh_token, expires_at, user_id,
            or None if no tokens are stored.
        """
        try:
            result = (
                supabase.table("sync_state")
                .select("value")
                .eq("key", SYNC_STATE_KEY)
                .execute()
            )
            if result.data and result.data[0].get("value"):
                return json.loads(result.data[0]["value"])
        except Exception as e:
            logger.error(f"Failed to load Withings tokens from sync_state: {e}")
        return None

    def _save_tokens(
        self,
        access_token: str,
        refresh_token: str,
        expires_in: int,
        user_id: str,
    ) -> None:
        """Persist OAuth tokens to the sync_state table.

        Args:
            access_token: The new access token.
            refresh_token: The new refresh token.
            expires_in: Token lifetime in seconds (typically 10800 = 3h).
            user_id: Withings user ID.
        """
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        tokens = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at.isoformat(),
            "user_id": str(user_id),
        }
        try:
            supabase.table("sync_state").upsert(
                {
                    "key": SYNC_STATE_KEY,
                    "value": json.dumps(tokens),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ).execute()
            logger.info("Withings tokens saved to sync_state")
        except Exception as e:
            logger.error(f"Failed to save Withings tokens: {e}")
            raise

    def _is_token_expired(self, tokens: dict) -> bool:
        """Check if the access token is expired or about to expire.

        Args:
            tokens: Token dict containing expires_at ISO string.

        Returns:
            True if token is expired or within the refresh buffer.
        """
        expires_at_str = tokens.get("expires_at", "")
        if not expires_at_str:
            return True
        try:
            expires_at = datetime.fromisoformat(expires_at_str)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            buffer = timedelta(minutes=TOKEN_REFRESH_BUFFER_MINUTES)
            return datetime.now(timezone.utc) >= (expires_at - buffer)
        except (ValueError, TypeError):
            return True

    def _get_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary.

        Returns:
            A valid access token string.

        Raises:
            WithingsAuthError: If no tokens are stored or refresh fails.
        """
        tokens = self._load_tokens()
        if not tokens:
            raise WithingsAuthError(
                status=401,
                message="No Withings tokens found in sync_state. Run OAuth flow first.",
            )

        if self._is_token_expired(tokens):
            logger.info("Withings access token expired or near expiry, refreshing")
            tokens = self._refresh_token(tokens["refresh_token"])

        return tokens["access_token"]

    def _refresh_token(self, current_refresh_token: str) -> dict:
        """Refresh the OAuth2 access token.

        Args:
            current_refresh_token: The current refresh token.

        Returns:
            Updated token dict (also saved to sync_state).

        Raises:
            WithingsAuthError: If the refresh request fails.
        """
        try:
            response = self._http.post(
                f"{self.BASE_URL}/v2/oauth2",
                data={
                    "action": "requesttoken",
                    "grant_type": "refresh_token",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": current_refresh_token,
                },
            )
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"Withings token refresh HTTP error: {e}")
            raise WithingsAuthError(status=401, message=f"Token refresh failed: {e}")

        status = data.get("status", -1)
        if status != 0:
            logger.error(f"Withings token refresh returned status {status}: {data}")
            raise WithingsAuthError(
                status=status,
                message=f"Token refresh failed with status {status}",
            )

        body = data.get("body", {})
        self._save_tokens(
            access_token=body["access_token"],
            refresh_token=body["refresh_token"],
            expires_in=body.get("expires_in", 10800),
            user_id=body.get("userid", ""),
        )

        # Return the full token dict so callers can use it immediately
        return self._load_tokens()

    # -------------------------------------------------------------------------
    # API Call Wrapper
    # -------------------------------------------------------------------------

    def _api_call(self, endpoint: str, params: dict, retry_on_401: bool = True) -> dict:
        """Make an authenticated POST call to the Withings API.

        Automatically attaches the access token and checks the response status.
        On a 401 (expired token), attempts one refresh and retries.

        Args:
            endpoint: API path, e.g. "/v2/measure".
            params: Form parameters to send (action, filters, etc.).
            retry_on_401: Whether to retry after refreshing on a 401 error.

        Returns:
            The "body" dict from the Withings response.

        Raises:
            WithingsError: If the API returns a non-zero status (after retry).
        """
        access_token = self._get_access_token()
        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            response = self._http.post(
                f"{self.BASE_URL}{endpoint}",
                data=params,
                headers=headers,
            )
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"Withings API HTTP error on {endpoint}: {e}")
            raise WithingsError(status=-1, message=str(e))

        status = data.get("status", -1)

        # Handle token expiry: refresh once and retry
        if status == 401 and retry_on_401:
            logger.warning("Withings API returned 401, attempting token refresh")
            tokens = self._load_tokens()
            if tokens and tokens.get("refresh_token"):
                try:
                    self._refresh_token(tokens["refresh_token"])
                    return self._api_call(endpoint, params, retry_on_401=False)
                except WithingsAuthError:
                    logger.error("Token refresh failed during 401 retry")
                    raise WithingsAuthError(status=401, message="Token expired and refresh failed")
            raise WithingsAuthError(status=401, message="Token expired, no refresh token available")

        if status != 0:
            logger.error(f"Withings API error on {endpoint}: status={status}, response={data}")
            raise WithingsError(status=status, message=str(data))

        return data.get("body", {})

    # -------------------------------------------------------------------------
    # OAuth2 Flow
    # -------------------------------------------------------------------------

    def get_authorize_url(self, state: str = "jarvis") -> str:
        """Build the OAuth2 authorization URL for user consent.

        Args:
            state: Opaque value passed through the OAuth flow for CSRF protection.

        Returns:
            Full authorization URL to redirect the user to.
        """
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.callback_url,
            "scope": "user.info,user.metrics,user.activity",
            "state": state,
        }
        return f"{self.OAUTH_URL}?{urlencode(params)}"

    def exchange_code(self, code: str) -> dict:
        """Exchange an authorization code for access and refresh tokens.

        Called after the user authorizes and is redirected back with a code.

        Args:
            code: The authorization code from the callback.

        Returns:
            Token data dict with access_token, refresh_token, expires_in, userid.

        Raises:
            WithingsAuthError: If the token exchange fails.
        """
        try:
            response = self._http.post(
                f"{self.BASE_URL}/v2/oauth2",
                data={
                    "action": "requesttoken",
                    "grant_type": "authorization_code",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "code": code,
                    "redirect_uri": self.callback_url,
                },
            )
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error(f"Withings code exchange HTTP error: {e}")
            raise WithingsAuthError(status=-1, message=f"Code exchange failed: {e}")

        status = data.get("status", -1)
        if status != 0:
            logger.error(f"Withings code exchange returned status {status}: {data}")
            raise WithingsAuthError(
                status=status,
                message=f"Code exchange failed with status {status}",
            )

        body = data.get("body", {})
        self._save_tokens(
            access_token=body["access_token"],
            refresh_token=body["refresh_token"],
            expires_in=body.get("expires_in", 10800),
            user_id=body.get("userid", ""),
        )

        logger.info(f"Withings OAuth complete for user {body.get('userid')}")
        return body

    # -------------------------------------------------------------------------
    # Data Endpoints
    # -------------------------------------------------------------------------

    def get_measurements(
        self,
        startdate: int,
        enddate: int,
        meastypes: list[int] | None = None,
    ) -> list[dict]:
        """Fetch body measurements (weight, fat, BP, SpO2, temperature, etc.).

        Args:
            startdate: Start of range as Unix timestamp.
            enddate: End of range as Unix timestamp.
            meastypes: Optional list of measurement type IDs to filter.
                Common types: 1=weight, 4=height, 5=fat_free_mass, 6=fat_ratio,
                8=fat_mass, 9=diastolic_bp, 10=systolic_bp, 11=heart_pulse,
                54=SpO2, 71=body_temperature, 73=skin_temperature.

        Returns:
            List of measure group dicts from the API response.
        """
        params: dict[str, Any] = {
            "action": "getmeas",
            "startdate": startdate,
            "enddate": enddate,
        }
        if meastypes:
            params["meastypes"] = ",".join(str(t) for t in meastypes)

        body = self._api_call("/measure", params)
        return body.get("measuregrps", [])

    def get_activity(
        self,
        startdateymd: str,
        enddateymd: str,
    ) -> list[dict]:
        """Fetch daily activity summaries (steps, distance, calories, etc.).

        Args:
            startdateymd: Start date as "YYYY-MM-DD".
            enddateymd: End date as "YYYY-MM-DD".

        Returns:
            List of daily activity dicts.
        """
        params = {
            "action": "getactivity",
            "startdateymd": startdateymd,
            "enddateymd": enddateymd,
            "data_fields": (
                "steps,distance,calories,totalcalories,elevation,"
                "soft,moderate,intense,hr_average,hr_min,hr_max"
            ),
        }
        body = self._api_call("/v2/measure", params)
        return body.get("activities", [])

    def get_intraday_activity(
        self,
        startdate: int,
        enddate: int,
    ) -> dict:
        """Fetch intraday activity data (sub-daily heart rate, steps, etc.).

        Args:
            startdate: Start of range as Unix timestamp.
            enddate: End of range as Unix timestamp.

        Returns:
            Series dict keyed by Unix timestamp strings, each containing
            heart_rate, steps, elevation, calories values.
        """
        params = {
            "action": "getintradayactivity",
            "startdate": startdate,
            "enddate": enddate,
            "data_fields": "heart_rate,steps,elevation,calories",
        }
        body = self._api_call("/v2/measure", params)
        return body.get("series", {})

    def get_sleep_summary(
        self,
        startdateymd: str,
        enddateymd: str,
    ) -> list[dict]:
        """Fetch sleep summaries with pagination support.

        Args:
            startdateymd: Start date as "YYYY-MM-DD".
            enddateymd: End date as "YYYY-MM-DD".

        Returns:
            List of all sleep summary dicts across all pages.
        """
        data_fields = (
            "nb_rem_episodes,sleep_efficiency,sleep_latency,"
            "total_sleep_time,total_timeinbed,wakeup_latency,waso,"
            "deepsleepduration,lightsleepduration,remsleepduration,"
            "hr_average,hr_max,hr_min,rr_average,rr_max,rr_min,"
            "sleep_score,snoring,snoringepisodecount,wakeupcount,"
            "breathing_disturbances_intensity"
        )

        all_series: list[dict] = []
        offset = 0
        more = True

        while more:
            params: dict[str, Any] = {
                "action": "getsummary",
                "startdateymd": startdateymd,
                "enddateymd": enddateymd,
                "data_fields": data_fields,
            }
            if offset:
                params["offset"] = offset

            body = self._api_call("/v2/sleep", params)
            series = body.get("series", [])
            all_series.extend(series)

            more = body.get("more", False)
            offset = body.get("offset", 0)

            if not series:
                break

        return all_series

    def get_ecg_list(
        self,
        startdate: int,
        enddate: int,
    ) -> list[dict]:
        """Fetch ECG recording metadata.

        Args:
            startdate: Start of range as Unix timestamp.
            enddate: End of range as Unix timestamp.

        Returns:
            List of ECG recording dicts with signal data references.
        """
        params = {
            "action": "list",
            "startdate": startdate,
            "enddate": enddate,
        }
        body = self._api_call("/v2/heart", params)
        return body.get("series", [])

    def get_workouts(
        self,
        startdateymd: str,
        enddateymd: str,
    ) -> list[dict]:
        """Fetch workout data with pagination.

        Args:
            startdateymd: Start date as "YYYY-MM-DD".
            enddateymd: End date as "YYYY-MM-DD".

        Returns:
            List of all workout dicts across all pages.
        """
        data_fields = (
            "calories,intensity,manual_distance,manual_calories,"
            "hr_average,hr_min,hr_max,hr_zone_0,hr_zone_1,hr_zone_2,hr_zone_3,"
            "pause_duration,algo_pause_duration,spo2_average,"
            "steps,distance,elevation,pool_laps,strokes,pool_length"
        )

        all_workouts: list[dict] = []
        offset = 0
        more = True

        while more:
            params: dict[str, Any] = {
                "action": "getworkouts",
                "startdateymd": startdateymd,
                "enddateymd": enddateymd,
                "data_fields": data_fields,
            }
            if offset:
                params["offset"] = offset

            body = self._api_call("/v2/measure", params)
            series = body.get("series", [])
            all_workouts.extend(series)

            more = body.get("more", False)
            offset = body.get("offset", 0)

            if not series:
                break

        return all_workouts

    def get_sleep_details(
        self,
        startdate: int,
        enddate: int,
    ) -> list[dict]:
        """Fetch high-frequency sleep data (per-timestamp HR, RR, HRV, etc.).

        Note: Maximum 24h per request. Callers should loop day-by-day
        for longer ranges.

        Args:
            startdate: Start of range as Unix timestamp.
            enddate: End of range as Unix timestamp.

        Returns:
            List of sleep segment dicts with time-series data.
        """
        params = {
            "action": "get",
            "startdate": startdate,
            "enddate": enddate,
            "data_fields": "hr,rr,snoring,sdnn_1,rmssd,mvt_score",
        }
        body = self._api_call("/v2/sleep", params)
        return body.get("series", [])

    # -------------------------------------------------------------------------
    # Webhook Management
    # -------------------------------------------------------------------------

    def subscribe_webhooks(
        self,
        callback_url: str,
        appli_list: list[int] | None = None,
    ) -> None:
        """Subscribe to Withings push notifications.

        Creates a subscription for each appli (data type). Withings sends
        a POST to the callback URL whenever new data is available.

        Args:
            callback_url: URL that Withings will POST notifications to.
            appli_list: List of appli codes to subscribe to.
                Defaults to weight, BP/SpO2, activity, sleep, ECG, HRV.
        """
        if appli_list is None:
            appli_list = DEFAULT_APPLI_LIST

        for appli in appli_list:
            try:
                self._api_call(
                    "/notify",
                    {
                        "action": "subscribe",
                        "callbackurl": callback_url,
                        "appli": appli,
                    },
                )
                logger.info(f"Subscribed to Withings notifications for appli={appli}")
            except WithingsError as e:
                # Status 294 = already subscribed, not a real error
                if e.status == 294:
                    logger.info(f"Already subscribed to Withings appli={appli}")
                else:
                    logger.error(f"Failed to subscribe to Withings appli={appli}: {e}")
                    raise

    def list_webhooks(self) -> list[dict]:
        """List active Withings webhook subscriptions.

        Returns:
            List of subscription dicts with callbackurl, appli, expires, etc.
        """
        body = self._api_call(
            "/notify",
            {"action": "list"},
        )
        return body.get("profiles", [])
