from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from src.provenance.fine_grain_provenance import RequestContext

import requests


class SpotifyAPIError(RuntimeError):
    pass


@dataclass
class SpotifyCredentials:
    client_id: str
    client_secret: str

    @staticmethod
    def from_env() -> "SpotifyCredentials":
        cid = os.getenv("SPOTIFY_CLIENT_ID")
        csec = os.getenv("SPOTIFY_CLIENT_SECRET")
        if not cid or not csec:
            raise SpotifyAPIError(
                "Missing Spotify credentials. Please set SPOTIFY_CLIENT_ID and "
                "SPOTIFY_CLIENT_SECRET in your environment (.env)."
            )
        return SpotifyCredentials(client_id=cid, client_secret=csec)


class SpotifyAPI:
    """
    Spotify Web API adapter (raw data fetcher).
    Uses Client Credentials OAuth (no user context).
    """

    AUTH_URL = "https://accounts.spotify.com/api/token"
    API_BASE = "https://api.spotify.com/v1"

    def __init__(
        self,
        credentials: Optional[SpotifyCredentials] = None,
        timeout_s: float = 20.0,
        max_retries: int = 4,
        user_agent: str = "artist-popularity-tracker/1.0",
    ):
        self.credentials = credentials or SpotifyCredentials.from_env()
        self.timeout_s = timeout_s
        self.max_retries = max_retries

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0  # epoch seconds

    # ----------------------------
    # Auth
    # ----------------------------
    def _ensure_token(self) -> str:
        now = time.time()
        # refresh slightly early to avoid edge cases
        if self._access_token and now < (self._token_expires_at - 20):
            return self._access_token

        data = {"grant_type": "client_credentials"}
        resp = self._session.post(
            self.AUTH_URL,
            data=data,
            auth=(self.credentials.client_id, self.credentials.client_secret),
            timeout=self.timeout_s,
        )

        if resp.status_code != 200:
            raise SpotifyAPIError(
                f"Spotify token request failed ({resp.status_code}): {resp.text}"
            )

        payload = resp.json()
        token = payload.get("access_token")
        expires_in = payload.get("expires_in", 0)

        if not token or not expires_in:
            raise SpotifyAPIError(f"Spotify token response malformed: {payload}")

        self._access_token = token
        self._token_expires_at = now + float(expires_in)
        return token

    # ----------------------------
    # HTTP helper with retry / rate limit
    # ----------------------------
    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], int]:
        token = self._ensure_token()

        url = f"{self.API_BASE}{path}"
        headers = {"Authorization": f"Bearer {token}"}

        last_err: Optional[str] = None

        for attempt in range(self.max_retries + 1):
            resp = self._session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=self.timeout_s,
            )

            # Rate limit
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else (1.0 + attempt)
                time.sleep(sleep_s)
                last_err = f"Rate limited (429). Slept {sleep_s}s."
                continue

            # Token expired/invalid: refresh once and retry
            if resp.status_code == 401 and attempt < self.max_retries:
                self._access_token = None
                token = self._ensure_token()
                headers["Authorization"] = f"Bearer {token}"
                last_err = "Unauthorized (401). Refreshed token."
                continue

            # Retry on transient server errors
            if resp.status_code in (500, 502, 503, 504) and attempt < self.max_retries:
                time.sleep(0.5 * (2 ** attempt))
                last_err = f"Server error ({resp.status_code}). Retrying."
                continue

            # Success
            if 200 <= resp.status_code < 300:
                if resp.text.strip() == "":
                    return {}
                return resp.json(), resp.status_code

            # Other errors: stop
            raise SpotifyAPIError(
                f"Spotify API error {resp.status_code} on {path}: {resp.text}"
            )

        raise SpotifyAPIError(f"Spotify API request failed after retries: {last_err or 'unknown'}")

    # ----------------------------
    # Public raw fetch methods
    # ----------------------------
    def get_artist(self, artist_id: str, request_ctx: Optional[RequestContext] = None) -> Dict[str, Any]:
        """Raw: GET /artists/{id}"""
        response, status_code = self._request("GET", f"/artists/{artist_id}")
        if request_ctx:
            request_ctx.set_endpoint(f"/artists/{artist_id}")
            request_ctx.set_http_status(status_code)
        return response

    def get_artists(self, artist_ids: List[str]) -> Dict[str, Any]:
        """Raw: GET /artists?ids=... (max 50)"""
        if not artist_ids:
            return {"artists": []}
        if len(artist_ids) > 50:
            raise SpotifyAPIError("Spotify get_artists supports up to 50 ids per call.")
        response, _ = self._request("GET", "/artists", params={"ids": ",".join(artist_ids)})
        return response

    def get_artist_top_tracks(self, artist_id: str, market: str = "FR", request_ctx: Optional[RequestContext] = None) -> Dict[str, Any]:
        """Raw: GET /artists/{id}/top-tracks (market required)"""
        response, status_code = self._request("GET", f"/artists/{artist_id}/top-tracks", params={"market": market})
        if request_ctx:
            request_ctx.set_endpoint(f"/artists/{artist_id}/top-tracks")
            request_ctx.set_params({"market": market})
            request_ctx.set_http_status(status_code)
        return response

    def get_artist_albums(
        self,
        artist_id: str,
        include_groups: str = "album,single,appears_on,compilation",
        market: Optional[str] = "FR",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Raw: GET /artists/{id}/albums (paginated)"""
        params: Dict[str, Any] = {
            "include_groups": include_groups,
            "limit": limit,
            "offset": offset,
        }
        if market:
            params["market"] = market
        response, _ = self._request("GET", f"/artists/{artist_id}/albums", params=params)
        return response

    def get_album(self, album_id: str) -> Dict[str, Any]:
        """Raw: GET /albums/{id}"""
        response, _ = self._request("GET", f"/albums/{album_id}")
        return response
    
    def get_track(self, track_id: str) -> Dict[str, Any]:
        """Raw: GET /tracks/{id}"""
        response, _ = self._request("GET", f"/tracks/{track_id}")
        return response

    def search_artist(self, query: str, limit: int = 10, market: Optional[str] = "FR") -> Dict[str, Any]:
        """Raw: GET /search?q=...&type=artist"""
        params: Dict[str, Any] = {"q": query, "type": "artist", "limit": limit}
        if market:
            params["market"] = market
        response, _ = self._request("GET", "/search", params=params)
        return response