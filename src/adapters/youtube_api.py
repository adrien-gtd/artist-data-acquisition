from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


class YouTubeAPIError(RuntimeError):
    pass


@dataclass
class YouTubeCredentials:
    api_key: str

    @staticmethod
    def from_env() -> "YouTubeCredentials":
        key = os.getenv("YOUTUBE_API_KEY")
        if not key:
            raise YouTubeAPIError("Missing YOUTUBE_API_KEY in environment (.env).")
        return YouTubeCredentials(api_key=key)


class YouTubeAPI:
    """
    YouTube Data API v3 raw adapter.
    Getting channel statistics:
    - subscribers
    - total views
    - video count
    """

    API_BASE = "https://www.googleapis.com/youtube/v3"

    def __init__(
        self,
        credentials: Optional[YouTubeCredentials] = None,
        timeout_s: float = 20.0,
        max_retries: int = 4,
        user_agent: str = "artist-popularity-tracker/1.0",
    ):
        self.credentials = credentials or YouTubeCredentials.from_env()
        self.timeout_s = timeout_s
        self.max_retries = max_retries

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    def _request(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.API_BASE}{path}"
        params = dict(params) # copy
        params["key"] = self.credentials.api_key

        last_err = None
        for attempt in range(self.max_retries + 1):
            resp = self._session.get(url, params=params, timeout=self.timeout_s)

            # quota / rate situations sometimes show as 403 with a reason in JSON
            if resp.status_code in (500, 502, 503, 504) and attempt < self.max_retries:
                time.sleep(0.5 * (2 ** attempt))
                last_err = f"Server error ({resp.status_code}). Retrying."
                continue

            if resp.status_code == 429:
                time.sleep(1.0 + attempt)
                last_err = "Rate limited (429). Retrying."
                continue

            if 200 <= resp.status_code < 300:
                if not resp.text.strip():
                    return {}
                return resp.json()

            # try to surface JSON error details if present
            try:
                err = resp.json()
            except Exception:
                err = resp.text

            raise YouTubeAPIError(f"YouTube API error {resp.status_code} on {path}: {err}")

        raise YouTubeAPIError(f"YouTube API request failed after retries: {last_err or 'unknown'}")

    def get_channel(self, channel_id: str) -> Dict[str, Any]:
        """
        Raw: channels.list
        https://developers.google.com/youtube/v3/docs/channels/list
        """
        return self._request(
            "/channels",
            params={
                "part": "snippet,statistics",
                "id": channel_id,
                "maxResults": 1,
            },
        )

    def search_channel(self, query: str, max_results: int = 5) -> Dict[str, Any]:
        """
        Raw: search.list for channels
        https://developers.google.com/youtube/v3/docs/search/list

        Used to find channel IDs by name for initial lookups.
        """
        return self._request(
            "/search",
            params={
                "part": "snippet",
                "q": query,
                "type": "channel",
                "maxResults": max_results,
            },
        )
