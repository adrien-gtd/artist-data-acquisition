from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

from src.provenance.fine_grain_provenance import RequestContext

import requests


class WikipediaAPIError(RuntimeError):
    pass


@dataclass
class WikipediaConfig:
    """
    Wikimedia APIs don't require an API key, but they do require a descriptive User-Agent.
    """
    user_agent: str
    timeout_s: float = 20.0
    max_retries: int = 4

    @staticmethod
    def from_env() -> WikipediaConfig:
        email = os.getenv("CONTACT_EMAIL", "")
        if not email:
            raise WikipediaAPIError(
                "Missing CONTACT_EMAIL environment variable for Wikipedia API User-Agent."
            )
        user_agent: str = f"artist-popularity-tracker/1.0 (contact:{email})"
        return WikipediaConfig(
            user_agent=user_agent,
        )

class WikipediaAPI:
    """
    Wikimedia raw API adapter.

    Fetching data from:
    - Wikimedia Pageviews API (daily views)
    - REST summary endpoint (for canonical title, description, etc.)
    """

    PAGEVIEWS_BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
    REST_BASE = "https://en.wikipedia.org/api/rest_v1"

    def __init__(self, config: Optional[WikipediaConfig] = None):
        self.config = config or WikipediaConfig.from_env()
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": self.config.user_agent})

    def _request(self, method: str, url: str) -> Tuple[Dict[str, Any], int]:
        last_err: Optional[str] = None
        for attempt in range(self.config.max_retries + 1):
            resp = self._session.request(method, url, timeout=self.config.timeout_s)

            # 429 / rate limit
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else (1.0 + attempt)
                time.sleep(sleep_s)
                last_err = f"Rate limited (429). Slept {sleep_s}s."
                continue

            # transient errors
            if resp.status_code in (500, 502, 503, 504) and attempt < self.config.max_retries:
                time.sleep(0.5 * (2 ** attempt))
                last_err = f"Server error ({resp.status_code}). Retrying."
                continue

            if 200 <= resp.status_code < 300:
                if not resp.text.strip():
                    return {}
                return resp.json(), resp.status_code

            raise WikipediaAPIError(f"Wikipedia API error {resp.status_code}: {resp.text}")

        raise WikipediaAPIError(f"Wikipedia API request failed after retries: {last_err or 'unknown'}")

    @staticmethod
    def _date_yyyymmdd(year: int, month: int, day: int) -> str:
        return f"{year:04d}{month:02d}{day:02d}"

    def get_pageviews_daily(
        self,
        *,
        title: str,
        start_yyyy_mm_dd: str,
        end_yyyy_mm_dd: str,
        project: str = "en.wikipedia",
        access: str = "all-access",
        agent: str = "user",        # e.g. "user", "spider", "bot" (type of agent making the request)
        request_ctx: Optional[RequestContext] = None,
    ) -> Dict[str, Any]:
        """
        Pageviews per-article endpoint.
        Returns items with timestamps and views.

        start_yyyy_mm_dd / end_yyyy_mm_dd are ISO dates like "2025-12-15".
        """
        # Convert ISO date -> yyyymmdd00 required by the API
        sy, sm, sd = map(int, start_yyyy_mm_dd.split("-"))
        ey, em, ed = map(int, end_yyyy_mm_dd.split("-"))
        start = self._date_yyyymmdd(sy, sm, sd) + "00"
        end = self._date_yyyymmdd(ey, em, ed) + "00"

        # Title must be URL-encoded
        enc_title = quote(title, safe="")

        url = (
            f"{self.PAGEVIEWS_BASE}/"
            f"{project}/{access}/{agent}/"
            f"{enc_title}/daily/{start}/{end}"
        )

        response, status_code = self._request("GET", url)
    
        if request_ctx:
            request_ctx.set_endpoint(f"/metrics/pageviews/per-article/{project}/{access}/{agent}/{enc_title}/daily/{start}/{end}")
            request_ctx.set_http_status(status_code)
            
        return response
    
    def get_page_summary(self, *, title: str) -> Dict[str, Any]:
        """
        Raw: Wikipedia REST summary endpoint.
        Handy for verifying title/redirects and getting canonical page URL.
        """
        enc_title = quote(title.replace(" ", "_"), safe="")
        url = f"{self.REST_BASE}/page/summary/{enc_title}"
        response, _ = self._request("GET", url)
        return response

    def search_page_title(self, query: str, limit: int = 1) -> str:
        """
        Uses the Action API to find the most relevant page title.
        """
        # Action API base URL
        url = (
            f"https://en.wikipedia.org/w/api.php?"
            f"action=query&list=search&srsearch={quote(query)}&"
            f"srlimit={limit}&format=json"
        )
        
        response, _ = self._request("GET", url)
        
        if not response.get("query", {}).get("search"):
            raise WikipediaAPIError(f"No results found for query: {query}")
            
        return response["query"]["search"][0]["title"]
