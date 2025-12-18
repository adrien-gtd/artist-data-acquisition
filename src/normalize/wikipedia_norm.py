"""
Wikipedia normalization layer.

Transforms raw Wikimedia Pageviews API responses into
daily Wikipedia popularity snapshots.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

from src.schema.artist_daily import WikiArtistDaily
from src.schema.artist_info import ArtistInfo
from src.utils import utc_now_iso


def normalize_wiki_daily(
    raw_pageviews: Dict[str, Any],
    *,
    local_artist_id: str,
    wiki_title: str,
    day_date: date,
    fetched_at: Optional[str] = None,
    job_run_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> WikiArtistDaily:
    """
    Normalize a Wikimedia Pageviews response into a daily Wikipedia snapshot.
    """
    fetched_at = fetched_at or utc_now_iso()

    pageviews = None
    items = raw_pageviews.get("items")
    if isinstance(items, list) and items:
        first = items[0]
        if isinstance(first, dict):
            pageviews = first.get("views")

    return WikiArtistDaily(
        local_artist_id=local_artist_id,
        wiki_title=wiki_title,
        day_date=day_date.isoformat(),
        fetched_at=fetched_at,
        job_run_id=job_run_id,
        request_id=request_id,
        pageviews=pageviews,
    )


def normalize_wiki_info_from_summary(
    raw_summary: Dict[str, Any],
    *,
    local_artist_id: str,
    fetched_at: Optional[str] = None,
    job_run_id: Optional[str] = None,
    request_id: Optional[str] = None,
) -> ArtistInfo:
    """
    Normalize Wikipedia REST summary metadata into ArtistInfo fields.
    """
    fetched_at = fetched_at or utc_now_iso()

    wikipedia_url = None
    content_urls = raw_summary.get("content_urls")
    if isinstance(content_urls, dict):
        desktop = content_urls.get("desktop")
        if isinstance(desktop, dict):
            wikipedia_url = desktop.get("page")

    return ArtistInfo(
        local_artist_id=local_artist_id,
        wiki_title=raw_summary.get("title"),
        wikipedia_url=wikipedia_url,
        wikipedia_fetched_at=fetched_at,
        wikipedia_job_run_id=job_run_id,
        wikipedia_request_id=request_id,
    )
