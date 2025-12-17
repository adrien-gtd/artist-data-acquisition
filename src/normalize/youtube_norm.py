"""
YouTube normalization layer.

Transforms raw YouTube Data API channel responses into
daily YouTube popularity snapshots.
"""

from __future__ import annotations

from datetime import datetime, date, timezone
from typing import Any, Dict, Optional

from src.schema.artist_daily import YouTubeArtistDaily
from src.schema.artist_info import ArtistInfo
from src.utils import utc_now_iso


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def normalize_youtube_daily(
    raw_channel: Dict[str, Any],
    *,
    local_artist_id: str,
    youtube_channel_id: str,
    day_date: date,
    fetched_at: Optional[str] = None,
    job_run_id: Optional[str] = None,
) -> YouTubeArtistDaily:
    """
    Normalize a YouTube channel response into a daily snapshot.
    """
    fetched_at = fetched_at or utc_now_iso()

    stats: Dict[str, Any] = {}
    items = raw_channel.get("items")
    if isinstance(items, list) and items:
        first = items[0]
        if isinstance(first, dict):
            stats = first.get("statistics") or {}

    return YouTubeArtistDaily(
        local_artist_id=local_artist_id,
        youtube_channel_id=youtube_channel_id,
        day_date=day_date.isoformat(),
        fetched_at=fetched_at,
        job_run_id=job_run_id,
        subscribers=_to_int(stats.get("subscriberCount")),
        total_views=_to_int(stats.get("viewCount")),
        video_count=_to_int(stats.get("videoCount")),
    )


def normalize_youtube_info_from_channel(
    raw_channel: Dict[str, Any],
    *,
    local_artist_id: str,
    fetched_at: Optional[str] = None,
    job_run_id: Optional[str] = None,
) -> ArtistInfo:
    """
    Normalize YouTube channel metadata into ArtistInfo fields.
    """
    fetched_at = fetched_at or utc_now_iso()

    channel_id = None
    items = raw_channel.get("items")
    if isinstance(items, list) and items:
        first = items[0]
        if isinstance(first, dict):
            channel_id = first.get("id")

    youtube_channel_url = (
        f"https://www.youtube.com/channel/{channel_id}"
        if channel_id else None
    )

    return ArtistInfo(
        local_artist_id=local_artist_id,
        youtube_channel_id=channel_id,
        youtube_channel_url=youtube_channel_url,
        fetched_at=fetched_at,
        job_run_id=job_run_id,
    )
