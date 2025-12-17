"""
Spotify normalization layer.

Transforms raw JSON responses returned by the Spotify Web API into
structured, validated Pydantic models used by the storage layer.

This module contains only pure transformation logic:
- No HTTP requests
- No authentication
- No database access
"""

from __future__ import annotations
from datetime import datetime, date, timezone
from typing import Any, Dict, Optional, Tuple

from src.schema.artist_info import ArtistInfo
from src.schema.artist_daily import SpotifyArtistDaily
from src.utils import utc_now_iso

def _pick_best_image_url(images: Any) -> Optional[str]:
    """
    Select the highest-resolution image URL from a Spotify image list.

    Spotify returns multiple images with different sizes; the image with
    the largest pixel area is selected.
    """
    if not images or not isinstance(images, list):
        return None

    best_url = None
    best_area = -1

    for im in images:
        if not isinstance(im, dict):
            continue
        url = im.get("url")
        w = im.get("width")
        h = im.get("height")
        if not url:
            continue
        try:
            area = int(w) * int(h) if w is not None and h is not None else 0
        except Exception:
            area = 0

        if area > best_area:
            best_area = area
            best_url = url

    return best_url


def _safe_mean(values) -> Optional[float]:
    """Compute the mean of numeric values, ignoring invalid entries."""
    nums = [v for v in values if isinstance(v, (int, float))]
    if not nums:
        return None
    return float(sum(nums)) / float(len(nums))


def _safe_max(values) -> Optional[float]:
    """Return the maximum of numeric values, ignoring invalid entries."""
    nums = [v for v in values if isinstance(v, (int, float))]
    if not nums:
        return None
    return float(max(nums))

def normalize_artist_info(
    raw_artist: Dict[str, Any],
    *,
    local_artist_id: str,
    fetched_at: Optional[str] = None,
    job_run_id: Optional[str] = None,
) -> ArtistInfo:
    """
    Normalize a Spotify artist object into an ArtistInfo record.

    This function extracts static or slowly-changing metadata used as
    reference information across platforms.
    """
    fetched_at = fetched_at or utc_now_iso()

    return ArtistInfo(
        local_artist_id=local_artist_id,
        artist_name=raw_artist.get("name"),
        spotify_artist_id=raw_artist.get("id"),
        genres=raw_artist.get("genres") or [],
        image_url=_pick_best_image_url(raw_artist.get("images")),
        spotify_url=(raw_artist.get("external_urls") or {}).get("spotify"),
        fetched_at=fetched_at,
        job_run_id=job_run_id,
    )

def normalize_spotify_daily(
    raw_artist: Dict[str, Any],
    *,
    local_artist_id: str,
    spotify_artist_id: Optional[str] = None,
    day_date: Optional[date] = None,
    fetched_at: Optional[str] = None,
    job_run_id: Optional[str] = None,
    top_tracks_payload: Optional[Dict[str, Any]] = None,
    artist_request_id: Optional[str] = None,
    top_tracks_request_id: Optional[str] = None,
) -> SpotifyArtistDaily:
    """
    Normalize Spotify artist statistics into a daily snapshot.

    Produces one record per artist per day, suitable for time-series analysis.
    """
    fetched_at = fetched_at or utc_now_iso()
    day_date = day_date or datetime.now(timezone.utc).date()

    followers_total = None
    followers = raw_artist.get("followers")
    if isinstance(followers, dict):
        followers_total = followers.get("total")

    popularity = raw_artist.get("popularity")
    spotify_artist_id = spotify_artist_id or raw_artist.get("id")

    top_max, top_mean, top_n = None, None, None
    if top_tracks_payload:
        top_max, top_mean, top_n = summarize_top_tracks(top_tracks_payload)

    return SpotifyArtistDaily(
        local_artist_id=local_artist_id,
        spotify_artist_id=spotify_artist_id,
        day_date=day_date.isoformat(),
        fetched_at=fetched_at,
        job_run_id=job_run_id,
        artist_request_id=artist_request_id,
        top_tracks_request_id=top_tracks_request_id,
        followers_total=followers_total,
        popularity=popularity,
        top_track_popularity_max=top_max,
        top_track_popularity_mean=top_mean,
        num_top_tracks=top_n,
    )

def summarize_top_tracks(
    raw_top_tracks: Dict[str, Any],
) -> Tuple[Optional[float], Optional[float], int]:
    """
    Extract popularity statistics from the Spotify top-tracks endpoint.

    Returns:
        (max_popularity, mean_popularity, number_of_tracks)
    """
    tracks = raw_top_tracks.get("tracks")
    if not isinstance(tracks, list) or not tracks:
        return None, None, 0

    popularities = [
        t.get("popularity")
        for t in tracks
        if isinstance(t, dict)
    ]

    numeric = [p for p in popularities if isinstance(p, (int, float))]
    return _safe_max(numeric), _safe_mean(numeric), len(numeric)


