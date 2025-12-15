"""Spotify -> unified schema normalizers.

This module contains *pure* transformation logic:
- Input: raw JSON dicts returned by the Spotify Web API adapter
- Output: your project's unified schema objects (Pydantic) when available,
  otherwise plain dictionaries.

Keep HTTP/auth/rate-limit logic in `src/adapters/spotify_api.py`.
"""

from __future__ import annotations

from datetime import datetime, date, timezone
from typing import Any, Dict, Optional, Tuple


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick_best_image_url(images: Any) -> Optional[str]:
    """Pick the largest image URL if present."""
    if not images or not isinstance(images, list):
        return None

    best = None
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
            area = int(w) * int(h) if (w is not None and h is not None) else 0
        except Exception:
            area = 0
        if area > best_area:
            best_area = area
            best = url

    return best


def _safe_mean(values) -> Optional[float]:
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return None
    return float(sum(vals)) / float(len(vals))


def _safe_max(values) -> Optional[float]:
    vals = [v for v in values if isinstance(v, (int, float))]
    if not vals:
        return None
    return float(max(vals))


def _maybe_build_pydantic(model_cls, payload: Dict[str, Any]):
    """Build a Pydantic v2 model if present, filtering unknown fields.

    If the schema doesn't exist or construction fails, return the payload dict.
    """
    if model_cls is None:
        return payload

    try:
        # Pydantic v2
        allowed = set(getattr(model_cls, "model_fields").keys())
        filtered = {k: v for k, v in payload.items() if k in allowed}
        return model_cls(**filtered)
    except Exception:
        # Fallback to dict so the pipeline can still run.
        return payload


def normalize_artist_info(
    raw_artist: Dict[str, Any],
    *,
    local_artist_id: str,
    fetched_at: Optional[str] = None,
):
    """Normalize Spotify's GET /artists/{id} response into ArtistInfo.

    Args:
        raw_artist: Spotify artist JSON (dict)
        local_artist_id: your local ID from tracked_artists.json
        fetched_at: optional ISO timestamp; defaults to now (UTC)

    Returns:
        ArtistInfo Pydantic model if available, else a dict.
    """

    fetched_at = fetched_at or _utc_now_iso()

    payload: Dict[str, Any] = {
        # Common identifiers
        "local_artist_id": local_artist_id,
        "platform": "spotify",
        "spotify_id": raw_artist.get("id"),

        # Static-ish info
        "name": raw_artist.get("name"),
        "genres": raw_artist.get("genres") or [],
        "image_url": _pick_best_image_url(raw_artist.get("images")),
        "spotify_url": (raw_artist.get("external_urls") or {}).get("spotify"),
        "uri": raw_artist.get("uri"),

        # Metadata
        "fetched_at": fetched_at,
    }

    # Try to map to your project's schema if it exists.
    try:
        from src.schema.artist_info import ArtistInfo  # type: ignore
    except Exception:
        ArtistInfo = None

    return _maybe_build_pydantic(ArtistInfo, payload)


def normalize_artist_daily(
    raw_artist: Dict[str, Any],
    *,
    local_artist_id: str,
    day_date: Optional[date] = None,
    fetched_at: Optional[str] = None,
    top_tracks_payload: Optional[Dict[str, Any]] = None,
):
    """Normalize Spotify artist JSON (+ optional top tracks) into ArtistDaily.

    This function produces *daily* snapshots. One row = one artist, one day.
    """

    fetched_at = fetched_at or _utc_now_iso()
    day_date = day_date or datetime.now(timezone.utc).date()

    followers_total = None
    followers = raw_artist.get("followers")
    if isinstance(followers, dict):
        followers_total = followers.get("total")

    popularity = raw_artist.get("popularity")

    # Optional: derive some extra daily features from top tracks
    top_max, top_mean, top_n = None, None, None
    if top_tracks_payload:
        top_max, top_mean, top_n = summarize_top_tracks(top_tracks_payload)

    payload: Dict[str, Any] = {
        "local_artist_id": local_artist_id,
        "platform": "spotify",
        "spotify_id": raw_artist.get("id"),

        # Time
        "day_date": day_date.isoformat(),
        "fetched_at": fetched_at,

        # Core daily metrics (officially available)
        "followers_total": followers_total,
        "popularity": popularity,

        # Optional derived metrics from top tracks
        "top_track_popularity_max": top_max,
        "top_track_popularity_mean": top_mean,
        "num_top_tracks": top_n,
    }

    try:
        from src.schema.artist_daily import ArtistDaily  # type: ignore
    except Exception:
        ArtistDaily = None

    return _maybe_build_pydantic(ArtistDaily, payload)


def summarize_top_tracks(raw_top_tracks: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[int]]:
    """Return (max_popularity, mean_popularity, n_tracks) from /top-tracks."""
    tracks = raw_top_tracks.get("tracks")
    if not isinstance(tracks, list) or not tracks:
        return None, None, 0

    pops = []
    for t in tracks:
        if not isinstance(t, dict):
            continue
        pops.append(t.get("popularity"))

    return _safe_max(pops), _safe_mean(pops), len([p for p in pops if p is not None])
