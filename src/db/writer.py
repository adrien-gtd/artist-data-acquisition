from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Union
import json

from dataclasses import is_dataclass
from src.db.tables import init_db

RowLike = Union[Dict[str, Any], Any]  # dict or Pydantic model


def _to_dict(obj: RowLike) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):  # Pydantic v2
        return obj.model_dump()
    if is_dataclass(obj):
        from dataclasses import asdict
        return asdict(obj)
    raise TypeError(f"Unsupported row type: {type(obj)}")


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def upsert_artist_info(conn: sqlite3.Connection, artist_info) -> None:
    d: Dict[str, Any] = _to_dict(artist_info)

    # Convert genres list -> JSON string for SQLite storage
    genres = d.get("genres", [])
    if not isinstance(genres, list):
        genres = []
    genres_json = json.dumps(genres, ensure_ascii=False)

    conn.execute(
        """
        INSERT INTO artist_info (
            local_artist_id, artist_name,
            spotify_artist_id, wiki_title, youtube_channel_id,
            country, debut_year,
            genres_json, image_url,
            spotify_url, wikipedia_url, youtube_channel_url,
            fetched_at, job_run_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_artist_id) DO UPDATE SET
            artist_name = excluded.artist_name,

            spotify_artist_id = COALESCE(excluded.spotify_artist_id, artist_info.spotify_artist_id),
            wiki_title = COALESCE(excluded.wiki_title, artist_info.wiki_title),
            youtube_channel_id = COALESCE(excluded.youtube_channel_id, artist_info.youtube_channel_id),

            country = COALESCE(excluded.country, artist_info.country),
            debut_year = COALESCE(excluded.debut_year, artist_info.debut_year),

            genres_json = COALESCE(excluded.genres_json, artist_info.genres_json),
            image_url = COALESCE(excluded.image_url, artist_info.image_url),

            spotify_url = COALESCE(excluded.spotify_url, artist_info.spotify_url),
            wikipedia_url = COALESCE(excluded.wikipedia_url, artist_info.wikipedia_url),
            youtube_channel_url = COALESCE(excluded.youtube_channel_url, artist_info.youtube_channel_url),

            fetched_at = COALESCE(excluded.fetched_at, artist_info.fetched_at),
            job_run_id = COALESCE(excluded.job_run_id, artist_info.job_run_id)
        ;
        """,
        (
            d.get("local_artist_id"),
            d.get("artist_name"),

            d.get("spotify_artist_id"),
            d.get("wiki_title"),
            d.get("youtube_channel_id"),

            d.get("country"),
            d.get("debut_year"),

            genres_json,
            d.get("image_url"),

            d.get("spotify_url"),
            d.get("wikipedia_url"),
            d.get("youtube_channel_url"),

            d.get("fetched_at"),
            d.get("job_run_id"),
        ),
    )
    conn.commit()


def upsert_spotify_daily(conn: sqlite3.Connection, row: RowLike) -> None:
    d = _to_dict(row)
    conn.execute(
        """
        INSERT INTO spotify_artist_daily (
            local_artist_id, spotify_artist_id, day_date, fetched_at, job_run_id,
            followers_total, popularity,
            top_track_popularity_max, top_track_popularity_mean, num_top_tracks
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_artist_id, day_date) DO UPDATE SET
            spotify_artist_id = excluded.spotify_artist_id,
            fetched_at = excluded.fetched_at,
            job_run_id = excluded.job_run_id,
            followers_total = excluded.followers_total,
            popularity = excluded.popularity,
            top_track_popularity_max = excluded.top_track_popularity_max,
            top_track_popularity_mean = excluded.top_track_popularity_mean,
            num_top_tracks = excluded.num_top_tracks
        ;
        """,
        (
            d.get("local_artist_id"),
            d.get("spotify_artist_id"),
            d.get("day_date"),
            d.get("fetched_at"),
            d.get("job_run_id"),
            d.get("followers_total"),
            d.get("popularity"),
            d.get("top_track_popularity_max"),
            d.get("top_track_popularity_mean"),
            d.get("num_top_tracks"),
        ),
    )
    conn.commit()


def upsert_wiki_daily(conn: sqlite3.Connection, row: RowLike) -> None:
    d = _to_dict(row)
    conn.execute(
        """
        INSERT INTO wiki_artist_daily (
            local_artist_id, wiki_title, day_date, fetched_at, job_run_id, pageviews
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_artist_id, day_date) DO UPDATE SET
            wiki_title = excluded.wiki_title,
            fetched_at = excluded.fetched_at,
            job_run_id = excluded.job_run_id,
            pageviews = excluded.pageviews
        ;
        """,
        (
            d.get("local_artist_id"),
            d.get("wiki_title"),
            d.get("day_date"),
            d.get("fetched_at"),
            d.get("job_run_id"),
            d.get("pageviews"),
        ),
    )
    conn.commit()


def upsert_youtube_daily(conn: sqlite3.Connection, row: RowLike) -> None:
    d = _to_dict(row)
    conn.execute(
        """
        INSERT INTO youtube_artist_daily (
            local_artist_id, youtube_channel_id, day_date, fetched_at, job_run_id,
            subscribers, total_views, video_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_artist_id, day_date) DO UPDATE SET
            youtube_channel_id = excluded.youtube_channel_id,
            fetched_at = excluded.fetched_at,
            job_run_id = excluded.job_run_id,
            subscribers = excluded.subscribers,
            total_views = excluded.total_views,
            video_count = excluded.video_count
        ;
        """,
        (
            d.get("local_artist_id"),
            d.get("youtube_channel_id"),
            d.get("day_date"),
            d.get("fetched_at"),
            d.get("job_run_id"),
            d.get("subscribers"),
            d.get("total_views"),
            d.get("video_count"),
        ),
    )
    conn.commit()


def upsert_artist_daily(conn: sqlite3.Connection, row: RowLike) -> None:
    d = _to_dict(row)
    conn.execute(
        """
        INSERT INTO artist_daily (
            local_artist_id, day_date, job_run_id,
            spotify_followers_total, spotify_popularity, spotify_top_track_popularity_mean,
            wiki_pageviews,
            youtube_subscribers, youtube_total_views
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_artist_id, day_date) DO UPDATE SET
            job_run_id = excluded.job_run_id,
            spotify_followers_total = excluded.spotify_followers_total,
            spotify_popularity = excluded.spotify_popularity,
            spotify_top_track_popularity_mean = excluded.spotify_top_track_popularity_mean,
            wiki_pageviews = excluded.wiki_pageviews,
            youtube_subscribers = excluded.youtube_subscribers,
            youtube_total_views = excluded.youtube_total_views
        ;
        """,
        (
            d.get("local_artist_id"),
            d.get("day_date"),
            d.get("job_run_id"),
            d.get("spotify_followers_total"),
            d.get("spotify_popularity"),
            d.get("spotify_top_track_popularity_mean"),
            d.get("wiki_pageviews"),
            d.get("youtube_subscribers"),
            d.get("youtube_total_views"),
        ),
    )
    conn.commit()
