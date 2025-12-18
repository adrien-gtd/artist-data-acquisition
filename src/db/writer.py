from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Union, Optional, List
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

    if hasattr(obj, "dict") and callable(getattr(obj, "dict")): # Pydantic v1
        return obj.dict()
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

            genres_json, image_url, spotify_url,
            wikipedia_url, youtube_channel_url,

            spotify_fetched_at, spotify_job_run_id, spotify_request_id,
            wikipedia_fetched_at, wikipedia_job_run_id, wikipedia_request_id,
            youtube_fetched_at, youtube_job_run_id, youtube_request_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_artist_id) DO UPDATE SET
            artist_name = excluded.artist_name,
            spotify_artist_id = excluded.spotify_artist_id,
            wiki_title = excluded.wiki_title,
            youtube_channel_id = excluded.youtube_channel_id,
            genres_json = excluded.genres_json,
            image_url = excluded.image_url,
            spotify_url = excluded.spotify_url,
            wikipedia_url = excluded.wikipedia_url,
            youtube_channel_url = excluded.youtube_channel_url,
            spotify_fetched_at = excluded.spotify_fetched_at,
            spotify_job_run_id = excluded.spotify_job_run_id,
            spotify_request_id = excluded.spotify_request_id,
            wikipedia_fetched_at = excluded.wikipedia_fetched_at,
            wikipedia_job_run_id = excluded.wikipedia_job_run_id,
            wikipedia_request_id = excluded.wikipedia_request_id,
            youtube_fetched_at = excluded.youtube_fetched_at,
            youtube_job_run_id = excluded.youtube_job_run_id,
            youtube_request_id = excluded.youtube_request_id
        ;
        """ ,
        (
            d.get("local_artist_id"),
            d.get("artist_name"),
            d.get("spotify_artist_id"),
            d.get("wiki_title"),
            d.get("youtube_channel_id"),
            genres_json,
            d.get("image_url"),
            d.get("spotify_url"),
            d.get("wikipedia_url"),
            d.get("youtube_channel_url"),
            d.get("spotify_fetched_at"),
            d.get("spotify_job_run_id"),
            d.get("spotify_request_id"),
            d.get("wikipedia_fetched_at"),
            d.get("wikipedia_job_run_id"),
            d.get("wikipedia_request_id"),
            d.get("youtube_fetched_at"),
            d.get("youtube_job_run_id"),
            d.get("youtube_request_id")
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
        INSERT INTO artist_day (
            local_artist_id, day_date,
            job_run_spotify, job_run_wiki, job_run_youtube,
            spotify_followers_total, spotify_popularity, spotify_top_track_popularity_mean,
            wiki_pageviews,
            youtube_subscribers, youtube_total_views
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(local_artist_id, day_date) DO UPDATE SET
            job_run_spotify = excluded.job_run_spotify,
            job_run_wiki = excluded.job_run_wiki,
            job_run_youtube = excluded.job_run_youtube,
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
            d.get("job_run_spotify"),
            d.get("job_run_wiki"),
            d.get("job_run_youtube"),
            d.get("spotify_followers_total"),
            d.get("spotify_popularity"),
            d.get("spotify_top_track_popularity_mean"),
            d.get("wiki_pageviews"),
            d.get("youtube_subscribers"),
            d.get("youtube_total_views"),
        ),
    )
    conn.commit()


# Pipelines for provenance tracking
def upsert_run_meta(
    conn: sqlite3.Connection,
    run_id: str,
    run_day: str,
    commit_hash: str,
    started_at: str,
    ended_at: Optional[str],
    duration_ms: Optional[int],
    status: str,
    error_message: Optional[str] = None,
    error_type: Optional[str] = None,
) -> str:
    conn.execute(
        """
        INSERT INTO pipeline_run (
          run_id, run_day, commit_hash,
          started_at, ended_at, duration_ms,
          status, error_message, error_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
          run_day = excluded.run_day,
          commit_hash = excluded.commit_hash,
          started_at = excluded.started_at,
          ended_at = excluded.ended_at,
          duration_ms = excluded.duration_ms,
          status = excluded.status,
          error_message = excluded.error_message,
          error_type = excluded.error_type
        ;
        """,
        (
            run_id, run_day, commit_hash,
            started_at, ended_at, duration_ms,
            status, error_message, error_type
        ),
    )
    conn.commit()

def upsert_run_step_meta(
    conn: sqlite3.Connection,
    run_id: str,
    step_run_id: str,
    step_name: str,
    started_at: str,
    ended_at: Optional[str],
    success_count: Optional[int],
    error_count: Optional[int],
    duration_ms: Optional[int],
    status: str,
    inputs: List[str],
    outputs: List[str],
    error_message: Optional[str] = None,
    error_type: Optional[str] = None,
) -> None:
    inputs_json = json.dumps(inputs, ensure_ascii=False)
    outputs_json = json.dumps(outputs, ensure_ascii=False)

    conn.execute(
        """
        INSERT INTO pipeline_run_step (
          step_run_id, run_id,
          step_name,
          started_at, ended_at, duration_ms,
          success_count, error_count,
          status, inputs_json, outputs_json,
          error_message, error_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(step_run_id) DO UPDATE SET
          run_id = excluded.run_id,
          step_name = excluded.step_name,
          started_at = excluded.started_at,
          ended_at = excluded.ended_at,
          duration_ms = excluded.duration_ms,
          success_count = excluded.success_count,
          error_count = excluded.error_count,
          status = excluded.status,
          inputs_json = excluded.inputs_json,
          outputs_json = excluded.outputs_json,
          error_message = excluded.error_message,
          error_type = excluded.error_type
        ;
        """,
        (
            step_run_id, run_id,
            step_name,
            started_at, ended_at, duration_ms,
            success_count, error_count,
            status, inputs_json, outputs_json,
            error_message, error_type
        ),
    )
    conn.commit()

def upsert_api_request(
    conn: sqlite3.Connection,
    run_id: str,
    step_run_id: str,
    request_id: str,
    source: str,
    local_artist_id: str,
    platform_id: str,
    endpoint: Optional[str],
    request_params: Optional[Dict[str, Any]],
    requested_at: str,
    finished_at: str,
    duration_ms: int,
    http_status: Optional[int],
    ok: int,
    error_type: Optional[str],
    error_message: Optional[str],
) -> None:
    params_json = json.dumps(request_params, ensure_ascii=False) if request_params is not None else None

    conn.execute(
        """
        INSERT INTO api_request (
          request_id, run_id, step_run_id,
          source, local_artist_id, platform_id,
          endpoint, request_params_json,
          requested_at, finished_at, duration_ms,
          http_status, ok,
          error_type, error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(request_id) DO UPDATE SET
          run_id = excluded.run_id,
          step_run_id = excluded.step_run_id,
          source = excluded.source,
          local_artist_id = excluded.local_artist_id,
          platform_id = excluded.platform_id,
          endpoint = excluded.endpoint,
          request_params_json = excluded.request_params_json,
          requested_at = excluded.requested_at,
          finished_at = excluded.finished_at,
          duration_ms = excluded.duration_ms,
          http_status = excluded.http_status,
          ok = excluded.ok,
          error_type = excluded.error_type,
          error_message = excluded.error_message
        ;
        """,
        (   
            request_id, run_id, step_run_id,
            source, local_artist_id, platform_id,
            endpoint, params_json,
            requested_at, finished_at, duration_ms,
            http_status, ok,
            error_type, error_message
        ),
    )
    conn.commit()


def merge_daily_data(artist_list: List[Dict[str, str]], conn: sqlite3.Connection, day_str: str) -> None:
    """
    Map-Reduce merge: For each (local_artist_id, day_date) pair, consolidate the latest
    data from Spotify, Wikipedia, and YouTube sources into the artist_day table.
    
    MAP: Extract (local_artist_id, day_date) pairs from each daily source table
    SHUFFLE: Group by (local_artist_id, day_date)
    REDUCE: Keep the latest fetch of each source
    """
    for artist in artist_list:
        local_artist_id = artist["local_artist_id"]
        
        # Query the latest fetch from each source for this artist and day
        cursor = conn.cursor()
        
        # Get latest Spotify data
        spotify_row = cursor.execute(
            """
            SELECT job_run_id, followers_total, popularity, top_track_popularity_mean
            FROM spotify_artist_daily
            WHERE local_artist_id = ? AND day_date = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (local_artist_id, day_str),
        ).fetchone()
        
        # Get latest Wikipedia data
        wiki_row = cursor.execute(
            """
            SELECT job_run_id, pageviews
            FROM wiki_artist_daily
            WHERE local_artist_id = ? AND day_date = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (local_artist_id, day_str),
        ).fetchone()
        
        # Get latest YouTube data
        youtube_row = cursor.execute(
            """
            SELECT job_run_id, subscribers, total_views
            FROM youtube_artist_daily
            WHERE local_artist_id = ? AND day_date = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (local_artist_id, day_str),
        ).fetchone()
        
        # Only merge if we have at least one source
        if spotify_row or wiki_row or youtube_row:
            merged_data = {
                "local_artist_id": local_artist_id,
                "day_date": day_str,
                "job_run_spotify": spotify_row[0] if spotify_row else None,
                "job_run_wiki": wiki_row[0] if wiki_row else None,
                "job_run_youtube": youtube_row[0] if youtube_row else None,
                "spotify_followers_total": spotify_row[1] if spotify_row else None,
                "spotify_popularity": spotify_row[2] if spotify_row else None,
                "spotify_top_track_popularity_mean": spotify_row[3] if spotify_row else None,
                "wiki_pageviews": wiki_row[1] if wiki_row else None,
                "youtube_subscribers": youtube_row[1] if youtube_row else None,
                "youtube_total_views": youtube_row[2] if youtube_row else None,
            }
            upsert_artist_daily(conn, merged_data)


def select_tracked_artists(conn: sqlite3.Connection) -> List[Dict[str, str]]:
    cursor = conn.cursor()
    rows = cursor.execute(
        """
        SELECT local_artist_id, wiki_title,spotify_artist_id, youtube_channel_id
        FROM artist_info
        WHERE wiki_title IS NOT NULL
        ;
        """
    ).fetchall()
    artist_list = [{"local_artist_id": row["local_artist_id"], "spotify_artist_id": row["spotify_artist_id"],"wiki_title": row["wiki_title"],"youtube_channel_id": row["youtube_channel_id"]} for row in rows]

    return artist_list
