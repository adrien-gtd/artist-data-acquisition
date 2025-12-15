from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict

"""
Daily artist data schemas (Pydantic models).
These correspond to daily snapshot tables in the database.
Those classes are used for normalization and validation of the data before writing to the DB.
"""


class SpotifyArtistDaily(BaseModel):
    """
    Daily Spotify snapshot for one artist.
    One row per (local_artist_id, day_date).
    """

    model_config = ConfigDict(extra="ignore")

    # Keys
    local_artist_id: str = Field(..., description="Internal artist identifier")
    spotify_artist_id: str = Field(..., description="Spotify artist id")
    day_date: str = Field(..., description="ISO date YYYY-MM-DD")

    # Provenance
    fetched_at: str = Field(..., description="UTC ISO timestamp")
    job_run_id: Optional[str] = Field(None, description="Pipeline execution id")

    # Metrics
    followers_total: Optional[int] = None
    popularity: Optional[int] = None

    # Derived from top tracks
    top_track_popularity_max: Optional[float] = None
    top_track_popularity_mean: Optional[float] = None
    num_top_tracks: Optional[int] = None





class WikiArtistDaily(BaseModel):
    """
    Daily Wikipedia snapshot for one artist.
    """

    model_config = ConfigDict(extra="ignore")

    # Keys
    local_artist_id: str = Field(..., description="Internal artist identifier")
    wiki_title: str = Field(..., description="Wikipedia page title")
    day_date: str = Field(..., description="ISO date YYYY-MM-DD")

    # Provenance
    fetched_at: str = Field(..., description="UTC ISO timestamp")
    job_run_id: Optional[str] = Field(None, description="Pipeline execution id")

    # Metrics
    pageviews: Optional[int] = Field(
        None, description="Wikipedia pageviews for that day"
    )





class YouTubeArtistDaily(BaseModel):
    """
    Daily YouTube channel snapshot for one artist.
    """

    model_config = ConfigDict(extra="ignore")

    # Keys
    local_artist_id: str = Field(..., description="Internal artist identifier")
    youtube_channel_id: str = Field(..., description="YouTube channel id")
    day_date: str = Field(..., description="ISO date YYYY-MM-DD")

    # Provenance
    fetched_at: str = Field(..., description="UTC ISO timestamp")
    job_run_id: Optional[str] = Field(None, description="Pipeline execution id")

    # Metrics
    subscribers: Optional[int] = None
    total_views: Optional[int] = None
    video_count: Optional[int] = None


class ArtistDay(BaseModel):
    """
    Unified daily artist record (cross-platform).
    """

    model_config = ConfigDict(extra="ignore")

    # Keys
    local_artist_id: str
    day_date: str

    # Spotify
    spotify_followers_total: Optional[int] = None
    spotify_popularity: Optional[int] = None
    spotify_top_track_popularity_mean: Optional[float] = None

    # Wikipedia
    wiki_pageviews: Optional[int] = None

    # YouTube
    youtube_subscribers: Optional[int] = None
    youtube_total_views: Optional[int] = None

    # Provenance
    job_run_id: Optional[str] = None
