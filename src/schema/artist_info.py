from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict

class ArtistInfo(BaseModel):
    """
    Static-ish artist reference info (dimension table).
    One row per artist, with per-platform identifiers used for joins.
    """

    model_config = ConfigDict(extra="ignore")

    # Key
    local_artist_id: str = Field(..., description="Internal artist identifier (PK)")

    # Canonical name you display/use in joins
    artist_name: Optional[str] = Field(None, description="Canonical artist name")

    # Platform identifiers (nullable if unknown)
    spotify_artist_id: Optional[str] = None
    wiki_title: Optional[str] = None
    youtube_channel_id: Optional[str] = None

    # Metadata
    country: Optional[str] = None
    debut_year: Optional[int] = None
    genres: List[str] = Field(default_factory=list)
    image_url: Optional[str] = None
    spotify_url: Optional[str] = None
    wikipedia_url: Optional[str] = None
    youtube_channel_url: Optional[str] = None

    # Provenance
    fetched_at: Optional[str] = Field(None, description="UTC ISO timestamp when last refreshed")
    job_run_id: Optional[str] = Field(None, description="Pipeline execution id")

