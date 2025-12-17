from __future__ import annotations

import uuid
from datetime import date, timedelta, datetime, timezone
import dotenv

from src.adapters.spotify_api import SpotifyAPI
from src.adapters.wikipedia_api import WikipediaAPI
from src.adapters.youtube_api import YouTubeAPI

from src.normalize.spotify_norm import normalize_artist_info, normalize_spotify_daily
from src.normalize.wikipedia_norm import normalize_wiki_daily
from src.normalize.youtube_norm import normalize_youtube_daily
from src.utils import utc_now_iso


def main() -> None:
    job_run_id = str(uuid.uuid4())
    fetched_at = utc_now_iso()
    day = date.today() - timedelta(days=1)

    local_artist_id = "the_weeknd"
    spotify_id = "1Xyo4u8uXC1ZmMpatF05PJ"
    wiki_title = "The_Weeknd"
    youtube_channel_id = "UC0WP5P-ufpRfjbNrmOWwLBQ"

    # --- Spotify raw
    spotify = SpotifyAPI()
    raw_artist = spotify.get_artist(spotify_id)
    raw_top = spotify.get_artist_top_tracks(spotify_id, market="FR")

    info = normalize_artist_info(
        raw_artist, local_artist_id=local_artist_id, fetched_at=fetched_at, job_run_id=job_run_id
    )
    daily_sp = normalize_spotify_daily(
        raw_artist,
        local_artist_id=local_artist_id,
        spotify_artist_id=spotify_id,
        day_date=day,
        fetched_at=fetched_at,
        job_run_id=job_run_id,
        top_tracks_payload=raw_top,
    )

    print("ArtistInfo:", info)
    print("SpotifyDaily:", daily_sp)

    # --- Wiki raw
    wiki = WikipediaAPI()
    raw_views = wiki.get_pageviews_daily(
        title=wiki_title,
        start_yyyy_mm_dd=day.isoformat(),
        end_yyyy_mm_dd=day.isoformat(),
    )
    daily_wiki = normalize_wiki_daily(
        raw_views,
        local_artist_id=local_artist_id,
        wiki_title=wiki_title,
        day_date=day,
        fetched_at=fetched_at,
        job_run_id=job_run_id,
    )
    print("WikiDaily:", daily_wiki)

    # --- YouTube raw
    yt = YouTubeAPI()
    raw_channel = yt.get_channel(youtube_channel_id)
    daily_yt = normalize_youtube_daily(
        raw_channel,
        local_artist_id=local_artist_id,
        youtube_channel_id=youtube_channel_id,
        day_date=day,
        fetched_at=fetched_at,
        job_run_id=job_run_id,
    )
    print("YouTubeDaily:", daily_yt)


if __name__ == "__main__":
    dotenv.load_dotenv()
    main()
