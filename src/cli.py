from datetime import date, timedelta
from typing import Dict, List, Any

# Import helpers
from src.utils import utc_now_iso
from src.provenance.workflow_provenance import RunContext, StepContext
from src.provenance.fine_grain_provenance import RequestContext

# Import the API adapters
from src.adapters.spotify_api import SpotifyAPI
from src.adapters.wikipedia_api import WikipediaAPI
from src.adapters.youtube_api import YouTubeAPI

# Import the data normalizers
from src.normalize.spotify_norm import normalize_spotify_daily
from src.normalize.wikipedia_norm import normalize_wiki_daily
from src.normalize.youtube_norm import normalize_youtube_daily

# Import the database adapter
from src.db.writer import merge_daily_data, upsert_spotify_daily, upsert_wiki_daily, upsert_youtube_daily

# Global variables
DAY_DATE = (date.today() - timedelta(days=1)) # Use yesterday for complete daily stats
DAY_ISO = DAY_DATE.isoformat()
DAY_STR = DAY_DATE.strftime("%Y-%m-%d")

def daily_job(artist_list: List[Dict[str, str]], commit_hash: str, conn: Any) -> None:
    """
    Function running the daily data acquisition, normalization, and storage job.

    Parameters
    ----------
    artist_list : List[Dict[str, str]]
        A list of artists keys, fro each element:
            {
                "local_artist_id": str,
                "spotify_artist_id": str,
                "wiki_title": str,
                "youtube_channel_id": str
            }
    """
    # RunContext will create a run record in the database (workflow provenance)
    with RunContext(run_day=DAY_STR, commit_hash=commit_hash, conn=conn) as run_ctx:
        job_run_id = run_ctx.run_id

        # Step 1: Fetch data from the APIs and store them in the database
        # Step 1.1: Fetch data from Spotify api
        process_spotify_data(artist_list, job_run_id, conn)

        # Step 1.2: Fetch data from Wikipedia api
        process_wikipedia_data(artist_list, job_run_id, conn)

        # Step 1.3: Fetch data from YouTube api
        process_youtube_data(artist_list, job_run_id, conn)

        # Step 2: Apply the map reduce to aggregate the data
        merge_daily_data(artist_list, conn, DAY_STR)

def process_spotify_data(artist_list: List[Dict[str, str]], job_run_id: str, conn: Any) -> None:
    spotify = SpotifyAPI()

    # StepContext will create a run_step record in the database (workflow provenance)
    with StepContext(
        run_id=job_run_id,
        step_name="process_spotify_data",
        inputs=[f"artist_list:{DAY_STR}"],
        outputs=[f"spotify_daily:{DAY_STR}"],
        conn=conn,
    ) as step_ctx:
        for artist in artist_list:
            try:
                spotify_artist_id = artist["spotify_artist_id"]
                local_artist_id = artist["local_artist_id"]

                # Fetch raw data from Spotify API
                with RequestContext(
                    run_id=job_run_id,
                    step_run_id=step_ctx.step_run_id,
                    source="spotify",
                    local_artist_id=local_artist_id,
                    platform_id=spotify_artist_id,
                    conn=conn,
                ) as req_ctx1:
                    print("Fetching artist info from Spotify")
                    raw_artist = spotify.get_artist(spotify_artist_id, request_ctx=req_ctx1)
                    top_tracks_request_id = req_ctx1.request_id

                with RequestContext(
                    run_id=job_run_id,
                    step_run_id=step_ctx.step_run_id,
                    source="spotify",
                    local_artist_id=local_artist_id,
                    platform_id=spotify_artist_id,
                    conn=conn,
                ) as req_ctx2:
                    print("Fetching artist top tracks from Spotify")
                    raw_top = spotify.get_artist_top_tracks(spotify_artist_id, market="FR", request_ctx=req_ctx2)
                    artist_request_id = req_ctx2.request_id

                fetched_at = utc_now_iso()
                # Normalize the data
                daily_spotify = normalize_spotify_daily(
                    raw_artist,
                    local_artist_id=local_artist_id,
                    spotify_artist_id=spotify_artist_id,
                    day_date=DAY_DATE,
                    fetched_at=fetched_at,
                    job_run_id=job_run_id,
                    artist_request_id=artist_request_id,
                    top_tracks_request_id=top_tracks_request_id,
                    top_tracks_payload=raw_top,
                )

                # Store the normalized data in the database
                upsert_spotify_daily(conn, daily_spotify)
                step_ctx.success_count += 1
            except Exception as e:
                step_ctx.error_count += 1
                print(f"Error processing Spotify data for artist {local_artist_id}: {e}")

def process_wikipedia_data(artist_list: List[Dict[str, str]], job_run_id: str, conn: Any) -> None:
    wikipedia = WikipediaAPI()

    # StepContext will create a run_step record in the database (workflow provenance)
    with StepContext(
        run_id=job_run_id,
        step_name="process_wikipedia_data",
        inputs=[f"artist_list:{DAY_STR}"],
        outputs=[f"wikipedia_daily:{DAY_STR}"],
        conn=conn,
    ) as step_ctx:

        for artist in artist_list:
            try:
                wiki_title = artist["wiki_title"]
                local_artist_id = artist["local_artist_id"]

                # Fetch raw data from Wikipedia API
                with RequestContext(
                    run_id=job_run_id,
                    step_run_id=step_ctx.step_run_id,
                    source="wikipedia",
                    local_artist_id=local_artist_id,
                    platform_id=wiki_title,
                    conn=conn,
                ) as req_ctx:
                    raw_views = wikipedia.get_pageviews_daily(
                        title=wiki_title,
                        start_yyyy_mm_dd=DAY_ISO,
                        end_yyyy_mm_dd=DAY_ISO,
                        request_ctx=req_ctx,
                    )
                    wiki_request_id = req_ctx.request_id

                fetched_at = utc_now_iso()
                # Normalize the data
                daily_wiki = normalize_wiki_daily(
                    raw_views,
                    local_artist_id=local_artist_id,
                    wiki_title=wiki_title,
                    day_date=DAY_DATE,
                    fetched_at=fetched_at,
                    job_run_id=job_run_id,
                    request_id=wiki_request_id,
                )

                # Store the normalized data in the database
                upsert_wiki_daily(conn, daily_wiki)
                step_ctx.success_count += 1
            except Exception as e:
                step_ctx.error_count += 1
                print(f"Error processing Wikipedia data for artist {local_artist_id}: {e}")

def process_youtube_data(artist_list: List[Dict[str, str]], job_run_id: str, conn: Any) -> None:
    youtube = YouTubeAPI()

    # StepContext will create a run_step record in the database (workflow provenance)
    with StepContext(
        run_id=job_run_id,
        step_name="process_youtube_data",
        inputs=[f"artist_list:{DAY_STR}"],
        outputs=[f"youtube_daily:{DAY_STR}"],
        conn=conn,
    ) as step_ctx:
        for artist in artist_list:
            try:
                youtube_channel_id = artist["youtube_channel_id"]
                local_artist_id = artist["local_artist_id"]

                # Fetch raw data from YouTube API
                with RequestContext(
                    run_id=job_run_id,
                    step_run_id=step_ctx.step_run_id,
                    source="youtube",
                    local_artist_id=local_artist_id,
                    platform_id=youtube_channel_id,
                    conn=conn,
                ) as req_ctx:
                    raw_channel = youtube.get_channel(youtube_channel_id, request_ctx=req_ctx)
                    youtube_request_id = req_ctx.request_id

                fetched_at = utc_now_iso()
                # Normalize the data
                daily_youtube = normalize_youtube_daily(
                    raw_channel,
                    local_artist_id=local_artist_id,
                    youtube_channel_id=youtube_channel_id,
                    day_date=DAY_DATE,
                    fetched_at=fetched_at,
                    job_run_id=job_run_id,
                    request_id=youtube_request_id,
                )

                # Store the normalized data in the database
                upsert_youtube_daily(conn, daily_youtube)
                step_ctx.success_count += 1
            except Exception as e:
                step_ctx.error_count += 1
                print(f"Error processing YouTube data for artist {local_artist_id}: {e}")
