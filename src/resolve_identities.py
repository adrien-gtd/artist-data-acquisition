from typing import Dict
import os
import dotenv
import json

# Import the API adapters
from src.adapters.wikipedia_api import WikipediaAPI
from src.adapters.youtube_api import YouTubeAPI
from src.adapters.spotify_api import SpotifyAPI

# Import the artist info writter and pydantic model
from src.db.writer import upsert_artist_info, connect_sqlite
from src.schema.artist_info import ArtistInfo

# Import provenance wrappers
from src.provenance.workflow_provenance import RunContext
from src.provenance.fine_grain_provenance import RequestContext

# Import normalizers
from src.normalize.spotify_norm import normalize_artist_info
from src.normalize.wikipedia_norm import normalize_wiki_info_from_summary
from src.normalize.youtube_norm import normalize_youtube_info_from_channel




ARTIST_JSON_PATH = os.path.join("src", "configs", "tracked_artists.json")

def get_wiki_title(artist: Dict[str, str], wiki_api: WikipediaAPI) -> str:
    local_artist_id = artist["local_artist_id"]
    wiki_title = wiki_api.search_page_title(local_artist_id, limit=1)
    return wiki_title

def get_youtube_channel_id(artist: Dict[str, str], youtube_api: YouTubeAPI) -> str:
    local_artist_id = artist["local_artist_id"]
    youtube_channel_id = youtube_api.search_channel(local_artist_id, max_results=1)
    return youtube_channel_id

def join_artist_info(
    base: ArtistInfo,
    updates: ArtistInfo,
) -> ArtistInfo:
    """
    Merge two ArtistInfo records, preferring non-None fields from updates.
    """
    return ArtistInfo(
        local_artist_id=base.local_artist_id,
        wikipedia_url=updates.wikipedia_url or base.wikipedia_url,
        youtube_channel_id=updates.youtube_channel_id or base.youtube_channel_id,
        spotify_artist_id=updates.spotify_artist_id or base.spotify_artist_id,
        artist_name=updates.artist_name or base.artist_name,
        genres=updates.genres or base.genres,
        image_url=updates.image_url or base.image_url,
        spotify_url=updates.spotify_url or base.spotify_url,
        youtube_channel_url=updates.youtube_channel_url or base.youtube_channel_url,
        spotify_fetched_at=updates.spotify_fetched_at or base.spotify_fetched_at,
        spotify_job_run_id=updates.spotify_job_run_id or base.spotify_job_run_id,
        spotify_request_id=updates.spotify_request_id or base.spotify_request_id,
        wikipedia_fetched_at=updates.wikipedia_fetched_at or base.wikipedia_fetched_at,
        wikipedia_job_run_id=updates.wikipedia_job_run_id or base.wikipedia_job_run_id,
        wikipedia_request_id=updates.wikipedia_request_id or base.wikipedia_request_id,
        youtube_fetched_at=updates.youtube_fetched_at or base.youtube_fetched_at,
        youtube_job_run_id=updates.youtube_job_run_id or base.youtube_job_run_id,
        youtube_request_id=updates.youtube_request_id or base.youtube_request_id,
    )

def retrieve_and_store_artist_info(artist_list: Dict[str, str], commit_hash: str, conn: any) -> None:
    """
    Retrieve artist info from all APIs and store/update in the database.
    """
    wiki_api = WikipediaAPI()
    youtube_api = YouTubeAPI()
    spotify_api = SpotifyAPI()

    with RunContext(run_day="identity_retrieval", commit_hash=commit_hash, conn=conn) as run_ctx:
        job_run_id = run_ctx.run_id

        for artist in artist_list:
            local_artist_id = artist["local_artist_id"]

            # Spotify info
            with RequestContext(
                run_id=job_run_id,
                step_run_id=None,
                source="spotify",
                local_artist_id=local_artist_id,
                platform_id=artist["spotify_artist_id"],
                conn=conn,
            ) as req_ctx:
                raw_spotify_artist = spotify_api.get_artist(artist["spotify_artist_id"], request_ctx=req_ctx)
                spotify_info = normalize_artist_info(
                    raw_spotify_artist,
                    local_artist_id=local_artist_id,
                    fetched_at=req_ctx.started_at.isoformat(),
                    job_run_id=job_run_id,
                    request_id=req_ctx.request_id,
                )

            # Wikipedia info
            wiki_title = get_wiki_title(artist, wiki_api)
            with RequestContext(
                run_id=job_run_id,
                step_run_id=None,
                source="wikipedia",
                local_artist_id=local_artist_id,
                platform_id=wiki_title,
                conn=conn,
            ) as req_ctx:
                raw_wiki_summary = wiki_api.get_page_summary(title=wiki_title, request_ctx=req_ctx)
                wiki_info = normalize_wiki_info_from_summary(
                    raw_wiki_summary,
                    local_artist_id=local_artist_id,
                    fetched_at=req_ctx.started_at.isoformat(),
                    job_run_id=job_run_id,
                    request_id=req_ctx.request_id,
                )

            # YouTube info
            youtube_channel_id = get_youtube_channel_id(artist, youtube_api)
            with RequestContext(
                run_id=job_run_id,
                step_run_id=None,
                source="youtube",
                local_artist_id=local_artist_id,
                platform_id=youtube_channel_id,
                conn=conn,
            ) as req_ctx:
                raw_youtube_channel = youtube_api.get_channel(youtube_channel_id, request_ctx=req_ctx)
                youtube_info = normalize_youtube_info_from_channel(
                    raw_youtube_channel,
                    local_artist_id=local_artist_id,
                    fetched_at=req_ctx.started_at.isoformat(),
                    job_run_id=job_run_id,
                    request_id=req_ctx.request_id,
                )

            print(type(spotify_info))

            # Merge all info
            combined_info = join_artist_info(
                join_artist_info(spotify_info, wiki_info),
                youtube_info,
            )

            print(type(combined_info))

            # Upsert into database
            upsert_artist_info(conn, combined_info)

if __name__ == "__main__":
    dotenv.load_dotenv()
    conn = connect_sqlite(os.getenv("SQLITE_DB_PATH", "artist_data.db"))

    with open(ARTIST_JSON_PATH, "r") as f:
        artist_list = json.load(f)

    # Get commit hash for provenance
    commit_hash = os.getenv("COMMIT_HASH", "unknown")

    retrieve_and_store_artist_info(artist_list, commit_hash, conn)






        
    