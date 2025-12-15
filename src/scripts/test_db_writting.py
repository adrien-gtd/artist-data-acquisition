from datetime import date
from dotenv import load_dotenv
from src.adapters.spotify_api import SpotifyAPI
from src.normalize.spotify_norm import normalize_artist_info, normalize_artist_daily
from src.db.writer import connect_sqlite, upsert_artist_info, insert_artist_daily



def run_one_artist(local_artist_id: str, spotify_artist_id: str):
    # Initialize API and DB connection
    api = SpotifyAPI()
    conn = connect_sqlite("data/artist_tracker.sqlite")

    # Fetch raw data from Spotify
    raw_artist = api.get_artist(spotify_artist_id)
    raw_top = api.get_artist_top_tracks(spotify_artist_id, market="FR")

    # Normalize
    info = normalize_artist_info(raw_artist, local_artist_id=local_artist_id)
    daily = normalize_artist_daily(
        raw_artist,
        local_artist_id=local_artist_id,
        day_date=date.today(),
        top_tracks_payload=raw_top,
    )

    # Write to DB
    upsert_artist_info(conn, info)
    insert_artist_daily(conn, daily)
    conn.close()

if __name__ == "__main__":
    # Load .env from repo root so Spotify credentials are available
    load_dotenv()

    # Map of local artist IDs to Spotify artist IDs
    artists = {
        "the_weeknd": "1Xyo4u8uXC1ZmMpatF05PJ",
        "daft_punk": "4tZwfgrHOc3mvqYlEYSvVi",
        "taylor_swift": "06HL4z0CvFAxyc27GXpf02",
    }

    for local_id, spotify_id in artists.items():
        print(f"Processing artist: {local_id}")
        run_one_artist(local_id, spotify_id)
