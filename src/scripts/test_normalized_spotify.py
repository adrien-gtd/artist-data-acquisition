import sys
from pathlib import Path

# Ensure repo root is on PYTHONPATH so `import src...` works
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from dotenv import load_dotenv

# Load .env from repo root so Spotify credentials are available
load_dotenv(dotenv_path=REPO_ROOT / ".env")

from src.adapters.spotify_api import SpotifyAPI
from src.normalize.spotify_norm import (
    normalize_artist_info,
    normalize_artist_daily,
)

api = SpotifyAPI()

artists = {
    "the_weeknd": "1Xyo4u8uXC1ZmMpatF05PJ",
    "daft_punk": "4tZwfgrHOc3mvqYlEYSvVi",
    "taylor_swift": "06HL4z0CvFAxyc27GXpf02",
}

for local_artist_id, spotify_id in artists.items():
    print("\n" + "=" * 60)
    print(f"Artist: {local_artist_id}")
    print("=" * 60)

    raw_artist = api.get_artist(spotify_id)
    raw_top = api.get_artist_top_tracks(spotify_id, market="FR")

    info = normalize_artist_info(raw_artist, local_artist_id=local_artist_id)
    daily = normalize_artist_daily(
        raw_artist,
        local_artist_id=local_artist_id,
        top_tracks_payload=raw_top,
    )

    print("=== ArtistInfo ===")
    print(info)
    print("=== ArtistDaily ===")
    print(daily)