from src.cli import daily_job
import dotenv
from src.db.writer import connect_sqlite
from src.db.writer import upsert_artist_info

if __name__ == "__main__":
    the_weeknd_config = [{
        "local_artist_id": "the_weeknd",
        "spotify_artist_id": "1Xyo4u8uXC1ZmMpatF05PJ",
        "wiki_title": "The_Weeknd",
        "youtube_channel_id": "UC0WP5P-ufpRfjbNrmOWwLBQ",
    }]
    conn = connect_sqlite("data/artist_tracker.sqlite")
    upsert_artist_info(conn, {
        "local_artist_id": "the_weeknd",
        "spotify_artist_id": "1Xyo4u8uXC1ZmMpatF05PJ",
        "wiki_title": "The_Weeknd",
        "youtube_channel_id": "UC0WP5P-ufpRfjbNrmOWwLBQ",
    })
    dotenv.load_dotenv()
    daily_job(the_weeknd_config, "some_commit_hash", conn)