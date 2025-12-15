from __future__ import annotations

import json
from datetime import date, timedelta
import dotenv

from src.adapters.spotify_api import SpotifyAPI
from src.adapters.wikipedia_api import WikipediaAPI
from src.adapters.youtube_api import YouTubeAPI



def pretty(obj) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)


def main() -> None:
    artist_name = "The Weeknd"
    wiki_title = "The_Weeknd"

    # Use yesterday for complete daily stats
    day = (date.today() - timedelta(days=1)).isoformat()

    # Spotify API
    spotify = SpotifyAPI()

    print("\n" + "=" * 80)
    print("SPOTIFY: search_artist")
    s_search = spotify.search_artist(artist_name, limit=5, market="FR")
    print(pretty(s_search))

    # Pick the first search result
    items = (((s_search or {}).get("artists") or {}).get("items") or [])
    if not items:
        raise RuntimeError("Spotify search returned no artists.")
    spotify_artist_id = items[0]["id"]

    print("\n" + "=" * 80)
    print(f"SPOTIFY: get_artist ({spotify_artist_id})")
    s_artist = spotify.get_artist(spotify_artist_id)
    print(pretty(s_artist))

    # Wikipedia API
    wiki = WikipediaAPI()

    print("\n" + "=" * 80)
    print(f"WIKIPEDIA: get_pageviews_daily title={wiki_title} day={day}")
    w_views = wiki.get_pageviews_daily(
        title=wiki_title,
        start_yyyy_mm_dd=day,
        end_yyyy_mm_dd=day,
        project="en.wikipedia",
        access="all-access",
        agent="user",
    )
    print(pretty(w_views))

    print("\n" + "=" * 80)
    print(f"WIKIPEDIA: get_page_summary title={wiki_title}")
    w_summary = wiki.get_page_summary(title=wiki_title)
    print(pretty(w_summary))

    # YouTube API
    yt = YouTubeAPI()

    print("\n" + "=" * 80)
    print("YOUTUBE: search_channel")
    y_search = yt.search_channel(artist_name, max_results=5)
    print(pretty(y_search))

    y_items = (y_search or {}).get("items") or []
    if not y_items:
        raise RuntimeError("YouTube search returned no channels.")

    # The channel id is inside id.channelId for search results
    channel_id = ((y_items[0].get("id") or {}).get("channelId"))
    if not channel_id:
        raise RuntimeError("Could not extract channelId from YouTube search result.")

    print("\n" + "=" * 80)
    print(f"YOUTUBE: get_channel ({channel_id})")
    y_channel = yt.get_channel(channel_id)
    print(pretty(y_channel))


if __name__ == "__main__":
    dotenv.load_dotenv()
    main()
