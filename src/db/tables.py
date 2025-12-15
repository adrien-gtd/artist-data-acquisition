from __future__ import annotations
import sqlite3

DDL_ARTIST_INFO = """
CREATE TABLE IF NOT EXISTS artist_info (
  local_artist_id TEXT PRIMARY KEY,

  artist_name TEXT,
  spotify_artist_id TEXT,
  wiki_title TEXT,
  youtube_channel_id TEXT,

  country TEXT,
  debut_year INTEGER,

  genres_json TEXT,
  image_url TEXT,
  spotify_url TEXT,
  wikipedia_url TEXT,
  youtube_channel_url TEXT,

  fetched_at TEXT,
  job_run_id TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_artist_info_spotify
	ON artist_info(spotify_artist_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_artist_info_wiki
	ON artist_info(wiki_title);

CREATE UNIQUE INDEX IF NOT EXISTS idx_artist_info_youtube
	ON artist_info(youtube_channel_id);
"""

DDL_SPOTIFY_DAILY = """
CREATE TABLE IF NOT EXISTS spotify_artist_daily (
	local_artist_id TEXT NOT NULL,
	spotify_artist_id TEXT NOT NULL,
	day_date TEXT NOT NULL,
	fetched_at TEXT NOT NULL,
	job_run_id TEXT,

	followers_total INTEGER,
	popularity INTEGER,
	top_track_popularity_max REAL,
	top_track_popularity_mean REAL,
	num_top_tracks INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id)
);
"""

DDL_WIKI_DAILY = """
CREATE TABLE IF NOT EXISTS wiki_artist_daily (
	local_artist_id TEXT NOT NULL,
	wiki_title TEXT NOT NULL,
	day_date TEXT NOT NULL,
	fetched_at TEXT NOT NULL,
	job_run_id TEXT,

	pageviews INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id)
);
"""

DDL_YOUTUBE_DAILY = """
CREATE TABLE IF NOT EXISTS youtube_artist_daily (
	local_artist_id TEXT NOT NULL,
	youtube_channel_id TEXT NOT NULL,
	day_date TEXT NOT NULL,
	fetched_at TEXT NOT NULL,
	job_run_id TEXT,

	subscribers INTEGER,
	total_views INTEGER,
	video_count INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id)
);
"""

DDL_ARTIST_DAY = """
CREATE TABLE IF NOT EXISTS artist_day (
	local_artist_id TEXT NOT NULL,
	day_date TEXT NOT NULL,
	job_run_id TEXT,

	spotify_followers_total INTEGER,
	spotify_popularity INTEGER,
	spotify_top_track_popularity_mean REAL,

	wiki_pageviews INTEGER,

	youtube_subscribers INTEGER,
	youtube_total_views INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id)
);
"""

DDL_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_spotify_daily_date ON spotify_artist_daily(day_date);
CREATE INDEX IF NOT EXISTS idx_wiki_daily_date ON wiki_artist_daily(day_date);
CREATE INDEX IF NOT EXISTS idx_youtube_daily_date ON youtube_artist_daily(day_date);
CREATE INDEX IF NOT EXISTS idx_artist_day_date ON artist_day(day_date);
"""

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(DDL_ARTIST_INFO)
    conn.executescript(DDL_SPOTIFY_DAILY)
    conn.executescript(DDL_WIKI_DAILY)
    conn.executescript(DDL_YOUTUBE_DAILY)
    conn.executescript(DDL_ARTIST_DAY)
    conn.executescript(DDL_INDEXES)
    conn.commit()
