from __future__ import annotations
import sqlite3

DDL_ARTIST_INFO = """
CREATE TABLE IF NOT EXISTS artist_info (
  local_artist_id TEXT PRIMARY KEY,

  artist_name TEXT,
  spotify_artist_id TEXT,
  wiki_title TEXT,
  youtube_channel_id TEXT,

  genres_json TEXT,
  image_url TEXT,
  spotify_url TEXT,
  wikipedia_url TEXT,
  youtube_channel_url TEXT,
  
  --- Provenance
  spotify_fetched_at TEXT NOT NULL,
  spotify_job_run_id TEXT NOT NULL,
  spotify_request_id TEXT NOT NULL,
  wikipedia_fetched_at TEXT NOT NULL,
  wikipedia_job_run_id TEXT NOT NULL,
  wikipedia_request_id TEXT NOT NULL,
  youtube_fetched_at TEXT NOT NULL,
  youtube_job_run_id TEXT NOT NULL,
  youtube_request_id TEXT NOT NULL,


  FOREIGN KEY (spotify_job_run_id) REFERENCES pipeline_run(run_id),
  FOREIGN KEY (wikipedia_job_run_id) REFERENCES pipeline_run(run_id),
  FOREIGN KEY (youtube_job_run_id) REFERENCES pipeline_run(run_id),
  FOREIGN KEY (spotify_request_id) REFERENCES api_request(request_id),
  FOREIGN KEY (wikipedia_request_id) REFERENCES api_request(request_id),
  FOREIGN KEY (youtube_request_id) REFERENCES api_request(request_id)
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
    
    --- Provenance
	job_run_id TEXT,						-- run id for workflow provenance
    top_tracks_request_id TEXT,				-- request id for fine grained tracing
    artist_request_id TEXT,					-- request id for fine grained tracing
    

    --- Metrics
	followers_total INTEGER,
	popularity INTEGER,
	top_track_popularity_max REAL,
	top_track_popularity_mean REAL,
	num_top_tracks INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id),
    FOREIGN KEY (job_run_id) REFERENCES pipeline_run(run_id)
    FOREIGN KEY (top_tracks_request_id) REFERENCES api_request(request_id),
	FOREIGN KEY (artist_request_id) REFERENCES api_request(request_id)
);
"""

DDL_WIKI_DAILY = """
CREATE TABLE IF NOT EXISTS wiki_artist_daily (
	local_artist_id TEXT NOT NULL,
	wiki_title TEXT NOT NULL,
	day_date TEXT NOT NULL,
	fetched_at TEXT NOT NULL,
    
    --- Provenance
	job_run_id TEXT,						-- run id for workflow provenance	
    request_id TEXT,						-- request id for fine grained tracing

    --- Metrics
	pageviews INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id),
    FOREIGN KEY (job_run_id) REFERENCES pipeline_run(run_id)
    FOREIGN KEY (request_id) REFERENCES api_request(request_id)
);
"""

DDL_YOUTUBE_DAILY = """
CREATE TABLE IF NOT EXISTS youtube_artist_daily (
	local_artist_id TEXT NOT NULL,
	youtube_channel_id TEXT NOT NULL,
	day_date TEXT NOT NULL,
	fetched_at TEXT NOT NULL,
    
    --- Provenance
	job_run_id TEXT,						-- run id for workflow provenance
    request_id TEXT,						-- request id for fine grained tracing

    --- Metrics
	subscribers INTEGER,
	total_views INTEGER,
	video_count INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id),
    FOREIGN KEY (job_run_id) REFERENCES pipeline_run(run_id)
    FOREIGN KEY (request_id) REFERENCES api_request(request_id)
);
"""

DDL_ARTIST_DAILY = """
CREATE TABLE IF NOT EXISTS artist_day (
	local_artist_id TEXT NOT NULL,
	day_date TEXT NOT NULL,
    
    --- Provenance
	job_run_spotify TEXT,
	job_run_wiki TEXT,
	job_run_youtube TEXT,
	request_id_spotify TEXT,
	request_id_wiki TEXT,
	request_id_youtube TEXT,
    
	--- Spotify metrics
	spotify_followers_total INTEGER,
	spotify_popularity INTEGER,
	spotify_top_track_popularity_mean REAL,

	--- Wikipedia metrics
	wiki_pageviews INTEGER,

	--- YouTube metrics
	youtube_subscribers INTEGER,
	youtube_total_views INTEGER,

	PRIMARY KEY (local_artist_id, day_date),
	FOREIGN KEY (local_artist_id) REFERENCES artist_info(local_artist_id)
    FOREIGN KEY (job_run_spotify) REFERENCES pipeline_run(run_id),
    FOREIGN KEY (job_run_wiki) REFERENCES pipeline_run(run_id),
	FOREIGN KEY (job_run_youtube) REFERENCES pipeline_run(run_id)
    FOREIGN KEY (request_id_spotify) REFERENCES pipeline_run_step(step_run_id),
    FOREIGN KEY (request_id_wiki) REFERENCES pipeline_run_step(step_run_id),
    FOREIGN KEY (request_id_youtube) REFERENCES pipeline_run_step(step_run_id)
);
"""

DDL_PIPELINE_RUN = """
CREATE TABLE IF NOT EXISTS pipeline_run (
  run_id TEXT PRIMARY KEY,
  run_day TEXT NOT NULL,              -- '2025-12-16'
  commit_hash TEXT NOT NULL,

  started_at TEXT NOT NULL,           -- ISO UTC timestamp
  ended_at TEXT,                      -- NULL while running
  duration_ms INTEGER,                -- NULL while running

  status TEXT NOT NULL,               -- in_progress / completed / failed
  error_message TEXT,
  error_type TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_day ON pipeline_run(run_day);
"""

DDL_PIPELINE_RUN_STEP = """
CREATE TABLE IF NOT EXISTS pipeline_run_step (
  step_run_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,

  step_name TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  duration_ms INTEGER,

  success_count INTEGER,
  error_count INTEGER,

  status TEXT NOT NULL,               -- in_progress / completed / failed / partial
  inputs_json TEXT NOT NULL,          -- JSON array
  outputs_json TEXT NOT NULL,         -- JSON array

  error_message TEXT,
  error_type TEXT,

  FOREIGN KEY (run_id) REFERENCES pipeline_run(run_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_step_run ON pipeline_run_step(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_step_name ON pipeline_run_step(step_name);
"""

DDL_REQUEST_TRACE = """
CREATE TABLE IF NOT EXISTS api_request (
  request_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  step_run_id TEXT,

  source TEXT NOT NULL,              -- 'spotify'|'wikipedia'|'youtube'
  local_artist_id TEXT,
  platform_id TEXT,                  -- spotify_artist_id OR wiki_title OR youtube_channel_id

  endpoint TEXT NOT NULL,
  request_params_json TEXT,

  requested_at TEXT NOT NULL,
  finished_at TEXT,
  duration_ms INTEGER,

  http_status INTEGER,
  ok INTEGER NOT NULL,               -- 1/0
  error_type TEXT,
  error_message TEXT,

  FOREIGN KEY (run_id) REFERENCES pipeline_run(run_id),
  FOREIGN KEY (step_run_id) REFERENCES pipeline_run_step(step_run_id)
);

CREATE INDEX IF NOT EXISTS idx_api_request_run ON api_request(run_id);
CREATE INDEX IF NOT EXISTS idx_api_request_step ON api_request(step_run_id);
CREATE INDEX IF NOT EXISTS idx_api_request_artist_day ON api_request(local_artist_id, requested_at);
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
    conn.executescript(DDL_ARTIST_DAILY)
    conn.executescript(DDL_PIPELINE_RUN)
    conn.executescript(DDL_PIPELINE_RUN_STEP)
    conn.executescript(DDL_REQUEST_TRACE)
    conn.executescript(DDL_INDEXES)
    conn.commit()

