# Artist Popularity Tracker

This project collects and organizes data on music artists’ popularity from multiple public sources such as Spotify, YouTube, and Wikipedia.
It extracts raw data, normalizes it into a consistent structure, and stores it for analysis or reporting.

---
## Project Structure

```
.
├── data
│   └── artist_tracker.sqlite                   # The actual database
├── spotifly                                    # The scrapper
│   ├── scrap_artists.sh                        # Script to automate scraping
│   └── spotifly
│       └── spiders
│           └── spotifly_spider.py              # Our main spider
└── src
    ├── adapters
    │  ├─ platform_x_api.py              # Fetches data from Platform X API
    │  ├─ platform_y_api.py              # Fetches data from Platform Y API
    │  └── ...
    ├── cli.py                           # Main data retrieving script
    ├── configs
    │   └── tracked_artists.json         # Artists that we track with api.py
    ├── db
    │   ├── tables.py                    # Structure of the database
    │   └── writer.py                    # Functions to write to database
    ├── normalize
    │  ├─ platform_x_norm.py             # Normalizes Platform X data
    │  ├─ platform_y_norm.py             # Normalizes Platform Y data
    │  └─ ...                            # One file per platform
    ├── provenance
    │   ├── fine_grain_provenance.py     # Fine grained provenance functions
    │   └── workflow_provenance.py       # Workflow provenance functions
    ├── resolve_identities.py            # One-time artist meta-information retrieval script
    ├── schema
    │   ├── artist_daily.py              # Database structure for daily info
    │   └── artist_info.py               # Database structure for artist meta-info
    ├── scripts
    │   └── ...                          # Testing scripts
    └── utils.py
```


- **adapters/** – Each file connects to one data source (Spotify, YouTube, etc.), handles authentication, and fetches raw data.  
- **normalize/** – Each file transforms raw API responses into a consistent schema shared across all platforms.  
- **schema/** – Contains Pydantic models defining the structure of the tables (artist reference, weekly data, KPIs).  
- **db/** – Responsible for saving or exporting processed data, either to a database or to local files.  
- **configs/tracked_artists.json** – Central configuration listing all artists being tracked, with their local IDs and the identifiers used by each platform.  
- **cli.py** – Entry point for running the full workflow: it loads the artist list, fetches data from each adapter, normalizes it, computes KPIs, and saves the results.  

This structure keeps the code modular and scalable, each platform has its own adapter and normalizer, while the shared schema and database logic make it easy to extend and maintain.

---

## Setup and Usage

### 1. Clone the repository
```
git clone https://github.com/adrien-gtd/artist-data-acquisition.git
cd artist-data-acquisition
```
### 2. Create and activate a Python virtual environment

macOS / Linux
```
python3 -m venv .venv
source .venv/bin/activate
```
Windows
```
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install dependencies
```
pip install -r requirements.txt
```
### 4. Set up environment variables

Copy the example file and fill in your API credentials:
```
cp .env.example .env
```
Then edit .env with your actual keys, for example:
```
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
YOUTUBE_API_KEY=your_youtube_api_key
```

### 5. Setup the static data

Run the static data setup script

```
python -m src/resolve_identities
```
This will fill the artist_info table that the daily runs will be based on.

### 6. Run the data collection

The daily gather job can be run using:
```
python -m src.cli
```
---

## Summary

- Each adapter handles fetching data from one API.  
- Each normalize module converts raw data into the unified schema in schema/.  
- The db folder manages writing processed results to files or a database.  
- The cli.py file runs the complete extraction and normalization workflow.  

This structure keeps the pipeline modular and easy to extend when adding new APIs or data sources.

## Miscelaneous

### Changing the tracked artists config

Our tracked_artists config file is obtained by scraping from a single spotify url
according to spotify's recomended artists (`fans also liked`).

To fetch a new artist list, you can run the scrapper by running the following:
```
cd spotifly
./scrap_artists.sh <max_num_artists> <min_artist_listeners> <max_artist_listeners>
```
