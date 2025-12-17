MAX_ARTISTS=${1:-2}
MIN_ARTIST_LISTENERS=${2:-0}
OUTPUT_FILE=../src/configs/tracked_artists.json


source ../env_data/bin/activate
mv $OUTPUT_FILE backup.json
scrapy crawl spotifly -a max_artists=$MAX_ARTISTS -a min_artist_listeners=$MIN_ARTIST_LISTENERS -o $OUTPUT_FILE