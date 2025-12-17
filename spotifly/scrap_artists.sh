MAX_ARTISTS=${1:-2}
OUTPUT_FILE=../src/configs/tracked_artists.json


source ../env_data/bin/activate
mv $OUTPUT_FILE backup.json
scrapy crawl spotifly -a max_artists=$MAX_ARTISTS -o $OUTPUT_FILE