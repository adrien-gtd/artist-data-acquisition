from pathlib import Path
import scrapy
import re
import bs4
from scrapy.exceptions import CloseSpider

#response.xpath("//div/h2[text()='Fans also like']/parent::div/following-sibling::div/child::div/child::a")/@href

class SpotiflySpider(scrapy.Spider):
    name = "spotifly"
    allowed_domains = ["open.spotify.com"]
    start_urls = [
        "https://open.spotify.com/artist/1Xyo4u8uXC1ZmMpatF05PJ"
    ]
    visited_artists = set()


    def __init__(self, *args, **kwargs):
        super(SpotiflySpider, self).__init__(*args, **kwargs)
        self.visited_artists = set()
        self.counter = 0
        self.max_artists = 2
    
    def parse(self, response):
        artist_name = response.css("title::text").re(r"(.+) \| *")[0]
        spotify_link = response.url

        assert spotify_link not in self.visited_artists
        yield {
            'artist_name': artist_name,
            'spotify_link': spotify_link
        }
        self.visited_artists.add(spotify_link)
        self.counter += 1
        print(self.counter)
        print(self.max_artists)
        if self.counter >= self.max_artists:
            raise CloseSpider(reason='Max artists reached')

        artist_links = response.xpath("//div/h2[text()='Fans also like']/parent::div/following-sibling::div/child::div/child::a/@href").getall()
        #artist_names = response.xpath("//div/h2[text()='Fans also like']/parent::div/following-sibling::div/child::div/child::a/child::span/text()").getall() 
        #artists_entries = zip(artist_names, artist_links)

        for artist_url in artist_links:
            artist_url = response.urljoin(artist_url)
            if artist_url not in self.visited_artists:
                yield scrapy.Request(
                    artist_url,
                    callback=self.parse
                )

        #soup = bs4.BeautifulSoup(response.text, 'html.parser')
        #Path(filename).write_text(soup.prettify())
        #self.log(f"Parsed file {filename}")

    def clean_number(self, num):
        return int(re.sub(",","",num))

    def get_url_extension(self,url):
        return url.split("/")[-1]
