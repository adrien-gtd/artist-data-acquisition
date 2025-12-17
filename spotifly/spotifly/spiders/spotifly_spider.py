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
        self.max_artists = int(kwargs.get('max_artists', 2))
        self.min_artist_listeners = int(kwargs.get('min_artist_listeners', 0))
        self.max_artist_listeners = int(kwargs.get('max_artist_listeners', 10000000000))

        print(f"Max artists to scrape: {self.max_artists}")
        print(f"Min artist listeners: {self.min_artist_listeners}")
        print(f"Max artist listeners: {self.max_artist_listeners}")

    def parse(self, response):
        artist_name = response.css("title::text").re(r"(.+) \| *")[0]
        spotify_link = response.url
        artist_id = spotify_link.replace("https://open.spotify.com/artist/","")
        monthly_listeners = int(response.xpath("//div/text()[contains(., 'monthly listeners')]").re('(.*) monthly listeners')[0].replace(",",""))

        assert spotify_link not in self.visited_artists
        self.visited_artists.add(spotify_link)
        if self.counter >= self.max_artists:
            raise CloseSpider(reason='Max artists reached')
        else:
            if monthly_listeners >= self.min_artist_listeners and monthly_listeners <= self.max_artist_listeners:
                yield {
                'artist_name': artist_name,
                'artist_id': artist_id,
                'monthly_listeners': monthly_listeners
                }
                self.counter += 1


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
