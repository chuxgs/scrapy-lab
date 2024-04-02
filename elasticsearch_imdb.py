from elasticsearch import Elasticsearch
import scrapy
import regex as re
import uuid
from scrapy.spiders import CrawlSpider


client = Elasticsearch(
    hosts=['https://8cec670044244e6db2cfe55a6ff6910c.eu-west-1.aws.found.io:443'],
    basic_auth=('elastic', 'UMKs4Jj9UQJ2EqtQl9NdjVi0'),
)

# API key should have cluster monitor rights
print(client.info())


class ImdbCrawlSpider(CrawlSpider):
    name = "imdb_new"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/title/tt0096463/fullcredits/"]
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'CLOSESPIDER_TIMEOUT': 3600  # Limits the spider to run for 300 seconds (5 minutes)
    }
    processed_movies = set()

    def parse_start_url(self, response):
        depth = response.meta.get('depth', 0)  # Get the current depth, defaulting to 0 if not set
        movie_id = response.xpath('//meta[@property="pageId"]/@content').extract_first()
        if movie_id not in self.processed_movies:
            self.processed_movies.add(movie_id)
            og_title_content = response.xpath('//meta[@property="og:title"]/@content').extract_first()
            title_year_match = re.search(r'(.*) \((\d{4})\)', og_title_content)
            if title_year_match:
                movie_name = title_year_match.group(1)
                movie_year = int(title_year_match.group(2))
                if movie_year < 1989 and movie_year >= 1980:

                    for actor_row in response.css('table.cast_list tr'):
                        href = actor_row.css('.primary_photo a::attr(href)').extract_first()
                        actor_name = actor_row.css('.primary_photo img::attr(alt)').extract_first()
                        character = actor_row.css('.character a::text').extract_first()
                        if not character:
                            character = actor_row.css('.character::text').extract_first()

                        if href and actor_name and character:
                            actor_id = href.split('/')[2]
                            # Build the bio URL
                            bio_url = f'https://www.imdb.com/name/{actor_id}/bio/'
                            actor_page_url = f'https://www.imdb.com/name/{actor_id}/'

                            # Pass the collected info so far to the next request
                            yield scrapy.Request(actor_page_url, callback=self.parse_actor_main_page, meta={
                                "movie_id": movie_id,
                                "movie_name": movie_name,
                                "movie_year": movie_year,
                                "actor_name": actor_name.strip(),
                                "actor_id": actor_id,
                                "role_name": character.strip(),
                                "depth": depth
                            })

    def parse_actor_main_page(self, response):
        height_selector = response.xpath(
            "//li[@data-testid='nm_pd_he']//span[@class='ipc-metadata-list-item__list-content-item']/text()")
        height = height_selector.get()
        # Regular expression to match the height in meters
        pattern = r"(\d+\.\d+)\s*m"

        # Using re.search to find the first occurrence that matches the pattern
        match = re.search(pattern, height) if height is not None else False

        if match:
            height_meters = match.group(1)  # Extract the matching group which contains the height in meters

            response.meta.update({
                "actor_height": height_meters,
            })

        depth = response.meta.get('depth', 0)  # Get the current depth, defaulting to 0 if not set
        max_depth = 30  # Set a maximum depth limit to prevent deep recursion
        for key in ["download_timeout", "download_slot", "download_latency"]:
            if key in response.meta.keys():
                response.meta.pop(key, None)

        # YIELD
        random_uuid = uuid.uuid4()
        client.index(index='imdb',
                     id = random_uuid,
                     body = response.meta)

        if depth < max_depth:
            for i, movie_row in enumerate(response.css('a.ipc-metadata-list-summary-item__t')):
                #if i < 2:  # Process only the first two movies found on the actor's page
                    movie_id = movie_row.css('a::attr(href)').extract_first().split('/')[2]
                    if movie_id not in self.processed_movies:
                        movie_url = f'https://www.imdb.com/title/{movie_id}/fullcredits/'
                        # Increment the depth and pass it to the next request
                        # print(response.meta)
                        yield scrapy.Request(movie_url, callback=self.parse_start_url, meta={'depth': depth + 1})

        # Extract movie IDs
        # movie_ids = response.css('div.filmo-row::attr(data-tconst)').extract()

        # Next, visit the bio page to get the actor's height
        # bio_url = f'{response.url}bio/'
        # return scrapy.Request(bio_url, callback=self.parse_actor_bio, meta=response.meta,
        # cb_kwargs={'movie_ids': movie_ids})

    def return_data(self, response, movie_id):
        bio_url = f'{response.url}bio/'
        return scrapy.Request(bio_url, callback=self.parse_actor_bio, meta=response.meta,
                              cb_kwargs={'movie_id': movie_id})

    def parse_actor_bio(self, response, movie_id):
        movie_info = response.meta

        # Extract the actor's height using regex
        body_as_unicode = response.body.decode('utf-8')
        height_match = re.search(r'<h4 class="inline">Height:</h4>\n([0-9\' ]+)&#34; ([0-9. m()]+)',
                                 body_as_unicode)
        height = f"{height_match.group(1)}\" {height_match.group(2)}" if height_match else 'Unknown'

        # Compile all info and yield it
        movie_info.update({
            "actor_height": height,
            "movie_id": movie_id
        })

        yield movie_info







