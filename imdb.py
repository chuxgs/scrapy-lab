import scrapy
import regex as re

class ImdbSpider(scrapy.Spider):
    name = "imdb"
    allowed_domains = ["www.imdb.com"]
    start_urls = ["https://www.imdb.com/title/tt0096463/fullcredits/"]

    def parse(self, response):
        # Hardcoded for demonstration; these can be dynamically extracted if needed
        # movie_id = "tt0096463"
        # movie_name = "Working Girl"
        # movie_year = 1988

        # Extract movie_id from the meta tag
        movie_id = response.xpath('//meta[@property="pageId"]/@content').extract_first()

        # Extract movie name and year from the og:title content
        og_title_content = response.xpath('//meta[@property="og:title"]/@content').extract_first()
        # Regular expression to extract name and year from the og:title format
        title_year_match = re.search(r'(.*) \((\d{4})\)', og_title_content)
        if title_year_match:
            movie_name = title_year_match.group(1)
            movie_year = int(title_year_match.group(2))
        else:
            movie_name, movie_year = 'Unknown', 'Unknown'

        # Iterate over each row in the actors' table
        for actor_row in response.css('table.cast_list tr'):
            # Extract the actor's IMDb ID and name from the 'href' attribute and the 'alt' attribute of the image respectively
            href = actor_row.css('.primary_photo a::attr(href)').extract_first()
            actor_name = actor_row.css('.primary_photo img::attr(alt)').extract_first()

            # Extract the character name
            character = actor_row.css('.character a::text').extract_first()
            if not character:  # If character name is not within an 'a' tag
                character = actor_row.css('.character::text').extract_first()

            if href and actor_name and character:
                actor_id = href.split('/')[2]
                yield {
                    "movie_id": movie_id,
                    "movie_name": movie_name,
                    "movie_year": movie_year,
                    "actor_name": actor_name.strip(),
                    "actor_id": actor_id,
                    "role_name": character.strip()
                }