import scrapy
import unidecode
import re

cleanString = lambda x: '' if x is None else unidecode.unidecode(re.sub(r'\s+', ' ', x))

class NytimesSpider(scrapy.Spider):
    name = 'nytimes'
    allowed_domains = ['www.nytimes.com']
    start_urls = ['https://www.nytimes.com/']

    def parse(self, response):
        for section in response.css("section.story-wrapper.css-zirthl"):
            article_url = section.css("a::attr(href)").get()
            title = cleanString(section.css("p.indicate-hover::text").get())
            summary = cleanString(section.css("p.summary-class::text").get())
            yield {
                'title': title,
                'article_url': article_url,
                'summary': summary
            }
            next_page = article_url
            if next_page is not None:
                yield response.follow(next_page, callback=self.parse_article)

    def parse_article(self, response):
        yield {
            'appears_ulr': response.url,
            'title': cleanString(response.css("h1[data-testid='headline']::text").get()),
            'authors': cleanString(', '.join(response.css("span[itemprop='name']::text").getall())),
        }