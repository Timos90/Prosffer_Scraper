from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import SupermarketScraperItem
from scrapy.loader import ItemLoader
from itemloaders.processors import Identity
import json


class AldiNordSpider(CrawlSpider):
    name = "aldi_nord"
    allowed_domains = ["aldi-nord.de"]
    start_urls = ['https://www.aldi-nord.de/sortiment.html']


    rules = (
        # Rule to follow category and subcategory pages
        Rule(LinkExtractor(allow=(r'/sortiment/.*\.html', r'/sortiment+/[A-Za-z0-9-]+\.html$' ,r'/sortiment/[A-Za-z0-9-]+/[A-Za-z0-9-]+\.html$',r'/sortiment/[A-Za-z0-9-]+/[A-Za-z0-9-]+/[A-Za-z0-9-]+\.html$')), follow=True),
        
        Rule(LinkExtractor(allow=(r'/produkt/[^/]+-\d+-0-0\.article\.html',r'/produkt/[^/]+-\d+-0-0\.article\.html#/.+/[^/]+$')), callback='parse_item'),

        Rule(LinkExtractor(allow=(r'/produkt/[^/]+-\d+-0-0\.article\.html#/.+/[^/]+/[^/]+$')), callback='parse_item'),
    )


    def parse_item(self, response):
        l = ItemLoader(item=SupermarketScraperItem(), response=response)

        store = 'Aldi Nord'
        l.add_value('store', store)

        name = ' '.join(response.css('div.mod-article-intro__header-headline h1::text').getall()).strip()
        l.add_value('name', name)

        price = response.css('div.price span.price__wrapper::text').get().strip()
        try:
            price = float(price)
        except ValueError:
            price = None

        l.add_value("price", price)

        currency = "€"
        l.add_value("currency", currency)

        l.get_output_processor('category').default_output_processor = Identity()
        category = response.css('ol.mod-breadcrumb__nav li.mod-breadcrumb__item span::text').getall()
        l.add_value("category", category)

        description = response.css('div.rte p::text').get()
        l.add_value('description', description)

        # Create a set to store unique image URLs
        image_urls = set()

        # Loop through all script tags and find the one with the ImageObject type
        for script in response.css('.mod-article-intro__gallery script::text').getall():
            if '"@type": "ImageObject"' in script:
                try:
                    # Parse the JSON-LD content
                    json_data = json.loads(script.strip())

                    # Extract the image URL from the 'contentUrl' field
                    image_url = json_data.get('contentUrl')

                    if image_url:
                        image_urls.add(image_url)  # Add only unique URLs
                except json.JSONDecodeError as e:
                    self.logger.error(f"Error parsing JSON: {e}")

        # Add the unique image URLs to the item
        for url in image_urls:
            l.add_value('image', url)

        l.add_value("link", response.url)

        # Return the loaded item
        return l.load_item()
