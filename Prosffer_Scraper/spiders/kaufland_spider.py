from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import SupermarketScraperItem
from scrapy.loader import ItemLoader
from itemloaders.processors import Identity


class KauflandSpider(CrawlSpider):
    name = "kaufland"
    allowed_domains = ["kaufland.de"]
    start_urls = ["https://www.kaufland.de/lebensmittel/"]

    rules = (
        # Rule to follow pagination links like 'c-N01/1', 'c-N01/2', etc.
        Rule(LinkExtractor(allow=r'/[a-zA-Z-]+/$'), follow=True),
     

        # Rule to follow category URLs containing 'Content-Kachel+Spirituosen'
        Rule(LinkExtractor(allow=(r'https://www.kaufland.de/product/\d{9}/.*',)), callback='parse_item'),
    )

    def parse_item(self, response):
        l = ItemLoader(item=SupermarketScraperItem(), response=response)

        store = 'Kaufland'
        l.add_value('store', store)

        l.add_css("name","h1::attr(title)")

        whole_price = response.css("span.rd-price-information__price::text").get().strip()

        try:
            price = whole_price.replace('\xa0€','').replace(',','.')
            price = float(price)
        except ValueError:
            price = None
        l.add_value("price", price)

        currency = whole_price[-1]

        l.add_value("currency", currency)

        #category = response.css("nav div:last-child a.rd-link>span.rd-link__text::text").get()
        l.get_output_processor('category').default_output_processor = Identity()
        l.add_css("category", "nav.rd-breadcrumb div.rd-breadcrumb__item a.rd-link > span.rd-link__text::text")

        description = response.css('div.rd-product-description__top-accordion-content-description p::text').get()

        if description:
            # Clean and strip whitespace
            description = description.strip()

            # Truncate the text to 150 characters or stop at the last period before 150 characters
            if len(description) > 150:
                # Find the last period before 150 characters
                last_period = description[:150].rfind('.')
                if last_period != -1:
                    # Truncate at the last period
                    truncated_description = description[:last_period + 1]
                else:
                    # Truncate at 150 characters if no period is found
                    truncated_description = description[:150]
            else:
                # Use the entire text if it's under 150 characters
                truncated_description = description

            # Add the truncated text to the item
            l.add_value('description', truncated_description)
        else:
            # Handle cases where no description was found
            self.logger.warning(f"No description found for {response.url}")


        image_urls = response.css('picture.product-picture>img::attr(src)').get()
        l.add_value('image', image_urls)

        l.add_value("link", response.url)

        return l.load_item()


