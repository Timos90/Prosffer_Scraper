from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import SupermarketScraperItem
from scrapy.loader import ItemLoader
from itemloaders.processors import Identity


class NettoSpider(CrawlSpider):
    name = "netto"
    allowed_domains = ["netto-online.de"]
    start_urls = ['https://www.netto-online.de/lebensmittel/c-N01',
                  'https://www.netto-online.de/drogerie/c-N02']


    rules = (
        Rule(LinkExtractor(allow=(r'c-N01/\d+', r'c-N02/\d+',))),

        Rule(LinkExtractor(allow=(r'Content-Kachel\+Spirituosen',r'/p-\d+$',)), callback='parse_item'),
    )

    def parse_item(self, response):
        l = ItemLoader(item=SupermarketScraperItem(), response=response)

        store = 'Netto'
        l.add_value('store', store)
        # Load name
        l.add_css("name", "h1.tc-pdp-productname::text")

        # Extract and combine integer and fractional part of the price
        integer_part = response.css('ins.product__current-price > span.prices__ins__inner > span.prices__ins--digits-before-comma::text').get().strip()
        fractional_part = response.css('ins.product__current-price > span.prices__ins__inner > span.prices__ins--digits-before-comma > span.product__current-price--digits-after-comma::text').get()
        if fractional_part:
        # Ensure we don't add an extra dot
            if integer_part.endswith('.'):
                price = f"{integer_part}{fractional_part.strip()}"
            else:
                price = f"{integer_part}.{fractional_part.strip()}"
        else:
            price = integer_part
        price = price.rstrip('–').strip()

        # If price ends with a dot and has no fractional part, remove the dot
        if price.endswith('.'):
            price = price.rstrip('.')
        try:
            price = float(price)
        except ValueError:
            price = None

        l.add_value("price", price)

        currency = "€"
        l.add_value("currency", currency)

        l.get_output_processor('category').default_output_processor = Identity()
        category = response.css("li.breadcrumb__item span[itemprop='name']::text").getall()
        l.add_value("category", category)

        description = response.xpath('//div[@class="detail-page-section__description-wrapper__description"]/div[@class="editContent tc-product-description"]/p[2]/text()').get()

        if description:
            # Clean up the description text
            description = description.strip()

            # Ensure description is no longer than 150 characters, ending at the last period
            if len(description) > 150:
                truncated = description[:150]  # Cut to the first 150 characters
                last_period = truncated.rfind('.')  # Find the last period before 150 chars
                if last_period != -1:
                    description_text = truncated[:last_period + 1]  # Include the period
                else:
                    description_text = truncated  # If no period, just truncate
            else:
                description_text = description  # Use the entire description if it's less than 150 chars

            l.add_value('description', description_text)

        else:
            # Handle missing description, check the first <p> tag
            description = response.xpath('//div[@class="detail-page-section__description-wrapper__description"]/div[@class="editContent tc-product-description"]/p[1]/text()').get()

            if description:
                description = description.strip()

                if len(description) > 150:
                    truncated = description[:150]
                    last_period = truncated.rfind('.')
                    if last_period != -1:
                        description_text = truncated[:last_period + 1]
                    else:
                        description_text = truncated
                else:
                    description_text = description

                l.add_value('description', description_text)
            else:
                l.add_value('description', 'No description available.')


        l.add_css("image", "img.productImage::attr(src)")

        l.add_value("link", response.url)

        return l.load_item()
