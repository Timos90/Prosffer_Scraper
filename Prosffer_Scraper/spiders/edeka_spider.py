from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import SupermarketScraperItem
from scrapy.loader import ItemLoader
from itemloaders.processors import Identity
import html


class EdekaSpider(CrawlSpider):
    name = "edeka"
    allowed_domains = ["edeka24.de"]
    start_urls = ['https://www.edeka24.de/']  # Start from the homepage

    # Set to store scraped product slugs to avoid duplicates
    scraped_products = set()

    # Define rules for following subcategory and sub-subcategory pages, and scraping product pages
    rules = (
        # Rule 1: Follow subcategory and sub-subcategory pages (allow only URLs with more than one segment after the domain)
        Rule(LinkExtractor(
                allow=(r'/[A-Za-z0-9-]+/[A-Za-z0-9-]+/?$',  # Match subcategories
                       r'/[A-Za-z0-9-]+/[A-Za-z0-9-]+/[A-Za-z0-9-]+/?$')),  # Match sub-subcategories
            follow=True, callback='log_category'),

        # Rule 2: Scrape product pages (URLs ending with .html)
        Rule(LinkExtractor(
                allow=(r'/[A-Za-z0-9-]+/[A-Za-z0-9-]+/[A-Za-z0-9-]+\.html$',  # Match product pages under subcategory
                       r'/[A-Za-z0-9-]+/[A-Za-z0-9-]+\.html$')),  # Match product pages directly under category
            callback='parse_item', follow=False),
    )

    def log_category(self, response):
        """Logs category, subcategory, or sub-subcategory URLs that are being followed"""
        self.logger.info(f"Following category/subcategory URL: {response.url}")

    def parse_item(self, response):
        """Scrapes the product detail page"""

        # Extract the product slug from the URL
        product_slug = response.url.split('/')[-1]
        
        # Check if the product slug has already been scraped
        if product_slug in self.scraped_products:
            self.logger.info(f"Skipping duplicate product: {product_slug}")
            return
        else:
            self.logger.info(f"Scraping new product: {product_slug}")
            self.scraped_products.add(product_slug)

        # Now scrape the product details
        l = ItemLoader(item=SupermarketScraperItem(), response=response)

        store = 'Edeka'
        l.add_value('store', store)
        # Extract product name
        name = response.css('h1::text').get()
        if name:
            l.add_value('name', name.strip())
        else:
            self.logger.warning(f"Product name not found on {response.url}")

        # Extract price (with error handling for missing price)
        price_str = response.css('div.price::text').get()

        if price_str:
            price_clean = price_str.replace('€', '').replace(',', '.').strip()
            try:
                price_float = float(price_clean)
            except ValueError:
                price_float = 0.0  # In case the price string is invalid
        else:
            price_float = 0.0  # Default price if not found

        l.add_value('price', price_float)


        # Extract currency
        currency = ''.join([char for char in price_str if not char.isdigit() and char != ',' and char != '.']).strip()
        l.add_value('currency', currency)

        l.get_output_processor('category').default_output_processor = Identity()
        # Extract category/subcategory (skip the first breadcrumb element)
        category = response.css('div.breadcrumb ul li a::attr(title)').getall()[1:]
        category = [html.unescape(cat) for cat in category]
        if category:
            l.add_value("category", category)
        else:
            self.logger.warning(f"Category not found on {response.url}")

        description = response.css('div#description .listing::text').getall()
    
        if description:
            # Concatenate all description parts
            description_text = ' '.join([desc.strip() for desc in description if desc.strip()])

            # Ensure description is no longer than 150 characters, ending at the last period
            if len(description_text) > 150:
                truncated = description_text[:150]  # Cut to the first 150 characters
                last_period = truncated.rfind('.')  # Find the last period before 150 chars
                if last_period != -1:
                    description_text = truncated[:last_period + 1]  # Include the period
                else:
                    description_text = truncated  # If no period, just truncate

            l.add_value('description', description_text.strip())
        else:
            # Handle missing description
            l.add_value('description', 'No description available.')

        # Extract image URL
        image_url = response.css('div.detail-image img::attr(src)').get()
        if image_url:
            l.add_value('image', image_url)
        else:
            self.logger.warning(f"Image URL not found on {response.url}")

        # Add the URL for reference
        l.add_value("link", response.url)

        # Return the scraped item
        return l.load_item()
