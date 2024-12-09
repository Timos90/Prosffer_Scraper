import scrapy
from itemloaders.processors import Identity, MapCompose, TakeFirst
import scrapy.item
from w3lib.html import remove_tags

class SupermarketScraperItem(scrapy.Item):
    # Name field: Strip HTML tags and take the first result
    name = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip),
        output_processor=TakeFirst(),
    )

    # Category field: Strip HTML tags and take the first result
    category = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip),
    )

    # Description field: Strip HTML tags, join multiple paragraphs with a space
    description = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip),
        output_processor=TakeFirst(),
    )

    # Image field: This field should directly return the image URL
    image = scrapy.Field(
        output_processor=TakeFirst(),
    )

    # Price field: You will need to extract, clean, and format the price
    price = scrapy.Field(
        output_processor=TakeFirst(),
    )

    currency = scrapy.Field(
        output_processor=TakeFirst(),
    )

    link = scrapy.Field(
        output_processor=TakeFirst(),
    )

    store = scrapy.Field(
        output_processor=TakeFirst(),
    )

    id_tag = scrapy.Field(
        output_processor=TakeFirst(),
    )