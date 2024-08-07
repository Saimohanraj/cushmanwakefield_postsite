# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class CusmanwakefieldItem(scrapy.Item):
    property_url = scrapy.Field()
    title = scrapy.Field()
    category = scrapy.Field()
    address = scrapy.Field()
    investment_detail = scrapy.Field()
    contract_detail = scrapy.Field()
    building_class = scrapy.Field()
    available_space = scrapy.Field()
    rental_price = scrapy.Field()
    building_size = scrapy.Field()
    construction_status = scrapy.Field()
    rate_type = scrapy.Field()
    sublease = scrapy.Field()
    lot_size = scrapy.Field()
    year_built = scrapy.Field()
    max_contiguous = scrapy.Field()
    min_divisible = scrapy.Field()
    sale_price = scrapy.Field()
    price_per_unit = scrapy.Field()
    images = scrapy.Field()
    key_features = scrapy.Field()
    description = scrapy.Field()
    property_brochure = scrapy.Field()
    broker = scrapy.Field()
    hash = scrapy.Field()
    scraped_date = scrapy.Field()

