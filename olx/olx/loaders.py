from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst

class OlxItemLoader(ItemLoader):
    default_output_processor = TakeFirst()