"""Unit tests for spider classes."""
# pylint: disable=unsubscriptable-object
from scrapy import Field, Spider as _Spider
from scrapy.spiders import CrawlSpider as _CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from taukit.webscraping.itemcls import Item as _Item, ItemLoader as _ItemLoader
from taukit.webscraping.spidercls import TauSpiderMixin
from taukit.webscraping.selectors import CSS

class Item(_Item):
    content = Field()
    url = Field()
    final_url = Field()

class ItemLoader(_ItemLoader):
    default_item_class = Item
    container_sel = CSS(".footer-sidebar")
    content_sel = CSS(".footer-sidebar-text::text")

class Spider(_Spider, TauSpiderMixin):
    name = 'test_wiki_spider'
    item_loader = ItemLoader
    start_urls = ['https://www.wikipedia.org/']
    item = None
    def parse(self, response):
        item = self.parse_item(response)
        self.__class__.item = item

class CrawlSpider(_CrawlSpider, TauSpiderMixin):
    name = 'test_wiki_crawl_spider'
    item_loader = ItemLoader
    start_urls = ['https://www.wikipedia.org/']
    items = []
    rules = (
        Rule(
            LinkExtractor(allow=('wikimediafoundation.org', )),
            follow=False,
            callback='parse_link'
        ),
    )
    def parse(self, response):
        self.parse_link(response)
        return super().parse(response)
    def parse_link(self, response):
        item = self.parse_item(response)
        self.__class__.items.append(item)


def test_spider(spider_process):
    spider_process.crawl(Spider)
    spider_process.crawl(CrawlSpider)
    spider_process.start()
    # Assert Spider
    assert Spider.item['final_url'] == Spider.start_urls[0]
    assert Spider.item['content'].startswith("Wikipedia is hosted by")
    # Assert CrawlSpider
    assert CrawlSpider.items[0]['final_url'] == CrawlSpider.start_urls[0]
    assert CrawlSpider.items[0]['content'].startswith("Wikipedia is hosted by")
    assert CrawlSpider.items[1]['final_url'] == 'https://wikimediafoundation.org/'
