"""Unit tests configuration for the webscraping submodule."""
import pytest
from scrapy.crawler import CrawlerProcess


@pytest.fixture(scope='function')
def spider_process():
    return CrawlerProcess()
