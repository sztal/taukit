"""Custom scrapy middleware classes."""
# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

# pylint: disable=W0613

# from scrapy import signals
from scrapy.exceptions import IgnoreRequest
from .utils import is_url_in_domains


class OffsiteFinalUrlDownloaderMiddleware:
    """Downloader middleware for filtering bad final urls.

    The aim of this downloader middleware is to recognize and drop responses
    of which final urls are not within domains allowed by a spider.

    This allows for omitting offsite redirects etc. that in general
    are of no use since they have different HTML markup and can not
    be scraped correctly.

    This middleware additionaly uses optional `blacklist_urls` attribute
    to filter out unwanted urls (based on fixed string and/or regexps).
    """
    def process_response(self, request, response, spider):
        """Process response hook."""
        allowed_domains = getattr(spider, 'allowed_domains', None)
        blacklist_urls = getattr(spider, 'blacklist_urls', [])
        url = response.url
        # Offsite check
        if not is_url_in_domains(url, allowed_domains):
            raise IgnoreRequest(request)
        # Blacklist check
        for bad_url in blacklist_urls:
            if (isinstance(bad_url, str) and url == bad_url) \
            or bad_url.search(url):
                raise IgnoreRequest(request)
        return response
