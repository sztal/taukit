"""Generic spider classes."""
from logging import getLogger
from scrapy import Request
from w3lib.url import canonicalize_url
from .utils import is_url_in_domains


class TauSpiderMixin:
    """Generic Taukit spider mixin class.

    It may be used to define both standard and crawl spiders.

    Attributes
    ----------
    limit : int or None
        Limit for number of requests being made.
    allowed_domains : list of str
        This is a standard :py:module:`scrapy` spider class attribute,
        but it is documented here as it has an additional meaning:
        the provided domains are used to filter out also starting responses
        (those generated from `start_urls`) if their final URLs
        (after redirects etc.) are not in the specified domains.
        Used in the
        :py:class:`taukit.webscraping.middlewares.OffsiteFinalUrlDownloaderMiddleware`.
    blacklist_urls : list of str or SRE_Pattern or None
        List of urls or compiled regexps for filtering unwanted urls.
        Used in the
        :py:class:`taukit.webscraping.middlewares.OffsiteFinalUrlDownloaderMiddleware`.
    start_urls_allowed_domains : list of str
        List of accepted domains used for filtering the `start_urls`.
        This is useful when one want to give a lot of start URLs and make
        the spider care for selecting only the proper ones.
    rules : list or tuple
        Documented in :py:class:`scrapy.spiders.CrawlSpider`.
    item_loader : ItemLoader
        Main item loader class.
    """
    name = None
    item_loader = None
    limit = None

    _logger = None

    rules = ()

    # Spider-level scrapy settings
    custom_settings = {}

    # -------------------------------------------------------------------------

    @property
    def logger(self):
        "Logger getter."""
        if not self._logger:
            self._logger = getLogger('scrapy.spider.'+self.name)
        return self._logger

    def parse_extra_args(self):
        """Parse additional arguments."""
        if self.limit is not None:
            self.limit = int(self.limit)

    def get_start_urls(self):
        """Get start urls."""
        if not self.start_urls:
            raise NotImplementedError
        yield from self.start_urls

    def start_requests(self):
        """Generate start requests."""
        self.parse_extra_args()
        urls = self.get_start_urls()
        n = 0
        for url in urls:
            start_urls_allowed_domains = getattr(self, 'start_urls_allowed_domains', None)
            if not start_urls_allowed_domains:
                start_urls_allowed_domains = ()
            if not is_url_in_domains(url, start_urls_allowed_domains):
                continue
            data = {'url': url}
            n += 1
            if self.limit and n > self.limit:
                break
            request = self.make_request(url, meta={ 'data': data })
            yield request

    def make_request(self, url, **kwds):
        """Make a request object."""
        return Request(url, **kwds)

    def parse_item(self, response, item_loader=None):
        """Default item parsing method."""
        if not getattr(self, 'item_loader', None):
            cn = self.__class__.__name__
            raise AttributeError(f"'{cn}' must define 'item_loader' class attribute")
        data = response.meta.get('data', {})
        data['final_url'] = canonicalize_url(response.url)
        if not data.get('url'):
            data['url'] = data['final_url']
        item_loader = item_loader if item_loader else self.item_loader
        loader = self.item_loader(response=response) # pylint: disable=not-callable
        loader.add_data(data)
        loader.setup()
        item = loader.load_item()
        return item
