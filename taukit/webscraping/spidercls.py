"""Generic spider classes."""
from logging import getLogger
from scrapy import Request
from w3lib.url import canonicalize_url


class TauSpiderMixin:

    name = None
    allowed_domains = None
    start_urls = None
    item_loader = None

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

    def get_start_urls(self):
        """Get start urls."""
        if not self.start_urls:
            raise NotImplementedError
        yield from self.start_urls

    def start_requests(self):
        """Generate start requests."""
        self.parse_extra_args()
        if self.args.test_url:
            urls = [ (self.test_url, { 'url': self.test_url }) ]
        else:
            urls = self.get_urls()
        n = 0
        for url in urls:
            data = {'url': url}
            n += 1
            if self.args.limit and n > self.args.limit:
                break
            request = self.make_request(url, meta={ 'data': data })
            yield request

    def parse_extra_args(self):
        pass

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
        item_loader = item_loader if item_loader else self.item_loader
        loader = self.item_loader(response=response) # pylint: disable=not-callable
        loader.add_data(data)
        loader.setup()
        item = loader.load_item()
        return item
