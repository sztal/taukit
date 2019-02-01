"""Web and webscraping related utilities.

This module also contain processors that for sake of avoiding circular imports
can not be placed in `misc.processors`.
"""
import re
from scrapy.http import HtmlResponse
from w3lib.html import remove_tags, remove_comments, strip_html5_whitespace
from w3lib.html import replace_entities, replace_escape_chars
import tldextract as tld

_rx_web_sectionize = re.compile(r"\n|\s\s+|\t")


def get_url_domain(url):
    """Get domain from an URL.

    Parameters
    ----------
    url : str
        URL.
    """
    return '.'.join([ p for p in tld.extract(url)[-2:] if p ])

def is_url_in_domains(url, domains):
    """Check if URL is in domain(s).

    Parameters
    ----------
    url : str
        URL.
    domains : str or iterable of str
        Domains to be checked.
    """
    if not domains:
        return True
    if isinstance(domains, str):
        domains = [ domains ]
    return get_url_domain(url) in domains

def normalize_web_content(x, keep_tags=(), replace=()):
    """Normalize web content.

    Parameters
    ----------
    keep : tuple
        HTML tags to keep.
    token : str or None
        Token to use for replacing kep HTML tags.
        Do not replace if `None`.
    """
    try:
        x = strip_html5_whitespace(x)
        x = remove_comments(x)
        x = remove_tags(x, keep=keep_tags)
        x = replace_entities(x)
        x = replace_escape_chars(x)
    except (TypeError, AttributeError):
        pass
    for old, new in replace:
        x = x.replace(old, new)
    for part in _rx_web_sectionize.split(x):
        if part:
            yield part.strip()

def load_item(body, item_loader, item=None, url='placeholder_url',
              callback=None, encoding='utf-8'):
    """Load item from HTML string.

    Parameters
    ----------
    body : str
        String with valid HTML markup.
    item_loader : BaseItemLoader
        Item loader class sublassing the `BaseItemLoader` defined in `items.py`.
    item : scrapy.Item
        Optional item class to be used instead of the `item_loader` default.
    url : str
        Optional url to pass to the response.
        For most of cases it should be left as is.
    callback : func
        Optional callback function to perform on item loader after setup.
        Callback should not return any value,
        but only modify the state of the loader.
        This meant mostly to use additional setup methods
        defined on a given item loader class.
    encoding : str
        Response encoding. Defaults to UTF-8.

    Returns
    -------
    scrapy.Item
        Item object populated with data extracted from an HTML markup.
    """
    response = HtmlResponse(url=url, body=body, encoding=encoding)
    if item:
        loader = item_loader(item=item(), response=response)
    else:
        loader = item_loader(response=response)
    loader.setup()
    if callback:
        callback(loader)
    item = loader.load_item()
    return item

def strip(x):
    """Strip a string.

    Parameters
    ----------
    x : any
        A str object which is to be stripped. Anything else is returned as is.
    """
    if isinstance(x, str):
        return x.strip()
    return x

def split(x, divider):
    """Split a string.

    Parameters
    ----------
    x : any
        A str object to be split. Anything else is returned as is.
    divider : str
        Divider string.
    """
    if isinstance(x, str):
        return x.split(divider)
    return x

def lower(x):
    """Lower a string."""
    if isinstance(x, str):
        return x.lower()
    return x

def upper(x):
    """Upper a string."""
    if isinstance(x, str):
        return x.upper()
    return x
