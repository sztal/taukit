"""Web and webscraping related utilities.

This module also contain processors that for sake of avoiding circular imports
can not be placed in `misc.processors`.
"""
import re
from scrapy.http import HtmlResponse
from w3lib.html import remove_tags, remove_comments, strip_html5_whitespace
from w3lib.html import replace_entities, replace_escape_chars, replace_tags
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

def normalize_web_content(x, keep=('h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong'),
                          token='____SECTION____'):
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
        x = remove_tags(x, keep=keep)
        if token:
            x = replace_tags(x, token=token)
        x = replace_entities(x)
        x = replace_escape_chars(x)
    except (TypeError, AttributeError):
        pass
    for part in _rx_web_sectionize.split(x):
        if part:
            yield part

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

def sectionize(parts, first_is_heading=False):
    """Join parts of the text after splitting into sections with headings.

    This function assumes that a text was splitted at section headings,
    so every two list elements after the first one is a heading-section pair.
    This assumption is used to join sections with their corresponding headings.

    Parameters
    ----------
    parts : list of str
        List of text parts.
    first_is_heading : bool
        Should first element be treated as heading in lists of length greater than 1.
    """
    parts = parts.copy()
    if len(parts) <= 1:
        return parts
    first = []
    if not first_is_heading:
        first.append(parts[0])
        del parts[0]
    sections = first + [ "\n".join(parts[i:i+2]) for i in range(0, len(parts), 2) ]
    return sections

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
