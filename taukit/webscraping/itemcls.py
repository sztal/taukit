"""Base item and item loader classes."""
import re
from scrapy import Item as _Item
from scrapy.loader import ItemLoader as _ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from cerberus import Validator
from .utils import normalize_web_content, strip
from .selectors import CSS, XPath

class ItemLoader(_ItemLoader):
    """Generic item loader class.

    It defines helper method used for initializing instances
    of specialized subclasses.
    """
    default_item_class = None
    default_input_processor = MapCompose(normalize_web_content, strip)
    default_output_processor = TakeFirst()

    # Selectors
    container_sel = None

    # Methods -----------------------------------------------------------------

    def __init__(self, *args, **kwds):
        """Initilization method."""
        super().__init__(*args, **kwds)
        self._container = None
        self.data = None

    @property
    def fields(self):
        """Fields getter."""
        if self.default_item_class is None:
            cn = self.__class__.__name__
            raise AttributeError(f"'{cn}' does not define 'default_item_class' attribute")
        return [ f for f in self.default_item_class.fields ]

    def assign_container_selector(self):
        """Assign container selector."""
        if self.container_sel is None:
            cn = self.__class__.__name__
            raise AttributeError(f"'{cn}' does not define 'container_sel' attribute")
        if isinstance(self.container_sel, XPath):
            self._container = self.nested_xpath(self.container_sel.selector)
        elif isinstance(self.container_sel, CSS):
            self._container = self.nested_css(self.container_sel.selector)
        else:
            raise ValueError("Unknown selector type: {}".format(
                self.container_sel.__class__.__name__
            ))

    def assign_selector(self, field_name):
        """Assign selector to a field.

        Parameters
        ----------
        field_name : str
            Field name.
        """
        rx = re.compile(field_name+r"_?\d*_sel")
        selectors = [ getattr(self, s) for s in dir(self) if rx.match(s) ]
        if not selectors:
            return
        for selector in selectors:
            if isinstance(selector, XPath):
                self._container.add_xpath(field_name, selector.selector)
            elif isinstance(selector, CSS):
                self._container.add_css(field_name, selector.selector)
            else:
                raise ValueError(f"Unknown selector type: {selector.__class__.__name__}")

    def setup(self, omit=()):
        """Setup loader selectors.

        Parameters
        ----------
        omit : list of str
            List of fields to omit.
        """
        self.assign_container_selector()
        for field in self.fields:
            if field in omit:
                continue
            self.assign_selector(field)

    def add_data(self, data):
        """Add response (meta)data.

        Parameters
        ----------
        data : dict-like
            Metadata obtained from the response-request.
        """
        itemcls = self.default_item_class
        data = itemcls.get_schema().normalized(data)
        self.data = data

    def load_item(self):
        """Load item."""
        item = super().load_item()
        if getattr(self, 'data', None):
            for k, v in self.data.items():
                item[k] = v
        return item


class Item(_Item):
    """Base item class."""
    _schema = {}

    @classmethod
    def get_schema(cls):
        """Schema getter."""
        if not isinstance(cls._schema, Validator):
            cls._schema = Validator({
                **cls.fields,
                **cls._schema
            }, allow_unknown=False, purge_unknown=True)
        return cls._schema
