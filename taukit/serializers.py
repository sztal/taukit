"""Serializer and deserializer functions and classes."""
# pylint: disable=E0202
from datetime import datetime, date
from json import JSONEncoder as _JSONEncoder
from scrapy import Item
from cerberus import Validator
from cerberus.schema import DefinitionSchema


class JSONEncoder(_JSONEncoder):
    """JSON serializer handling :py:class:`datetime.datetime` objects.

    It also serializes :py:class:`scrapy.Item` and
    :py:class:`cerberus.schema.DefinitionSchema` instances.
    """
    def default(self, o):
        """Serializer method."""
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        if isinstance(o, (Item, DefinitionSchema)):
            return dict(o)
        if isinstance(o, Validator):
            return dict(o.schema)
        return super().default(o)


class UniversalJSONEncoder(JSONEncoder):
    """Universal JSON encoder.

    It tries to dump all non-serializable objects
    (other than those handled by :py:class:`taukit.serializers.JSONEncoder`)
    to their standard string representation.
    """
    def default(self, o):
        """Seralizer method."""
        try:
            return super().default(o)
        except TypeError:
            return str(o)
