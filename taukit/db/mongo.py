"""MongoDB models and model mixins.

The mixins are supposed to enhance standard *MongoEngine* models
with various helper methods for importing, exporting and validating data.
"""
from collections import defaultdict
from collections.abc import Sequence
from mongoengine import Document as _Document
from mongoengine import EmbeddedDocument as _EmbeddedDocument
from mongoengine import StringField, URLField, EmailField
from mongoengine import BooleanField, IntField, FloatField, DateTimeField
from mongoengine import DictField, ListField
from mongoengine import EmbeddedDocumentField, EmbeddedDocumentListField
from taukit.utils import parse_date, parse_bool
from taukit.storage import MongoStorage, JSONLinesStorage


def obj_to_query(obj, name):
    if not isinstance(obj, dict):
        return { name: obj }
    query = {}
    for k, v in obj.items():
        query.update(**obj_to_query(v, name+'.'+k))
    return query


BASESCHEMA = {
    StringField: {
        'type': 'string',
        'coerce': str
    },
    URLField: { 'type': 'string' },
    EmailField: { 'type': 'string' },
    BooleanField: {
        'type': 'boolean',
        'coerce': parse_bool
    },
    IntField: {
        'type': 'integer',
        'coerce': int
    },
    FloatField: {
        'type': 'float',
        'coerce': float
    },
    DateTimeField: {
        'type': 'datetime',
        'coerce': parse_date
    },
    ListField: {
        'type': 'list',
        'coerce': list
    },
    DictField: {
        'type': 'dict',
        'coerce': dict
    },
    EmbeddedDocumentField: {},
    EmbeddedDocumentListField: {}
}


class DocumentMixin:
    """Abstract document class mixin providing helper methods."""

    _field_names_map = {}
    _ignore_fields = ()
    _baseschema = BASESCHEMA
    _schema = None
    _schema_allow_unknown = False
    _schema_purge_unknown = True

    # Class methods and properties --------------------------------------------

    @classmethod
    def _get_fields_defs(cls, *args, ignore_fields=True):
        """Get fields definitions."""
        fields = cls._fields
        if ignore_fields:
            fields = {
                k: v for k, v in fields.items()
                if k not in [ *cls._ignore_fields, *args ]
            }
        return fields

    @classmethod
    def _extract_field_schema(cls, field_name, field):
        """Extract field schema from *Mongoengine* field object."""
        if isinstance(field, _EmbeddedDocument):
            yield field.get_schema()
        field_type = field.__class__
        _schema = cls._baseschema.get(field_type, {}).copy()
        _schema.update(getattr(field, 'schema', {}))
        rename = cls._field_names_map.get(field_name, [])
        if rename and isinstance(rename, str):
            rename = [ rename ]
        if rename:
            for r in rename:
                yield { r: { 'rename': field_name } }
        yield { field_name: _schema }

    @classmethod
    def get_schema(cls, *args, **kwds):
        """Initialize (if needed) and get the schema object.

        Parameters
        ----------
        *args :
            Positional arguments passed to `_get_fields_defs` method.
        **kwds :
            Keyword arguments passed to `_get_fields_defs` method.
        """
        if not cls._schema:
            fields = cls._get_fields_defs(*args, **kwds)
            schema = defaultdict(lambda: {})
            for k, v in fields.items():
                field_schema = cls._extract_field_schema(k, v)
                for fs in field_schema:
                    schema.update(fs)
            cls._schema = schema
        return cls._schema

    @classmethod
    def from_dict(cls, dct, *args, only_dict=False, **kwds):
        """Dict-based class constructor skeleton method.

        Parameters
        ----------
        dct : dict-like
            Record.
        only_dict : bool
            Should only dictionary instead of
            the document class instance be returned.
        *args :
            Positional arguments passed to `get_schema`.
        **kwds :
            Keyword arguments passed to `get_schema`.
        """
        schema = cls.get_schema(*args, **kwds)
        doc = {}
        for k, v in dct.items():
            if k not in schema:
                if not cls._schema_allow_unknown and not cls._schema_purge_unknown:
                    raise ValueError(f"Field '{k}' is not allowed")
                elif cls._schema_purge_unknown:
                    continue
            s = schema[k]
            if 'rename' in s:
                k = s['rename']
                s = schema[k]
            if 'coerce' in s:
                v = s['coerce'](v)
            doc[k] = v
        if only_dict:
            return doc
        return cls(**doc)

    def to_dict(self, ignore_fields=True, additional_ignore=(), only=(),
                remove_empty_fields=False):
        """Dump document object to a dict.

        Parameters
        ----------
        ignore_fields : bool
            Should fields be ignored.
            Ignored fields are defined in `_ignore_fields` class attribute
            and may be extended with `additional_ignore`.
        only : list of str
            Return only selected fields.
        remove_empty_fields : bool
            Should empty fields (``None``) be removed.
        """
        def dump(value):
            """Dump field value."""
            if isinstance(value, _EmbeddedDocument):
                return value.to_dict()
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                return [ dump(x) for x in value ]
            return value

        if only:
            fields = only
        else:
            ignore = getattr(self, '_ignore_fields', []) if ignore_fields else []
            ignore = [ *ignore, *additional_ignore ]
            fields = [ f for f in self._fields if f not in ignore ]
        dct = { f: dump(getattr(self, f)) for f in fields }
        if remove_empty_fields:
            dct = { k: v for k, v in dct.items() if v is not None }
        return dct

    @classmethod
    def store(cls, **kwds):
        """Get `MongoStorage` initialized on the collection.

        Parameters
        ----------
        **kwds :
            Keyword arguments passed to the
            :py:class:`taukit.storage.MongoStorage`.
        """
        return MongoStorage(**{
            'model': cls,
            'updater': getattr(cls, 'updater', None),
            **kwds
        })

    @classmethod
    def import_from_jl(cls, path, mode='r', **kwds):
        """Import documents from `jsonlines` file.

        Parameters
        ----------
        path : str
            Filepath.
        mode : str
            File open mode.
        **kwds :
            Keyword arguments passed to `store` method for
            setting up a storage object.
        """
        docs = JSONLinesStorage.load_jl(path, mode)
        kwds = { 'batch_size': 20000, **kwds }
        cls.store(**kwds).bulk_update(docs)


class Document(_Document, DocumentMixin):
    """Document model class."""
    meta = { 'abstract': True }


class EmbeddedDocument(_EmbeddedDocument, DocumentMixin):
    """Embedded document model class."""
    meta = { 'abstract': True }
