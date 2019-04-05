"""MongoDB models and model mixins.

The mixins are supposed to enhance standard *MongoEngine* models
with various helper methods for importing, exporting and validating data.
They also expose a common API used by all DB models, which is utilized
by database persisters.
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
    _ignore_fields = [ '_id', 'id' ]
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

    def tk__persist(self):
        """Persist document."""
        self.save()

    @classmethod
    def tk__persist_many(cls, items, action_hook, bulk_write_kws=None, **kwds):
        """Perform bulk write.

        Parameters
        ----------
        docs : iterable
            Documents to write / update.
        action_hook : callable or None
            Called at every item in `items`. Should return an object
            corresponding to the desired bulk write operation.
        bulk_write_kws : dict
            Additional keyword arguments passed to `bulk_write`.
        **kwds :
            Keyword arguments passed additionaly to `action_hook` calls.
        """
        if bulk_write_kws is None:
            bulk_write_kws = {}
        bulk_ops = [ action_hook(item, **kwds) for item in items ]
        res = cls._get_collection().bulk_write(bulk_ops, **bulk_write_kws)
        return res.bulk_api_result

    @classmethod
    def tk__persist_many_merge_results(cls, results, new_result, logger=None):
        """Merge results of `_persist_many`.

        Parameters
        ----------
        results : dict
            Bulk update results as returned by *PyMongo*.
        new_result : dict
            New bulk update results.
        logger : logging.Logger
            If define then it will be used to log results.
        """
        if not results:
            results = {
                'writeErrors': [],
                'writeConcernErrors': [],
                'nInserted': 0,
                'nUpserted': 0,
                'nMatched': 0,
                'nModified': 0,
                'nRemoved': 0
            }
        for k in results:
            results[k] += new_result[k]
        if logger:
            logger.info("%s bulk update performed: %s", cls.__module__, str(results))
        return results

    @classmethod
    def tk__query(cls, **kwds):
        """Query collection."""
        return cls.objects(**kwds)

    @classmethod
    def tk__drop(cls, **kwds):
        # pylint: disable=unused-argument
        cls.drop_collection()


class Document(_Document, DocumentMixin):
    """Document model class."""
    meta = { 'abstract': True }


class EmbeddedDocument(_EmbeddedDocument, DocumentMixin):
    """Embedded document model class."""
    meta = { 'abstract': True }
