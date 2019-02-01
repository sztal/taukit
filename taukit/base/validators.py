"""Base validator classes."""
from collections.abc import Callable, Iterable, Sequence, Mapping
from cerberus import Validator as _Validator, TypeDefinition


def copy_schema(schema):
    """Copy schema definition."""
    if isinstance(schema, Validator):
        schema = schema.schema
    return dict(schema.copy())


class Validator(_Validator):
    """Base validator extends standard *Cerberos* validator with more types."""
    types_mapping = _Validator.types_mapping.copy()
    types_mapping['callable'] = TypeDefinition('callable', (Callable,), ())
    types_mapping['iterable'] = TypeDefinition('iterable', (Iterable), ())
    types_mapping['sequence'] = TypeDefinition('sequence', (Sequence,), ())
    types_mapping['mapping'] = TypeDefinition('mapping', (Mapping,), ())

    def __init__(self, *args, allow_unknown=True, purge_unknown=False, **kwds):
        """Initialization method.

        See Also
        --------
        cerberus.Validator : `Validator` class and its `__init__` method
        """
        kwds.update(allow_unknown=allow_unknown, purge_unknown=purge_unknown)
        super().__init__(*args, **kwds)
