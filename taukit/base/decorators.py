"""Decorators."""
from inspect import signature
from .validators import Validator


def interface(schema, validator_cls=Validator):
    """Interface decorator."""
    # pylint: disable=unused-argument,protected-access
    def decorator(func):
        validator = validator_cls({ **getattr(func, '_schema', {}), **schema })
        def wrapper(*args, **kwds):
            sig = signature(getattr(func, '_f', func))
            args__ = { k: v for v, k in zip(locals()['args'], sig.parameters.keys()) }
            args__.update(locals()['kwds'])
            args__ = validator.validated(args__)
            if args__ is None:
                raise ValueError(validator.errors)
            return func(**args__)
        wrapper._f = getattr(func, '_f', func)
        wrapper._schema = validator._schema
        return wrapper
    return decorator
