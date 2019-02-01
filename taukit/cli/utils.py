"""Command-line interface utilities."""
# pylint: disable=W0613
from collections.abc import Sequence, Mapping
import json
from types import GeneratorType
import click
from ..utils import safe_print
from ..serializers import UniversalJSONEncoder


def pprint(obj, indent=2):
    """Pretty print json-like objects.

    Parameters
    ----------
    obj : any
        Some object.
    indent : int or None
        Indentation length.
        If `None` then config value is used.
    """
    if isinstance(obj, (list, tuple, Mapping)):
        safe_print(json.dumps(obj, sort_keys=True, indent=indent, cls=UniversalJSONEncoder))
    else:
        safe_print(obj)

def show_unique(iterator):
    """Show unique values in an iterator."""
    shown = []
    for item in iterator:
        if item in shown:
            continue
        shown.append(item)
        pprint(item)

def eager_callback(callback):
    """Warpper for executing callbacks of eager options.

    Parameters
    ----------
    ctx : :py:class:`click.Context`
        Context object.
    value : any
        Value.
    *args :
        Positional arguments passed to the callback.
    **kwds :
        Keyword arguments passed to the callback.
    """
    def callback_wrapper(ctx, param, value, *args, **kwds):
        """Wrapped callback."""
        if not value or ctx.resilient_parsing:
            return
        callback(*args, **kwds)
        ctx.exit()
    return callback_wrapper

def to_console(obj, unique=False, processor=None, **kwds):
    """Print object to the console.

    Parameters
    ----------
    obj : any
        String and non-terables are printed as is.
        Other iterables are iterated and printed.
        Iterables within iterables are printed as is.
    unique : bool
        Should duplicated objects be shown only once.
    processor : func or None
        Optional processing function to call on each object.
    **kwds :
        Keyword arguments passed to the processor function.
    """
    if isinstance(obj, str) or not isinstance(obj, (Sequence, GeneratorType)):
        obj = [obj]
    if unique:
        show_unique(obj)
    for o in obj:
        if processor:
            o = processor(o, **kwds)
        pprint(o)

def do_dry_run(dry, *args):
    """Dry run."""
    if dry:
        for arg in args:
            to_console(arg)
        ctx = click.get_current_context()
        ctx.exit()
