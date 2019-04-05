"""General purpose utilities."""
import re
import os
import io
from importlib import import_module
from datetime import datetime, date
import dateparser
import joblib
from click import echo

_rx_pp = re.compile(r"^[\w_.:]+$", re.ASCII)
_rx_file = re.compile(r"\.[a-z]*$", re.IGNORECASE)

def safe_print(x, nl=True, **kwds):
    """Fault-safe print function.

    Parameters
    ----------
    x : any
        An object dumpable to str.
    nl : bool
        Should new line be printed after the content.
    **kwds :
        Other arguments passed to `click.echo`.
    """
    try:
        echo(x, nl=nl, **kwds)
    except UnicodeEncodeError:
        x = str(x).encode('utf-8', 'replace')
        echo(x, **kwds)

def import_python(path, package=None):
    """Get python module or object.

    Parameters
    ----------
    path : str
        Fully-qualified python path, i.e. `package.module:object`.
    package : str or None
        Package name to use as an anchor if `path` is relative.
    """
    parts = path.split(':')
    if len(parts) > 2:
        msg = f"Not a correct path ('{path}' has more than one object qualifier)"
        raise ValueError(msg)
    elif len(parts) == 2:
        module_path, obj = parts
    else:
        module_path, obj = path, None
    module = import_module(module_path, package=package)
    if obj:
        return getattr(module, obj)
    return module

def is_python_path(path, object_only=False):
    """Check if a string is a valid python path.

    Parameters
    ----------
    path : str
        String.
    object_only : bool
        Should `True` be returned only for object specifying paths.
    """
    m = _rx_pp.match(path)
    n_colons = path.count(':')
    if not m or n_colons > 1 or (object_only and n_colons != 1):
        return False
    return True

def make_hash(obj, salt=None, **kwds):
    """Make hash string from an object.

    Parameters
    ----------
    obj : object
        Some python object.
    salt : str, False or None
        Optional salt added to the string for additional obfuscation.
    **kwds :
        Keyword arguments passed to :py:func:`joblib.hashing.hash`
    """
    hash_id = joblib.hashing.hash(obj, **kwds)
    if salt is not None:
        hash_id = joblib.hashing.hash(hash_id+salt)
    return hash_id

def is_file(path):
    """Tell if a path is a file path.

    In case of non-existent file paths the file extenstions
    serves as a file signature.

    Parameters
    ----------
    path : str
        Some path.
    """
    if os.path.exists(path):
        return os.path.isfile(path)
    return bool(_rx_file.search(path))

def make_path(*args, create_dir=True, **kwds):
    """Make path from fragments and create dir if does not exist.

    Parameters
    ----------
    *args :
        Path fragments.
    create_dir : bool
        Should directory be created if necessary.
    **kwds :
        Other arguments passed to `os.makedirs`.
    """
    path = os.path.join(*args)
    dirpath = os.path.dirname(path) if is_file(path) else path
    if create_dir and not os.path.exists(dirpath):
        os.makedirs(dirpath, exist_ok=True, **kwds)
    return path

def make_filepath(filename, dirpath, inc_if_taken=True):
    """Make filepath for a given filename.

    This function allows for not overwriting existing files
    and incrementing filename counter instead.

    Parameters
    ----------
    filename : str
        File name.
    dirpath : str
        Directory path.
    inc_if_taken : bool
        Should file counter be used and incremented if a name is already taken.
    """
    def add_num(fn, n):
        l = fn.split('.')
        n = '-'+str(n)
        if len(l) == 1:
            return fn+n
        return '.'.join(l[:-1])+n+'.'+l[-1]
    n = 0
    filepath = os.path.join(dirpath, filename)
    if inc_if_taken:
        while os.path.exists(filepath):
            n += 1
            filepath = os.path.join(dirpath, add_num(filename, n))
    return filepath

def date_from_string(dt, fmt, preprocessor=None, **kwds):
    """Convert string to :py:class:`datetime.datetime` object.

    Parameters
    ----------
    dt : str
        Date string.
    frm : str
        Date formatting string.
    preprocessor : func
        Optional preprocessing function.
        Useful for normalizing date strings to conform to one format string.
    **kwds :
        Arguments passed to `preprocessor`.
    """
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, date):
        return datetime(*dt.timetuple()[:6])
    if preprocessor:
        dt = preprocessor(dt, **kwds)
    return datetime.strptime(dt, fmt)

def parse_date(dt, preprocessor=None, date_formats=None, **kwds):
    """Parse date string flexibly using `dateutil` module.

    Parameters
    ----------
    dt : str
        Date string.
    preprocessor : func
        Optional preprocessing function.
        Useful for normalizing date strings to conform to one format string.
    date_formats : list of str or None
        Passed to :py:func:`dateparser.parse`.
    **kwds :
        Arguments passed to `preprocessor`.
    """
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, date):
        return datetime(*dt.timetuple()[:6])
    if preprocessor:
        dt = preprocessor(dt, **kwds)
    return dateparser.parse(dt, date_formats=date_formats)

def parse_bool(x, true=('true', 'yes', '1', 'on'), add_true=(),
               false=('false', 'no', '0', 'off'), add_false=()):
    """Parse boolean string.

    Parameters
    ----------
    x : bool or str
        Boolean value as `bool` or `str`.
    true : list of str
        List of accepted string representations of `True` value.
    add_true  : list of str
        Optional list to of `True` representations to append to the default list.
    false : list of str
        List of accepted string representations of `False` value.
    add_false : list of str
        Optional list of `False` representations to append to the default list.

    Notes
    -----
    `true` and `false` should always consist of only lowercase strings,
    as all comparisons are done after lowercasing `x`.

    Raises
    ------
    ValueError
        If `x` is not `bool` and not contained either in `true` or `false`.
    """
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)) and x == 0 or x == 1:
        return bool(x)
    x = str(x).lower()
    if add_true:
        true = (*true, *add_true)
    if add_false:
        false = (*false, *add_false)
    if x in true:
        return True
    if x in false:
        return False
    raise ValueError("Value '{}' can not be interpreted as boolean".format(x))

def slice_chunks(x, n):
    """Yield successive n-sized chunks from an iterable"""
    l = []
    for item in x:
        l.append(item)
        if len(l) >= n:
            yield l
            l = []
    if l:
        yield l

def to_stream(obj):
    """Dump object to a bytes stream."""
    stream = io.BytesIO()
    joblib.dump(obj, stream)
    return stream

def from_stream(obj):
    """Load object from a bytes stream."""
    return joblib.load(obj)

def to_bytes(obj):
    """Dump object to bytes."""
    # pylint: disable=no-member
    return to_stream(obj).getvalue()

def from_bytes(obj):
    """Load objects from bytes."""
    return joblib.load(io.BytesIO(obj))

def get_default(value, name, defaults):
    """Get default value for a variable.

    Parameters
    ----------
    value : any
        Variable value.
    name : str
        Variable name.
    defaults : dict
        Dictionary of default values.

    Notes
    -----
    Function operates on a shallow copy of `defaults`.
    If deep copy is needed than it has to be done by hand
    before passing `defaults` as an argument.
    """
    defaults = defaults.copy()
    if value is None and name in defaults:
        value = defaults[name]
    return value
