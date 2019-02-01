"""General purpose utilities."""
import re
import os
import hashlib
from importlib import import_module
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

def hash_string(string, salt=None):
    """Get MD5 hash from a string.

    Parameters
    ----------
    string : str
        Some string.
    salt : str, False or None
        Optional salt added to the string for additional obfuscation.
    """
    if salt is not None:
        string += salt
    return hashlib.md5(string.encode('utf-8')).hexdigest()

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

def make_filepath(filename, dirpath, inc_if_taken=True, **kwds):
    """Make filepath for a given filename.

    This function allows for not overwriting existing files
    and incrementing filename counter instead.
    In such a case the filename must be a formattable string
    with a named placeholder `{n}`. Other named placeholder may be
    filled through `**kwds`.

    Parameters
    ----------
    filename : str
        File name.
    dirpath : str
        Directory path.
    inc_if_taken : bool
        Should file counter be used and incremented if a name is already taken.
    **kwds :
        Optional keyword arguments passed to the format string.
    """
    n = 0
    _filepath = os.path.join(dirpath, filename)
    filepath = _filepath
    while inc_if_taken:
        n += 1
        filepath = _filepath.format(n=n, **kwds)
        if not os.path.exists(filepath):
            break
    return filepath
