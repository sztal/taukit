"""Test cases for various utility functions."""
import os
from tempfile import mkdtemp
import pytest
from taukit.utils import import_python, make_filepath
from taukit.utils import get_default
import taukit.base.metacls
from taukit.base.metacls import Composable


@pytest.mark.parametrize('path,package,expected', [
    ('taukit.base.metacls', None, taukit.base.metacls),
    ('taukit.base.metacls:Composable', None, Composable),
    ('.base.metacls', 'taukit', taukit.base.metacls),
    ('.base.metacls:Composable', 'taukit', Composable)
])
def test_import_python(path, package, expected):
    """Test cases for `import_python`."""
    output = import_python(path, package)
    assert output == expected

@pytest.mark.parametrize('increment', [False, True])
@pytest.mark.parametrize('ext', [False, True])
def test_make_filepath(increment, ext):
    fd = mkdtemp()
    fn = 'testfile'
    if ext:
        fn += '.txt'
    fp = os.path.join(fd, fn)
    if increment:
        f = open(fp, 'w')
    try:
        filepath = make_filepath(fn, fd)
        if increment:
            assert filepath != fp
    except Exception as ex:
        f.close()
        raise ex

@pytest.mark.parametrize('a', [10, None])
@pytest.mark.parametrize('b', [5, None])
def test_get_default(a, b):
    defaults = {'a': 1, 'b': 1}
    def func(a, b):
        nonlocal defaults
        a = get_default(a, 'a', defaults)
        b = get_default(b, 'b', defaults)
        return a + b

    output = func(a, b)
    expected = get_default(a, 'b', defaults) + get_default(b, 'b', defaults)
    assert output == expected
