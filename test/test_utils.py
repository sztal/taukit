"""Test cases for various utility functions."""
import pytest
from taukit.utils import import_python
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
