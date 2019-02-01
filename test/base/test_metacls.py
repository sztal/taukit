"""Unit tests for metaclasses."""
# pylint: disable=W0212,W0231,W0621,E1101
import re
import pytest
from taukit.base.metacls import Composable


class Something:

    def __init__(self, x):
        self.x = x

    def meth(self):
        return self.x

class ParentClass(metaclass=Composable):

    __components = [
        ('somex', Something(10))
    ]

    def __init__(self, x):
        self._setcomponents([
            ('somey', Something(x))
        ])

class SomeClass(ParentClass, metaclass=Composable):

    __components = [
        ('regex', re.compile(r"foo", re.IGNORECASE))
    ]

    def __init__(self, pattern):
        """Instance-level components defined at runtime."""
        self._setcomponents([
            ('rx', re.compile(pattern, re.IGNORECASE))
        ])

@pytest.fixture
def some_instance():
    return SomeClass(r"bar")


class TestComposable:

    def test_simple(self, some_instance):
        assert hasattr(some_instance, 'regex')
        assert 'regex' in dir(some_instance)
        assert hasattr(some_instance, 'rx')
        assert 'rx' in dir(some_instance)
        assert some_instance.regex == re.compile(r"foo", re.IGNORECASE)
        assert some_instance.rx == re.compile(r"bar", re.IGNORECASE)
        assert some_instance.search("bar") is not None
        assert some_instance.search("foo") is None
        assert '__components' in dir(some_instance)
        assert '_SomeClass__components' in dir(some_instance)

    def test_complex(self, some_instance):
        """Complex test case."""
        assert hasattr(some_instance, 'somex')
        assert 'somex' in dir(some_instance)
        assert not hasattr(some_instance, 'somey')
        assert 'somey' not in dir(some_instance)
        assert hasattr(some_instance, 'meth')
        assert 'meth' not in dir(some_instance)
        assert some_instance.meth() == 10
        assert '_ParentClass__components' in dir(some_instance)

    def test_getcomponents_and_getattribute(self, some_instance):
        """Test case for 'getcomponents_` and `getattribute_` methods."""
        search1 = some_instance._getcomponents('SomeClass')['regex'].search
        search2 = some_instance._getattribute('search', 'regex', 'SomeClass')
        search3 = some_instance.regex.search
        rx_search = some_instance.search
        assert search1 == search2 == search3
        assert search1 != rx_search
        with pytest.raises(AttributeError):
            some_instance._getattribute('search', 'regex')
        with pytest.raises(AttributeError):
            some_instance._getcomponent('regex')

    def test_setattribute_instance(self, some_instance):
        """Test case for `setattribute_` method."""
        some_instance._setattribute('regex', 'foo', on_component=False)
        regex1 = some_instance.regex
        regex2 = some_instance._getcomponent('regex', 'SomeClass')
        assert regex1 != regex2

    def test_setattribute_component(self, some_instance):
        """Test case for `setattribute_` method."""
        some_instance._setattribute('x', 'foo', on_component=True)
        x1 = some_instance.x
        x2 = some_instance._getattribute('x', 'somex', 'ParentClass')
        assert x1 == x2
