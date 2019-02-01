"""Unit tests for decorators."""
import pytest
from taukit.base.decorators import interface


@interface({
    'x': {'type': 'string'},
    'y': {'type': 'integer'}
})
def tfunc(x, y, z):
    return x*(y+int(z))

tfunc2 = interface({'z': {'type': 'integer'}})(tfunc)

class A:
    @interface({
        'x': {'type': 'string'}
    })
    def __init__(self, x, y):
        self.x = x
        self.y = y

class B(A):
    @interface({
        'y': {'type': 'integer'}
    })
    def __init__(self, x, y):
        super().__init__(x, y)

@pytest.mark.parametrize('x,y,z,expected', [
    ('a', 7, 2, 'a'*9),
    ('b', 'a', 2, ValueError),
    ('b', 2, '3', ValueError)
])
def test_interface(x, y, z, expected):
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            tfunc(x, y=y, z=z)
            tfunc2(x, y=y, z=z)
        return
    output = tfunc(x, y=y, z=z)
    output = tfunc(x, y, z)
    output = tfunc2(x, y=y, z=z)
    output = tfunc2(x, y, z)
    assert output == expected

@pytest.mark.parametrize('x,y,expected', [
    ('a', 2, ('a', 2)),
    (2, 2, ValueError)
])
def test_interface_cls(x, y, expected):
    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected):
            a = A(x, y)
            b = B(x, y)
        return
    a = A(x, y)
    b = B(x, y)
    assert (a.x, a.y) == expected
    assert (b.x, b.y) == expected
