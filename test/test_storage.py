"""Unit tests for storage classes."""
from shutil import rmtree
from tempfile import mkdtemp
import pytest
from taukit.storage import JSONLinesStorage


class TestJSONLinesStorage:

    @pytest.fixture(scope='function')
    def jl_store(self):
        fd = mkdtemp()
        fn = 'testfile.jl'
        store = JSONLinesStorage(fn, fd)
        yield store
        rmtree(store.dirpath)

    @pytest.mark.parametrize('items', [
        ({'a': 1, 'b': 2}, {'a': 1, 'b': 2}, {'a': 1, 'b': 2})
    ])
    def test_save(self, jl_store, items):
        for item in items:
            jl_store.save(item)
        saved_items = jl_store.load()
        for saved_item, item in zip(saved_items, items):
            assert saved_item == item
