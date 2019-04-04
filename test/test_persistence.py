"""Unit tests for persistence classes."""
from shutil import rmtree
from tempfile import mkdtemp
import pytest
from taukit.persistence import JSONLinesPersister


class TestJSONLinesPersister:

    @pytest.fixture(scope='function')
    def persister(self):
        fd = mkdtemp()
        fn = 'testfile.jl'
        pers = JSONLinesPersister(fn, fd)
        yield pers
        rmtree(pers.dirpath)

    @pytest.mark.parametrize('items', [
        ({'a': 1, 'b': 2}, {'a': 1, 'b': 2}, {'a': 1, 'b': 2})
    ])
    def test_persist(self, persister, items):
        for item in items:
            persister.persist(item)
        persisted_items = persister.load()
        for persisted_item, item in zip(persisted_items, items):
            assert persisted_item == item
