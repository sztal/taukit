"""Test MongoDB models, model mixins and related utilities."""
# pylint: disable=redefined-outer-name
import os
from datetime import datetime
from itertools import repeat, chain
from tempfile import mkstemp
import json
import pytest
from pymongo import InsertOne
import mongoengine
from mongoengine import StringField, IntField
from mongoengine import DateTimeField, EmbeddedDocumentField
from taukit.serializers import JSONEncoder
from taukit.storage import MongoStorage
from taukit.db.mongo import Document, EmbeddedDocument


@pytest.fixture(scope='session')
def mdb():
    conn = mongoengine.connect('mdbmock', host='mongomock://localhost')
    yield conn
    conn.close()

@pytest.fixture(scope='session')
def TestDocument(mdb):
    # pylint: disable=redefined-outer-name,unused-argument
    class EmbeddedDoc(EmbeddedDocument):
        name = StringField(required=True)
        surname = StringField(required=True)

    class TestDoc(Document):
        title = StringField(required=True)
        rating = IntField()
        created_at = DateTimeField(required=False)
        author = EmbeddedDocumentField(EmbeddedDoc)

        _ignore_fields = ('_id', 'id')

    yield TestDoc
    TestDoc.drop_collection()

@pytest.fixture(scope='module')
def mdbstore(TestDocument):
    return MongoStorage(
        model=TestDocument,
        updater=InsertOne
    )


TEST_DOCUMENT_DATA = [{
    'title': 'A',
    'rating': 2,
    'created_at': datetime.now(),
    'author': None
}, {
    'title': 'B',
    'rating': None,
    'created_at': '2017-08-11',
    'author': {'name': 'Jane', 'surname': 'Doe'}
}]

@pytest.fixture(scope='session')
def jl_datapath():
    _, path = mkstemp()
    with open(path, 'w') as f:
        for doc in TEST_DOCUMENT_DATA:
            f.write(json.dumps(doc, cls=JSONEncoder)+"\n")
    yield path
    os.remove(path)


class TestDocumentMixin:

    @pytest.mark.parametrize('item', TEST_DOCUMENT_DATA)
    def test_dict_converters(self, item, TestDocument):
        doc = TestDocument.from_dict(item)
        dct = doc.to_dict(ignore_fields=True)
        if isinstance(item['created_at'], str):
            dct['created_at'] = dct['created_at'].strftime("%Y-%m-%d")
        assert dct == item


class TestMongoStorage:

    @pytest.mark.parametrize('item', TEST_DOCUMENT_DATA)
    def test_save(self, item, mdbstore):
        n0 = len(mdbstore.model.objects)
        mdbstore.save(item)
        n1 = len(mdbstore.model.objects)
        assert n1 == n0 + 1

    @pytest.mark.parametrize('items', [TEST_DOCUMENT_DATA])
    @pytest.mark.parametrize('batch_size', [0, 50])
    def test_bulk_update(self, items, batch_size, TestDocument):
        items = [ x.copy() for x in chain.from_iterable(repeat(items, 50)) ]
        output = TestDocument.store(
            batch_size=batch_size,
            updater=InsertOne
        ).bulk_update(items)
        assert output['nInserted'] == 100

    def test_import_from_jl(self, jl_datapath, TestDocument):
        TestDocument.import_from_jl(jl_datapath, updater=InsertOne)
