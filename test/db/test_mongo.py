"""Test MongoDB models, model mixins and related utilities."""
# pylint: disable=redefined-outer-name
from datetime import datetime
from itertools import repeat, chain
import pytest
from pymongo import InsertOne
import mongoengine
from mongoengine import StringField, IntField, DateTimeField, EmbeddedDocumentField
from taukit.persistence import DBPersister
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
        created_at = DateTimeField(required=True)
        author = EmbeddedDocumentField(EmbeddedDoc)

    yield TestDoc
    TestDoc.drop_collection()

@pytest.fixture(scope='module')
def mongopers(TestDocument):
    return DBPersister(model=TestDocument)


TEST_DOCUMENT_DATA = [{
    'title': 'A',
    'rating': 2,
    'created_at': datetime.now(),
}, {
    'title': 'B',
    'created_at': '2017-08-11',
    'author': {'name': 'Jane', 'surname': 'Doe'}
}]


class TestDocumentMixin:

    @pytest.mark.parametrize('item', TEST_DOCUMENT_DATA)
    def test_dict_converters(self, item, TestDocument):
        doc = TestDocument.from_dict(item)
        dct = doc.to_dict(remove_empty_fields=True)
        if isinstance(item['created_at'], str):
            dct['created_at'] = dct['created_at'].strftime("%Y-%m-%d")
        assert dct == item


class TestMongoPersister:

    @pytest.mark.parametrize('item', TEST_DOCUMENT_DATA)
    def test_persist_and_query(self, item, mongopers):
        n0 = len(mongopers.query())
        mongopers.persist(item)
        n1 = len(mongopers.query())
        assert n1 == n0 + 1

    @pytest.mark.parametrize('items', [TEST_DOCUMENT_DATA])
    @pytest.mark.parametrize('batch_size', [0, 50])
    def test_persist_many(self, items, batch_size, mongopers):
        items = [ x.copy() for x in chain.from_iterable(repeat(items, 50)) ]
        output = mongopers.persist_many(items, InsertOne, batch_size=batch_size)
        assert output['nInserted'] == 100
