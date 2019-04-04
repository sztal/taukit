"""Test MongoDB models, model mixins and related utilities."""
# pylint: disable=redefined-outer-name
from datetime import datetime
import pytest
import mongoengine
from mongoengine import StringField, IntField, DateTimeField, EmbeddedDocumentField
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

    return TestDoc


class TestDocumentMixin:

    @pytest.mark.parametrize('item', [{
        'title': 'A',
        'rating': 2,
        'created_at': datetime.now(),
    }, {
        'title': 'B',
        'created_at': '2017-08-11',
        'author': {'name': 'Jane', 'surname': 'Doe'}
    }])
    def test_dict_converters(self, item, TestDocument):
        doc = TestDocument.from_dict(item)
        dct = doc.to_dict(remove_empty_fields=True)
        if isinstance(item['created_at'], str):
            dct['created_at'] = dct['created_at'].strftime("%Y-%m-%d")
        assert dct == item
