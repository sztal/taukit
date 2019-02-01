"""Test cases for :py:module:`smcore.utils.serializers`."""
import json
from datetime import datetime
from taukit.serializers import JSONEncoder


class TestJSONEncoder:
    """Test cases for
    :py:class:`smcore.utils.serializers.JSONEncoder`.
    """
    def test_dump(self):
        now = datetime.now()
        jsonified = json.loads(json.dumps(now, cls=JSONEncoder))
        assert jsonified == now.isoformat()
