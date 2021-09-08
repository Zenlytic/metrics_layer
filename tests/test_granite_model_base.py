from granite.core.model.base import GraniteBase


def test_base():
    obj = GraniteBase({"name": "testing"})
    assert obj.name == "testing"
