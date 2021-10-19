from metrics_layer.core.model.base import MetricsLayerBase


def test_base():
    obj = MetricsLayerBase({"name": "testing"})
    assert obj.name == "testing"
