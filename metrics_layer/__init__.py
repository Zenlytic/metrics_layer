from metrics_layer.cli import cli_group  # noqa
from metrics_layer.core import MetricsLayerConnection  # noqa

try:
    import importlib.metadata as importlib_metadata
except ModuleNotFoundError:
    import importlib_metadata

__version__ = importlib_metadata.version(__name__)
