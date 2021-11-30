from metrics_layer.cli import cli_group  # noqa
from metrics_layer.core import MetricsLayerConnection  # noqa
from metrics_layer.core.utils import lazy_import

try:
    import importlib.metadata as lazy_importlib_metadata
except ModuleNotFoundError:
    lazy_importlib_metadata = lazy_import("importlib_metadata")

__version__ = lazy_importlib_metadata.version(__name__)
