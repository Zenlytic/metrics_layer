import logging
import sys

LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}
DEFAULT_LEVEL = "info"
FORMAT = "[%(asctime)s] [%(process)d|%(threadName)10s|%(name)s] [%(levelname)s] %(message)s"


def parse_log_level(log_level):
    return LEVELS.get(log_level, LEVELS[DEFAULT_LEVEL])


def setup_logging(log_level=DEFAULT_LEVEL):
    # Turn off any existing handlers
    root = logging.getLogger()
    for h in root.handlers[:]:
        root.removeHandler(h)
        h.close()

    logging.basicConfig(stream=sys.stderr, format=FORMAT, level=parse_log_level(log_level))
