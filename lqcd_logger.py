# Customized logger setup for lqcd analysis server
import logging
from logging.handlers import RotatingFileHandler
import sys

# add filter to filter out local monitoring tool such as Prometheus
# checking /metrics

class FilterLocalMonitoring(logging.Filter):
    def filter(self, record):
        # ignore message checking /metrics
        if record.getMessage().find("GET /metrics") == -1 :
            return True
        return False
    
class AnsiColorFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        no_style = "\033[0m"
        bold = "\033[1m"
        grey = "\033[90m"
        yellow = "\033[93m"
        green = "\033[32m"
        red = "\033[31m"
        red_light = "\033[91m"
        cyan = "\033[36m"
        start_style = {
            "DEBUG": cyan,
            "INFO": green,
            "WARNING": yellow,
            "ERROR": red,
            "CRITICAL": red_light + bold,
        }.get(record.levelname, no_style)
        msg_style = {
            "DEBUG": bold,
        }.get(record.levelname, no_style)
        end_style = no_style
        record.levelname = f"{start_style}{record.levelname:8s}{end_style}"
        record.msg = f"{msg_style}{record.msg}{end_style}"
        return f"{super().format(record)}"


class SimpleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord):
        return f"{super().format(record)}"


# Get a stream handler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the handler
# separator style formatting
sep = ":"
wspace = " "
fancy_formatter = AnsiColorFormatter(
    fmt=f"{{levelname}}{sep:<2s}{{name:<7s}}| {{message}}",
    datefmt="%m/%d/%y %H:%M:%S",
    style="{",
)
simple_formatter = SimpleFormatter(
    fmt=f"{{asctime}}{wspace}{{levelname:8s}}{sep:<2s}{{name:<7s}}| {{message}}",
    datefmt="%m/%d/%y %H:%M:%S",
    style="{",
)

if stream_handler.stream.isatty():
    stream_handler.setFormatter(fancy_formatter)
else:
    stream_handler.setFormatter(simple_formatter)

# Create a custom logger
lqcd_logger = logging.getLogger("lqcd")

# default add stream handler
lqcd_logger.addHandler(stream_handler)

lqcd_logger.setLevel(logging.DEBUG)


# Set up lqcd logger to use file handler
def setup_lqcd_file_logger(file_name: str):
    file_handler = RotatingFileHandler(
        file_name, maxBytes=1024 * 1024 * 5, backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(simple_formatter)

    # 1. LQCD Logger Setup
    # Remove existing handlers (like stdout) to avoid double logging to console + file if not intended
    # or just add file handler. User wants file logging.
    lqcd_logger.handlers.clear()
    lqcd_logger.addHandler(file_handler)
    # add filter
    lqcd_logger.addFilter(FilterLocalMonitoring())
    lqcd_logger.propagate = False  # Prevent bubbling up to root, fixing duplication

    # 2. Root Logger Setup (for all other libraries)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    # Remove default handlers (usually stderr) to avoid double logging or clutter
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)

    # 3. Uvicorn Logger Setup
    # Uvicorn configures its own loggers. We need to override them.
    for logger_name in ["uvicorn", "uvicorn.access", "uvicorn.error"]:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)  # Ensure it captures standard traffic
        logger.handlers.clear()
        logger.addHandler(file_handler)
        # add the same filter
        logger.addFilter(FilterLocalMonitoring())
        logger.propagate = False  # Don't bubble up to root (which also has the handler)


# Check the logger is logging to a file or console
def is_logging_to_file() -> bool:
    return isinstance(lqcd_logger.handlers[0], RotatingFileHandler)
