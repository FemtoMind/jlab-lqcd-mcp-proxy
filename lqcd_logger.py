# Customized logger setup for lqcd analysis server
import logging


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
        record.levelname = f"{start_style}{record.levelname}{end_style}"
        record.msg = f"{msg_style}{record.msg}{end_style}"
        return f"{super().format(record)}"


# Create a custom logger
lqcd_logger = logging.getLogger("lqcd")

# Get a stream handler
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

# Create a formatter and set it for the handler
# separator style formatting
sep = ":"
formatter = AnsiColorFormatter(
    f"{{levelname}}{sep:<5s} {{name:<10s}}| {{message}}", style="{"
)

stream_handler.setFormatter(formatter)
lqcd_logger.addHandler(stream_handler)

# Set the logger level
lqcd_logger.setLevel(logging.INFO)
