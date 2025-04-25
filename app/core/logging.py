import logging
import sys
import contextvars
from pythonjsonlogger import jsonlogger
from core.config import settings  # Assuming config is setup

# Context variable to hold the correlation_id
correlation_id_cv = contextvars.ContextVar("correlation_id", default=None)


class CorrelationIdFilter(logging.Filter):
    """Injects the correlation_id into the log record."""

    def filter(self, record):
        record.correlation_id = correlation_id_cv.get()
        record.service = settings.SERVICE_NAME  # Add service name from settings
        return True


def setup_logging():
    """Configures JSON logging based on logging.mdc rules."""
    log_level = settings.LOG_LEVEL.upper()
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    log_format = "%(asctime)s %(levelname)s %(message)s %(correlation_id)s %(service)s"

    formatter = jsonlogger.JsonFormatter(
        fmt=log_format, rename_fields={"asctime": "timestamp", "levelname": "level"}
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    handler.addFilter(
        CorrelationIdFilter()
    )  # Add filter to inject correlation_id/service

    # Get the root logger and remove existing handlers
    root_logger = logging.getLogger()
    for h in root_logger.handlers[:]:
        root_logger.removeHandler(h)
        h.close()

    # Add our configured handler and set the level
    root_logger.addHandler(handler)
    root_logger.setLevel(numeric_level)

    # Configure uvicorn loggers to use our handler if needed
    logging.getLogger("uvicorn.error").propagate = False
    logging.getLogger("uvicorn.access").propagate = False

    logging.info("Logging configured", extra={"log_level": log_level})


# Example usage: Call setup_logging() early in your application startup.
# Then use standard logging:
# import logging
# logger = logging.getLogger(__name__)
# logger.info("This is an info message", extra={"custom_field": "value"})
# logger.error("This is an error", extra={"error_details": "something failed"})

# To set correlation_id (typically in middleware):
# from core.logging import correlation_id_cv
# correlation_id_cv.set("your-uuid-here")
