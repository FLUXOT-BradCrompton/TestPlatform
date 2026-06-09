"""
Structured JSON logging configuration for edge services.
Uses python-json-logger to emit machine-parseable log records.
"""

import logging
import sys
from pythonjsonlogger import jsonlogger


class _CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    Extends the base JSON formatter to always include a consistent set of
    fields regardless of what the logger record contains.
    """

    def add_fields(self, log_record: dict, record: logging.LogRecord, message_dict: dict) -> None:
        super().add_fields(log_record, record, message_dict)

        # Normalise field names to snake_case for downstream consumers
        if not log_record.get("timestamp"):
            log_record["timestamp"] = self.formatTime(record, self.datefmt)
        if not log_record.get("level"):
            log_record["level"] = record.levelname
        if not log_record.get("module"):
            log_record["module"] = record.module
        if not log_record.get("logger"):
            log_record["logger"] = record.name
        if not log_record.get("function"):
            log_record["function"] = record.funcName
        if not log_record.get("line"):
            log_record["line"] = record.lineno

        # Remove duplicate fields that the base class adds with different names
        log_record.pop("levelname", None)
        log_record.pop("name", None)


def setup_logging(level: str = "INFO") -> None:
    """
    Configure the root logger to emit structured JSON to stdout.

    Args:
        level: One of DEBUG / INFO / WARNING / ERROR / CRITICAL.
               Case-insensitive.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)

    formatter = _CustomJsonFormatter(
        fmt="%(timestamp)s %(level)s %(module)s %(logger)s %(function)s %(line)d %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S.%fZ",
        json_ensure_ascii=False,
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    # Remove any pre-existing handlers so we don't double-log
    root.handlers.clear()
    root.addHandler(handler)

    # Quieten noisy third-party libraries unless we are in DEBUG
    if numeric_level > logging.DEBUG:
        for noisy in ("asyncua", "pymodbus", "paho", "urllib3", "httpx", "httpcore"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A :class:`logging.Logger` instance.
    """
    return logging.getLogger(name)
