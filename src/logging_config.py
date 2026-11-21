"""Structured logging setup shared by the pipeline, scripts and API.

Log lines use a consistent pipe-delimited layout, and messages carry
``key=value`` pairs so they are easy to grep or parse::

    2025-01-01 10:00:00 | INFO | src.data_cleaning | step=remove_duplicates removed=25
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_HANDLER_MARKER = "_sales_pipeline_handler"


def setup_logging(log_level: str = "INFO", log_file: Path | None = None) -> None:
    """Configure root logging with a console handler and an optional file handler.

    Safe to call multiple times: handlers installed by a previous call are replaced.
    """
    root = logging.getLogger()
    root.setLevel(log_level)

    for handler in list(root.handlers):
        if getattr(handler, _HANDLER_MARKER, False):
            root.removeHandler(handler)
            handler.close()

    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    setattr(console, _HANDLER_MARKER, True)
    root.addHandler(console)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        setattr(file_handler, _HANDLER_MARKER, True)
        root.addHandler(file_handler)

    # py4j is extremely chatty at INFO/DEBUG level.
    logging.getLogger("py4j").setLevel(logging.WARNING)


def format_fields(**fields: object) -> str:
    """Render keyword arguments as ``key=value`` pairs for structured log messages."""
    return " ".join(f"{key}={value}" for key, value in fields.items())
