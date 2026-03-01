# File: libs/perf/logger.py
"""Shared logging setup for S3NTINEL runtime modules."""

from __future__ import annotations

import logging


def get_logger(name: str = "s3ntinel") -> logging.Logger:
    """Get a configured logger with a stable default formatter."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger
