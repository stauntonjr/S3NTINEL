# File: libs/__init__.py
"""Core library package for S3NTINEL."""

from libs.perf.annotations import hot_path, is_hot_path

__all__ = ["hot_path", "is_hot_path"]
