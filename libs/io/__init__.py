# File: libs/io/__init__.py
"""I/O and contracts for Spark-backed V2 tables."""

from libs.io.pandas_spark import pandas_records_for_spark, spark_safe_value

__all__ = ["pandas_records_for_spark", "spark_safe_value"]
