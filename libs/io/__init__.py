# File: libs/io/__init__.py
"""I/O and contracts for Spark-backed V2 tables."""

from libs.io.contracts import (
    AdaptiveWindowRow,
    DatatypeLabelRow,
    DatatypeProfiledRow,
    DetectedEventRow,
    EventLabelRow,
    EventValidatorSnapshot,
    PhaseAssignmentRow,
    PhaseBaselineRow,
    PhaseWindowRow,
    ProfilerValidatorSnapshot,
    TelemetryRow,
    WindowScoreRow,
    WindowXRow,
)
from libs.io.pandas_spark import pandas_records_for_spark, spark_safe_value

__all__ = [
    "TelemetryRow",
    "DetectedEventRow",
    "EventLabelRow",
    "AdaptiveWindowRow",
    "WindowXRow",
    "PhaseAssignmentRow",
    "PhaseBaselineRow",
    "PhaseWindowRow",
    "WindowScoreRow",
    "DatatypeLabelRow",
    "DatatypeProfiledRow",
    "EventValidatorSnapshot",
    "ProfilerValidatorSnapshot",
    "pandas_records_for_spark",
    "spark_safe_value",
]
