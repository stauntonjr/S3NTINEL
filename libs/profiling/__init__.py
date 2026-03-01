# File: libs/profiling/__init__.py
"""Telemetry profiling, routing, and synthetic generation utilities."""

from libs.profiling.profile import build_categorical_distribution, build_parameter_profile
from libs.profiling.routing import build_channel_routing
from libs.profiling.synthetic import (
    ParameterSpec,
    SyntheticTelemetryRecord,
    default_parameter_specs,
    generate_synthetic_normal_telemetry,
    iter_parameter_records,
    iter_synthetic_telemetry_records,
    iter_synthetic_telemetry_rows,
)

__all__ = [
    "build_parameter_profile",
    "build_categorical_distribution",
    "build_channel_routing",
    "ParameterSpec",
    "SyntheticTelemetryRecord",
    "default_parameter_specs",
    "iter_parameter_records",
    "iter_synthetic_telemetry_records",
    "iter_synthetic_telemetry_rows",
    "generate_synthetic_normal_telemetry",
]
