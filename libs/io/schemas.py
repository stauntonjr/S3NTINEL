# File: libs/io/schemas.py
"""Canonical schema contracts for v1 tables."""

from __future__ import annotations


RAW_TELEMETRY_COLUMNS = [
    "tail_id",
    "flight_id",
    "timestamp_utc",
    "parameter_name",
    "parameter_value",
    "date_utc",
]

EVENTS_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "ts",
    "sensor",
    "subsystem",
    "event_type",
    "payload",
    "date_utc",
]

WINDOWS_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "t_start",
    "t_end",
    "duration_ms",
    "event_count",
    "zoh_version",
    "date_utc",
]

SIGNATURES_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "phase_id",
    "sig_version",
    "pivot_block",
    "cur_block",
    "event_block",
    "cat_block",
    "breadth",
    "drift_mag",
    "drift_dir",
    "date_utc",
]

ANOMALIES_COLUMNS = [
    "tail_id",
    "flight_id",
    "win_id",
    "ts",
    "phase_state",
    "phase_id",
    "phase_confidence",
    "distance_to_centroid",
    "drift_magnitude",
    "breadth",
    "global_score",
    "p_value",
    "severity",
    "dominant_subsystem",
    "dominant_block",
    "panel_context",
    "subsystems",
    "raw",
    "versions",
    "date_utc",
]
