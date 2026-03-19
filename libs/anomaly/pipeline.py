"""Spark adapters for anomaly attribution artifacts."""

from __future__ import annotations

from libs.anomaly.artifacts import (
    build_anomaly_attribution_context_table,
    build_anomaly_event_attribution_table,
    build_anomaly_telemetry_attribution_table,
    build_anomaly_window_attribution_from_context_table,
)
from libs.anomaly.panel_context import build_window_panel_context_table
from libs.anomaly.subsystem_context import build_window_subsystem_context_table
from libs.perf.annotations import hot_path


@hot_path
def build_anomaly_window_attribution_table(
    calibrated_df: "DataFrame",
    phase_windows_df: "DataFrame",
    windows_df: "DataFrame",
    events_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
    raw_df: "DataFrame",
    top_k_per_subsystem: int = 5,
) -> "DataFrame":
    attribution_context_df = build_anomaly_attribution_context_table(
        subsystem_context_df=build_window_subsystem_context_table(
            events_df=events_df,
            windows_df=windows_df,
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            top_k_per_subsystem=top_k_per_subsystem,
        ),
        panel_context_df=build_window_panel_context_table(
            raw_df=raw_df,
            windows_df=windows_df,
        ),
    )
    return build_anomaly_window_attribution_from_context_table(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        attribution_context_df=attribution_context_df,
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
