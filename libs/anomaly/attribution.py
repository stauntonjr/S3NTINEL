"""Thin anomaly attribution dataframe adapters."""

from __future__ import annotations

from libs.anomaly.model import (
    AnomalyAttributionContext,
    AnomalyEventAttribution,
    AnomalyTelemetryAttribution,
    AnomalyWindowAttribution,
)
from libs.anomaly.panel import build_window_panel_context_df
from libs.anomaly.subsystem import build_window_subsystem_attribution_context_df
from libs.perf.annotations import hot_path


@hot_path
def build_anomaly_telemetry_attribution_df(
    calibrated_df: "DataFrame",
    windows_df: "DataFrame",
    raw_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
) -> "DataFrame":
    return AnomalyTelemetryAttribution.from_frames(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        raw_df=raw_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
    ).dataframe


@hot_path
def build_anomaly_event_attribution_df(
    calibrated_df: "DataFrame",
    windows_df: "DataFrame",
    events_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
) -> "DataFrame":
    return AnomalyEventAttribution.from_frames(
        calibrated_df=calibrated_df,
        windows_df=windows_df,
        events_df=events_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
    ).dataframe


@hot_path
def build_anomaly_window_attribution_df(
    calibrated_df: "DataFrame",
    phase_windows_df: "DataFrame",
    windows_df: "DataFrame",
    events_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
    raw_df: "DataFrame",
    top_k_per_subsystem: int = 5,
) -> "DataFrame":
    attribution_context = AnomalyAttributionContext.from_frames(
        subsystem_context_df=build_window_subsystem_attribution_context_df(
            events_df=events_df,
            windows_df=windows_df,
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            top_k_per_subsystem=top_k_per_subsystem,
        ),
        panel_context_df=build_window_panel_context_df(
            raw_df=raw_df,
            windows_df=windows_df,
        ),
    )
    return AnomalyWindowAttribution.from_frames(
        calibrated_df=calibrated_df,
        phase_windows_df=phase_windows_df,
        windows_df=windows_df,
        attribution_context_df=attribution_context.dataframe,
    ).dataframe


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
