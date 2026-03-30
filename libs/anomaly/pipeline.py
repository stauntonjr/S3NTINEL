"""Spark adapters for anomaly attribution artifacts."""

from __future__ import annotations

from dataclasses import dataclass

from libs.anomaly.frames import (
    AnomalyAttributionContextFrame,
    AnomalyPanelContextFrame,
    AnomalySubsystemContextFrame,
)
from libs.anomaly.tables import (
    AnomalyEventAttributionTable,
    AnomalyTelemetryAttributionTable,
    AnomalyWindowAttributionTable,
)
from libs.perf.annotations import hot_path


@dataclass(frozen=True)
class AnomalyArtifactSet:
    window_attribution: AnomalyWindowAttributionTable
    telemetry_attribution: AnomalyTelemetryAttributionTable
    event_attribution: AnomalyEventAttributionTable


@dataclass(frozen=True)
class AnomalyAttributionPlan:
    top_k_per_subsystem: int = 5

    def build_window_attribution(
        self,
        *,
        calibrated_df: "DataFrame",
        phase_windows_df: "DataFrame",
        windows_df: "DataFrame",
        events_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
        raw_df: "DataFrame",
    ) -> AnomalyWindowAttributionTable:
        attribution_context = AnomalyAttributionContextFrame.from_context_frames(
            subsystem_context=AnomalySubsystemContextFrame.from_events_and_windows(
                events_df=events_df,
                windows_df=windows_df,
                hierarchy_sensor_map_df=hierarchy_sensor_map_df,
                top_k_per_subsystem=self.top_k_per_subsystem,
            ),
            panel_context=AnomalyPanelContextFrame.from_raw_and_windows(
                raw_df=raw_df,
                windows_df=windows_df,
            ),
        )
        return AnomalyWindowAttributionTable.from_calibrated_windows_and_context(
            calibrated_df=calibrated_df,
            phase_windows_df=phase_windows_df,
            windows_df=windows_df,
            attribution_context_df=attribution_context.to_dataframe(),
        )

    def build_telemetry_attribution(
        self,
        *,
        calibrated_df: "DataFrame",
        windows_df: "DataFrame",
        raw_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
    ) -> AnomalyTelemetryAttributionTable:
        return AnomalyTelemetryAttributionTable.from_calibrated_windows_raw_and_hierarchy(
            calibrated_df=calibrated_df,
            windows_df=windows_df,
            raw_df=raw_df,
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        )

    def build_event_attribution(
        self,
        *,
        calibrated_df: "DataFrame",
        windows_df: "DataFrame",
        events_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
    ) -> AnomalyEventAttributionTable:
        return AnomalyEventAttributionTable.from_calibrated_windows_events_and_hierarchy(
            calibrated_df=calibrated_df,
            windows_df=windows_df,
            events_df=events_df,
            hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        )

    @hot_path
    def build(
        self,
        *,
        calibrated_df: "DataFrame",
        phase_windows_df: "DataFrame",
        windows_df: "DataFrame",
        events_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
        raw_df: "DataFrame",
    ) -> AnomalyArtifactSet:
        return AnomalyArtifactSet(
            window_attribution=self.build_window_attribution(
                calibrated_df=calibrated_df,
                phase_windows_df=phase_windows_df,
                windows_df=windows_df,
                events_df=events_df,
                hierarchy_sensor_map_df=hierarchy_sensor_map_df,
                raw_df=raw_df,
            ),
            telemetry_attribution=self.build_telemetry_attribution(
                calibrated_df=calibrated_df,
                windows_df=windows_df,
                raw_df=raw_df,
                hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            ),
            event_attribution=self.build_event_attribution(
                calibrated_df=calibrated_df,
                windows_df=windows_df,
                events_df=events_df,
                hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            ),
        )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
