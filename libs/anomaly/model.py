"""Anomaly attribution model objects and dataframe assembly."""

from __future__ import annotations

from dataclasses import dataclass

from libs.perf.annotations import hot_path


@dataclass(frozen=True)
class AnomalyAttributionContext:
    dataframe: "DataFrame"

    @classmethod
    @hot_path
    def from_frames(
        cls,
        *,
        subsystem_context_df: "DataFrame",
        panel_context_df: "DataFrame",
    ) -> "AnomalyAttributionContext":
        from pyspark.sql import functions as F

        null_panel_context = F.lit(None).cast(
            "struct<text:array<string>,message_codes:array<string>,source:array<string>>"
        )
        empty_sensor_scores = F.expr("cast(map() as map<string,double>)")
        dataframe = (
            subsystem_context_df.alias("s")
            .join(panel_context_df.alias("p"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="full_outer")
            .select(
                F.coalesce(F.col("s.tail_id"), F.col("p.tail_id")).alias("tail_id"),
                F.coalesce(F.col("s.flight_id"), F.col("p.flight_id")).alias("flight_id"),
                F.coalesce(F.col("s.win_id"), F.col("p.win_id")).alias("win_id"),
                F.coalesce(F.col("s.date_utc"), F.col("p.date_utc")).alias("date_utc"),
                F.coalesce(F.col("s.top_sensors_by_subsystem"), F.expr("cast(map() as map<string,array<struct<parameter_name:string,sensor_score:double,event_score:double,categorical_event_score:double>>>)")).alias("top_sensors_by_subsystem"),
                F.coalesce(F.col("s.sensor_scores"), empty_sensor_scores).alias("sensor_scores"),
                F.coalesce(F.col("p.panel_context"), null_panel_context).alias("panel_context"),
            )
        )
        return cls(dataframe=dataframe)


@dataclass(frozen=True)
class AnomalyTelemetryAttribution:
    dataframe: "DataFrame"

    @classmethod
    @hot_path
    def from_frames(
        cls,
        *,
        calibrated_df: "DataFrame",
        windows_df: "DataFrame",
        raw_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
    ) -> "AnomalyTelemetryAttribution":
        from pyspark.sql import functions as F

        datatype_col = (
            F.col("r.parameter_datatype_label")
            if "parameter_datatype_label" in raw_df.columns
            else F.lit(None).cast("string")
        )
        dataframe = (
            calibrated_df.alias("c")
            .where(F.col("emit_ready") == F.lit(True))
            .join(windows_df.alias("w"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="inner")
            .join(
                raw_df.alias("r"),
                on=(
                    (F.col("c.tail_id") == F.col("r.tail_id"))
                    & (F.col("c.flight_id") == F.col("r.flight_id"))
                    & (F.col("r.timestamp_utc") >= F.col("w.t_start"))
                    & (F.col("r.timestamp_utc") <= F.col("w.t_end"))
                ),
                how="inner",
            )
            .join(hierarchy_sensor_map_df.alias("h"), on=F.col("r.parameter_name") == F.col("h.parameter_name"), how="left")
            .select(
                F.col("c.tail_id").alias("tail_id"),
                F.col("c.flight_id").alias("flight_id"),
                F.col("c.win_id").alias("win_id"),
                F.col("r.timestamp_utc").alias("timestamp_utc"),
                F.col("r.parameter_name").alias("parameter_name"),
                F.col("r.parameter_value").alias("parameter_value"),
                datatype_col.alias("parameter_datatype_label"),
                F.col("h.system_id").alias("system_id"),
                F.col("h.subsystem_id").alias("subsystem_id"),
                F.col("h.module_id").alias("module_id"),
                F.col("c.global_score").cast("double").alias("window_global_score"),
                F.col("c.severity").alias("severity"),
                F.col("c.date_utc").alias("date_utc"),
            )
        )
        return cls(dataframe=dataframe)


@dataclass(frozen=True)
class AnomalyEventAttribution:
    dataframe: "DataFrame"

    @classmethod
    @hot_path
    def from_frames(
        cls,
        *,
        calibrated_df: "DataFrame",
        windows_df: "DataFrame",
        events_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
    ) -> "AnomalyEventAttribution":
        from pyspark.sql import functions as F

        anomaly_type_col = (
            F.col("e.anomaly_type_detected")
            if "anomaly_type_detected" in events_df.columns
            else F.lit(None).cast("string")
        )
        anomaly_score_col = (
            F.col("e.anomaly_score_detected")
            if "anomaly_score_detected" in events_df.columns
            else F.lit(None).cast("double")
        )
        dataframe = (
            calibrated_df.alias("c")
            .where(F.col("emit_ready") == F.lit(True))
            .join(windows_df.alias("w"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="inner")
            .join(
                events_df.alias("e"),
                on=(
                    (F.col("c.tail_id") == F.col("e.tail_id"))
                    & (F.col("c.flight_id") == F.col("e.flight_id"))
                    & (F.col("e.timestamp_utc") >= F.col("w.t_start"))
                    & (F.col("e.timestamp_utc") <= F.col("w.t_end"))
                ),
                how="inner",
            )
            .join(hierarchy_sensor_map_df.alias("h"), on=F.col("e.parameter_name") == F.col("h.parameter_name"), how="left")
            .select(
                F.col("c.tail_id").alias("tail_id"),
                F.col("c.flight_id").alias("flight_id"),
                F.col("c.win_id").alias("win_id"),
                F.col("e.timestamp_utc").alias("timestamp_utc"),
                F.col("e.parameter_name").alias("parameter_name"),
                F.col("e.event_type_detected").alias("event_type_detected"),
                anomaly_type_col.alias("anomaly_type_detected"),
                anomaly_score_col.cast("double").alias("anomaly_score_detected"),
                F.col("h.system_id").alias("system_id"),
                F.col("h.subsystem_id").alias("subsystem_id"),
                F.col("h.module_id").alias("module_id"),
                F.col("c.global_score").cast("double").alias("window_global_score"),
                F.col("c.severity").alias("severity"),
                F.col("c.date_utc").alias("date_utc"),
            )
        )
        return cls(dataframe=dataframe)


@dataclass(frozen=True)
class AnomalyWindowAttribution:
    dataframe: "DataFrame"

    @classmethod
    @hot_path
    def from_frames(
        cls,
        *,
        calibrated_df: "DataFrame",
        phase_windows_df: "DataFrame",
        windows_df: "DataFrame",
        attribution_context_df: "DataFrame",
    ) -> "AnomalyWindowAttribution":
        from pyspark.sql import functions as F

        empty_sensor_scores = F.expr("cast(map() as map<string,double>)")
        null_panel_context = F.lit(None).cast(
            "struct<text:array<string>,message_codes:array<string>,source:array<string>>"
        )
        base = (
            calibrated_df.alias("c")
            .join(phase_windows_df.alias("p"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")
            .join(windows_df.alias("w"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")
            .join(attribution_context_df.alias("a"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")
            .where(F.col("c.emit_ready") == F.lit(True))
        )

        dataframe = base.select(
            F.col("c.tail_id").alias("tail_id"),
            F.col("c.flight_id").alias("flight_id"),
            F.col("c.win_id").alias("win_id"),
            F.coalesce(F.col("w.t_end"), F.col("w.t_start"), F.current_timestamp()).alias("timestamp_utc"),
            F.col("c.phase_state_detected").alias("phase_state_detected"),
            F.col("c.phase_id_detected").cast("int").alias("phase_id_detected"),
            F.col("c.phase_confidence_detected").cast("double").alias("phase_confidence_detected"),
            F.col("c.distance_to_centroid_detected").cast("double").alias("distance_to_centroid_detected"),
            F.col("c.drift_magnitude").cast("double").alias("drift_magnitude"),
            F.col("c.breadth").cast("double").alias("breadth"),
            F.col("c.global_score").cast("double").alias("global_score"),
            F.col("c.p_value").cast("double").alias("p_value"),
            F.col("c.severity").alias("severity"),
            F.col("c.dominant_subsystem_id").alias("dominant_subsystem_id"),
            F.col("c.dominant_score_component").alias("dominant_score_component"),
            F.coalesce(F.col("a.panel_context"), null_panel_context).alias("panel_context"),
            F.expr(
                """
                transform(
                  map_entries(coalesce(c.subsystem_scores, cast(map() as map<string,double>))),
                  x -> named_struct(
                    'id', x.key,
                    'name', x.key,
                    'score', cast(x.value as double),
                    'score_component_contrib', map(
                        'structure', cast(coalesce(c.score_component_scores['structure'], 0D) * x.value as double),
                        'reconstruction', cast(coalesce(c.score_component_scores['reconstruction'], 0D) * x.value as double)
                    ),
                    'top_sensors', coalesce(
                        element_at(a.top_sensors_by_subsystem, x.key),
                        cast(array() as array<struct<parameter_name:string,sensor_score:double,event_score:double,categorical_event_score:double>>)
                    )
                  )
                )
                """
            ).alias("subsystems"),
            F.struct(
                F.col("c.score_component_scores").alias("score_component_scores"),
                F.coalesce(F.col("a.sensor_scores"), empty_sensor_scores).alias("sensor_scores"),
            ).alias("attribution_context"),
            F.struct(
                F.lit(1).alias("backbone"),
                F.lit(1).alias("graph"),
                F.lit(1).alias("phase"),
                F.lit(1).alias("scoring"),
                F.lit(1).alias("calibration"),
            ).alias("artifact_versions"),
            F.col("c.date_utc").alias("date_utc"),
        )
        return cls(dataframe=dataframe)


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
