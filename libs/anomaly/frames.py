"""Typed Spark frames for reusable anomaly attribution context."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.perf.annotations import hot_path
from libs.pyspark import Frame
from libs.scoring.channels import (
    ACCUMULATION_VIOLATION_CHANNEL,
    BOUND_VIOLATION_CHANNEL,
    COHERENCE_BREAK_CHANNEL,
    EVENT_DISCORDANCE_CHANNEL,
    RECONSTRUCTION_ERROR_CHANNEL,
    REGIME_DEVIATION_CHANNEL,
    RESPONSE_VIOLATION_CHANNEL,
    STATE_VIOLATION_CHANNEL,
)

ANOMALY_LOCALIZATION_PARAMETER_TOP_K = 3
ANOMALY_PARAMETER_SELECTION_TOP_K = 5
ANOMALY_LOCALIZATION_TARGET_TOP_K = 3


def _normalized_clamped_avg_expr(*columns: "Column") -> "Column":
    from pyspark.sql import functions as F

    if not columns:
        return F.lit(0.0).cast("double")
    total = F.lit(0.0)
    for column in columns:
        total = total + F.least(F.lit(1.0), F.greatest(F.coalesce(column.cast("double"), F.lit(0.0)), F.lit(0.0)))
    return (total / F.lit(float(len(columns)))).cast("double")


def _component_score_expr(component_scores_col: str, channel_name: str) -> "Column":
    from pyspark.sql import functions as F

    return F.coalesce(F.element_at(F.col(component_scores_col), F.lit(channel_name)), F.lit(0.0)).cast("double")


def _log_component_weight_expr(component_scores_col: str, channel_name: str) -> "Column":
    from pyspark.sql import functions as F

    component_mass_cols = [
        F.log1p(_component_score_expr(component_scores_col, REGIME_DEVIATION_CHANNEL)),
        F.log1p(_component_score_expr(component_scores_col, RECONSTRUCTION_ERROR_CHANNEL)),
        F.log1p(_component_score_expr(component_scores_col, EVENT_DISCORDANCE_CHANNEL)),
        F.log1p(_component_score_expr(component_scores_col, BOUND_VIOLATION_CHANNEL)),
        F.log1p(_component_score_expr(component_scores_col, ACCUMULATION_VIOLATION_CHANNEL)),
        F.log1p(_component_score_expr(component_scores_col, RESPONSE_VIOLATION_CHANNEL)),
        F.log1p(_component_score_expr(component_scores_col, STATE_VIOLATION_CHANNEL)),
        F.log1p(_component_score_expr(component_scores_col, COHERENCE_BREAK_CHANNEL)),
    ]
    component_mass_total = component_mass_cols[0]
    for component_mass_col in component_mass_cols[1:]:
        component_mass_total = component_mass_total + component_mass_col
    return F.when(
        component_mass_total > F.lit(0.0),
        F.log1p(_component_score_expr(component_scores_col, channel_name)) / component_mass_total,
    ).otherwise(F.lit(0.0)).cast("double")


def mapped_events_in_supported_windows(
    *,
    events_df: "DataFrame",
    windows_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    duration_ms = (
        F.col("w.duration_ms").cast("long")
        if "duration_ms" in windows_df.columns
        else (F.unix_millis("w.t_end") - F.unix_millis("w.t_start")).cast("long")
    )
    support_shoulder_ms = F.greatest(duration_ms, F.lit(1).cast("long"))
    support_start = F.timestamp_millis(F.unix_millis("w.t_start") - support_shoulder_ms)
    support_end = F.timestamp_millis(F.unix_millis("w.t_end") + support_shoulder_ms)

    mapped_events = events_df.join(
        hierarchy_sensor_map_df.select("parameter_name", "system_id", "subsystem_id", "module_id"),
        on="parameter_name",
        how="inner",
    )
    return (
        mapped_events.alias("e")
        .join(
            windows_df.alias("w"),
            on=(
                (F.col("e.tail_id") == F.col("w.tail_id"))
                & (F.col("e.flight_id") == F.col("w.flight_id"))
                & (F.col("e.timestamp_utc") >= support_start)
                & (F.col("e.timestamp_utc") <= support_end)
            ),
            how="inner",
        )
        .select(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.col("w.date_utc").alias("date_utc"),
            F.col("e.timestamp_utc").alias("timestamp_utc"),
            F.col("e.parameter_name").alias("parameter_name"),
            F.col("e.event_type_detected").alias("event_type_detected"),
            F.col("e.anomaly_type_detected").alias("anomaly_type_detected"),
            F.col("e.anomaly_score_detected").alias("anomaly_score_detected"),
            F.col("e.system_id").alias("system_id"),
            F.col("e.subsystem_id").alias("subsystem_id"),
            F.col("e.module_id").alias("module_id"),
        )
    )


@dataclass(frozen=True)
class AnomalySubsystemContextFrame(Frame):
    @classmethod
    @hot_path
    def from_events_and_windows(
        cls,
        events_df: "DataFrame",
        windows_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
        *,
        top_k_per_subsystem: int,
    ) -> "AnomalySubsystemContextFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        events_in_windows = (
            mapped_events_in_supported_windows(
                events_df=events_df,
                windows_df=windows_df,
                hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            )
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                "subsystem_id",
                "parameter_name",
                "event_type_detected",
            )
        )

        sensor_counts = events_in_windows.groupBy(
            "tail_id", "flight_id", "win_id", "date_utc", "subsystem_id", "parameter_name"
        ).agg(
            F.count("*").alias("sensor_event_count"),
            F.sum(
                F.when(
                    F.col("event_type_detected").isin(
                        "transition",
                        "dropped",
                        "state_enter",
                        "state_exit",
                        "dwell_bucket",
                        "dwell_guard",
                        "dwell_violation",
                        "illegal_transition",
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("categorical_count"),
        )

        subsystem_totals = sensor_counts.groupBy(
            "tail_id", "flight_id", "win_id", "date_utc", "subsystem_id"
        ).agg(F.sum("sensor_event_count").alias("subsystem_event_total"))

        with_scores = (
            sensor_counts.alias("s")
            .join(
                subsystem_totals.alias("t"),
                on=["tail_id", "flight_id", "win_id", "date_utc", "subsystem_id"],
                how="inner",
            )
            .withColumn(
                "sensor_score",
                F.when(
                    F.col("subsystem_event_total") > F.lit(0),
                    F.col("sensor_event_count") / F.col("subsystem_event_total"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("events_score", F.col("sensor_score"))
            .withColumn(
                "categorical_score",
                F.when(
                    F.col("subsystem_event_total") > F.lit(0),
                    F.col("categorical_count") / F.col("subsystem_event_total"),
                ).otherwise(F.lit(0.0)),
            )
        )

        rank_window = Window.partitionBy(
            "tail_id", "flight_id", "win_id", "date_utc", "subsystem_id"
        ).orderBy(F.col("sensor_event_count").desc(), F.col("parameter_name").asc())
        top_sensors = (
            with_scores.withColumn("rn", F.row_number().over(rank_window))
            .where(F.col("rn") <= F.lit(max(int(top_k_per_subsystem), 1)))
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                "subsystem_id",
                "rn",
                F.struct(
                    F.col("parameter_name").alias("parameter_name"),
                    F.col("sensor_score").cast("double").alias("sensor_score"),
                    F.col("events_score").cast("double").alias("event_score"),
                    F.col("categorical_score").cast("double").alias("categorical_event_score"),
                ).alias("sensor_struct"),
            )
        )

        top_sensors_by_subsystem = (
            top_sensors.groupBy("tail_id", "flight_id", "win_id", "date_utc", "subsystem_id")
            .agg(F.collect_list(F.struct(F.col("rn"), F.col("sensor_struct"))).alias("ranked_sensors"))
            .withColumn("top_sensors", F.expr("transform(array_sort(ranked_sensors), x -> x.sensor_struct)"))
            .drop("ranked_sensors")
        )

        sensor_scores = (
            top_sensors.groupBy(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.col("sensor_struct.parameter_name").alias("parameter_name"),
            )
            .agg(F.sum(F.col("sensor_struct.sensor_score")).cast("double").alias("sensor_score"))
            .groupBy("tail_id", "flight_id", "win_id", "date_utc")
            .agg(
                F.map_from_entries(F.collect_list(F.struct(F.col("parameter_name"), F.col("sensor_score")))).alias(
                    "sensor_scores"
                )
            )
        )

        top_sensors_map = top_sensors_by_subsystem.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
            F.map_from_entries(F.collect_list(F.struct(F.col("subsystem_id"), F.col("top_sensors")))).alias(
                "top_sensors_by_subsystem"
            )
        )

        return cls(
            dataframe=top_sensors_map.join(sensor_scores, on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")
        )


@dataclass(frozen=True)
class AnomalyPanelContextFrame(Frame):
    @classmethod
    @hot_path
    def from_raw_and_windows(
        cls,
        raw_df: "DataFrame",
        windows_df: "DataFrame",
        *,
        max_items: int = 5,
    ) -> "AnomalyPanelContextFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        ts_col = "timestamp_utc" if "timestamp_utc" in raw_df.columns else ("ts" if "ts" in raw_df.columns else None)
        if ts_col is None:
            return cls(
                dataframe=windows_df.select("tail_id", "flight_id", "win_id", "date_utc")
                .where(F.lit(False))
                .select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    F.lit(None)
                    .cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>")
                    .alias("panel_context"),
                )
            )

        name_col = "parameter_name" if "parameter_name" in raw_df.columns else ("sensor" if "sensor" in raw_df.columns else None)
        value_col = "parameter_value" if "parameter_value" in raw_df.columns else ("state" if "state" in raw_df.columns else None)
        if name_col is None or value_col is None:
            return cls(
                dataframe=windows_df.select("tail_id", "flight_id", "win_id", "date_utc")
                .where(F.lit(False))
                .select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    F.lit(None)
                    .cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>")
                    .alias("panel_context"),
                )
            )

        keyword_expr = "(lcd|panel|msg|message|cas|warn|warning|fault|caution|annunc|text)"
        candidates = (
            raw_df.select(
                F.col("tail_id"),
                F.col("flight_id"),
                F.col(ts_col).cast("timestamp").alias("ts"),
                F.col(name_col).cast("string").alias("source_name"),
                F.trim(F.col(value_col).cast("string")).alias("text_value"),
                F.col("date_utc"),
            )
            .where(F.col("ts").isNotNull())
            .where(F.col("source_name").isNotNull())
            .where(F.col("text_value").isNotNull() & (F.col("text_value") != F.lit("")))
            .where(F.expr("try_cast(text_value as double) is null"))
            .withColumn(
                "keyword_hit",
                (F.lower(F.col("source_name")).rlike(keyword_expr) | F.lower(F.col("text_value")).rlike(keyword_expr)).cast(
                    "int"
                ),
            )
        )

        events_in_windows = (
            candidates.alias("r")
            .join(
                windows_df.alias("w"),
                on=(
                    (F.col("r.tail_id") == F.col("w.tail_id"))
                    & (F.col("r.flight_id") == F.col("w.flight_id"))
                    & (F.col("r.ts") >= F.col("w.t_start"))
                    & (F.col("r.ts") <= F.col("w.t_end"))
                ),
                how="inner",
            )
            .select(
                F.col("w.tail_id").alias("tail_id"),
                F.col("w.flight_id").alias("flight_id"),
                F.col("w.win_id").alias("win_id"),
                F.col("w.date_utc").alias("date_utc"),
                F.col("r.ts").alias("ts"),
                F.col("r.source_name").alias("source_name"),
                F.col("r.text_value").alias("text_value"),
                F.col("r.keyword_hit").alias("keyword_hit"),
            )
        )

        rank_window = Window.partitionBy("tail_id", "flight_id", "win_id", "date_utc").orderBy(
            F.col("keyword_hit").desc(),
            F.col("ts").desc(),
            F.col("source_name").asc(),
            F.col("text_value").asc(),
        )

        limited = (
            events_in_windows.withColumn("rn", F.row_number().over(rank_window))
            .where(F.col("rn") <= F.lit(max(int(max_items), 1)))
            .groupBy("tail_id", "flight_id", "win_id", "date_utc")
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("rn").alias("rn"),
                        F.col("text_value").alias("text_value"),
                        F.col("source_name").alias("source_name"),
                    )
                ).alias("items")
            )
            .withColumn("ordered_items", F.expr("array_sort(items)"))
            .drop("items")
        )

        return cls(
            dataframe=limited.select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.struct(
                    F.array_sort(F.array_distinct(F.expr("transform(ordered_items, x -> x.text_value)"))).alias("text"),
                    F.array_sort(
                        F.array_distinct(
                            F.expr(
                                "filter(transform(ordered_items, x -> regexp_extract(x.text_value, '([A-Z]{2,}[A-Z0-9_-]*)', 1)), x -> x != '')"
                            )
                        )
                    ).alias("message_codes"),
                    F.array_sort(F.array_distinct(F.expr("transform(ordered_items, x -> x.source_name)"))).alias("source"),
                ).alias("panel_context"),
            )
        )


@dataclass(frozen=True)
class AnomalyParameterLocalizationFrame(Frame):
    def _module_support_df(self) -> "DataFrame":
        from pyspark.sql import functions as F

        support_df = self.to_dataframe()
        return support_df.groupBy(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            "subsystem_id",
            "module_id",
        ).agg(
            F.sum("parameter_localization_support").cast("double").alias("module_support"),
            F.sum(
                F.col("parameter_localization_support")
                / F.greatest(F.col("parameter_support_rank_in_window").cast("double"), F.lit(1.0))
            )
            .cast("double")
            .alias("module_rank_weighted_support"),
            F.max("parameter_localization_support").cast("double").alias("module_peak_support"),
            F.min("parameter_support_rank_in_window").cast("int").alias("module_best_rank"),
        )

    @classmethod
    @hot_path
    def from_calibrated_phase_windows_events_and_hierarchy(
        cls,
        *,
        calibrated_df: "DataFrame",
        phase_windows_df: "DataFrame",
        events_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
        parameter_behavior_profile_df: "DataFrame | None" = None,
        top_k_per_window: int = ANOMALY_PARAMETER_SELECTION_TOP_K,
    ) -> "AnomalyParameterLocalizationFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        empty_double_map = F.expr("cast(map() as map<string,double>)")
        required_phase_window_cols = {"t_start", "t_end", "backbone_residual_by_parameter"}
        if not required_phase_window_cols.issubset(set(phase_windows_df.columns)):
            return cls(
                dataframe=calibrated_df.select("tail_id", "flight_id", "win_id", "date_utc")
                .where(F.lit(False))
                .select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    F.lit(None).cast("string").alias("parameter_name"),
                    F.lit(None).cast("string").alias("system_id"),
                    F.lit(None).cast("string").alias("subsystem_id"),
                    F.lit(None).cast("string").alias("module_id"),
                    F.lit(None).cast("double").alias("parameter_localization_support"),
                    F.lit(None).cast("int").alias("parameter_support_rank_in_window"),
                )
            )
        emitted_score_context_df = calibrated_df.where(F.col("emit_ready") == F.lit(True)).groupBy(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
        ).agg(
            F.first(F.coalesce(F.col("score_component_scores"), empty_double_map), ignorenulls=True).alias(
                "score_component_scores"
            ),
            F.first(F.col("dominant_score_component").cast("string"), ignorenulls=True).alias("dominant_score_component"),
        )
        emitted_phase_windows_df = emitted_score_context_df.join(
            phase_windows_df.select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                "t_start",
                "t_end",
                (
                    F.coalesce(
                        F.col("duration_ms").cast("long"),
                        (F.unix_millis("t_end") - F.unix_millis("t_start")).cast("long"),
                    )
                ).alias("duration_ms"),
                "backbone_residual_by_parameter",
            ),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="inner",
        )
        hierarchy_localization_df = (
            hierarchy_sensor_map_df.select(
                F.col("parameter_name").cast("string").alias("parameter_name"),
                F.col("system_id").cast("string").alias("system_id"),
                F.col("subsystem_id").cast("string").alias("subsystem_id"),
                F.coalesce(F.col("module_id").cast("string"), F.col("parameter_name").cast("string")).alias("module_id"),
            )
            .where(F.col("parameter_name").isNotNull() & F.col("subsystem_id").isNotNull())
            .dropDuplicates(["parameter_name"])
        )
        module_size_df = hierarchy_localization_df.groupBy("subsystem_id", "module_id").agg(
            F.countDistinct("parameter_name").cast("double").alias("module_parameter_count")
        )
        parameter_event_counts_df = (
            mapped_events_in_supported_windows(
                events_df=events_df,
                windows_df=emitted_phase_windows_df.select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    "t_start",
                    "t_end",
                    "duration_ms",
                ),
                hierarchy_sensor_map_df=hierarchy_sensor_map_df,
            )
            .groupBy("tail_id", "flight_id", "win_id", "date_utc", "parameter_name")
            .agg(
                F.count("*").cast("double").alias("event_support_count"),
                F.sum(
                    F.when(
                        F.col("event_type_detected").isin("threshold", "drift_guard"),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("bound_event_count"),
                F.sum(
                    F.when(
                        F.col("event_type_detected").isin(
                            "threshold",
                            "slope_pos",
                            "slope_neg",
                            "switch",
                            "extrema",
                            "oscillation",
                            "drift_guard",
                        ),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("response_event_count"),
                F.sum(
                    F.when(
                        F.col("event_type_detected").isin(
                            "state_enter",
                            "state_exit",
                            "dropped",
                            "dwell_bucket",
                            "transition",
                            "dwell_violation",
                            "illegal_transition",
                            "dwell_guard",
                        ),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("state_event_count"),
            )
        )
        residual_rows_df = (
            emitted_phase_windows_df.select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.explode_outer(
                    F.map_entries(
                        F.coalesce(F.col("backbone_residual_by_parameter"), empty_double_map)
                    )
                ).alias("residual_entry"),
            )
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.col("residual_entry.key").cast("string").alias("parameter_name"),
                F.abs(F.col("residual_entry.value").cast("double")).alias("residual_weight"),
            )
            .where(F.col("parameter_name").isNotNull() & (F.col("residual_weight") > F.lit(0.0)))
        )
        residual_totals_df = residual_rows_df.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
            F.sum("residual_weight").cast("double").alias("residual_total_weight")
        )
        candidate_parameter_df = (
            residual_rows_df.select("tail_id", "flight_id", "win_id", "date_utc", "parameter_name")
            .unionByName(parameter_event_counts_df.select("tail_id", "flight_id", "win_id", "date_utc", "parameter_name"))
            .dropDuplicates(["tail_id", "flight_id", "win_id", "date_utc", "parameter_name"])
        )
        event_totals_df = parameter_event_counts_df.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
            F.sum("event_support_count").cast("double").alias("window_event_support_total"),
            F.sum("bound_event_count").cast("double").alias("window_bound_event_total"),
            F.sum("response_event_count").cast("double").alias("window_response_event_total"),
            F.sum("state_event_count").cast("double").alias("window_state_event_total"),
        )
        parameter_support_df = (
            candidate_parameter_df.join(
                emitted_phase_windows_df.select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    "score_component_scores",
                    "dominant_score_component",
                ),
                on=["tail_id", "flight_id", "win_id", "date_utc"],
                how="inner",
            )
            .join(
                residual_rows_df,
                on=["tail_id", "flight_id", "win_id", "date_utc", "parameter_name"],
                how="left",
            )
            .join(
                residual_totals_df,
                on=["tail_id", "flight_id", "win_id", "date_utc"],
                how="left",
            )
            .join(F.broadcast(hierarchy_localization_df), on="parameter_name", how="left")
            .where(F.col("subsystem_id").isNotNull())
            .join(F.broadcast(module_size_df), on=["subsystem_id", "module_id"], how="left")
            .join(
                parameter_event_counts_df,
                on=["tail_id", "flight_id", "win_id", "date_utc", "parameter_name"],
                how="left",
            )
            .join(
                event_totals_df,
                on=["tail_id", "flight_id", "win_id", "date_utc"],
                how="left",
            )
        )
        if parameter_behavior_profile_df is not None:
            parameter_support_df = parameter_support_df.join(
                F.broadcast(
                    parameter_behavior_profile_df.select(
                        "parameter_name",
                        "persistent_run_strength_profiled",
                        "run_reinforcement_score_profiled",
                        "accumulative_score_profiled",
                        "monotone_accumulation_score_profiled",
                        "reset_drop_rate_profiled",
                    )
                ),
                on="parameter_name",
                how="left",
            )
        for column_name in (
            "persistent_run_strength_profiled",
            "run_reinforcement_score_profiled",
            "accumulative_score_profiled",
            "monotone_accumulation_score_profiled",
            "reset_drop_rate_profiled",
        ):
            if column_name not in parameter_support_df.columns:
                parameter_support_df = parameter_support_df.withColumn(column_name, F.lit(0.0).cast("double"))
        parameter_support_df = (
            parameter_support_df.withColumn(
                "residual_share",
                F.coalesce(F.col("residual_weight"), F.lit(0.0))
                / F.greatest(F.coalesce(F.col("residual_total_weight"), F.lit(0.0)), F.lit(1e-12)),
            )
            .withColumn(
                "localized_residual_support",
                (
                    F.col("residual_share")
                    / F.sqrt(F.greatest(F.coalesce(F.col("module_parameter_count"), F.lit(1.0)), F.lit(1.0)))
                ).cast("double"),
            )
            .withColumn(
                "localized_reconstruction_support",
                F.col("localized_residual_support"),
            )
            .withColumn(
                "event_support_share",
                F.coalesce(F.col("event_support_count"), F.lit(0.0))
                / F.greatest(F.coalesce(F.col("window_event_support_total"), F.lit(0.0)), F.lit(1e-12)),
            )
            .withColumn(
                "bound_support_share",
                F.coalesce(F.col("bound_event_count"), F.lit(0.0))
                / F.greatest(F.coalesce(F.col("window_bound_event_total"), F.lit(0.0)), F.lit(1e-12)),
            )
            .withColumn(
                "accumulation_profile_relevance",
                _normalized_clamped_avg_expr(
                    F.col("accumulative_score_profiled"),
                    F.col("monotone_accumulation_score_profiled"),
                    F.col("persistent_run_strength_profiled"),
                    F.col("run_reinforcement_score_profiled"),
                    F.col("reset_drop_rate_profiled"),
                ),
            )
            .withColumn(
                "response_support_share",
                F.coalesce(F.col("response_event_count"), F.lit(0.0))
                / F.greatest(F.coalesce(F.col("window_response_event_total"), F.lit(0.0)), F.lit(1e-12)),
            )
            .withColumn(
                "state_support_share",
                F.coalesce(F.col("state_event_count"), F.lit(0.0))
                / F.greatest(F.coalesce(F.col("window_state_event_total"), F.lit(0.0)), F.lit(1e-12)),
            )
            .withColumn("regime_weight", _log_component_weight_expr("score_component_scores", REGIME_DEVIATION_CHANNEL))
            .withColumn(
                "reconstruction_weight",
                _log_component_weight_expr("score_component_scores", RECONSTRUCTION_ERROR_CHANNEL),
            )
            .withColumn("event_weight", _log_component_weight_expr("score_component_scores", EVENT_DISCORDANCE_CHANNEL))
            .withColumn("bound_weight", _log_component_weight_expr("score_component_scores", BOUND_VIOLATION_CHANNEL))
            .withColumn(
                "accumulation_weight",
                _log_component_weight_expr("score_component_scores", ACCUMULATION_VIOLATION_CHANNEL),
            )
            .withColumn(
                "response_weight",
                _log_component_weight_expr("score_component_scores", RESPONSE_VIOLATION_CHANNEL),
            )
            .withColumn("state_weight", _log_component_weight_expr("score_component_scores", STATE_VIOLATION_CHANNEL))
            .withColumn("coherence_weight", _log_component_weight_expr("score_component_scores", COHERENCE_BREAK_CHANNEL))
            .withColumn(
                "normalized_event_support",
                F.col("event_support_share")
                / F.sqrt(F.greatest(F.coalesce(F.col("module_parameter_count"), F.lit(1.0)), F.lit(1.0))),
            )
            .withColumn(
                "normalized_bound_support",
                F.col("bound_support_share")
                / F.sqrt(F.greatest(F.coalesce(F.col("module_parameter_count"), F.lit(1.0)), F.lit(1.0))),
            )
            .withColumn(
                "normalized_accumulation_support",
                (F.col("localized_residual_support") * F.col("accumulation_profile_relevance")).cast("double"),
            )
            .withColumn(
                "normalized_response_support",
                F.col("response_support_share")
                / F.sqrt(F.greatest(F.coalesce(F.col("module_parameter_count"), F.lit(1.0)), F.lit(1.0))),
            )
            .withColumn(
                "normalized_state_support",
                F.col("state_support_share")
                / F.sqrt(F.greatest(F.coalesce(F.col("module_parameter_count"), F.lit(1.0)), F.lit(1.0))),
            )
            .withColumn(
                "localized_reconstruction_support",
                F.col("localized_residual_support"),
            )
            .withColumn(
                "parameter_localization_support",
                (
                    F.col("localized_residual_support") * (F.col("regime_weight") + F.col("coherence_weight"))
                    + F.col("localized_reconstruction_support") * F.col("reconstruction_weight")
                    + F.col("normalized_event_support") * F.col("event_weight")
                    + F.col("normalized_bound_support") * F.col("bound_weight")
                    + F.col("normalized_accumulation_support") * F.col("accumulation_weight")
                    + F.col("normalized_response_support") * F.col("response_weight")
                    + F.col("normalized_state_support") * F.col("state_weight")
                ).cast("double"),
            )
        )
        module_support_df = parameter_support_df.groupBy(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            "subsystem_id",
            "module_id",
        ).agg(
            F.sqrt(F.sum(F.pow(F.col("parameter_localization_support"), F.lit(2.0)))).cast("double").alias("module_support"),
            F.sum("parameter_localization_support").cast("double").alias("module_total_support"),
            F.max("parameter_localization_support").cast("double").alias("module_peak_support"),
        )
        module_support_totals_df = module_support_df.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
            F.sum("module_support").cast("double").alias("module_support_total")
        )
        parameter_support_df = (
            parameter_support_df.join(
                module_support_df,
                on=["tail_id", "flight_id", "win_id", "date_utc", "subsystem_id", "module_id"],
                how="left",
            )
            .join(
                module_support_totals_df,
                on=["tail_id", "flight_id", "win_id", "date_utc"],
                how="left",
            )
            .withColumn(
                "module_support_share",
                F.coalesce(F.col("module_support"), F.lit(0.0))
                / F.greatest(F.coalesce(F.col("module_support_total"), F.lit(0.0)), F.lit(1e-12)),
            )
            .withColumn(
                "parameter_localization_support",
                (
                    F.col("parameter_localization_support")
                    * (F.lit(1.0) + F.coalesce(F.col("module_support_share"), F.lit(0.0)))
                ).cast("double"),
            )
        )
        support_rank_window = Window.partitionBy("tail_id", "flight_id", "win_id", "date_utc").orderBy(
            F.col("parameter_localization_support").desc(),
            F.col("module_support_share").desc(),
            F.col("state_support_share").desc(),
            F.col("response_support_share").desc(),
            F.col("accumulation_profile_relevance").desc(),
            F.col("event_support_share").desc(),
            F.col("residual_share").desc(),
            F.col("parameter_name").asc(),
        )
        return cls(
            dataframe=(
                parameter_support_df.withColumn("parameter_support_rank_in_window", F.row_number().over(support_rank_window))
                .where(F.col("parameter_support_rank_in_window") <= F.lit(max(int(top_k_per_window), 1)))
                .select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    "parameter_name",
                    "system_id",
                    "subsystem_id",
                    "module_id",
                    "parameter_localization_support",
                    "parameter_support_rank_in_window",
                )
            )
        )

    @hot_path
    def localized_targets_df(
        self,
        *,
        top_k_per_window: int = ANOMALY_LOCALIZATION_TARGET_TOP_K,
        parameter_support_top_k: int | None = None,
    ) -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        support_df = self.to_dataframe()
        if parameter_support_top_k is not None:
            support_df = support_df.where(
                F.col("parameter_support_rank_in_window") <= F.lit(max(int(parameter_support_top_k), 1))
            )
        module_support_df = AnomalyParameterLocalizationFrame(dataframe=support_df)._module_support_df()
        subsystem_rank_window = Window.partitionBy("tail_id", "flight_id", "win_id", "date_utc").orderBy(
            F.col("subsystem_best_module_support").desc(),
            F.col("subsystem_rank_weighted_support").desc(),
            F.col("subsystem_peak_support").desc(),
            F.col("subsystem_best_rank").asc(),
            F.col("subsystem_id").asc(),
        )
        subsystem_candidates_df = (
            module_support_df.groupBy("tail_id", "flight_id", "win_id", "date_utc", "subsystem_id")
            .agg(
                F.max("module_rank_weighted_support").cast("double").alias("subsystem_best_module_support"),
                F.sum("module_rank_weighted_support").cast("double").alias("subsystem_rank_weighted_support"),
                F.max("module_peak_support").cast("double").alias("subsystem_peak_support"),
                F.min("module_best_rank").cast("int").alias("subsystem_best_rank"),
            )
            .withColumn("rn", F.row_number().over(subsystem_rank_window))
            .where(F.col("rn") <= F.lit(max(int(top_k_per_window), 1)))
            .groupBy("tail_id", "flight_id", "win_id", "date_utc")
            .agg(
                F.expr(
                    """
                    transform(
                      sort_array(
                        collect_list(
                          named_struct(
                            'rank_position', rn,
                            'id', subsystem_id,
                            'support', subsystem_rank_weighted_support,
                            'best_rank', subsystem_best_rank
                          )
                        )
                      ),
                      x -> named_struct(
                        'id', x.id,
                        'support', cast(x.support as double),
                        'best_rank', cast(x.best_rank as int)
                      )
                    )
                    """
                ).alias("top_subsystem_candidates")
            )
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.element_at(F.col("top_subsystem_candidates"), F.lit(1)).getField("id").alias("dominant_subsystem_id"),
                "top_subsystem_candidates",
            )
        )
        module_rank_window = Window.partitionBy("tail_id", "flight_id", "win_id", "date_utc").orderBy(
            F.col("module_rank_weighted_support").desc(),
            F.col("module_peak_support").desc(),
            F.col("module_best_rank").asc(),
            F.col("module_id").asc(),
        )
        module_candidates_df = (
            module_support_df.withColumn("rn", F.row_number().over(module_rank_window))
            .where(F.col("rn") <= F.lit(max(int(top_k_per_window), 1)))
            .groupBy("tail_id", "flight_id", "win_id", "date_utc")
            .agg(
                F.expr(
                    """
                    transform(
                      sort_array(
                        collect_list(
                          named_struct(
                            'rank_position', rn,
                            'id', module_id,
                            'subsystem_id', subsystem_id,
                            'support', module_rank_weighted_support,
                            'best_rank', module_best_rank
                          )
                        )
                      ),
                      x -> named_struct(
                        'id', x.id,
                        'subsystem_id', x.subsystem_id,
                        'support', cast(x.support as double),
                        'best_rank', cast(x.best_rank as int)
                      )
                    )
                    """
                ).alias("top_module_candidates")
            )
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.element_at(F.col("top_module_candidates"), F.lit(1)).getField("id").alias("dominant_module_id"),
                "top_module_candidates",
            )
        )
        return subsystem_candidates_df.join(
            module_candidates_df,
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="outer",
        )

    @hot_path
    def dominant_targets_df(self) -> "DataFrame":
        return self.localized_targets_df(
            top_k_per_window=1,
            parameter_support_top_k=ANOMALY_LOCALIZATION_PARAMETER_TOP_K,
        ).select(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            "dominant_subsystem_id",
            "dominant_module_id",
        )


@dataclass(frozen=True)
class AnomalyAttributionContextFrame(Frame):
    @classmethod
    @hot_path
    def from_context_frames(
        cls,
        *,
        subsystem_context: AnomalySubsystemContextFrame,
        panel_context: AnomalyPanelContextFrame,
    ) -> "AnomalyAttributionContextFrame":
        from pyspark.sql import functions as F

        null_panel_context = F.lit(None).cast(
            "struct<text:array<string>,message_codes:array<string>,source:array<string>>"
        )
        empty_sensor_scores = F.expr("cast(map() as map<string,double>)")
        subsystem_context_df = subsystem_context.to_dataframe()
        panel_context_df = panel_context.to_dataframe()
        return cls(
            dataframe=(
                subsystem_context_df.alias("s")
                .join(panel_context_df.alias("p"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="full_outer")
                .select(
                    F.coalesce(F.col("s.tail_id"), F.col("p.tail_id")).alias("tail_id"),
                    F.coalesce(F.col("s.flight_id"), F.col("p.flight_id")).alias("flight_id"),
                    F.coalesce(F.col("s.win_id"), F.col("p.win_id")).alias("win_id"),
                    F.coalesce(F.col("s.date_utc"), F.col("p.date_utc")).alias("date_utc"),
                    F.coalesce(
                        F.col("s.top_sensors_by_subsystem"),
                        F.expr(
                            "cast(map() as map<string,array<struct<parameter_name:string,sensor_score:double,event_score:double,categorical_event_score:double>>>)"
                        ),
                    ).alias("top_sensors_by_subsystem"),
                    F.coalesce(F.col("s.sensor_scores"), empty_sensor_scores).alias("sensor_scores"),
                    F.coalesce(F.col("p.panel_context"), null_panel_context).alias("panel_context"),
                )
            )
        )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
