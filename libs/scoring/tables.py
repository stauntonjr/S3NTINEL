"""Typed Spark tables for scoring artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES, EventType
from libs.io.schemas.scoring import WINDOW_SCORES_CALIBRATED_SCHEMA, WINDOW_SCORES_RAW_SCHEMA
from libs.pyspark import Table
from libs.scoring.channels import (
    BOUND_VIOLATION_CHANNEL,
    COHERENCE_BREAK_CHANNEL,
    EVENT_DISCORDANCE_CHANNEL,
    RECONSTRUCTION_ERROR_CHANNEL,
    REGIME_DEVIATION_CHANNEL,
    RESPONSE_VIOLATION_CHANNEL,
    STATE_VIOLATION_CHANNEL,
    active_channel_mean_expr,
    dominant_score_component_expr,
    score_component_map_expr,
)

EMIT_READY_P_VALUE_THRESHOLD = 0.10
HIGH_SEVERITY_LABEL = "high"
LOW_SEVERITY_LABEL = "low"
MEDIUM_SEVERITY_LABEL = "medium"
NORMAL_SEVERITY_LABEL = "normal"

_EVENT_WINDOW_KEYS = ["tail_id", "flight_id", "win_id", "date_utc"]
_PHASE_GROUP_KEYS = ["tail_id", "phase_id_detected"]


@dataclass(frozen=True)
class _LocalizedHierarchySupportFrames:
    module_ranked_df: "DataFrame"
    dominant_modules_df: "DataFrame"
    subsystem_ranked_df: "DataFrame"
    dominant_subsystems_df: "DataFrame"


def _phase_metric_baselines(metric_df: "DataFrame", *, metric_cols: tuple[str, ...]) -> "DataFrame":
    from pyspark.sql import functions as F

    median_df = metric_df.groupBy(*_PHASE_GROUP_KEYS).agg(
        *[
            F.percentile_approx(F.col(metric_name), F.lit(0.5), 1000).cast("double").alias(f"{metric_name}_median")
            for metric_name in metric_cols
        ]
    )
    deviation_df = metric_df.join(median_df, on=_PHASE_GROUP_KEYS, how="left").select(
        *_PHASE_GROUP_KEYS,
        *[
            F.abs(F.col(metric_name) - F.col(f"{metric_name}_median")).cast("double").alias(f"{metric_name}_abs_dev")
            for metric_name in metric_cols
        ],
    )
    mad_df = deviation_df.groupBy(*_PHASE_GROUP_KEYS).agg(
        *[
            F.percentile_approx(F.col(f"{metric_name}_abs_dev"), F.lit(0.5), 1000)
            .cast("double")
            .alias(f"{metric_name}_mad")
            for metric_name in metric_cols
        ]
    )
    return median_df.join(mad_df, on=_PHASE_GROUP_KEYS, how="inner")


def _normalized_positive_deviation(metric_name: str) -> "Column":
    from pyspark.sql import functions as F

    return F.greatest(
        F.lit(0.0),
        (
            F.coalesce(F.col(metric_name), F.lit(0.0))
            - F.coalesce(F.col(f"{metric_name}_median"), F.lit(0.0))
        )
        / F.greatest(F.coalesce(F.col(f"{metric_name}_mad"), F.lit(0.0)), F.lit(1e-6)),
    ).cast("double")


def _normalized_abs_deviation(metric_name: str) -> "Column":
    from pyspark.sql import functions as F

    return (
        F.abs(
            F.coalesce(F.col(metric_name), F.lit(0.0))
            - F.coalesce(F.col(f"{metric_name}_median"), F.lit(0.0))
        )
        / F.greatest(F.coalesce(F.col(f"{metric_name}_mad"), F.lit(0.0)), F.lit(1e-6))
    ).cast("double")


def _metric_map_value(map_col: str, key: str) -> "Column":
    from pyspark.sql import functions as F

    return F.coalesce(F.element_at(F.col(map_col), F.lit(str(key))), F.lit(0.0)).cast("double")


def _normalized_clamped_avg(*columns: "Column") -> "Column":
    from pyspark.sql import functions as F

    if not columns:
        return F.lit(0.0).cast("double")
    total = F.lit(0.0)
    for column in columns:
        total = total + F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.coalesce(column, F.lit(0.0)).cast("double")))
    return (total / F.lit(float(len(columns)))).cast("double")


def _window_aligned_event_rows(events_df: "DataFrame", *, windows_df: "DataFrame | None") -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    event_id_col = (
        F.col("event_seq_id").cast("long").alias("_event_join_id")
        if "event_seq_id" in events_df.columns
        else F.monotonically_increasing_id().alias("_event_join_id")
    )
    base_event_rows = (
        events_df.select(
            event_id_col,
            F.col("tail_id").alias("tail_id"),
            F.col("flight_id").alias("flight_id"),
            F.col("win_id").cast("int").alias("win_id"),
            F.col("date_utc").alias("date_utc"),
            F.col("timestamp_utc").alias("timestamp_utc"),
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col("event_type_detected").cast("string").alias("event_type_detected"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("event_type_detected").isNotNull())
    )
    direct_rows = base_event_rows.where(F.col("win_id").isNotNull()).select(
        "tail_id",
        "flight_id",
        "win_id",
        "date_utc",
        "timestamp_utc",
        "parameter_name",
        "event_type_detected",
    )
    if windows_df is None or "timestamp_utc" not in events_df.columns:
        return direct_rows

    window_bounds_df = windows_df.select(
        "tail_id",
        "flight_id",
        F.col("date_utc").alias("window_date_utc"),
        F.col("win_id").cast("int").alias("window_win_id"),
        F.col("t_start").alias("window_t_start"),
        F.col("t_end").alias("window_t_end"),
    )
    unresolved_rows = base_event_rows.where(F.col("win_id").isNull() & F.col("timestamp_utc").isNotNull())
    resolved_candidates_df = unresolved_rows.alias("e").join(
        F.broadcast(window_bounds_df).alias("ww"),
        on=[
            F.col("e.tail_id") == F.col("ww.tail_id"),
            F.col("e.flight_id") == F.col("ww.flight_id"),
            F.col("e.date_utc") == F.col("ww.window_date_utc"),
            F.col("e.timestamp_utc") >= F.col("ww.window_t_start"),
            F.col("e.timestamp_utc") <= F.col("ww.window_t_end"),
        ],
        how="inner",
    )
    resolved_window = Window.partitionBy(F.col("e._event_join_id")).orderBy(
        F.col("ww.window_t_start").desc(),
        F.col("ww.window_win_id").asc(),
    )
    resolved_rows = (
        resolved_candidates_df.withColumn("_window_match_rank", F.row_number().over(resolved_window))
        .where(F.col("_window_match_rank") == 1)
        .select(
            F.col("e.tail_id").alias("tail_id"),
            F.col("e.flight_id").alias("flight_id"),
            F.col("ww.window_win_id").alias("win_id"),
            F.coalesce(F.col("e.date_utc"), F.col("ww.window_date_utc")).alias("date_utc"),
            F.col("e.timestamp_utc").alias("timestamp_utc"),
            F.col("e.parameter_name").alias("parameter_name"),
            F.col("e.event_type_detected").alias("event_type_detected"),
        )
    )
    return direct_rows.unionByName(resolved_rows)


def _localized_hierarchy_support_frames(
    residual_rows_df: "DataFrame",
    residual_totals_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
    *,
    parameter_event_counts_df: "DataFrame | None",
) -> _LocalizedHierarchySupportFrames:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    module_id_col = (
        F.col("module_id").cast("string")
        if "module_id" in hierarchy_sensor_map_df.columns
        else F.col("parameter_name").cast("string")
    )
    hierarchy_localization_df = (
        hierarchy_sensor_map_df.select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col("subsystem_id").cast("string").alias("subsystem_id"),
            F.coalesce(module_id_col, F.col("parameter_name").cast("string")).alias("module_id"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("subsystem_id").isNotNull())
        .dropDuplicates(["parameter_name"])
    )
    module_size_df = hierarchy_localization_df.groupBy("subsystem_id", "module_id").agg(
        F.countDistinct("parameter_name").cast("double").alias("module_parameter_count")
    )
    subsystem_size_df = hierarchy_localization_df.groupBy("subsystem_id").agg(
        F.countDistinct("module_id").cast("double").alias("subsystem_module_count")
    )

    localized_parameter_support_df = (
        residual_rows_df.join(residual_totals_df, on=_EVENT_WINDOW_KEYS, how="left")
        .join(F.broadcast(hierarchy_localization_df), on="parameter_name", how="left")
        .where(F.col("subsystem_id").isNotNull())
        .join(F.broadcast(module_size_df), on=["subsystem_id", "module_id"], how="left")
        .join(F.broadcast(subsystem_size_df), on="subsystem_id", how="left")
    )
    if parameter_event_counts_df is not None:
        localized_parameter_support_df = localized_parameter_support_df.join(
            parameter_event_counts_df.select(*_EVENT_WINDOW_KEYS, "parameter_name", "event_support_count"),
            on=[*_EVENT_WINDOW_KEYS, "parameter_name"],
            how="left",
        )
    else:
        localized_parameter_support_df = localized_parameter_support_df.withColumn(
            "event_support_count",
            F.lit(0.0).cast("double"),
        )
    localized_parameter_support_df = (
        localized_parameter_support_df.withColumn(
            "residual_share",
            F.col("residual_weight") / F.greatest(F.col("residual_total_weight"), F.lit(1e-12)),
        )
        .withColumn(
            "parameter_localization_support",
            (
                F.col("residual_share")
                * (F.lit(1.0) + F.log1p(F.coalesce(F.col("event_support_count"), F.lit(0.0))))
                / F.sqrt(F.greatest(F.coalesce(F.col("module_parameter_count"), F.lit(1.0)), F.lit(1.0)))
            ).cast("double"),
        )
    )

    module_support_df = localized_parameter_support_df.groupBy(
        *_EVENT_WINDOW_KEYS,
        "subsystem_id",
        "module_id",
        "subsystem_module_count",
    ).agg(
        F.sqrt(F.sum(F.pow(F.col("parameter_localization_support"), F.lit(2.0)))).cast("double").alias("module_support"),
        F.sum("parameter_localization_support").cast("double").alias("module_total_support"),
        F.max("parameter_localization_support").cast("double").alias("module_peak_parameter_support"),
    )
    module_support_totals_df = module_support_df.groupBy(*_EVENT_WINDOW_KEYS).agg(
        F.sum("module_support").cast("double").alias("module_support_total")
    )
    module_ranked_df = (
        module_support_df.join(module_support_totals_df, on=_EVENT_WINDOW_KEYS, how="left")
        .select(
            *_EVENT_WINDOW_KEYS,
            "subsystem_id",
            "module_id",
            (
                F.col("module_support") / F.greatest(F.col("module_support_total"), F.lit(1e-12))
            ).cast("double").alias("module_score"),
            F.col("module_total_support").cast("double").alias("module_total_support"),
            F.col("module_peak_parameter_support").cast("double").alias("module_peak_parameter_support"),
        )
        .localCheckpoint(eager=True)
    )
    dominant_module_window = Window.partitionBy(*_EVENT_WINDOW_KEYS).orderBy(
        F.col("module_score").desc(),
        F.col("module_peak_parameter_support").desc(),
        F.col("module_total_support").desc(),
        F.col("subsystem_id").asc(),
        F.col("module_id").asc(),
    )
    dominant_modules_df = (
        module_ranked_df.withColumn("rn", F.row_number().over(dominant_module_window))
        .where(F.col("rn") == 1)
        .select(*_EVENT_WINDOW_KEYS, F.col("module_id").alias("dominant_module_id"))
    )
    subsystem_support_df = (
        module_support_df.groupBy(*_EVENT_WINDOW_KEYS, "subsystem_id", "subsystem_module_count")
        .agg(
            F.sum(F.pow(F.col("module_support"), F.lit(2.0))).cast("double").alias("subsystem_support_energy"),
            F.sum("module_support").cast("double").alias("subsystem_total_support"),
            F.max("module_support").cast("double").alias("subsystem_peak_module_support"),
        )
        .withColumn(
            "subsystem_score_raw",
            (
                F.col("subsystem_support_energy")
                / F.sqrt(F.greatest(F.coalesce(F.col("subsystem_module_count"), F.lit(1.0)), F.lit(1.0)))
            ).cast("double"),
        )
    )
    subsystem_score_totals_df = subsystem_support_df.groupBy(*_EVENT_WINDOW_KEYS).agg(
        F.sum("subsystem_score_raw").cast("double").alias("subsystem_score_total")
    )
    subsystem_ranked_df = (
        subsystem_support_df.join(subsystem_score_totals_df, on=_EVENT_WINDOW_KEYS, how="left")
        .withColumn(
            "subsystem_score",
            F.col("subsystem_score_raw") / F.greatest(F.col("subsystem_score_total"), F.lit(1e-12)),
        )
        .select(
            *_EVENT_WINDOW_KEYS,
            "subsystem_id",
            "subsystem_score",
            "subsystem_total_support",
            "subsystem_peak_module_support",
        )
        .localCheckpoint(eager=True)
    )
    dominant_window = Window.partitionBy(*_EVENT_WINDOW_KEYS).orderBy(
        F.col("subsystem_score").desc(),
        F.col("subsystem_peak_module_support").desc(),
        F.col("subsystem_total_support").desc(),
        F.col("subsystem_id").asc(),
    )
    dominant_subsystems_df = (
        subsystem_ranked_df.withColumn("rn", F.row_number().over(dominant_window))
        .where(F.col("rn") == 1)
        .select(*_EVENT_WINDOW_KEYS, F.col("subsystem_id").alias("dominant_subsystem_id"))
    )
    return _LocalizedHierarchySupportFrames(
        module_ranked_df=module_ranked_df,
        dominant_modules_df=dominant_modules_df,
        subsystem_ranked_df=subsystem_ranked_df,
        dominant_subsystems_df=dominant_subsystems_df,
    )


@dataclass(frozen=True)
class WindowScoresRawTable(Table):
    partition_by: tuple[str, ...] = ("tail_id",)

    @classmethod
    def spark_schema(cls):
        return WINDOW_SCORES_RAW_SCHEMA()

    @classmethod
    def from_phase_tables(
        cls,
        phase_windows: "PhaseWindowsTable",
        phase_baselines: "PhaseBaselinesTable",
        hierarchy_sensor_map: "HierarchySensorMapTable",
        *,
        windows: "WindowsTable | None" = None,
        events: "EventsTable | None" = None,
        parameter_behavior_profile: "ParameterBehaviorProfile | None" = None,
        parameter_event_profile: "ParameterEventProfile | None" = None,
    ) -> "WindowScoresRawTable":
        return cls.from_phase_dataframes(
            phase_windows.to_dataframe(),
            phase_baselines.to_dataframe(),
            hierarchy_sensor_map.to_dataframe(),
            windows_df=None if windows is None else windows.to_dataframe(),
            events_df=None if events is None else events.to_dataframe(),
            parameter_behavior_profile_df=(
                None if parameter_behavior_profile is None else parameter_behavior_profile.to_dataframe()
            ),
            parameter_event_profile_df=(
                None if parameter_event_profile is None else parameter_event_profile.to_dataframe()
            ),
        )

    @classmethod
    def from_phase_dataframes(
        cls,
        phase_windows_df: "DataFrame",
        phase_baselines_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
        *,
        windows_df: "DataFrame | None" = None,
        events_df: "DataFrame | None" = None,
        parameter_behavior_profile_df: "DataFrame | None" = None,
        parameter_event_profile_df: "DataFrame | None" = None,
    ) -> "WindowScoresRawTable":
        from libs.io.schemas import WINDOW_SCORES_RAW_COLUMNS
        from pyspark.sql import functions as F

        baselines = phase_baselines_df.select(
            "tail_id",
            "phase_id_detected",
            "s_w_centroid",
            "reconstruction_median",
            "reconstruction_mad",
            "distance_median",
            "distance_mad",
        )
        empty_int_map = F.expr("cast(map() as map<string,int>)")
        empty_double_map = F.expr("cast(map() as map<string,double>)")

        joined = (
            phase_windows_df.alias("w")
            .join(
                F.broadcast(baselines).alias("b"),
                on=[
                    F.col("w.tail_id") == F.col("b.tail_id"),
                    F.col("w.phase_id_detected") == F.col("b.phase_id_detected"),
                ],
                how="left",
            )
            .withColumn(
                "structure_distance",
                F.when(F.col("b.s_w_centroid").isNull(), F.lit(None).cast("double")).otherwise(
                    F.expr(
                        """
                        sqrt(
                          aggregate(
                            zip_with(
                              coalesce(w.s_w, array()),
                              coalesce(b.s_w_centroid, array()),
                              (x, y) -> pow(coalesce(x, 0D) - coalesce(y, 0D), 2D)
                            ),
                            cast(0.0 as double),
                            (acc, value) -> acc + value
                          )
                        )
                        """
                    )
                ),
            )
            .withColumn(
                REGIME_DEVIATION_CHANNEL,
                F.when(F.col("b.s_w_centroid").isNull(), F.lit(0.0).cast("double")).otherwise(
                    F.greatest(
                        F.lit(0.0),
                        (F.col("structure_distance") - F.coalesce(F.col("b.distance_median"), F.lit(0.0)))
                        / F.greatest(F.coalesce(F.col("b.distance_mad"), F.lit(0.0)), F.lit(1e-6)),
                    )
                ),
            )
            .withColumn(
                RECONSTRUCTION_ERROR_CHANNEL,
                F.when(
                    F.col("b.s_w_centroid").isNull(),
                    F.coalesce(F.col("w.backbone_reconstruction_error"), F.lit(0.0)).cast("double"),
                ).otherwise(
                    F.greatest(
                        F.lit(0.0),
                        (
                            F.coalesce(F.col("w.backbone_reconstruction_error"), F.lit(0.0))
                            - F.coalesce(F.col("b.reconstruction_median"), F.lit(0.0))
                        )
                        / F.greatest(F.coalesce(F.col("b.reconstruction_mad"), F.lit(0.0)), F.lit(1e-6)),
                    )
                ),
            )
        )

        if windows_df is not None:
            window_context_df = windows_df.select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.coalesce(F.col("duration_ms"), F.lit(0)).cast("double").alias("window_duration_ms"),
                F.coalesce(F.col("real_event_count"), F.col("event_count"), F.lit(0)).cast("double").alias(
                    "window_real_event_count"
                ),
                F.coalesce(F.col("event_type_counts"), empty_int_map).alias("window_event_type_counts"),
                F.coalesce(F.col("close_reason"), F.lit("")).alias("window_close_reason"),
            )
            joined = joined.join(window_context_df.alias("ww"), on=_EVENT_WINDOW_KEYS, how="left")
        else:
            joined = (
                joined.withColumn("window_duration_ms", F.lit(0.0).cast("double"))
                .withColumn("window_real_event_count", F.lit(0.0).cast("double"))
                .withColumn("window_event_type_counts", empty_int_map)
                .withColumn("window_close_reason", F.lit(""))
            )

        parameter_event_counts_df = None
        event_active_parameter_counts_df = None
        event_subsystem_scores_df = None
        if events_df is not None:
            event_rows = _window_aligned_event_rows(events_df, windows_df=windows_df)
            parameter_event_counts_df = event_rows.groupBy(*_EVENT_WINDOW_KEYS, "parameter_name").agg(
                F.count(F.lit(1)).cast("double").alias("event_support_count"),
                F.sum(F.when(F.col("event_type_detected") == F.lit(EventType.THRESHOLD), F.lit(1.0)).otherwise(F.lit(0.0)))
                .cast("double")
                .alias("threshold_event_count"),
                F.sum(F.when(F.col("event_type_detected") == F.lit(EventType.DRIFT_GUARD), F.lit(1.0)).otherwise(F.lit(0.0)))
                .cast("double")
                .alias("drift_guard_event_count"),
                F.sum(F.when(F.col("event_type_detected") == F.lit(EventType.SWITCH), F.lit(1.0)).otherwise(F.lit(0.0)))
                .cast("double")
                .alias("switch_event_count"),
                F.sum(
                    F.when(
                        F.col("event_type_detected").isin(EventType.SLOPE_POS, EventType.SLOPE_NEG, EventType.EXTREMA),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("slope_event_count"),
                F.sum(
                    F.when(F.col("event_type_detected") == F.lit(EventType.OSCILLATION), F.lit(1.0)).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("oscillation_event_count"),
                F.sum(
                    F.when(
                        F.col("event_type_detected").isin(
                            EventType.TRANSITION,
                            EventType.STATE_ENTER,
                            EventType.STATE_EXIT,
                            EventType.DROPPED,
                            EventType.DWELL_BUCKET,
                            EventType.DWELL_GUARD,
                        ),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("state_event_count"),
                F.sum(
                    F.when(F.col("event_type_detected") == F.lit(EventType.ILLEGAL_TRANSITION), F.lit(1.0)).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("illegal_transition_event_count"),
                F.sum(
                    F.when(F.col("event_type_detected") == F.lit(EventType.DWELL_VIOLATION), F.lit(1.0)).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("dwell_violation_event_count"),
                F.sum(
                    F.when(
                        F.col("event_type_detected").isin(EventType.STATE_ENTER, EventType.STATE_EXIT),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                )
                .cast("double")
                .alias("state_enter_exit_event_count"),
            )
            event_active_parameter_counts_df = parameter_event_counts_df.groupBy(*_EVENT_WINDOW_KEYS).agg(
                F.countDistinct("parameter_name").cast("double").alias("active_parameter_count")
            )
            event_subsystem_counts_df = (
                event_rows.join(
                    F.broadcast(hierarchy_sensor_map_df.select("parameter_name", "subsystem_id")),
                    on="parameter_name",
                    how="left",
                )
                .where(F.col("subsystem_id").isNotNull())
                .groupBy(*_EVENT_WINDOW_KEYS, "subsystem_id")
                .agg(F.count(F.lit(1)).cast("double").alias("event_subsystem_count"))
            )
            event_subsystem_totals_df = event_subsystem_counts_df.groupBy(*_EVENT_WINDOW_KEYS).agg(
                F.sum("event_subsystem_count").cast("double").alias("event_subsystem_total")
            )
            event_subsystem_scores_df = (
                event_subsystem_counts_df.join(event_subsystem_totals_df, on=_EVENT_WINDOW_KEYS, how="inner")
                .withColumn(
                    "event_subsystem_score",
                    F.col("event_subsystem_count") / F.greatest(F.col("event_subsystem_total"), F.lit(1e-12)),
                )
            )

        joined = joined.join(
            event_active_parameter_counts_df
            if event_active_parameter_counts_df is not None
            else joined.select(
                F.col("w.tail_id").alias("tail_id"),
                F.col("w.flight_id").alias("flight_id"),
                F.col("w.win_id").alias("win_id"),
                F.col("w.date_utc").alias("date_utc"),
            )
            .distinct()
            .withColumn("active_parameter_count", F.lit(0.0).cast("double")),
            on=_EVENT_WINDOW_KEYS,
            how="left",
        )

        event_window_metrics_df = (
            joined.select(
                F.col("w.tail_id").alias("tail_id"),
                F.col("w.phase_id_detected").cast("int").alias("phase_id_detected"),
                "flight_id",
                "win_id",
                "date_utc",
                (
                    F.coalesce(F.col("window_real_event_count"), F.lit(0.0))
                    / F.greatest(F.coalesce(F.col("window_duration_ms"), F.lit(0.0)) / F.lit(1000.0), F.lit(1e-3))
                ).cast("double").alias("event_rate_hz"),
                F.size(F.map_keys(F.coalesce(F.col("window_event_type_counts"), empty_int_map))).cast("double").alias(
                    "event_type_diversity"
                ),
                (
                    (
                        _metric_map_value("window_event_type_counts", EventType.THRESHOLD)
                        + _metric_map_value("window_event_type_counts", EventType.SWITCH)
                        + _metric_map_value("window_event_type_counts", EventType.OSCILLATION)
                        + _metric_map_value("window_event_type_counts", EventType.DRIFT_GUARD)
                        + _metric_map_value("window_event_type_counts", EventType.SLOPE_POS)
                        + _metric_map_value("window_event_type_counts", EventType.SLOPE_NEG)
                    )
                    / F.greatest(F.coalesce(F.col("window_real_event_count"), F.lit(0.0)), F.lit(1.0))
                ).cast("double").alias("continuous_event_share"),
                (
                    (
                        _metric_map_value("window_event_type_counts", EventType.STATE_ENTER)
                        + _metric_map_value("window_event_type_counts", EventType.STATE_EXIT)
                        + _metric_map_value("window_event_type_counts", EventType.DROPPED)
                        + _metric_map_value("window_event_type_counts", EventType.DWELL_BUCKET)
                        + _metric_map_value("window_event_type_counts", EventType.TRANSITION)
                        + _metric_map_value("window_event_type_counts", EventType.DWELL_VIOLATION)
                        + _metric_map_value("window_event_type_counts", EventType.ILLEGAL_TRANSITION)
                        + _metric_map_value("window_event_type_counts", EventType.DWELL_GUARD)
                    )
                    / F.greatest(F.coalesce(F.col("window_real_event_count"), F.lit(0.0)), F.lit(1.0))
                ).cast("double").alias("state_event_share"),
                F.coalesce(F.col("active_parameter_count"), F.lit(0.0)).cast("double").alias("active_parameter_count"),
            )
        )
        event_metric_cols = (
            "event_rate_hz",
            "event_type_diversity",
            "continuous_event_share",
            "state_event_share",
            "active_parameter_count",
        )
        event_metric_baselines_df = _phase_metric_baselines(event_window_metrics_df, metric_cols=event_metric_cols)
        event_discordance_df = (
            event_window_metrics_df.join(F.broadcast(event_metric_baselines_df), on=_PHASE_GROUP_KEYS, how="left")
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                (
                    (
                        _normalized_abs_deviation("event_rate_hz")
                        + _normalized_abs_deviation("event_type_diversity")
                        + _normalized_abs_deviation("continuous_event_share")
                        + _normalized_abs_deviation("state_event_share")
                        + _normalized_abs_deviation("active_parameter_count")
                    )
                    / F.lit(float(len(event_metric_cols)))
                ).alias(EVENT_DISCORDANCE_CHANNEL),
            )
        )

        residual_rows = (
            joined.select(
                F.col("w.tail_id").alias("tail_id"),
                F.col("w.flight_id").alias("flight_id"),
                F.col("w.win_id").alias("win_id"),
                F.col("w.date_utc").alias("date_utc"),
                F.col("w.phase_id_detected").cast("int").alias("phase_id_detected"),
                F.explode_outer(
                    F.map_entries(
                        F.coalesce(F.col("w.backbone_residual_by_parameter"), F.expr("cast(map() as map<string,double>)"))
                    )
                ).alias("entry"),
            )
            .select(
                *_EVENT_WINDOW_KEYS,
                "phase_id_detected",
                F.col("entry.key").cast("string").alias("parameter_name"),
                F.abs(F.col("entry.value").cast("double")).alias("residual_weight"),
            )
            .where(F.col("parameter_name").isNotNull())
        )
        residual_totals_df = residual_rows.groupBy(*_EVENT_WINDOW_KEYS).agg(
            F.sum("residual_weight").cast("double").alias("residual_total_weight")
        )

        localization_support = _localized_hierarchy_support_frames(
            residual_rows,
            residual_totals_df,
            hierarchy_sensor_map_df,
            parameter_event_counts_df=parameter_event_counts_df,
        )

        behavior_score_df = None
        if (
            parameter_behavior_profile_df is not None
            and parameter_event_profile_df is not None
            and parameter_event_counts_df is not None
        ):
            enriched_parameter_evidence_df = (
                residual_rows.join(residual_totals_df, on=_EVENT_WINDOW_KEYS, how="left")
                .join(
                    F.broadcast(
                        parameter_behavior_profile_df.select(
                            "parameter_name",
                            "regulated_score_profiled",
                            "tracking_score_profiled",
                            "inertial_score_profiled",
                            "accumulative_score_profiled",
                            "discrete_state_score_profiled",
                            "excursion_rate_profiled",
                            "excursion_return_ratio_profiled",
                            "bound_occupancy_profiled",
                            "saturation_rate_profiled",
                            "oscillation_score_profiled",
                            "tracking_error_score_profiled",
                            "tracking_recovery_score_profiled",
                            "lagged_response_score_profiled",
                            "transition_rate_profiled",
                            "dominant_state_ratio_profiled",
                            "state_chatter_rate_profiled",
                        )
                    ),
                    on="parameter_name",
                    how="left",
                )
                .join(
                    F.broadcast(
                        parameter_event_profile_df.select(
                            "parameter_name",
                            "repeatability_score_profiled",
                            "smoothness_score_profiled",
                            "recommended_emit_threshold",
                            "recommended_emit_oscillation",
                        )
                    ),
                    on="parameter_name",
                    how="left",
                )
                .join(parameter_event_counts_df, on=[*_EVENT_WINDOW_KEYS, "parameter_name"], how="left")
                .withColumn(
                    "residual_share",
                    F.col("residual_weight") / F.greatest(F.col("residual_total_weight"), F.lit(1e-12)),
                )
            )
            bound_profile_relevance = _normalized_clamped_avg(
                F.col("bound_occupancy_profiled"),
                F.col("saturation_rate_profiled"),
                F.col("excursion_rate_profiled"),
                F.col("accumulative_score_profiled"),
            )
            bound_event_novelty = _normalized_clamped_avg(
                F.lit(1.0) - F.coalesce(F.col("recommended_emit_threshold").cast("double"), F.lit(0.0)),
                F.lit(1.0) - F.coalesce(F.col("bound_occupancy_profiled"), F.lit(0.0)),
                F.lit(1.0) - F.coalesce(F.col("saturation_rate_profiled"), F.lit(0.0)),
            )
            response_profile_relevance = _normalized_clamped_avg(
                F.greatest(
                    F.coalesce(F.col("tracking_score_profiled"), F.lit(0.0)),
                    F.coalesce(F.col("regulated_score_profiled"), F.lit(0.0)),
                    F.coalesce(F.col("inertial_score_profiled"), F.lit(0.0)),
                ),
                F.col("tracking_error_score_profiled"),
                F.col("lagged_response_score_profiled"),
                F.col("oscillation_score_profiled"),
            )
            response_event_novelty = _normalized_clamped_avg(
                F.lit(1.0) - F.coalesce(F.col("smoothness_score_profiled"), F.lit(0.0)),
                F.lit(1.0) - F.coalesce(F.col("repeatability_score_profiled"), F.lit(0.0)),
                F.lit(1.0) - F.coalesce(F.col("recommended_emit_oscillation").cast("double"), F.lit(0.0)),
            )
            state_profile_relevance = _normalized_clamped_avg(
                F.col("discrete_state_score_profiled"),
                F.col("transition_rate_profiled"),
                F.col("state_chatter_rate_profiled"),
                F.lit(1.0) - F.coalesce(F.col("dominant_state_ratio_profiled"), F.lit(0.0)),
            )
            behavior_activation_df = (
                enriched_parameter_evidence_df.select(
                    *_EVENT_WINDOW_KEYS,
                    "phase_id_detected",
                    (
                        F.col("residual_share")
                        * F.log1p(
                            F.coalesce(F.col("threshold_event_count"), F.lit(0.0))
                            + F.coalesce(F.col("drift_guard_event_count"), F.lit(0.0))
                            + (F.lit(0.5) * F.coalesce(F.col("switch_event_count"), F.lit(0.0)))
                        )
                        * (F.lit(0.5) + (F.lit(0.25) * bound_profile_relevance) + (F.lit(0.25) * bound_event_novelty))
                    ).alias("bound_violation_raw"),
                    (
                        F.col("residual_share")
                        * F.log1p(
                            F.coalesce(F.col("slope_event_count"), F.lit(0.0))
                            + F.coalesce(F.col("switch_event_count"), F.lit(0.0))
                            + F.coalesce(F.col("oscillation_event_count"), F.lit(0.0))
                        )
                        * (
                            F.lit(0.5)
                            + (F.lit(0.25) * response_profile_relevance)
                            + (F.lit(0.25) * response_event_novelty)
                        )
                    ).alias("response_violation_raw"),
                    (
                        F.col("residual_share")
                        * F.log1p(
                            F.coalesce(F.col("state_event_count"), F.lit(0.0))
                            + F.coalesce(F.col("state_enter_exit_event_count"), F.lit(0.0))
                            + (F.lit(2.0) * F.coalesce(F.col("illegal_transition_event_count"), F.lit(0.0)))
                            + (F.lit(2.0) * F.coalesce(F.col("dwell_violation_event_count"), F.lit(0.0)))
                        )
                        * (F.lit(0.5) + (F.lit(0.5) * state_profile_relevance))
                    ).alias("state_violation_raw"),
                )
                .groupBy(*_EVENT_WINDOW_KEYS, "phase_id_detected")
                .agg(
                    F.sum("bound_violation_raw").cast("double").alias("bound_violation_raw"),
                    F.sum("response_violation_raw").cast("double").alias("response_violation_raw"),
                    F.sum("state_violation_raw").cast("double").alias("state_violation_raw"),
                )
            )
            behavior_metric_cols = ("bound_violation_raw", "response_violation_raw", "state_violation_raw")
            behavior_baselines_df = _phase_metric_baselines(behavior_activation_df, metric_cols=behavior_metric_cols)
            behavior_score_df = (
                behavior_activation_df.join(F.broadcast(behavior_baselines_df), on=_PHASE_GROUP_KEYS, how="left")
                .select(
                    *_EVENT_WINDOW_KEYS,
                    _normalized_positive_deviation("bound_violation_raw").alias(BOUND_VIOLATION_CHANNEL),
                    _normalized_positive_deviation("response_violation_raw").alias(RESPONSE_VIOLATION_CHANNEL),
                    _normalized_positive_deviation("state_violation_raw").alias(STATE_VIOLATION_CHANNEL),
                )
            )

        coherence_break_df = None
        if event_subsystem_scores_df is not None:
            residual_event_subsystem_df = localization_support.subsystem_ranked_df.select(
                *_EVENT_WINDOW_KEYS,
                F.col("subsystem_id").alias("residual_subsystem_id"),
                F.col("subsystem_score").cast("double").alias("residual_subsystem_score"),
            )
            event_support_totals_df = event_subsystem_scores_df.groupBy(*_EVENT_WINDOW_KEYS).agg(
                F.sum("event_subsystem_count").cast("double").alias("event_support_total")
            )
            coherence_raw_df = (
                residual_event_subsystem_df.alias("r").join(
                    event_subsystem_scores_df.select(
                        *_EVENT_WINDOW_KEYS,
                        F.col("subsystem_id").alias("event_subsystem_id"),
                        F.col("event_subsystem_score").cast("double").alias("event_subsystem_score"),
                    ).alias("e"),
                    on=[
                        F.col("r.tail_id") == F.col("e.tail_id"),
                        F.col("r.flight_id") == F.col("e.flight_id"),
                        F.col("r.win_id") == F.col("e.win_id"),
                        F.col("r.date_utc") == F.col("e.date_utc"),
                        F.col("r.residual_subsystem_id") == F.col("e.event_subsystem_id"),
                    ],
                    how="full_outer",
                )
            )
            coherence_raw_df = (
                coherence_raw_df.select(
                    F.coalesce(F.col("r.tail_id"), F.col("e.tail_id")).alias("tail_id"),
                    F.coalesce(F.col("r.flight_id"), F.col("e.flight_id")).alias("flight_id"),
                    F.coalesce(F.col("r.win_id"), F.col("e.win_id")).alias("win_id"),
                    F.coalesce(F.col("r.date_utc"), F.col("e.date_utc")).alias("date_utc"),
                    F.abs(
                        F.coalesce(F.col("r.residual_subsystem_score"), F.lit(0.0))
                        - F.coalesce(F.col("e.event_subsystem_score"), F.lit(0.0))
                    )
                    .cast("double")
                    .alias("subsystem_score_delta"),
                )
                .groupBy(*_EVENT_WINDOW_KEYS)
                .agg((F.sum("subsystem_score_delta") / F.lit(2.0)).cast("double").alias("coherence_break_raw"))
            )
            coherence_raw_df = (
                joined.select(
                    F.col("w.tail_id").alias("tail_id"),
                    F.col("w.flight_id").alias("flight_id"),
                    F.col("w.win_id").alias("win_id"),
                    F.col("w.date_utc").alias("date_utc"),
                    F.col("w.phase_id_detected").cast("int").alias("phase_id_detected"),
                )
                .join(coherence_raw_df, on=_EVENT_WINDOW_KEYS, how="left")
                .join(event_support_totals_df, on=_EVENT_WINDOW_KEYS, how="left")
                .withColumn(
                    "coherence_break_raw",
                    F.coalesce(F.col("coherence_break_raw"), F.lit(0.0))
                    * F.least(F.lit(1.0), F.coalesce(F.col("event_support_total"), F.lit(0.0)) / F.lit(3.0)),
                )
            )
            coherence_baselines_df = _phase_metric_baselines(coherence_raw_df, metric_cols=("coherence_break_raw",))
            coherence_break_df = (
                coherence_raw_df.join(F.broadcast(coherence_baselines_df), on=_PHASE_GROUP_KEYS, how="left")
                .select(
                    *_EVENT_WINDOW_KEYS,
                    _normalized_positive_deviation("coherence_break_raw").alias(COHERENCE_BREAK_CHANNEL),
                )
            )

        result = joined.select(
            F.col("w.tail_id").alias("tail_id"),
            F.col("w.flight_id").alias("flight_id"),
            F.col("w.win_id").alias("win_id"),
            F.col("w.phase_state_detected").alias("phase_state_detected"),
            F.col("w.phase_id_detected").cast("int").alias("phase_id_detected"),
            F.col("w.phase_confidence_detected").cast("double").alias("phase_confidence_detected"),
            F.col("w.distance_to_centroid_detected").cast("double").alias("distance_to_centroid_detected"),
            F.col("w.drift_magnitude").cast("double").alias("drift_magnitude"),
            F.col("w.breadth").cast("double").alias("breadth"),
            F.col(REGIME_DEVIATION_CHANNEL).cast("double").alias(REGIME_DEVIATION_CHANNEL),
            F.col(RECONSTRUCTION_ERROR_CHANNEL).cast("double").alias(RECONSTRUCTION_ERROR_CHANNEL),
            F.col("w.date_utc").alias("date_utc"),
        )

        result = result.join(event_discordance_df, on=_EVENT_WINDOW_KEYS, how="left")
        if behavior_score_df is not None:
            result = result.join(behavior_score_df, on=_EVENT_WINDOW_KEYS, how="left")
        if coherence_break_df is not None:
            result = result.join(coherence_break_df, on=_EVENT_WINDOW_KEYS, how="left")
        result = (
            result.join(localization_support.dominant_subsystems_df, on=_EVENT_WINDOW_KEYS, how="left")
            .join(localization_support.dominant_modules_df, on=_EVENT_WINDOW_KEYS, how="left")
            .withColumn("subsystem_scores", empty_double_map)
        )
        for score_name in (
            EVENT_DISCORDANCE_CHANNEL,
            BOUND_VIOLATION_CHANNEL,
            RESPONSE_VIOLATION_CHANNEL,
            STATE_VIOLATION_CHANNEL,
            COHERENCE_BREAK_CHANNEL,
        ):
            if score_name in result.columns:
                result = result.withColumn(score_name, F.coalesce(F.col(score_name), F.lit(0.0)).cast("double"))
            else:
                result = result.withColumn(score_name, F.lit(0.0).cast("double"))

        component_expr_by_name = {
            REGIME_DEVIATION_CHANNEL: F.col(REGIME_DEVIATION_CHANNEL),
            RECONSTRUCTION_ERROR_CHANNEL: F.col(RECONSTRUCTION_ERROR_CHANNEL),
            EVENT_DISCORDANCE_CHANNEL: F.col(EVENT_DISCORDANCE_CHANNEL),
            BOUND_VIOLATION_CHANNEL: F.col(BOUND_VIOLATION_CHANNEL),
            RESPONSE_VIOLATION_CHANNEL: F.col(RESPONSE_VIOLATION_CHANNEL),
            STATE_VIOLATION_CHANNEL: F.col(STATE_VIOLATION_CHANNEL),
            COHERENCE_BREAK_CHANNEL: F.col(COHERENCE_BREAK_CHANNEL),
        }
        result = (
            result.withColumn("global_score", active_channel_mean_expr(component_expr_by_name))
            .withColumn(
                "severity",
                F.when(F.col("global_score") >= F.lit(6.0), F.lit(HIGH_SEVERITY_LABEL))
                .when(F.col("global_score") >= F.lit(3.0), F.lit(MEDIUM_SEVERITY_LABEL))
                .when(F.col("global_score") > F.lit(1.0), F.lit(LOW_SEVERITY_LABEL))
                .otherwise(F.lit(NORMAL_SEVERITY_LABEL)),
            )
            .withColumn("dominant_score_component", dominant_score_component_expr(component_expr_by_name))
            .withColumn("score_component_scores", score_component_map_expr(component_expr_by_name))
            .withColumn("p_value", F.lit(1.0).cast("double"))
            .select(*WINDOW_SCORES_RAW_COLUMNS)
        )
        return cls(dataframe=result)


@dataclass(frozen=True)
class WindowScoresCalibratedTable(Table):
    partition_by: tuple[str, ...] = ("tail_id",)

    @classmethod
    def spark_schema(cls):
        return WINDOW_SCORES_CALIBRATED_SCHEMA()

    @staticmethod
    def emit_ready_expr():
        from pyspark.sql import functions as F

        severity = F.lower(F.coalesce(F.col("severity"), F.lit(NORMAL_SEVERITY_LABEL)))
        return (
            F.col("warm")
            & F.col("p_value").isNotNull()
            & (
                severity.isin(MEDIUM_SEVERITY_LABEL, HIGH_SEVERITY_LABEL)
                | ((severity == F.lit(LOW_SEVERITY_LABEL)) & (F.col("p_value") <= F.lit(float(EMIT_READY_P_VALUE_THRESHOLD))))
            )
        )

    @classmethod
    def from_scores(cls, scores_df: "DataFrame", *, min_warm: int) -> "WindowScoresCalibratedTable":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        phase_window = Window.partitionBy("tail_id", "flight_id", "phase_id_detected")
        order_window = Window.partitionBy("tail_id", "flight_id", "phase_id_detected").orderBy("win_id")
        score_desc_window = Window.partitionBy("tail_id", "flight_id", "phase_id_detected").orderBy(
            F.col("global_score").desc()
        )

        enriched = (
            scores_df.withColumn("phase_count", F.count(F.lit(1)).over(phase_window))
            .withColumn("phase_rank", F.row_number().over(order_window))
            .withColumn("warm", F.col("phase_count") >= F.lit(int(min_warm)))
        )

        calibrated = (
            enriched.withColumn("empirical_tail", F.cume_dist().over(score_desc_window))
            .withColumn(
                "p_value",
                F.when(F.col("warm"), F.col("empirical_tail").cast("double")).otherwise(F.lit(None).cast("double")),
            )
            .withColumn("emit_ready", cls.emit_ready_expr())
        )

        return cls(
            dataframe=calibrated.select(
                "tail_id",
                "flight_id",
                "win_id",
                "phase_state_detected",
                "phase_id_detected",
                "phase_confidence_detected",
                "distance_to_centroid_detected",
                "drift_magnitude",
                "breadth",
                "global_score",
                "p_value",
                "severity",
                "dominant_subsystem_id",
                "dominant_module_id",
                "dominant_score_component",
                "subsystem_scores",
                "score_component_scores",
                "warm",
                "emit_ready",
                F.lit(int(min_warm)).alias("min_warm"),
                "date_utc",
            )
        )


if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from libs.events.tables import EventsTable
    from libs.graph.tables import HierarchySensorMapTable
    from libs.phase.tables import PhaseBaselinesTable, PhaseWindowsTable
    from libs.profiling.profiles import ParameterBehaviorProfile
    from libs.windows.tables import WindowsTable
    from libs.events.profiling import ParameterEventProfile
