"""Backbone artifact table adapters for Spark pipeline stages."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from libs.backbone.artifacts import BackboneModel, BackboneSpec
from libs.backbone.fit import aggregate_backbone_gh

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _spark_functions():
    from pyspark.sql import functions as F

    return F


def _backbone_sensor_energy_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("parameter_name", T.StringType(), False),
            T.StructField("energy", T.DoubleType(), False),
            T.StructField("support_count", T.IntegerType(), False),
            T.StructField("event_prior", T.DoubleType(), False),
            T.StructField("selection_score", T.DoubleType(), False),
        ]
    )


def build_backbone_artifacts_from_window_features_table(
    window_features_df: pd.DataFrame,
    *,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    event_prior_alpha: float = 0.35,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if window_features_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    window_feature_rows = window_features_df.to_dict(orient="records")
    if not window_feature_rows:
        return pd.DataFrame(), pd.DataFrame()
    backbone_model, sensor_energies = BackboneModel.from_window_feature_rows(
        window_feature_rows,
        spec=BackboneSpec(
            sensor_count=backbone_sensor_count,
            ridge_lambda=backbone_ridge_lambda,
            event_prior_alpha=event_prior_alpha,
        ),
    )
    return pd.DataFrame([backbone_model.to_row()]), pd.DataFrame([item.to_row() for item in sensor_energies])


def build_backbone_sensor_energy_spark_table(
    window_feature_df: DataFrame,
    *,
    event_prior_alpha: float = 0.35,
) -> DataFrame:
    """Aggregate per-sensor energy from ``window_x`` without collecting fact rows."""
    F = _spark_functions()
    vector_entries = (
        window_feature_df.select(
            F.explode_outer(F.map_entries("continuous_vector_t_end_scaled")).alias("entry"),
            "continuous_event_summary",
        )
        .select(
            F.col("entry.key").cast("string").alias("parameter_name"),
            F.col("entry.value").cast("double").alias("scaled_value"),
            F.coalesce(
                F.element_at(F.col("continuous_event_summary.slope_abs_impulse_by_parameter"), F.col("entry.key")).cast(
                    "double"
                ),
                F.lit(0.0),
            ).alias("slope_abs_impulse"),
            F.coalesce(
                F.element_at(F.col("continuous_event_summary.switch_count_by_parameter"), F.col("entry.key")).cast(
                    "double"
                ),
                F.lit(0.0),
            ).alias("switch_count"),
            F.coalesce(
                F.element_at(F.col("continuous_event_summary.threshold_count_by_parameter"), F.col("entry.key")).cast(
                    "double"
                ),
                F.lit(0.0),
            ).alias("threshold_count"),
            F.coalesce(
                F.element_at(F.col("continuous_event_summary.oscillation_count_by_parameter"), F.col("entry.key")).cast(
                    "double"
                ),
                F.lit(0.0),
            ).alias("oscillation_count"),
            F.coalesce(
                F.element_at(F.col("continuous_event_summary.drift_guard_count_by_parameter"), F.col("entry.key")).cast(
                    "double"
                ),
                F.lit(0.0),
            ).alias("drift_guard_count"),
            F.coalesce(
                F.element_at(
                    F.col("continuous_event_summary.slope_reinforcement_count_by_parameter"),
                    F.col("entry.key"),
                ).cast("double"),
                F.lit(0.0),
            ).alias("slope_reinforcement_count"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("scaled_value").isNotNull())
    )
    energy_df = vector_entries.groupBy("parameter_name").agg(
        F.sum(F.col("scaled_value") * F.col("scaled_value")).cast("double").alias("energy"),
        F.count(F.lit(1)).cast("int").alias("support_count"),
        F.sum(F.col("slope_abs_impulse")).cast("double").alias("slope_abs_impulse_total"),
        F.sum(F.col("switch_count")).cast("double").alias("switch_count_total"),
        F.sum(F.col("threshold_count")).cast("double").alias("threshold_count_total"),
        F.sum(F.col("oscillation_count")).cast("double").alias("oscillation_count_total"),
        F.sum(F.col("drift_guard_count")).cast("double").alias("drift_guard_count_total"),
        F.sum(F.col("slope_reinforcement_count")).cast("double").alias("slope_reinforcement_count_total"),
    ).withColumn(
        "event_prior",
        F.log1p(F.greatest(F.col("slope_abs_impulse_total"), F.lit(0.0)))
        + (F.lit(0.75) * F.log1p(F.greatest(F.col("switch_count_total"), F.lit(0.0))))
        + (
            F.lit(0.5)
            * F.log1p(F.greatest(F.col("threshold_count_total") + F.col("oscillation_count_total"), F.lit(0.0)))
        )
        + (F.lit(0.25) * F.log1p(F.greatest(F.col("drift_guard_count_total"), F.lit(0.0))))
        + (F.lit(0.25) * F.log1p(F.greatest(F.col("slope_reinforcement_count_total"), F.lit(0.0)))),
    )
    stats_df = energy_df.agg(
        F.avg(F.log1p(F.col("energy"))).cast("double").alias("energy_log_mean"),
        F.stddev_pop(F.log1p(F.col("energy"))).cast("double").alias("energy_log_std"),
        F.avg(F.col("event_prior")).cast("double").alias("event_prior_mean"),
        F.stddev_pop(F.col("event_prior")).cast("double").alias("event_prior_std"),
    )
    return (
        energy_df.crossJoin(stats_df)
        .withColumn(
            "energy_zscore",
            F.when(
                F.coalesce(F.col("energy_log_std"), F.lit(0.0)) > F.lit(0.0),
                (F.log1p(F.col("energy")) - F.col("energy_log_mean")) / F.col("energy_log_std"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "event_prior_zscore",
            F.when(
                F.coalesce(F.col("event_prior_std"), F.lit(0.0)) > F.lit(0.0),
                (F.col("event_prior") - F.col("event_prior_mean")) / F.col("event_prior_std"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "selection_score",
            F.col("energy_zscore") + (F.lit(float(event_prior_alpha)) * F.col("event_prior_zscore")),
        )
        .select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col("energy").cast("double").alias("energy"),
            F.col("support_count").cast("int").alias("support_count"),
            F.col("event_prior").cast("double").alias("event_prior"),
            F.col("selection_score").cast("double").alias("selection_score"),
        )
    )


def select_backbone_sensors_by_energy_spark(energy_df: DataFrame, *, k: int) -> list[str]:
    """Select the top-k backbone sensors from distributed energy rows."""
    F = _spark_functions()

    ordering = (
        [F.col("selection_score").desc(), F.col("energy").desc(), F.col("parameter_name").asc()]
        if "selection_score" in energy_df.columns
        else [F.col("energy").desc(), F.col("parameter_name").asc()]
    )
    rows = (
        energy_df.orderBy(*ordering)
        .limit(max(int(k), 1))
        .select("parameter_name")
        .collect()
    )
    return [str(row["parameter_name"]) for row in rows if str(row["parameter_name"])]


def build_backbone_selected_sensor_frame(window_feature_df: DataFrame, *, selected_sensors: list[str]) -> DataFrame:
    """Project the selected continuous window vector entries once for downstream backbone aggregation."""
    F = _spark_functions()
    backbone_sensors = [str(item) for item in selected_sensors if str(item)]
    key_columns = ["tail_id", "flight_id", "win_id"]
    return window_feature_df.select(
        *key_columns,
        *[
            F.coalesce(F.element_at("continuous_vector_t_end_scaled", F.lit(sensor)).cast("double"), F.lit(0.0)).alias(
                f"x_{idx}"
            )
            for idx, sensor in enumerate(backbone_sensors)
        ]
    )


def build_backbone_g_spark_table(
    window_feature_df: DataFrame,
    *,
    selected_sensors: list[str],
    selected_sensor_frame_df: DataFrame | None = None,
) -> DataFrame:
    """Compute the global backbone Gram matrix ``G`` in Spark."""
    F = _spark_functions()
    backbone_sensors = [str(item) for item in selected_sensors if str(item)]
    base = selected_sensor_frame_df or build_backbone_selected_sensor_frame(
        window_feature_df,
        selected_sensors=backbone_sensors,
    )
    aggregations = [F.count(F.lit(1)).cast("long").alias("window_count")]
    for i in range(len(backbone_sensors)):
        for j in range(len(backbone_sensors)):
            aggregations.append((F.sum(F.col(f"x_{i}") * F.col(f"x_{j}")).cast("double")).alias(f"g_{i}_{j}"))
    return base.agg(*aggregations)


def build_backbone_h_spark_table(
    window_feature_df: DataFrame,
    *,
    selected_sensors: list[str],
    selected_sensor_frame_df: DataFrame | None = None,
) -> DataFrame:
    """Compute the global backbone cross term ``H`` in Spark as long-form sensor rows."""
    F = _spark_functions()
    backbone_sensors = [str(item) for item in selected_sensors if str(item)]
    key_columns = ["tail_id", "flight_id", "win_id"]
    base = selected_sensor_frame_df or build_backbone_selected_sensor_frame(
        window_feature_df,
        selected_sensors=backbone_sensors,
    )
    exploded = (
        window_feature_df.select(*key_columns, F.explode_outer(F.map_entries("continuous_vector_t_end_scaled")).alias("entry"))
        .select(
            *key_columns,
            F.col("entry.key").cast("string").alias("parameter_name"),
            F.col("entry.value").cast("double").alias("scaled_value"),
        )
    )
    joined = base.join(exploded, on=key_columns, how="inner")
    aggregations = [
        F.sum(F.col(f"x_{idx}") * F.col("scaled_value")).cast("double").alias(f"h_{idx}")
        for idx in range(len(backbone_sensors))
    ]
    return (
        joined.where(F.col("parameter_name").isNotNull() & F.col("scaled_value").isNotNull())
        .groupBy("parameter_name")
        .agg(*aggregations)
        .select(
            "parameter_name",
            F.array(*[F.coalesce(F.col(f"h_{idx}"), F.lit(0.0)) for idx in range(len(backbone_sensors))]).alias("h_vector_c"),
        )
        .orderBy("parameter_name")
    )
