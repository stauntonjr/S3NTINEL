"""Typed Spark backbone artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.io.schemas.backbone import BACKBONE_SCHEMA, BACKBONE_SENSOR_ENERGY_SCHEMA
from libs.pyspark import Frame, Table


@dataclass(frozen=True)
class BackboneSelectedSensorFrame(Frame):
    @classmethod
    def from_window_features(
        cls,
        window_feature_df: "DataFrame",
        *,
        selected_sensors: list[str],
    ) -> "BackboneSelectedSensorFrame":
        from pyspark.sql import functions as F

        backbone_sensors = [str(item) for item in selected_sensors if str(item)]
        key_columns = ["tail_id", "flight_id", "win_id"]
        return cls(
            dataframe=window_feature_df.select(
                *key_columns,
                *[
                    F.coalesce(F.element_at("continuous_vector_t_end_scaled", F.lit(sensor)).cast("double"), F.lit(0.0)).alias(
                        f"x_{idx}"
                    )
                    for idx, sensor in enumerate(backbone_sensors)
                ],
            )
        )


@dataclass(frozen=True)
class BackboneSensorEnergyTable(Table):
    @classmethod
    def spark_schema(cls):
        return BACKBONE_SENSOR_ENERGY_SCHEMA()

    @classmethod
    def from_window_features(
        cls,
        window_feature_df: "DataFrame",
        *,
        event_prior_alpha: float = 0.35,
    ) -> "BackboneSensorEnergyTable":
        from pyspark.sql import functions as F

        vector_entries = (
            window_feature_df.select(
                F.explode_outer(F.map_entries("continuous_vector_t_end_scaled")).alias("entry"),
                "continuous_event_summary",
            )
            .select(
                F.col("entry.key").cast("string").alias("parameter_name"),
                F.col("entry.value").cast("double").alias("scaled_value"),
                F.coalesce(
                    F.element_at(F.col("continuous_event_summary.slope_abs_impulse_by_parameter"), F.col("entry.key")).cast("double"),
                    F.lit(0.0),
                ).alias("slope_abs_impulse"),
                F.coalesce(
                    F.element_at(F.col("continuous_event_summary.switch_count_by_parameter"), F.col("entry.key")).cast("double"),
                    F.lit(0.0),
                ).alias("switch_count"),
                F.coalesce(
                    F.element_at(F.col("continuous_event_summary.threshold_count_by_parameter"), F.col("entry.key")).cast("double"),
                    F.lit(0.0),
                ).alias("threshold_count"),
                F.coalesce(
                    F.element_at(F.col("continuous_event_summary.oscillation_count_by_parameter"), F.col("entry.key")).cast("double"),
                    F.lit(0.0),
                ).alias("oscillation_count"),
                F.coalesce(
                    F.element_at(F.col("continuous_event_summary.drift_guard_count_by_parameter"), F.col("entry.key")).cast("double"),
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
        return cls(
            dataframe=energy_df.crossJoin(stats_df)
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
                F.lit(None).cast("boolean").alias("selected_backbone"),
                F.lit(None).cast("int").alias("backbone_version"),
            ),
        )


@dataclass(frozen=True)
class BackboneGramFrame(Frame):
    @classmethod
    def from_window_features(
        cls,
        window_feature_df: "DataFrame",
        *,
        selected_sensors: list[str],
        selected_sensor_frame_df: "DataFrame | None" = None,
    ) -> "BackboneGramFrame":
        from pyspark.sql import functions as F

        backbone_sensors = [str(item) for item in selected_sensors if str(item)]
        base = selected_sensor_frame_df or BackboneSelectedSensorFrame.from_window_features(
            window_feature_df,
            selected_sensors=backbone_sensors,
        ).to_dataframe()
        aggregations = [F.count(F.lit(1)).cast("long").alias("window_count")]
        for i in range(len(backbone_sensors)):
            for j in range(len(backbone_sensors)):
                aggregations.append((F.sum(F.col(f"x_{i}") * F.col(f"x_{j}")).cast("double")).alias(f"g_{i}_{j}"))
        return cls(
            dataframe=base.agg(*aggregations)
        )


@dataclass(frozen=True)
class BackboneCrossTermFrame(Frame):
    @classmethod
    def from_window_features(
        cls,
        window_feature_df: "DataFrame",
        *,
        selected_sensors: list[str],
        selected_sensor_frame_df: "DataFrame | None" = None,
    ) -> "BackboneCrossTermFrame":
        from pyspark.sql import functions as F

        backbone_sensors = [str(item) for item in selected_sensors if str(item)]
        key_columns = ["tail_id", "flight_id", "win_id"]
        base = selected_sensor_frame_df or BackboneSelectedSensorFrame.from_window_features(
            window_feature_df,
            selected_sensors=backbone_sensors,
        ).to_dataframe()
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
        return cls(
            dataframe=joined.where(F.col("parameter_name").isNotNull() & F.col("scaled_value").isNotNull())
            .groupBy("parameter_name")
            .agg(*aggregations)
            .select(
                "parameter_name",
                F.array(*[F.coalesce(F.col(f"h_{idx}"), F.lit(0.0)) for idx in range(len(backbone_sensors))]).alias("h_vector_c"),
            )
            .orderBy("parameter_name")
        )


@dataclass(frozen=True)
class BackboneTable(Table):
    @classmethod
    def spark_schema(cls):
        return BACKBONE_SCHEMA()


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
