"""Profiling-domain artifact builders for telemetry characterization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar

from libs.behavior.primitives import (
    DISCRETE_PRIMITIVE_FEATURE_COLUMNS,
    NUMERIC_PRIMITIVE_FEATURE_COLUMNS,
    BehaviorChoiceThresholds,
    build_behavior_choice_columns,
    build_behavior_family_score_columns,
)
from libs.common import ParameterDataType, try_cast_double
from libs.io.schemas import (
    CONTINUOUS_SCALING_PROFILE_SCHEMA,
    PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_SCHEMA,
    PARAMETER_BEHAVIOR_PROFILE_SCHEMA,
    PARAMETER_DATATYPE_PROFILE_SCHEMA,
)
from libs.perf.annotations import hot_path
from libs.pyspark import Table

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


@dataclass(frozen=True)
class TelemetryProfileSource:
    """Canonical raw-telemetry view used by production profiling builders."""

    raw_input_df: "DataFrame"

    def resolved_timestamp_column(self) -> str:
        if "timestamp_utc" in self.raw_input_df.columns:
            return "timestamp_utc"
        if "timestamp" in self.raw_input_df.columns:
            return "timestamp"
        raise ValueError("input dataframe must include either 'timestamp_utc' or 'timestamp'")

    def base_projection_df(self) -> "DataFrame":
        from pyspark.sql import functions as F

        return self.raw_input_df.select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col(self.resolved_timestamp_column()).cast("timestamp").alias("timestamp_utc"),
            F.trim(F.col("parameter_value").cast("string")).alias("parameter_value"),
        ).where(F.col("parameter_name").isNotNull() & F.col("timestamp_utc").isNotNull())

    def numeric_value_column(self, source_column: str = "parameter_value") -> "Column":
        return try_cast_double(source_column)

    def numeric_projection_df(self) -> "DataFrame":
        from pyspark.sql import functions as F

        return self.base_projection_df().select(
            "parameter_name",
            "timestamp_utc",
            self.numeric_value_column().alias("parameter_value_num"),
        )


@dataclass(frozen=True)
class ParameterProfile:
    """Observed telemetry statistics used to derive profiling artifacts."""

    @classmethod
    @hot_path
    def build_dataframe(
        cls,
        raw_input_df: "DataFrame",
        numeric_ratio_threshold: float = 0.8,
        categorical_cardinality_max: int = 200,
    ) -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        source = TelemetryProfileSource(raw_input_df)
        prepped = (
            source.base_projection_df()
            .withColumn(
                "is_missing",
                F.col("parameter_value").isNull()
                | (F.col("parameter_value") == F.lit(""))
                | (F.lower(F.col("parameter_value")).isin("null", "nan", "none")),
            )
            .withColumn("value_num", source.numeric_value_column())
            .withColumn("is_numeric", F.col("value_num").isNotNull())
        )

        base_stats = prepped.groupBy("parameter_name").agg(
            F.count(F.lit(1)).alias("total_count"),
            F.sum(F.when(F.col("is_missing"), F.lit(1)).otherwise(F.lit(0))).alias("missing_count"),
            F.sum(F.when(F.col("is_numeric"), F.lit(1)).otherwise(F.lit(0))).alias("numeric_count"),
            F.approx_count_distinct("parameter_value").alias("distinct_value_count"),
        )

        numeric_stats = prepped.where(F.col("value_num").isNotNull()).groupBy("parameter_name").agg(
            F.avg("value_num").alias("num_mean"),
            F.stddev_pop("value_num").alias("num_std"),
            F.min("value_num").alias("num_min"),
            F.max("value_num").alias("num_max"),
            F.percentile_approx("value_num", F.array(F.lit(0.01), F.lit(0.5), F.lit(0.99)), 1000).alias("num_quantiles"),
        )

        sample_window = Window.partitionBy("parameter_name").orderBy("timestamp_utc")
        sample_stats = (
            prepped.withColumn("prev_ts", F.lag("timestamp_utc").over(sample_window))
            .withColumn("diff_ms", F.unix_millis("timestamp_utc") - F.unix_millis("prev_ts"))
            .where(F.col("diff_ms") > 0)
            .groupBy("parameter_name")
            .agg(F.percentile_approx("diff_ms", F.lit(0.5), 1000).alias("median_interval_ms"))
            .withColumn(
                "sampling_rate_hz",
                F.when(F.col("median_interval_ms") > 0, F.lit(1000.0) / F.col("median_interval_ms")).otherwise(F.lit(None)),
            )
        )

        return (
            base_stats.alias("b")
            .join(numeric_stats.alias("n"), on="parameter_name", how="left")
            .join(sample_stats.alias("s"), on="parameter_name", how="left")
            .withColumn("missing_rate", F.col("missing_count") / F.greatest(F.col("total_count"), F.lit(1)))
            .withColumn("observed_count", F.col("total_count") - F.col("missing_count"))
            .withColumn("numeric_rate", F.col("numeric_count") / F.greatest(F.col("observed_count"), F.lit(1)))
            .withColumn(
                "binary_numeric_like",
                (F.col("numeric_rate") >= F.lit(numeric_ratio_threshold))
                & (F.col("distinct_value_count") == F.lit(2))
                & F.col("num_min").isNotNull()
                & F.col("num_max").isNotNull()
                & (F.col("num_min") >= F.lit(0.0))
                & (F.col("num_max") <= F.lit(1.0)),
            )
            .withColumn(
                "detected_type",
                F.when(F.col("distinct_value_count") <= 1, F.lit(ParameterDataType.CONSTANT.value))
                .when(
                    (F.col("numeric_rate") >= F.lit(numeric_ratio_threshold))
                    & ((F.col("distinct_value_count") > 2) | ~F.col("binary_numeric_like")),
                    F.lit(ParameterDataType.NUMERIC.value),
                )
                .when(F.col("distinct_value_count") == 2, F.lit(ParameterDataType.CATEGORICAL.value))
                .when(
                    F.col("distinct_value_count") <= F.lit(int(categorical_cardinality_max)),
                    F.lit(ParameterDataType.CATEGORICAL.value),
                )
                .otherwise(F.lit(ParameterDataType.HIGH_CARDINALITY.value)),
            )
            .select(
                "parameter_name",
                "detected_type",
                F.col("total_count").cast("long").alias("total_count"),
                F.col("missing_count").cast("long").alias("missing_count"),
                F.col("missing_rate").cast("double").alias("missing_rate"),
                F.col("numeric_rate").cast("double").alias("numeric_rate"),
                F.col("distinct_value_count").cast("long").alias("distinct_value_count"),
                F.col("num_mean").cast("double").alias("num_mean"),
                F.coalesce(F.col("num_std"), F.lit(0.0)).cast("double").alias("num_std"),
                F.col("num_min").cast("double").alias("num_min"),
                F.col("num_max").cast("double").alias("num_max"),
                F.col("num_quantiles").getItem(0).cast("double").alias("num_q01"),
                F.col("num_quantiles").getItem(1).cast("double").alias("num_q50"),
                F.col("num_quantiles").getItem(2).cast("double").alias("num_q99"),
                F.col("median_interval_ms").cast("double").alias("median_interval_ms"),
                F.col("sampling_rate_hz").cast("double").alias("sampling_rate_hz"),
            )
        )


@dataclass(frozen=True)
class CategoricalDistribution:
    """Top observed categorical values per parameter."""

    @classmethod
    def build_dataframe(cls, raw_input_df: "DataFrame", top_k: int = 10) -> "DataFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        value_df = (
            raw_input_df.select(
                F.col("parameter_name").cast("string").alias("parameter_name"),
                F.trim(F.col("parameter_value").cast("string")).alias("parameter_value"),
            )
            .where(F.col("parameter_name").isNotNull())
            .where(F.col("parameter_value").isNotNull() & (F.col("parameter_value") != F.lit("")))
        )

        counts = value_df.groupBy("parameter_name", "parameter_value").agg(F.count(F.lit(1)).alias("value_count"))
        rank_window = Window.partitionBy("parameter_name").orderBy(F.col("value_count").desc(), F.col("parameter_value").asc())
        return (
            counts.withColumn("rank", F.row_number().over(rank_window))
            .where(F.col("rank") <= F.lit(int(top_k)))
            .select("parameter_name", "parameter_value", "value_count", "rank")
        )


@dataclass(frozen=True)
class ParameterDatatypeProfile(Table):
    """Canonical datatype profile artifact."""

    @classmethod
    def spark_schema(cls):
        return PARAMETER_DATATYPE_PROFILE_SCHEMA()

    @classmethod
    def from_parameter_profile(cls, profile_df: "DataFrame") -> "ParameterDatatypeProfile":
        from pyspark.sql import functions as F

        return cls(
            dataframe=profile_df.select(
                "parameter_name",
                F.col("detected_type").cast("string").alias("parameter_datatype_profiled"),
                F.col("total_count").cast("long").alias("total_count"),
                F.col("missing_count").cast("long").alias("missing_count"),
                F.col("missing_rate").cast("double").alias("missing_rate"),
                F.col("numeric_rate").cast("double").alias("numeric_rate"),
                F.col("distinct_value_count").cast("long").alias("distinct_value_count"),
                F.col("num_mean").cast("double").alias("num_mean"),
                F.col("num_std").cast("double").alias("num_std"),
                F.col("num_min").cast("double").alias("num_min"),
                F.col("num_max").cast("double").alias("num_max"),
                F.col("num_q01").cast("double").alias("num_q01"),
                F.col("num_q50").cast("double").alias("num_q50"),
                F.col("num_q99").cast("double").alias("num_q99"),
                F.col("median_interval_ms").cast("double").alias("median_interval_ms"),
                F.col("sampling_rate_hz").cast("double").alias("sampling_rate_profiled_hz"),
            )
        )


@dataclass(frozen=True)
class ContinuousScalingProfile(Table):
    """Robust scaling metadata for continuous parameters."""

    @classmethod
    def spark_schema(cls):
        return CONTINUOUS_SCALING_PROFILE_SCHEMA()

    @classmethod
    def from_raw_input(
        cls,
        raw_input_df: "DataFrame",
        datatype_profile_df: "DataFrame",
    ) -> "ContinuousScalingProfile":
        from pyspark.sql import functions as F

        source = TelemetryProfileSource(raw_input_df)
        numeric_like = datatype_profile_df.select("parameter_name", "parameter_datatype_profiled").where(
            F.col("parameter_datatype_profiled").isin("numeric", "constant")
        )
        numeric_df = (
            source.numeric_projection_df()
            .join(numeric_like, on="parameter_name", how="inner")
            .where(F.col("parameter_value_num").isNotNull())
        )
        quantiles_df = numeric_df.groupBy("parameter_name").agg(
            F.count(F.lit(1)).alias("support_count"),
            F.percentile_approx(
                "parameter_value_num",
                F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75)),
                1000,
            ).alias("scaling_quantiles"),
        )
        return cls(
            dataframe=quantiles_df.select(
                "parameter_name",
                F.col("support_count").cast("long").alias("support_count"),
                F.col("scaling_quantiles").getItem(0).cast("double").alias("scaling_q25"),
                F.col("scaling_quantiles").getItem(1).cast("double").alias("scaling_center_median"),
                F.col("scaling_quantiles").getItem(2).cast("double").alias("scaling_q75"),
                (
                    F.col("scaling_quantiles").getItem(2).cast("double")
                    - F.col("scaling_quantiles").getItem(0).cast("double")
                ).alias("scaling_iqr"),
            )
        )


@dataclass(frozen=True)
class ParameterBehaviorPrimitiveProfile(Table):
    """Shared primitive evidence profile derived directly from raw telemetry."""

    NUMERIC_SIGNIFICANT_DIFF_THRESHOLD: ClassVar[float] = 0.05
    CENTER_BAND_WIDTH: ClassVar[float] = 1.0
    SOFT_BOUND_WIDTH: ClassVar[float] = 2.5
    HARD_BOUND_WIDTH: ClassVar[float] = 2.0
    RAW_GEOMETRY_DIFF_MULTIPLIER: ClassVar[float] = 2.0
    RAW_GEOMETRY_IQR_FLOOR_RATIO: ClassVar[float] = 0.12
    BLENDED_SCALED_RETURN_WEIGHT: ClassVar[float] = 0.85
    BLENDED_RAW_RETURN_WEIGHT: ClassVar[float] = 0.15

    @classmethod
    def spark_schema(cls):
        return PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_SCHEMA()

    @classmethod
    def _source_df(cls, raw_input_df: "DataFrame", datatype_profile_df: "DataFrame") -> "DataFrame":
        source = TelemetryProfileSource(raw_input_df)
        profile_lookup_df = datatype_profile_df.select("parameter_name", "parameter_datatype_profiled")
        return source.base_projection_df().join(profile_lookup_df, on="parameter_name", how="left")

    @classmethod
    def _common_summary_df(cls, source_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        return source_df.groupBy("parameter_name", "parameter_datatype_profiled").agg(
            F.count(F.lit(1)).cast("long").alias("sample_count"),
            F.min("timestamp_utc").alias("profile_window_start_utc"),
            F.max("timestamp_utc").alias("profile_window_end_utc"),
        )

    @classmethod
    def _ordered_source_df(cls, source_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        ordered_window = Window.partitionBy("parameter_name").orderBy("timestamp_utc")
        return (
            source_df.withColumn("parameter_value_num", try_cast_double("parameter_value"))
            .withColumn("prev_value", F.lag("parameter_value").over(ordered_window))
            .withColumn("prev_prev_value", F.lag("parameter_value", 2).over(ordered_window))
            .withColumn("prev_value_num", F.lag("parameter_value_num").over(ordered_window))
            .withColumn(
                "diff_num",
                F.when(
                    F.col("parameter_value_num").isNotNull() & F.col("prev_value_num").isNotNull(),
                    F.col("parameter_value_num") - F.col("prev_value_num"),
                ),
            )
        )

    @classmethod
    def _discrete_features_df(cls, source_df: "DataFrame", common_summary_df: "DataFrame", ordered_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        return (
            ordered_df.groupBy("parameter_name").agg(
                F.countDistinct("parameter_value").cast("double").alias("distinct_state_count_profiled"),
                F.sum(
                    F.when(
                        F.col("prev_value").isNotNull() & (F.col("parameter_value") != F.col("prev_value")),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).alias("transition_count"),
                F.sum(
                    F.when(
                        F.col("prev_prev_value").isNotNull()
                        & F.col("prev_value").isNotNull()
                        & (F.col("parameter_value") == F.col("prev_prev_value"))
                        & (F.col("parameter_value") != F.col("prev_value")),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).alias("state_chatter_count"),
            )
            .join(common_summary_df.select("parameter_name", "sample_count"), on="parameter_name", how="left")
            .join(
                source_df.groupBy("parameter_name", "parameter_value")
                .agg(F.count(F.lit(1)).alias("state_count"))
                .groupBy("parameter_name")
                .agg(F.max("state_count").cast("double").alias("dominant_state_count")),
                on="parameter_name",
                how="left",
            )
            .select(
                "parameter_name",
                (
                    F.col("transition_count") / F.greatest(F.col("sample_count").cast("double") - F.lit(1.0), F.lit(1.0))
                ).cast("double").alias("transition_rate_profiled"),
                (
                    F.col("sample_count").cast("double") / F.greatest(F.col("transition_count") + F.lit(1.0), F.lit(1.0))
                ).cast("double").alias("mean_dwell_profiled"),
                (
                    F.coalesce(F.col("dominant_state_count"), F.lit(0.0))
                    / F.greatest(F.col("sample_count").cast("double"), F.lit(1.0))
                ).cast("double").alias("dominant_state_ratio_profiled"),
                (
                    F.coalesce(F.col("state_chatter_count"), F.lit(0.0))
                    / F.greatest(F.col("sample_count").cast("double") - F.lit(2.0), F.lit(1.0))
                ).cast("double").alias("state_chatter_rate_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        F.lit(1.0)
                        - F.least(
                            F.greatest(F.coalesce(F.col("distinct_state_count_profiled"), F.lit(0.0)) - F.lit(1.0), F.lit(0.0)),
                            F.lit(9.0),
                        ) / F.lit(9.0),
                    ),
                ).cast("double").alias("discrete_low_cardinality_score_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(F.lit(0.0), F.lit(1.0) - (
                        F.col("transition_count") / F.greatest(F.col("sample_count").cast("double") - F.lit(1.0), F.lit(1.0))
                    )),
                ).cast("double").alias("discrete_low_transition_score_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        F.least(
                            F.col("sample_count").cast("double") / F.greatest(F.col("transition_count") + F.lit(1.0), F.lit(1.0)),
                            F.lit(10.0),
                        ) / F.lit(10.0),
                    ),
                ).cast("double").alias("discrete_dwell_score_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        F.lit(1.0)
                        - F.abs(
                            (
                                F.col("transition_count") / F.greatest(F.col("sample_count").cast("double") - F.lit(1.0), F.lit(1.0))
                            ) - F.lit(0.18)
                        ) / F.lit(0.18),
                    ),
                ).cast("double").alias("transition_balance_score_profiled"),
            )
        )

    @classmethod
    def _numeric_ordered_df(
        cls,
        ordered_df: "DataFrame",
        scaling_profile_df: "DataFrame",
        *,
        significant_diff_threshold: float,
    ) -> "DataFrame":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        ordered_window = Window.partitionBy("parameter_name").orderBy("timestamp_utc")
        return (
            ordered_df.where(F.col("parameter_datatype_profiled").isin("numeric", "constant"))
            .where(F.col("parameter_value_num").isNotNull())
            .join(
                scaling_profile_df.select("parameter_name", "scaling_center_median", "scaling_iqr"),
                on="parameter_name",
                how="left",
            )
            .withColumn(
                "scaled_value_num",
                (F.col("parameter_value_num") - F.coalesce(F.col("scaling_center_median"), F.lit(0.0)))
                / F.greatest(F.abs(F.coalesce(F.col("scaling_iqr"), F.lit(1.0))), F.lit(1e-6)),
            )
            .withColumn(
                "raw_abs_deviation_num",
                F.abs(F.col("parameter_value_num") - F.coalesce(F.col("scaling_center_median"), F.lit(0.0))),
            )
            .withColumn("prev_raw_abs_deviation_num", F.lag("raw_abs_deviation_num").over(ordered_window))
            .withColumn("prev_scaled_value_num", F.lag("scaled_value_num").over(ordered_window))
            .withColumn(
                "scaled_diff_num",
                F.when(
                    F.col("scaled_value_num").isNotNull() & F.col("prev_scaled_value_num").isNotNull(),
                    F.col("scaled_value_num") - F.col("prev_scaled_value_num"),
                ),
            )
            .withColumn("prev_scaled_diff_num", F.lag("scaled_diff_num").over(ordered_window))
            .withColumn(
                "significant_sign",
                F.when(
                    F.abs(F.col("scaled_diff_num")) >= F.lit(float(significant_diff_threshold)),
                    F.signum(F.col("scaled_diff_num")),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn(
                "run_break",
                F.when(F.col("significant_sign") == F.lit(0.0), F.lit(1))
                .when(F.lag("significant_sign").over(ordered_window).isNull(), F.lit(1))
                .when(F.col("significant_sign") != F.lag("significant_sign").over(ordered_window), F.lit(1))
                .otherwise(F.lit(0)),
            )
            .withColumn("run_id", F.sum("run_break").over(ordered_window))
        )

    @classmethod
    def _numeric_features_df(
        cls,
        numeric_df: "DataFrame",
        *,
        center_band_width: float,
        soft_bound_width: float,
        hard_bound_width: float,
    ) -> "DataFrame":
        from pyspark.sql import functions as F

        raw_geometry_df = (
            numeric_df.groupBy("parameter_name")
            .agg(
                F.percentile_approx(
                    F.abs(F.col("diff_num")),
                    F.array(F.lit(0.50), F.lit(0.75)),
                    1000,
                ).alias("raw_abs_diff_quantiles"),
                F.max(F.abs(F.coalesce(F.col("scaling_iqr"), F.lit(0.0)))).cast("double").alias("scaling_iqr_abs"),
            )
            .select(
                "parameter_name",
                F.col("raw_abs_diff_quantiles").getItem(0).cast("double").alias("raw_abs_diff_q50"),
                F.col("raw_abs_diff_quantiles").getItem(1).cast("double").alias("raw_abs_diff_q75"),
                F.greatest(
                    F.coalesce(F.col("raw_abs_diff_quantiles").getItem(1).cast("double"), F.lit(0.0))
                    * F.lit(float(cls.RAW_GEOMETRY_DIFF_MULTIPLIER)),
                    F.coalesce(F.col("scaling_iqr_abs"), F.lit(0.0)) * F.lit(float(cls.RAW_GEOMETRY_IQR_FLOOR_RATIO)),
                    F.lit(1e-6),
                ).cast("double").alias("raw_geometry_unit"),
            )
        )
        numeric_geometry_df = numeric_df.join(raw_geometry_df, on="parameter_name", how="left")

        run_summary_df = (
            numeric_geometry_df.where(F.col("significant_sign") != F.lit(0.0))
            .groupBy("parameter_name", "run_id")
            .agg(F.count(F.lit(1)).cast("double").alias("run_length"))
            .groupBy("parameter_name")
            .agg(
                F.max("run_length").cast("double").alias("max_run_length"),
                F.avg("run_length").cast("double").alias("mean_run_length"),
                F.sum(F.when(F.col("run_length") >= F.lit(2.0), F.lit(1.0)).otherwise(F.lit(0.0))).cast("double").alias("persistent_run_count"),
            )
        )

        numeric_diff_summary_df = numeric_geometry_df.groupBy("parameter_name").agg(
            F.count("scaled_diff_num").cast("double").alias("diff_count"),
            F.sum(F.abs("diff_num")).cast("double").alias("gross_change"),
            F.avg(F.col("scaled_diff_num") * F.col("scaled_diff_num")).cast("double").alias("diff_energy"),
            F.sum(F.when(F.col("scaled_diff_num") > 0, F.lit(1.0)).otherwise(F.lit(0.0))).cast("double").alias("positive_diff_count"),
            F.sum(F.when(F.col("scaled_diff_num") < 0, F.lit(1.0)).otherwise(F.lit(0.0))).cast("double").alias("negative_diff_count"),
            F.sum(
                F.when(
                    F.col("prev_scaled_diff_num").isNotNull()
                    & F.col("scaled_diff_num").isNotNull()
                    & ((F.col("prev_scaled_diff_num") * F.col("scaled_diff_num")) < 0),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).cast("double").alias("sign_flip_count"),
            F.count("prev_value_num").cast("double").alias("lag_pair_count"),
            F.sum(F.coalesce(F.col("prev_value_num"), F.lit(0.0))).cast("double").alias("sum_prev"),
            F.sum(F.when(F.col("prev_value_num").isNotNull(), F.col("parameter_value_num")).otherwise(F.lit(0.0))).cast("double").alias("sum_curr"),
            F.sum(F.when(F.col("prev_value_num").isNotNull(), F.col("prev_value_num") * F.col("parameter_value_num")).otherwise(F.lit(0.0))).cast("double").alias("sum_prev_curr"),
            F.sum(F.when(F.col("prev_value_num").isNotNull(), F.col("prev_value_num") * F.col("prev_value_num")).otherwise(F.lit(0.0))).cast("double").alias("sum_prev_sq"),
            F.sum(F.when(F.col("prev_value_num").isNotNull(), F.col("parameter_value_num") * F.col("parameter_value_num")).otherwise(F.lit(0.0))).cast("double").alias("sum_curr_sq"),
            F.min_by("parameter_value_num", "timestamp_utc").cast("double").alias("first_value_num"),
            F.max_by("parameter_value_num", "timestamp_utc").cast("double").alias("last_value_num"),
            F.avg(F.col("scaled_value_num") * F.col("scaled_value_num")).cast("double").alias("level_energy"),
            F.max(F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6))).cast("double").alias("raw_geometry_unit"),
            F.avg(
                F.when(F.abs(F.col("scaled_value_num")) <= F.lit(float(center_band_width)), F.lit(1.0)).otherwise(F.lit(0.0))
            ).cast("double").alias("scaled_center_occupancy_profiled"),
            F.avg(
                F.when(F.abs(F.col("scaled_value_num")) > F.lit(float(center_band_width)), F.lit(1.0)).otherwise(F.lit(0.0))
            ).cast("double").alias("scaled_excursion_rate_profiled"),
            F.avg(
                F.when(F.abs(F.col("scaled_value_num")) <= F.lit(float(soft_bound_width)), F.lit(1.0)).otherwise(F.lit(0.0))
            ).cast("double").alias("scaled_bound_occupancy_profiled"),
            F.avg(
                F.when(F.abs(F.col("scaled_value_num")) >= F.lit(float(hard_bound_width)), F.lit(1.0)).otherwise(F.lit(0.0))
            ).cast("double").alias("scaled_saturation_rate_profiled"),
            F.avg(
                F.when(
                    F.col("raw_abs_deviation_num")
                    <= (F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6)) * F.lit(float(center_band_width))),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).cast("double").alias("raw_center_occupancy_profiled"),
            F.avg(
                F.when(
                    F.col("raw_abs_deviation_num")
                    > (F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6)) * F.lit(float(center_band_width))),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).cast("double").alias("raw_excursion_rate_profiled"),
            F.avg(
                F.when(
                    F.col("raw_abs_deviation_num")
                    <= (F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6)) * F.lit(float(soft_bound_width))),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).cast("double").alias("raw_bound_occupancy_profiled"),
            F.avg(
                F.when(
                    F.col("raw_abs_deviation_num")
                    >= (F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6)) * F.lit(float(hard_bound_width))),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).cast("double").alias("raw_saturation_rate_profiled"),
        )
        numeric_diff_df = numeric_diff_summary_df.select(
            "parameter_name",
            "diff_count",
            "gross_change",
            "diff_energy",
            "positive_diff_count",
            "negative_diff_count",
            "sign_flip_count",
            "lag_pair_count",
            "sum_prev",
            "sum_curr",
            "sum_prev_curr",
            "sum_prev_sq",
            "sum_curr_sq",
            "first_value_num",
            "last_value_num",
            "level_energy",
            "raw_geometry_unit",
            F.coalesce(F.col("scaled_center_occupancy_profiled"), F.lit(0.0)).cast("double").alias("center_occupancy_profiled"),
            F.coalesce(F.col("scaled_excursion_rate_profiled"), F.lit(0.0)).cast("double").alias("excursion_rate_profiled"),
            F.coalesce(F.col("scaled_bound_occupancy_profiled"), F.lit(0.0)).cast("double").alias("bound_occupancy_profiled"),
            F.coalesce(F.col("scaled_saturation_rate_profiled"), F.lit(0.0)).cast("double").alias("saturation_rate_profiled"),
            F.coalesce(F.col("raw_excursion_rate_profiled"), F.lit(0.0)).cast("double").alias("raw_excursion_rate_profiled"),
            F.coalesce(F.col("raw_center_occupancy_profiled"), F.lit(0.0)).cast("double").alias("raw_center_occupancy_profiled"),
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    F.coalesce(F.col("gross_change"), F.lit(0.0))
                    / F.greatest(
                        F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6))
                        * F.greatest(F.coalesce(F.col("diff_count"), F.lit(0.0)), F.lit(1.0)),
                        F.lit(1e-6),
                    ),
                ),
            ).cast("double").alias("raw_motion_activity_score"),
        )

        numeric_reset_df = (
            numeric_geometry_df.join(
                numeric_diff_df.select("parameter_name", "positive_diff_count", "negative_diff_count"),
                on="parameter_name",
                how="left",
            )
            .groupBy("parameter_name")
            .agg(
                F.sum(
                    F.when(
                        (F.col("positive_diff_count") >= F.col("negative_diff_count")) & (F.col("scaled_diff_num") < F.lit(-1.5)),
                        F.lit(1.0),
                    ).when(
                        (F.col("negative_diff_count") > F.col("positive_diff_count")) & (F.col("scaled_diff_num") > F.lit(1.5)),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).cast("double").alias("reset_like_count"),
            )
        )

        numeric_regulation_summary_df = (
            numeric_geometry_df.withColumn("prev_scaled_deviation_num", F.abs(F.col("prev_scaled_value_num")))
            .withColumn("scaled_deviation_num", F.abs(F.col("scaled_value_num")))
            .withColumn("prev_raw_deviation_num", F.col("prev_raw_abs_deviation_num"))
            .withColumn("raw_deviation_num", F.col("raw_abs_deviation_num"))
            .groupBy("parameter_name")
            .agg(
                F.sum(
                    F.when(
                        F.col("prev_value_num").isNotNull()
                        & (F.col("prev_scaled_deviation_num") > F.lit(float(center_band_width))),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).cast("double").alias("scaled_excursion_pair_count"),
                F.sum(
                    F.when(
                        F.col("prev_value_num").isNotNull()
                        & (F.col("prev_scaled_deviation_num") > F.lit(float(center_band_width)))
                        & (F.col("scaled_deviation_num") <= F.col("prev_scaled_deviation_num"))
                        & (F.col("scaled_deviation_num") <= F.lit(float(center_band_width))),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).cast("double").alias("scaled_excursion_return_count"),
                F.sum(
                    F.when(
                        F.col("prev_value_num").isNotNull()
                        & (
                            F.col("prev_raw_deviation_num")
                            > (F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6)) * F.lit(float(center_band_width)))
                        ),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).cast("double").alias("raw_excursion_pair_count"),
                F.sum(
                    F.when(
                        F.col("prev_value_num").isNotNull()
                        & (
                            F.col("prev_raw_deviation_num")
                            > (F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6)) * F.lit(float(center_band_width)))
                        )
                        & (F.col("raw_deviation_num") <= F.col("prev_raw_deviation_num"))
                        & (
                            F.col("raw_deviation_num")
                            <= (F.coalesce(F.col("raw_geometry_unit"), F.lit(1e-6)) * F.lit(float(center_band_width)))
                        ),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).cast("double").alias("raw_excursion_return_count"),
            )
        )
        numeric_regulation_df = (
            numeric_regulation_summary_df
            .select(
                "parameter_name",
                (
                    (F.lit(float(cls.BLENDED_SCALED_RETURN_WEIGHT))
                    * (
                        F.coalesce(F.col("scaled_excursion_return_count"), F.lit(0.0))
                        / F.greatest(F.coalesce(F.col("scaled_excursion_pair_count"), F.lit(0.0)), F.lit(1.0))
                    ))
                    + (F.lit(float(cls.BLENDED_RAW_RETURN_WEIGHT))
                    * (
                        F.coalesce(F.col("raw_excursion_return_count"), F.lit(0.0))
                        / F.greatest(F.coalesce(F.col("raw_excursion_pair_count"), F.lit(0.0)), F.lit(1.0))
                    ))
                ).cast("double").alias("excursion_return_ratio_profiled"),
            )
        )

        lag1_corr = F.when(
            F.sqrt(
                F.greatest((F.col("lag_pair_count") * F.col("sum_prev_sq")) - (F.col("sum_prev") * F.col("sum_prev")), F.lit(0.0))
                * F.greatest((F.col("lag_pair_count") * F.col("sum_curr_sq")) - (F.col("sum_curr") * F.col("sum_curr")), F.lit(0.0))
            ) > F.lit(1e-12),
            ((F.col("lag_pair_count") * F.col("sum_prev_curr")) - (F.col("sum_prev") * F.col("sum_curr")))
            / F.sqrt(
                F.greatest((F.col("lag_pair_count") * F.col("sum_prev_sq")) - (F.col("sum_prev") * F.col("sum_prev")), F.lit(0.0))
                * F.greatest((F.col("lag_pair_count") * F.col("sum_curr_sq")) - (F.col("sum_curr") * F.col("sum_curr")), F.lit(0.0))
            ),
        ).otherwise(F.lit(0.0))

        numeric_features_df = (
            numeric_diff_df.join(run_summary_df, on="parameter_name", how="left")
            .join(numeric_regulation_df, on="parameter_name", how="left")
            .join(numeric_reset_df, on="parameter_name", how="left")
            .select(
                "parameter_name",
                (F.coalesce(F.col("max_run_length"), F.lit(0.0)) / F.greatest(F.col("diff_count"), F.lit(1.0))).cast("double").alias("persistent_run_strength_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        F.coalesce(F.col("persistent_run_count"), F.lit(0.0))
                        / F.greatest(F.coalesce(F.col("mean_run_length"), F.lit(1.0)), F.lit(1.0)),
                    ),
                ).cast("double").alias("run_reinforcement_score_profiled"),
                (F.coalesce(F.col("sign_flip_count"), F.lit(0.0)) / F.greatest(F.col("diff_count") - F.lit(1.0), F.lit(1.0))).cast("double").alias("reversal_rate_profiled"),
                (F.coalesce(F.col("sign_flip_count"), F.lit(0.0)) / F.greatest(F.col("diff_count") - F.lit(1.0), F.lit(1.0))).cast("double").alias("sign_flip_rate_profiled"),
                "center_occupancy_profiled",
                "excursion_rate_profiled",
                F.coalesce(F.col("excursion_return_ratio_profiled"), F.lit(0.0)).cast("double").alias("excursion_return_ratio_profiled"),
                "bound_occupancy_profiled",
                "saturation_rate_profiled",
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        (F.lit(0.50) * F.greatest(F.col("positive_diff_count") / F.greatest(F.col("diff_count"), F.lit(1.0)), F.col("negative_diff_count") / F.greatest(F.col("diff_count"), F.lit(1.0))))
                        + (F.lit(0.35) * (F.abs(F.col("last_value_num") - F.col("first_value_num")) / F.greatest(F.col("gross_change"), F.lit(1e-6))))
                        + (F.lit(0.15) * (F.lit(1.0) - (F.coalesce(F.col("sign_flip_count"), F.lit(0.0)) / F.greatest(F.col("diff_count") - F.lit(1.0), F.lit(1.0))))),
                    ),
                ).cast("double").alias("monotone_accumulation_score_profiled"),
                (F.coalesce(F.col("reset_like_count"), F.lit(0.0)) / F.greatest(F.col("diff_count"), F.lit(1.0))).cast("double").alias("reset_drop_rate_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        (F.lit(0.55) * (F.coalesce(F.col("sign_flip_count"), F.lit(0.0)) / F.greatest(F.col("diff_count") - F.lit(1.0), F.lit(1.0))))
                        + (F.lit(0.25) * F.coalesce(F.col("bound_occupancy_profiled"), F.lit(0.0)))
                        + (F.lit(0.20) * (F.lit(1.0) - (F.abs(F.col("last_value_num") - F.col("first_value_num")) / F.greatest(F.col("gross_change"), F.lit(1e-6))))),
                    ),
                ).cast("double").alias("oscillation_score_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        (F.lit(0.10) * F.coalesce(F.col("bound_occupancy_profiled"), F.lit(0.0)))
                        + (F.lit(0.05) * (F.lit(1.0) - F.coalesce(F.col("saturation_rate_profiled"), F.lit(0.0))))
                        + (
                            F.lit(0.10)
                            * F.least(
                                F.lit(1.0),
                                F.greatest(F.lit(0.0), F.coalesce(F.col("excursion_rate_profiled"), F.lit(0.0)) * F.lit(2.0)),
                            )
                        )
                        + (
                            F.lit(0.15)
                            * F.least(
                                F.lit(1.0),
                                F.greatest(F.lit(0.0), F.coalesce(F.col("raw_excursion_rate_profiled"), F.lit(0.0)) * F.lit(1.5)),
                            )
                        )
                        + (F.lit(0.30) * F.coalesce(F.col("persistent_run_strength_profiled"), F.lit(0.0)))
                        + (F.lit(0.10) * (F.lit(1.0) - F.coalesce(F.col("center_occupancy_profiled"), F.lit(0.0))))
                        + (F.lit(0.30) * F.coalesce(F.col("raw_motion_activity_score"), F.lit(0.0))),
                    ),
                ).cast("double").alias("tracking_error_score_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        (F.lit(0.65) * F.coalesce(F.col("excursion_return_ratio_profiled"), F.lit(0.0)))
                        + (F.lit(0.35) * F.coalesce(F.col("excursion_rate_profiled"), F.lit(0.0))),
                    ),
                ).cast("double").alias("tracking_recovery_score_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        (F.lit(0.35) * F.least(F.lit(1.0), F.greatest(F.lit(0.0), (lag1_corr + F.lit(1.0)) / F.lit(2.0))))
                        + (F.lit(0.25) * (F.lit(1.0) - F.least(F.coalesce(F.col("diff_energy"), F.lit(0.0)) / F.greatest(F.coalesce(F.col("level_energy"), F.lit(0.0)), F.lit(1e-6)), F.lit(1.0))))
                        + (
                            F.lit(0.12)
                            * F.least(
                                F.lit(1.0),
                                F.greatest(F.lit(0.0), F.coalesce(F.col("excursion_rate_profiled"), F.lit(0.0)) * F.lit(1.5)),
                            )
                        )
                        + (F.lit(0.10) * F.coalesce(F.col("persistent_run_strength_profiled"), F.lit(0.0)))
                        + (F.lit(0.05) * (F.lit(1.0) - F.coalesce(F.col("center_occupancy_profiled"), F.lit(0.0))))
                        + (
                            F.lit(0.08)
                            * F.least(
                                F.lit(1.0),
                                F.greatest(F.lit(0.0), F.coalesce(F.col("raw_excursion_rate_profiled"), F.lit(0.0)) * F.lit(1.5)),
                            )
                        )
                        + (F.lit(0.05) * F.coalesce(F.col("raw_motion_activity_score"), F.lit(0.0))),
                    ),
                ).cast("double").alias("lagged_response_score_profiled"),
            )
        )

        return numeric_features_df

    @classmethod
    def from_raw_input(
        cls,
        raw_input_df: "DataFrame",
        datatype_profile_df: "DataFrame",
        scaling_profile_df: "DataFrame",
        *,
        significant_diff_threshold: float = NUMERIC_SIGNIFICANT_DIFF_THRESHOLD,
        center_band_width: float = CENTER_BAND_WIDTH,
        soft_bound_width: float = SOFT_BOUND_WIDTH,
        hard_bound_width: float = HARD_BOUND_WIDTH,
    ) -> "ParameterBehaviorPrimitiveProfile":
        source_df = cls._source_df(raw_input_df, datatype_profile_df)
        common_summary_df = cls._common_summary_df(source_df)
        ordered_df = cls._ordered_source_df(source_df)
        discrete_features_df = cls._discrete_features_df(source_df, common_summary_df, ordered_df)
        numeric_features_df = cls._numeric_features_df(
            cls._numeric_ordered_df(
                ordered_df,
                scaling_profile_df,
                significant_diff_threshold=float(significant_diff_threshold),
            ),
            center_band_width=float(center_band_width),
            soft_bound_width=float(soft_bound_width),
            hard_bound_width=float(hard_bound_width),
        )

        return cls(
            dataframe=common_summary_df.join(numeric_features_df, on="parameter_name", how="left")
            .join(discrete_features_df, on="parameter_name", how="left")
            .select(
                "parameter_name",
                "parameter_datatype_profiled",
                "sample_count",
                "profile_window_start_utc",
                "profile_window_end_utc",
                *NUMERIC_PRIMITIVE_FEATURE_COLUMNS,
                *DISCRETE_PRIMITIVE_FEATURE_COLUMNS,
                "discrete_low_cardinality_score_profiled",
                "discrete_low_transition_score_profiled",
                "discrete_dwell_score_profiled",
                "transition_balance_score_profiled",
            )
        )

@dataclass(frozen=True)
class ParameterBehaviorProfile(Table):
    """Canonical behavior-family profile artifact."""

    @classmethod
    def spark_schema(cls):
        return PARAMETER_BEHAVIOR_PROFILE_SCHEMA()

    @classmethod
    def from_primitive_profile(cls, primitive_profile_df: "DataFrame") -> "ParameterBehaviorProfile":
        from pyspark.sql import functions as F

        family_scores = build_behavior_family_score_columns(
            parameter_datatype_column=F.col("parameter_datatype_profiled"),
            value_for=lambda column_name: F.coalesce(F.col(column_name), F.lit(0.0)),
        )
        choice = build_behavior_choice_columns(family_scores)

        return cls(
            dataframe=primitive_profile_df.select(
                "parameter_name",
                "parameter_datatype_profiled",
                choice.family.alias("behavior_family_profiled"),
                choice.confidence.cast("double").alias("behavior_profile_confidence"),
                family_scores["regulated"].cast("double").alias("regulated_score_profiled"),
                family_scores["tracking"].cast("double").alias("tracking_score_profiled"),
                family_scores["inertial"].cast("double").alias("inertial_score_profiled"),
                family_scores["accumulative"].cast("double").alias("accumulative_score_profiled"),
                family_scores["discrete_state"].cast("double").alias("discrete_state_score_profiled"),
                choice.mixed_unknown_score.cast("double").alias("mixed_unknown_score_profiled"),
                "sample_count",
                "profile_window_start_utc",
                "profile_window_end_utc",
                *NUMERIC_PRIMITIVE_FEATURE_COLUMNS,
                *DISCRETE_PRIMITIVE_FEATURE_COLUMNS,
            )
        )

    @classmethod
    def from_primitive_profile_with_thresholds(
        cls,
        primitive_profile_df: "DataFrame",
        *,
        mixed_unknown_low_score_threshold: float,
        mixed_unknown_ambiguous_score_threshold: float,
        mixed_unknown_ambiguous_margin_threshold: float,
    ) -> "ParameterBehaviorProfile":
        from pyspark.sql import functions as F

        family_scores = build_behavior_family_score_columns(
            parameter_datatype_column=F.col("parameter_datatype_profiled"),
            value_for=lambda column_name: F.coalesce(F.col(column_name), F.lit(0.0)),
        )
        choice = build_behavior_choice_columns(
            family_scores,
            thresholds=BehaviorChoiceThresholds(
                low_score_threshold=float(mixed_unknown_low_score_threshold),
                ambiguous_score_threshold=float(mixed_unknown_ambiguous_score_threshold),
                ambiguous_margin_threshold=float(mixed_unknown_ambiguous_margin_threshold),
            ),
        )
        return cls(
            dataframe=primitive_profile_df.select(
                "parameter_name",
                "parameter_datatype_profiled",
                choice.family.alias("behavior_family_profiled"),
                choice.confidence.cast("double").alias("behavior_profile_confidence"),
                family_scores["regulated"].cast("double").alias("regulated_score_profiled"),
                family_scores["tracking"].cast("double").alias("tracking_score_profiled"),
                family_scores["inertial"].cast("double").alias("inertial_score_profiled"),
                family_scores["accumulative"].cast("double").alias("accumulative_score_profiled"),
                family_scores["discrete_state"].cast("double").alias("discrete_state_score_profiled"),
                choice.mixed_unknown_score.cast("double").alias("mixed_unknown_score_profiled"),
                "sample_count",
                "profile_window_start_utc",
                "profile_window_end_utc",
                *NUMERIC_PRIMITIVE_FEATURE_COLUMNS,
                *DISCRETE_PRIMITIVE_FEATURE_COLUMNS,
            )
        )
