"""Profiling-domain artifact builders for telemetry characterization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.common import ParameterDataType, try_cast_double
from libs.perf.annotations import hot_path

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
                "detected_type",
                F.when(F.col("distinct_value_count") <= 1, F.lit(ParameterDataType.CONSTANT.value))
                .when(
                    (F.col("numeric_rate") >= F.lit(numeric_ratio_threshold)) & (F.col("distinct_value_count") > 2),
                    F.lit(ParameterDataType.NUMERIC.value),
                )
                .when(F.col("distinct_value_count") == 2, F.lit(ParameterDataType.BINARY.value))
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
class ParameterDatatypeProfile:
    """Canonical datatype profile artifact."""

    @classmethod
    def from_parameter_profile(cls, profile_df: "DataFrame") -> "DataFrame":
        from pyspark.sql import functions as F

        return profile_df.select(
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


@dataclass(frozen=True)
class ContinuousScalingProfile:
    """Robust scaling metadata for continuous parameters."""

    @classmethod
    def build_dataframe(cls, raw_input_df: "DataFrame", datatype_profile_df: "DataFrame") -> "DataFrame":
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
        return quantiles_df.select(
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


@dataclass(frozen=True)
class ParameterBehaviorProfile:
    """Canonical behavior-family profile artifact."""

    @classmethod
    def build_dataframe(
        cls,
        raw_input_df: "DataFrame",
        datatype_profile_df: "DataFrame",
    ) -> "DataFrame":
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        source = TelemetryProfileSource(raw_input_df)
        profile_lookup_df = datatype_profile_df.select("parameter_name", "parameter_datatype_profiled")
        source_df = (
            source.base_projection_df()
            .join(profile_lookup_df, on="parameter_name", how="left")
        )

        common_summary_df = source_df.groupBy("parameter_name", "parameter_datatype_profiled").agg(
            F.count(F.lit(1)).cast("long").alias("sample_count"),
            F.min("timestamp_utc").alias("profile_window_start_utc"),
            F.max("timestamp_utc").alias("profile_window_end_utc"),
        )

        discrete_value_counts_df = source_df.groupBy("parameter_name", "parameter_value").agg(
            F.count(F.lit(1)).cast("long").alias("state_count")
        )
        discrete_dominant_df = discrete_value_counts_df.groupBy("parameter_name").agg(
            F.max("state_count").cast("long").alias("dominant_state_count")
        )

        ordered_window = Window.partitionBy("parameter_name").orderBy("timestamp_utc")
        ordered_df = (
            source_df.withColumn("parameter_value_num", try_cast_double("parameter_value"))
            .withColumn("prev_value", F.lag("parameter_value").over(ordered_window))
            .withColumn("prev_value_num", F.lag("parameter_value_num").over(ordered_window))
            .withColumn(
                "diff_num",
                F.when(
                    F.col("parameter_value_num").isNotNull() & F.col("prev_value_num").isNotNull(),
                    F.col("parameter_value_num") - F.col("prev_value_num"),
                ),
            )
            .withColumn("prev_diff_num", F.lag("diff_num").over(ordered_window))
        )

        discrete_features_df = (
            ordered_df.groupBy("parameter_name").agg(
                F.countDistinct("parameter_value").cast("double").alias("distinct_state_count_profiled"),
                F.sum(
                    F.when(
                        F.col("prev_value").isNotNull() & (F.col("parameter_value") != F.col("prev_value")),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).alias("transition_count"),
            )
            .join(discrete_dominant_df, on="parameter_name", how="left")
            .join(common_summary_df.select("parameter_name", "sample_count"), on="parameter_name", how="left")
            .select(
                "parameter_name",
                F.col("distinct_state_count_profiled"),
                (
                    F.col("transition_count") / F.greatest(F.col("sample_count").cast("double") - F.lit(1.0), F.lit(1.0))
                ).alias("transition_rate_profiled"),
                (
                    F.col("sample_count").cast("double")
                    / F.greatest(F.col("transition_count") + F.lit(1.0), F.lit(1.0))
                ).alias("mean_dwell_profiled"),
                (
                    F.coalesce(F.col("dominant_state_count").cast("double"), F.lit(0.0))
                    / F.greatest(F.col("sample_count").cast("double"), F.lit(1.0))
                ).alias("dominant_state_ratio_profiled"),
            )
        )

        numeric_df = ordered_df.where(F.col("parameter_datatype_profiled").isin("numeric", "constant")).where(
            F.col("parameter_value_num").isNotNull()
        )

        numeric_summary_df = numeric_df.groupBy("parameter_name").agg(
            F.min_by("parameter_value_num", "timestamp_utc").cast("double").alias("first_value_num"),
            F.max_by("parameter_value_num", "timestamp_utc").cast("double").alias("last_value_num"),
            F.avg("parameter_value_num").cast("double").alias("mean_value_num"),
            F.avg(F.col("parameter_value_num") * F.col("parameter_value_num")).cast("double").alias("level_energy"),
            F.min("parameter_value_num").cast("double").alias("min_value_num"),
            F.max("parameter_value_num").cast("double").alias("max_value_num"),
            F.percentile_approx(
                "parameter_value_num",
                F.array(F.lit(0.25), F.lit(0.5), F.lit(0.75)),
                1000,
            ).alias("quantiles"),
        ).select(
            "parameter_name",
            "first_value_num",
            "last_value_num",
            "mean_value_num",
            "level_energy",
            "min_value_num",
            "max_value_num",
            F.col("quantiles").getItem(0).cast("double").alias("q25"),
            F.col("quantiles").getItem(1).cast("double").alias("q50"),
            F.col("quantiles").getItem(2).cast("double").alias("q75"),
            (F.col("max_value_num") - F.col("min_value_num")).alias("total_range"),
        )

        numeric_diff_df = numeric_df.groupBy("parameter_name").agg(
            F.count("diff_num").cast("long").alias("diff_count"),
            F.sum(F.abs("diff_num")).cast("double").alias("gross_change"),
            F.avg(F.col("diff_num") * F.col("diff_num")).cast("double").alias("diff_energy"),
            F.sum(F.when(F.col("diff_num") > 0, F.lit(1.0)).otherwise(F.lit(0.0))).alias("positive_diff_count"),
            F.sum(F.when(F.col("diff_num") < 0, F.lit(1.0)).otherwise(F.lit(0.0))).alias("negative_diff_count"),
            F.sum(
                F.when(
                    F.col("prev_diff_num").isNotNull()
                    & F.col("diff_num").isNotNull()
                    & ((F.col("prev_diff_num") * F.col("diff_num")) < 0),
                    F.lit(1.0),
                ).otherwise(F.lit(0.0))
            ).alias("sign_flip_count"),
            F.count("prev_value_num").cast("double").alias("lag_pair_count"),
            F.sum(F.coalesce(F.col("prev_value_num"), F.lit(0.0))).cast("double").alias("sum_prev"),
            F.sum(F.when(F.col("prev_value_num").isNotNull(), F.col("parameter_value_num")).otherwise(F.lit(0.0))).cast("double").alias("sum_curr"),
            F.sum(
                F.when(F.col("prev_value_num").isNotNull(), F.col("prev_value_num") * F.col("parameter_value_num")).otherwise(F.lit(0.0))
            ).cast("double").alias("sum_prev_curr"),
            F.sum(F.when(F.col("prev_value_num").isNotNull(), F.col("prev_value_num") * F.col("prev_value_num")).otherwise(F.lit(0.0))).cast("double").alias("sum_prev_sq"),
            F.sum(F.when(F.col("prev_value_num").isNotNull(), F.col("parameter_value_num") * F.col("parameter_value_num")).otherwise(F.lit(0.0))).cast("double").alias("sum_curr_sq"),
        )

        numeric_band_df = (
            numeric_df.join(numeric_summary_df.select("parameter_name", "q25", "q50", "q75"), on="parameter_name", how="left")
            .groupBy("parameter_name")
            .agg(
                F.avg(
                    F.when(
                        F.abs(F.col("parameter_value_num") - F.col("q50"))
                        <= (F.lit(1.5) * F.greatest(F.col("q75") - F.col("q25"), F.lit(1e-6))),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0))
                ).cast("double").alias("central_band_occupancy_profiled")
            )
        )

        numeric_features_df = (
            numeric_summary_df.join(numeric_diff_df, on="parameter_name", how="left")
            .join(numeric_band_df, on="parameter_name", how="left")
            .select(
                "parameter_name",
                F.col("central_band_occupancy_profiled"),
                (F.lit(1.0) - F.coalesce(F.col("central_band_occupancy_profiled"), F.lit(0.0))).alias("excursion_rate_profiled"),
                F.least(
                    F.lit(1.0),
                    F.greatest(
                        F.lit(0.0),
                        F.coalesce(F.col("sign_flip_count"), F.lit(0.0))
                        / F.greatest(F.col("diff_count").cast("double") - F.lit(1.0), F.lit(1.0))
                        * F.lit(2.0),
                    ),
                ).alias("mean_reversion_score_profiled"),
                (F.lit(1.0) / (F.lit(1.0) + F.greatest(F.coalesce(F.col("total_range"), F.lit(0.0)), F.lit(0.0)))).alias(
                    "boundedness_score_profiled"
                ),
                F.when(
                    (
                        F.sqrt(
                            F.greatest((F.col("lag_pair_count") * F.col("sum_prev_sq")) - (F.col("sum_prev") * F.col("sum_prev")), F.lit(0.0))
                            * F.greatest((F.col("lag_pair_count") * F.col("sum_curr_sq")) - (F.col("sum_curr") * F.col("sum_curr")), F.lit(0.0))
                        )
                    )
                    > F.lit(1e-12),
                    ((F.col("lag_pair_count") * F.col("sum_prev_curr")) - (F.col("sum_prev") * F.col("sum_curr")))
                    / F.sqrt(
                        F.greatest((F.col("lag_pair_count") * F.col("sum_prev_sq")) - (F.col("sum_prev") * F.col("sum_prev")), F.lit(0.0))
                        * F.greatest((F.col("lag_pair_count") * F.col("sum_curr_sq")) - (F.col("sum_curr") * F.col("sum_curr")), F.lit(0.0))
                    ),
                ).otherwise(
                    F.when(F.greatest(F.coalesce(F.col("total_range"), F.lit(0.0)), F.lit(0.0)) <= F.lit(1e-9), F.lit(1.0)).otherwise(
                        F.lit(0.0)
                    )
                ).alias("lag1_autocorr_profiled"),
                (
                    F.coalesce(F.col("diff_energy"), F.lit(0.0))
                    / F.greatest(F.coalesce(F.col("level_energy"), F.lit(0.0)), F.lit(1e-6))
                ).alias("diff_energy_ratio_profiled"),
                (
                    F.coalesce(F.col("sign_flip_count"), F.lit(0.0))
                    / F.greatest(F.col("diff_count").cast("double") - F.lit(1.0), F.lit(1.0))
                ).alias("sign_flip_rate_profiled"),
                (
                    F.lit(1.0)
                    - F.least(
                        F.coalesce(F.col("diff_energy"), F.lit(0.0))
                        / F.greatest(F.coalesce(F.col("level_energy"), F.lit(0.0)), F.lit(1e-6)),
                        F.lit(1.0),
                    )
                ).alias("smoothness_score_profiled"),
                F.greatest(
                    F.coalesce(F.col("positive_diff_count"), F.lit(0.0)) / F.greatest(F.col("diff_count").cast("double"), F.lit(1.0)),
                    F.coalesce(F.col("negative_diff_count"), F.lit(0.0)) / F.greatest(F.col("diff_count").cast("double"), F.lit(1.0)),
                ).alias("monotonicity_score_profiled"),
                (
                    F.abs(F.coalesce(F.col("last_value_num"), F.lit(0.0)) - F.coalesce(F.col("first_value_num"), F.lit(0.0)))
                    / F.greatest(F.coalesce(F.col("gross_change"), F.lit(0.0)), F.lit(1e-6))
                ).alias("net_change_ratio_profiled"),
            )
        )

        feature_df = (
            common_summary_df.join(numeric_features_df, on="parameter_name", how="left")
            .join(discrete_features_df, on="parameter_name", how="left")
            .select(
                "parameter_name",
                "parameter_datatype_profiled",
                "sample_count",
                "profile_window_start_utc",
                "profile_window_end_utc",
                "central_band_occupancy_profiled",
                "excursion_rate_profiled",
                "mean_reversion_score_profiled",
                "boundedness_score_profiled",
                "lag1_autocorr_profiled",
                "diff_energy_ratio_profiled",
                "sign_flip_rate_profiled",
                "smoothness_score_profiled",
                "monotonicity_score_profiled",
                "net_change_ratio_profiled",
                "distinct_state_count_profiled",
                "transition_rate_profiled",
                "mean_dwell_profiled",
                "dominant_state_ratio_profiled",
            )
        )

        regulated_score = F.when(
            F.col("parameter_datatype_profiled").isin("numeric", "constant"),
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.coalesce(F.col("central_band_occupancy_profiled"), F.lit(0.0))
                        + F.coalesce(F.col("mean_reversion_score_profiled"), F.lit(0.0))
                        + F.coalesce(F.col("boundedness_score_profiled"), F.lit(0.0))
                    )
                    / F.lit(3.0),
                ),
            ),
        ).otherwise(F.lit(0.0))

        inertial_score = F.when(
            F.col("parameter_datatype_profiled").isin("numeric", "constant"),
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.least(
                            F.lit(1.0),
                            F.greatest(
                                F.lit(0.0),
                                (F.coalesce(F.col("lag1_autocorr_profiled"), F.lit(0.0)) + F.lit(1.0)) / F.lit(2.0),
                            ),
                        )
                        + F.coalesce(F.col("smoothness_score_profiled"), F.lit(0.0))
                        + F.least(
                            F.lit(1.0),
                            F.greatest(F.lit(0.0), F.lit(1.0) - F.coalesce(F.col("sign_flip_rate_profiled"), F.lit(0.0))),
                        )
                    )
                    / F.lit(3.0),
                ),
            ),
        ).otherwise(F.lit(0.0))

        accumulative_score = F.when(
            F.col("parameter_datatype_profiled").isin("numeric", "constant"),
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.lit(0.3)
                        * F.least(
                            F.lit(1.0),
                            F.greatest(
                                F.lit(0.0),
                                (F.coalesce(F.col("lag1_autocorr_profiled"), F.lit(0.0)) + F.lit(1.0)) / F.lit(2.0),
                            ),
                        )
                        + F.lit(0.35) * F.coalesce(F.col("monotonicity_score_profiled"), F.lit(0.0))
                        + F.lit(0.2) * F.coalesce(F.col("net_change_ratio_profiled"), F.lit(0.0))
                        + F.lit(0.15)
                        * F.least(
                            F.lit(1.0),
                            F.greatest(F.lit(0.0), F.lit(1.0) - F.coalesce(F.col("sign_flip_rate_profiled"), F.lit(0.0))),
                        )
                    ),
                ),
            ),
        ).otherwise(F.lit(0.0))

        discrete_state_score = F.when(
            F.col("parameter_datatype_profiled").isin("binary", "categorical", "high_cardinality"),
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    (
                        F.lit(0.3)
                        * F.least(
                            F.lit(1.0),
                            F.greatest(
                                F.lit(0.0),
                                F.lit(1.0)
                                - F.least(
                                    F.greatest(F.coalesce(F.col("distinct_state_count_profiled"), F.lit(0.0)) - F.lit(1.0), F.lit(0.0)),
                                    F.lit(9.0),
                                )
                                / F.lit(9.0),
                            ),
                        )
                        + F.lit(0.25)
                        * F.least(
                            F.lit(1.0),
                            F.greatest(F.lit(0.0), F.lit(1.0) - F.coalesce(F.col("transition_rate_profiled"), F.lit(0.0))),
                        )
                        + F.lit(0.25)
                        * F.least(
                            F.lit(1.0),
                            F.greatest(
                                F.lit(0.0),
                                F.least(F.coalesce(F.col("mean_dwell_profiled"), F.lit(0.0)), F.lit(10.0)) / F.lit(10.0),
                            ),
                        )
                        + F.lit(0.2)
                        * F.least(
                            F.lit(1.0),
                            F.greatest(F.lit(0.0), F.coalesce(F.col("dominant_state_ratio_profiled"), F.lit(0.0))),
                        )
                    ),
                ),
            ),
        ).otherwise(F.lit(0.0))

        mixed_unknown_score = (
            F.when(
                F.col("parameter_datatype_profiled").isin("binary", "categorical", "high_cardinality"),
                F.least(F.lit(1.0), F.greatest(F.lit(0.0), F.lit(1.0) - discrete_state_score)),
            )
            .when(
                F.col("parameter_datatype_profiled").isin("numeric", "constant"),
                F.least(
                    F.lit(1.0),
                    F.greatest(F.lit(0.0), F.lit(1.0) - F.greatest(regulated_score, inertial_score, accumulative_score)),
                ),
            )
            .otherwise(F.lit(0.75))
        )

        ranked_scores = F.array_sort(
            F.array(
                F.struct(regulated_score.alias("score"), F.lit("regulated").alias("family")),
                F.struct(inertial_score.alias("score"), F.lit("inertial").alias("family")),
                F.struct(accumulative_score.alias("score"), F.lit("accumulative").alias("family")),
                F.struct(discrete_state_score.alias("score"), F.lit("discrete_state").alias("family")),
                F.struct(mixed_unknown_score.alias("score"), F.lit("mixed_unknown").alias("family")),
            ),
            lambda left, right: (
                F.when(left["score"] < right["score"], F.lit(1))
                .when(left["score"] > right["score"], F.lit(-1))
                .when(left["family"] < right["family"], F.lit(1))
                .when(left["family"] > right["family"], F.lit(-1))
                .otherwise(F.lit(0))
            ),
        )

        top_score = ranked_scores.getItem(0)["score"]
        top_family = ranked_scores.getItem(0)["family"]
        second_score = ranked_scores.getItem(1)["score"]
        use_mixed_unknown = (top_score < F.lit(0.55)) | ((top_score - second_score) < F.lit(0.05))
        effective_top_family = F.when(use_mixed_unknown, F.lit("mixed_unknown")).otherwise(top_family)
        effective_top_score = F.when(
            use_mixed_unknown,
            F.greatest(top_score, F.greatest(mixed_unknown_score, F.lit(1.0) - top_score)),
        ).otherwise(top_score)

        confidence = F.greatest(
            F.lit(0.0),
            F.least(
                F.lit(1.0),
                F.lit(0.5) * effective_top_score + F.lit(0.5) * F.greatest(effective_top_score - second_score, F.lit(0.0)),
            ),
        )
        confidence = F.when(
            effective_top_family == F.lit("mixed_unknown"),
            F.greatest(confidence, mixed_unknown_score),
        ).otherwise(confidence)

        return feature_df.select(
            "parameter_name",
            "parameter_datatype_profiled",
            effective_top_family.alias("behavior_family_profiled"),
            confidence.cast("double").alias("behavior_profile_confidence"),
            regulated_score.cast("double").alias("regulated_score_profiled"),
            inertial_score.cast("double").alias("inertial_score_profiled"),
            accumulative_score.cast("double").alias("accumulative_score_profiled"),
            discrete_state_score.cast("double").alias("discrete_state_score_profiled"),
            mixed_unknown_score.cast("double").alias("mixed_unknown_score_profiled"),
            F.col("sample_count").cast("long").alias("sample_count"),
            "profile_window_start_utc",
            "profile_window_end_utc",
        )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
