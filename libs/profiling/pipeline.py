"""Fitting-stage profiling artifact builders for the active V2 path."""

from __future__ import annotations

from typing import TYPE_CHECKING

from libs.behavior import build_default_behavior_registry
from libs.io.pandas_spark import pandas_records_for_spark

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def _resolve_timestamp_column(df: "DataFrame") -> str:
    if "timestamp_utc" in df.columns:
        return "timestamp_utc"
    if "timestamp" in df.columns:
        return "timestamp"
    raise ValueError("input dataframe must include either 'timestamp_utc' or 'timestamp'")


def build_parameter_datatype_profile_table(raw_input_df: "DataFrame") -> "DataFrame":
    """Build the canonical datatype/rate profile artifact."""
    from libs.profiling.profile import build_parameter_profile
    from pyspark.sql import functions as F

    profile_df = build_parameter_profile(raw_input_df)
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


def build_continuous_scaling_profile_table(raw_input_df: "DataFrame", datatype_profile_df: "DataFrame") -> "DataFrame":
    """Build robust scaling metadata for continuous parameters."""
    from pyspark.sql import functions as F

    ts_col = _resolve_timestamp_column(raw_input_df)
    numeric_like = datatype_profile_df.select("parameter_name", "parameter_datatype_profiled").where(
        F.col("parameter_datatype_profiled").isin("numeric", "constant")
    )
    numeric_df = (
        raw_input_df.select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col(ts_col).cast("timestamp").alias("timestamp_utc"),
            F.expr("try_cast(parameter_value as double)").alias("parameter_value_num"),
        )
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


def build_parameter_behavior_profile_table(
    raw_input_df: "DataFrame",
    datatype_profile_df: "DataFrame",
) -> "DataFrame":
    """Build the canonical behavior profile artifact from observed telemetry."""
    from pyspark.sql import functions as F
    from pyspark.sql import Window
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    import pandas as pd

    ts_col = _resolve_timestamp_column(raw_input_df)
    behavior_schema = StructType(
        [
            StructField("parameter_name", StringType(), False),
            StructField("parameter_datatype_profiled", StringType(), True),
            StructField("behavior_family_profiled", StringType(), False),
            StructField("behavior_profile_confidence", DoubleType(), False),
            StructField("regulated_score_profiled", DoubleType(), False),
            StructField("inertial_score_profiled", DoubleType(), False),
            StructField("accumulative_score_profiled", DoubleType(), False),
            StructField("discrete_state_score_profiled", DoubleType(), False),
            StructField("mixed_unknown_score_profiled", DoubleType(), False),
            StructField("sample_count", LongType(), False),
            StructField("profile_window_start_utc", TimestampType(), True),
            StructField("profile_window_end_utc", TimestampType(), True),
        ]
    )

    spark = raw_input_df.sparkSession
    profile_lookup_df = datatype_profile_df.select("parameter_name", "parameter_datatype_profiled")
    source_df = (
        raw_input_df.select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col(ts_col).cast("timestamp").alias("timestamp_utc"),
            F.col("parameter_value").cast("string").alias("parameter_value"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("timestamp_utc").isNotNull())
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
        source_df.withColumn("parameter_value_num", F.expr("try_cast(parameter_value as double)"))
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
            F.when(
                F.col("prev_value_num").isNotNull(),
                F.col("prev_value_num") * F.col("parameter_value_num"),
            ).otherwise(F.lit(0.0))
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
                        F.greatest(
                            (F.col("lag_pair_count") * F.col("sum_prev_sq")) - (F.col("sum_prev") * F.col("sum_prev")),
                            F.lit(0.0),
                        )
                        * F.greatest(
                            (F.col("lag_pair_count") * F.col("sum_curr_sq")) - (F.col("sum_curr") * F.col("sum_curr")),
                            F.lit(0.0),
                        )
                    )
                )
                > F.lit(1e-12),
                (
                    (F.col("lag_pair_count") * F.col("sum_prev_curr")) - (F.col("sum_prev") * F.col("sum_curr"))
                )
                / F.sqrt(
                    F.greatest(
                        (F.col("lag_pair_count") * F.col("sum_prev_sq")) - (F.col("sum_prev") * F.col("sum_prev")),
                        F.lit(0.0),
                    )
                    * F.greatest(
                        (F.col("lag_pair_count") * F.col("sum_curr_sq")) - (F.col("sum_curr") * F.col("sum_curr")),
                        F.lit(0.0),
                    )
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

    feature_pdf = feature_df.toPandas()
    if feature_pdf.empty:
        return spark.createDataFrame([], schema=behavior_schema)

    registry = build_default_behavior_registry()
    regulated_behavior = registry.get("regulated")
    inertial_behavior = registry.get("inertial")
    accumulative_behavior = registry.get("accumulative")
    discrete_behavior = registry.get("discrete_state")

    output_rows: list[dict[str, object]] = []
    for row in feature_pdf.to_dict(orient="records"):
        parameter_name = str(row.get("parameter_name", "") or "")
        datatype = str(row.get("parameter_datatype_profiled", "mixed_unknown") or "mixed_unknown")
        feature_map = {key: value for key, value in row.items() if key not in {"parameter_name", "parameter_datatype_profiled"}}
        regulated_score = 0.0
        inertial_score = 0.0
        accumulative_score = 0.0
        discrete_state_score = 0.0
        mixed_unknown_score = 0.0

        if datatype in {"binary", "categorical", "high_cardinality"}:
            discrete_profile = discrete_behavior.profiler.profile(parameter_name=parameter_name, features=feature_map)
            discrete_state_score = float(discrete_profile.score_by_family.get("discrete_state", 0.0))
            mixed_unknown_score = float(discrete_profile.score_by_family.get("mixed_unknown", 0.0))
        elif datatype in {"numeric", "constant"}:
            regulated_profile = regulated_behavior.profiler.profile(parameter_name=parameter_name, features=feature_map)
            inertial_profile = inertial_behavior.profiler.profile(parameter_name=parameter_name, features=feature_map)
            accumulative_profile = accumulative_behavior.profiler.profile(parameter_name=parameter_name, features=feature_map)
            regulated_score = float(regulated_profile.score_by_family.get("regulated", 0.0))
            inertial_score = float(inertial_profile.score_by_family.get("inertial", 0.0))
            accumulative_score = float(accumulative_profile.score_by_family.get("accumulative", 0.0))
            mixed_unknown_score = float(max(0.0, 1.0 - max(regulated_score, inertial_score, accumulative_score)))
        else:
            mixed_unknown_score = 0.75

        family_scores = {
            "regulated": regulated_score,
            "inertial": inertial_score,
            "accumulative": accumulative_score,
            "discrete_state": discrete_state_score,
            "mixed_unknown": mixed_unknown_score,
        }
        ranked = sorted(family_scores.items(), key=lambda item: (item[1], item[0]), reverse=True)
        top_family, top_score = ranked[0]
        second_score = ranked[1][1] if len(ranked) > 1 else 0.0
        if float(top_score) < 0.55 or (float(top_score) - float(second_score)) < 0.05:
            top_family = "mixed_unknown"
            mixed_unknown_score = max(float(mixed_unknown_score), 1.0 - float(top_score))
            top_score = max(float(top_score), float(mixed_unknown_score))
        confidence = float(max(0.0, min(1.0, 0.5 * float(top_score) + 0.5 * max(float(top_score) - float(second_score), 0.0))))
        if top_family == "mixed_unknown":
            confidence = max(confidence, float(mixed_unknown_score))

        output_rows.append(
            {
                "parameter_name": parameter_name,
                "parameter_datatype_profiled": datatype,
                "behavior_family_profiled": top_family,
                "behavior_profile_confidence": float(confidence),
                "regulated_score_profiled": float(regulated_score),
                "inertial_score_profiled": float(inertial_score),
                "accumulative_score_profiled": float(accumulative_score),
                "discrete_state_score_profiled": float(discrete_state_score),
                "mixed_unknown_score_profiled": float(mixed_unknown_score),
                "sample_count": int(row.get("sample_count", 0) or 0),
                "profile_window_start_utc": row.get("profile_window_start_utc"),
                "profile_window_end_utc": row.get("profile_window_end_utc"),
            }
        )

    output_pdf = pd.DataFrame(pandas_records_for_spark(pd.DataFrame(output_rows)))
    return spark.createDataFrame(output_pdf, schema=behavior_schema)
