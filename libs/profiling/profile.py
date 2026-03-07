# File: libs/profiling/profile.py
"""Spark-native telemetry parameter profiling and data characterization."""

from __future__ import annotations

from libs.common import SensorDataType
from libs.perf.annotations import hot_path


def _resolve_timestamp_column(df: "DataFrame") -> str:
    if "timestamp_utc" in df.columns:
        return "timestamp_utc"
    if "timestamp" in df.columns:
        return "timestamp"
    raise ValueError("input dataframe must include either 'timestamp_utc' or 'timestamp'")


@hot_path
def build_parameter_profile(
    raw_input_df: "DataFrame",
    numeric_ratio_threshold: float = 0.8,
    categorical_cardinality_max: int = 200,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    ts_col = _resolve_timestamp_column(raw_input_df)
    prepped = (
        raw_input_df.select(
            F.col("parameter_name").cast("string").alias("parameter_name"),
            F.col(ts_col).cast("timestamp").alias("timestamp_utc"),
            F.trim(F.col("parameter_value").cast("string")).alias("parameter_value"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("timestamp_utc").isNotNull())
        .withColumn(
            "is_missing",
            F.col("parameter_value").isNull()
            | (F.col("parameter_value") == F.lit(""))
            | (F.lower(F.col("parameter_value")).isin("null", "nan", "none")),
        )
        .withColumn("value_num", F.expr("try_cast(parameter_value as double)"))
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

    prof = (
        base_stats.alias("b")
        .join(numeric_stats.alias("n"), on="parameter_name", how="left")
        .join(sample_stats.alias("s"), on="parameter_name", how="left")
        .withColumn("missing_rate", F.col("missing_count") / F.greatest(F.col("total_count"), F.lit(1)))
        .withColumn("numeric_rate", F.col("numeric_count") / F.greatest(F.col("total_count"), F.lit(1)))
        .withColumn(
            "detected_type",
            F.when(F.col("distinct_value_count") <= 1, F.lit(SensorDataType.CONSTANT.value))
            .when(
                (F.col("numeric_rate") >= F.lit(numeric_ratio_threshold)) & (F.col("distinct_value_count") > 2),
                F.lit(SensorDataType.NUMERIC.value),
            )
            .when(F.col("distinct_value_count") == 2, F.lit(SensorDataType.BINARY.value))
            .when(F.col("distinct_value_count") <= F.lit(int(categorical_cardinality_max)), F.lit(SensorDataType.CATEGORICAL.value))
            .otherwise(F.lit(SensorDataType.HIGH_CARDINALITY.value)),
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
    return prof


def build_categorical_distribution(raw_input_df: "DataFrame", top_k: int = 10) -> "DataFrame":
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


@hot_path
def build_sensor_datatype_profile(
    telemetry_df: "DataFrame",
    numeric_ratio_threshold: float = 0.8,
    categorical_cardinality_max: int = 200,
) -> "DataFrame":
    from pyspark.sql import functions as F

    if "sensor" in telemetry_df.columns:
        sensor_col = F.col("sensor")
    elif "parameter_name" in telemetry_df.columns:
        sensor_col = F.col("parameter_name")
    else:
        raise ValueError("telemetry dataframe must include either 'sensor' or 'parameter_name' column")

    if "parameter_value" in telemetry_df.columns:
        value_text = F.trim(F.col("parameter_value").cast("string"))
    else:
        value_text = F.lit(None).cast("string")

    if "parameter_value" in telemetry_df.columns:
        value_num = F.expr("try_cast(parameter_value as double)")
    else:
        value_num = F.lit(None).cast("double")

    prepped = (
        telemetry_df.select(
            sensor_col.cast("string").alias("sensor"),
            value_text.alias("value_text"),
            value_num.alias("value_num"),
        )
        .where(F.col("sensor").isNotNull())
        .withColumn(
            "is_missing",
            (
                F.col("value_text").isNull()
                | (F.col("value_text") == F.lit(""))
                | F.lower(F.col("value_text")).isin("null", "nan", "none")
            )
            & F.col("value_num").isNull(),
        )
        .withColumn("is_numeric", F.col("value_num").isNotNull())
    )

    base_stats = prepped.groupBy("sensor").agg(
        F.count(F.lit(1)).alias("total_count"),
        F.sum(F.when(F.col("is_missing"), F.lit(1)).otherwise(F.lit(0))).alias("missing_count"),
        F.sum(F.when(F.col("is_numeric"), F.lit(1)).otherwise(F.lit(0))).alias("numeric_count"),
        # Exclude missing tokens from cardinality so empty/null placeholders do not skew type inference.
        F.approx_count_distinct(F.when(~F.col("is_missing"), F.col("value_text"))).alias("distinct_value_count"),
    )

    return (
        base_stats.withColumn("missing_rate", F.col("missing_count") / F.greatest(F.col("total_count"), F.lit(1)))
        .withColumn("observed_count", F.col("total_count") - F.col("missing_count"))
        .withColumn("numeric_rate", F.col("numeric_count") / F.greatest(F.col("observed_count"), F.lit(1)))
        .withColumn(
            "detected_type",
            F.when(F.col("distinct_value_count") <= 1, F.lit(SensorDataType.CONSTANT.value))
            .when(
                (F.col("numeric_rate") >= F.lit(float(numeric_ratio_threshold))) & (F.col("distinct_value_count") > 2),
                F.lit(SensorDataType.NUMERIC.value),
            )
            .when(F.col("distinct_value_count") == 2, F.lit(SensorDataType.BINARY.value))
            .when(F.col("distinct_value_count") <= F.lit(int(categorical_cardinality_max)), F.lit(SensorDataType.CATEGORICAL.value))
            .otherwise(F.lit(SensorDataType.HIGH_CARDINALITY.value)),
        )
        .select(
            "sensor",
            "detected_type",
            F.col("total_count").cast("long").alias("total_count"),
            F.col("missing_count").cast("long").alias("missing_count"),
            F.col("missing_rate").cast("double").alias("missing_rate"),
            F.col("numeric_rate").cast("double").alias("numeric_rate"),
            F.col("distinct_value_count").cast("long").alias("distinct_value_count"),
        )
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
