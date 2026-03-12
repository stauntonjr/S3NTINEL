"""Window-features dataframe adapters for active Spark and local paths."""

from __future__ import annotations

import pandas as pd

from libs.io.schemas import WINDOW_X_SCHEMA
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.contracts import WindowXRow
from libs.windows.context import WindowContextResolver
from libs.windows.features import WindowFeatures, WindowScaler


def build_window_features_dataframe(raw_df: pd.DataFrame, events_df: pd.DataFrame, windows_df: pd.DataFrame) -> pd.DataFrame:
    raw_rows = WindowContextResolver.prepare_raw_telemetry(raw_df)
    window_rows = WindowContextResolver.prepare_windows(windows_df)
    if raw_rows.empty or window_rows.empty:
        return pd.DataFrame()

    resolver = WindowContextResolver.from_frames(raw_df=raw_df, events_df=events_df)
    prepared_window_rows = resolver.resolve(windows_df)
    scaler = WindowScaler.from_telemetry_df(raw_rows)
    previous_scaled_by_flight: dict[tuple[str, str], dict[str, float]] = {}
    window_x_rows: list[WindowXRow] = []
    for context in prepared_window_rows:
        window_x_rows.append(
            WindowFeatures.from_context(
                context=context,
                scaler=scaler,
                previous_scaled_by_flight=previous_scaled_by_flight,
                phase_label=None,
            ).row
        )
    return pd.DataFrame(window_x_rows)


def build_window_features_spark_dataframe(raw_df: "DataFrame", events_df: "DataFrame", windows_df: "DataFrame") -> "DataFrame":
    """Build the window-features Spark DataFrame with grouped per-flight execution."""
    from pyspark.sql import functions as F

    raw_columns = set(raw_df.columns)
    if "parameter_value" in raw_columns:
        raw_value_expr = F.col("parameter_value").cast("string")
    elif "parameter_value_clean" in raw_columns:
        raw_value_expr = F.col("parameter_value_clean").cast("string")
    else:
        raw_value_expr = F.lit(None).cast("string")

    raw_rows = raw_df.select(
        "tail_id",
        "flight_id",
        F.lit("raw").alias("row_type"),
        F.col("timestamp_utc"),
        F.lit(None).cast("int").alias("win_id"),
        F.lit(None).cast("timestamp").alias("t_start"),
        F.lit(None).cast("timestamp").alias("t_end"),
        F.lit(None).cast("int").alias("duration_ms"),
        F.lit(None).cast("int").alias("event_count"),
        F.col("parameter_name"),
        raw_value_expr.alias("parameter_value"),
        F.lit(None).cast("string").alias("event_type_detected"),
        F.expr("cast(null as map<string,string>)").alias("payload"),
    )
    event_rows = events_df.select(
        "tail_id",
        "flight_id",
        F.lit("event").alias("row_type"),
        F.col("timestamp_utc"),
        F.lit(None).cast("int").alias("win_id"),
        F.lit(None).cast("timestamp").alias("t_start"),
        F.lit(None).cast("timestamp").alias("t_end"),
        F.lit(None).cast("int").alias("duration_ms"),
        F.lit(None).cast("int").alias("event_count"),
        F.col("parameter_name"),
        F.lit(None).cast("string").alias("parameter_value"),
        F.col("event_type_detected"),
        F.col("payload").cast("map<string,string>").alias("payload"),
    )
    window_rows = windows_df.select(
        "tail_id",
        "flight_id",
        F.lit("window").alias("row_type"),
        F.col("t_end").alias("timestamp_utc"),
        F.col("win_id"),
        F.col("t_start"),
        F.col("t_end"),
        F.col("duration_ms"),
        F.col("event_count"),
        F.lit(None).cast("string").alias("parameter_name"),
        F.lit(None).cast("string").alias("parameter_value"),
        F.lit(None).cast("string").alias("event_type_detected"),
        F.expr("cast(null as map<string,string>)").alias("payload"),
    )
    flight_context_df = raw_rows.unionByName(event_rows).unionByName(window_rows)

    def _emit_window_features(group_pdf: pd.DataFrame) -> pd.DataFrame:
        raw_pdf = group_pdf[group_pdf["row_type"] == "raw"][
            ["tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value"]
        ].copy()
        event_pdf = group_pdf[group_pdf["row_type"] == "event"][
            ["tail_id", "flight_id", "timestamp_utc", "parameter_name", "event_type_detected", "payload"]
        ].copy()
        window_pdf = group_pdf[group_pdf["row_type"] == "window"][
            ["tail_id", "flight_id", "win_id", "t_start", "t_end", "duration_ms", "event_count"]
        ].copy()
        return build_window_features_dataframe(raw_pdf, event_pdf, window_pdf)

    return flight_context_df.groupBy("tail_id", "flight_id").applyInPandas(_emit_window_features, schema=WINDOW_X_SCHEMA)


def window_features_pandas_to_spark_dataframe(spark: "SparkSession", window_features_df: pd.DataFrame) -> "DataFrame":
    if window_features_df.empty:
        return spark.createDataFrame([], schema=WINDOW_X_SCHEMA)
    return spark.createDataFrame(pandas_records_for_spark(window_features_df), schema=WINDOW_X_SCHEMA)
