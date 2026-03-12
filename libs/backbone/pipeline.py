"""Backbone artifact table adapters for Spark pipeline stages."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from libs.backbone.fit import aggregate_backbone_gh, compute_backbone_gh_by_flight
from libs.backbone.model import BackboneModel, BackboneSpec
from libs.io.pandas_spark import pandas_records_for_spark
from libs.windows import build_window_features_dataframe

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
        ]
    )


def _backbone_gh_schema():
    from pyspark.sql import types as T

    return T.StructType(
        [
            T.StructField("tail_id", T.StringType(), False),
            T.StructField("flight_id", T.StringType(), False),
            T.StructField("window_count", T.IntegerType(), False),
            T.StructField("g_f", T.ArrayType(T.ArrayType(T.DoubleType(), containsNull=False), containsNull=False), False),
            T.StructField("h_f", T.ArrayType(T.ArrayType(T.DoubleType(), containsNull=False), containsNull=False), False),
        ]
    )


def build_backbone_artifacts_from_window_x_table(
    window_x_df: pd.DataFrame,
    *,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if window_x_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    window_x_rows = window_x_df.to_dict(orient="records")
    if not window_x_rows:
        return pd.DataFrame(), pd.DataFrame()
    backbone_model, sensor_energies = BackboneModel.from_window_x_rows(
        window_x_rows,
        spec=BackboneSpec(
            sensor_count=backbone_sensor_count,
            ridge_lambda=backbone_ridge_lambda,
        ),
    )
    return pd.DataFrame([backbone_model.to_row()]), pd.DataFrame([item.to_row() for item in sensor_energies])


def build_backbone_sensor_energy_spark_table(window_x_df: DataFrame) -> DataFrame:
    """Aggregate per-sensor energy from ``window_x`` without collecting fact rows."""
    F = _spark_functions()
    vector_entries = (
        window_x_df.select(F.explode_outer(F.map_entries("continuous_vector_t_end_scaled")).alias("entry"))
        .select(
            F.col("entry.key").cast("string").alias("parameter_name"),
            F.col("entry.value").cast("double").alias("scaled_value"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("scaled_value").isNotNull())
    )
    return vector_entries.groupBy("parameter_name").agg(
        F.sum(F.col("scaled_value") * F.col("scaled_value")).cast("double").alias("energy"),
        F.count(F.lit(1)).cast("int").alias("support_count"),
    )


def build_backbone_gh_spark_table(window_x_df: DataFrame, *, selected_sensors: list[str]) -> DataFrame:
    """Compute per-flight ``G_f`` and ``H_f`` in grouped Spark execution."""
    F = _spark_functions()
    backbone_sensors = [str(item) for item in selected_sensors if str(item)]
    all_sensors = (
        window_x_df.select(F.explode_outer(F.map_keys("continuous_vector_t_end_scaled")).alias("parameter_name"))
        .where(F.col("parameter_name").isNotNull())
        .distinct()
        .orderBy("parameter_name")
        .toPandas()["parameter_name"]
        .astype(str)
        .tolist()
    )

    grouped_input = (
        window_x_df.select("tail_id", "flight_id", "continuous_vector_t_end_scaled")
        .groupBy("tail_id", "flight_id")
        .applyInPandas(
            lambda flight_pdf: _build_backbone_gh_rows_for_flight(
                flight_pdf,
                selected_sensors=backbone_sensors,
                all_sensors=all_sensors,
            ),
            schema=_backbone_gh_schema(),
        )
    )
    return grouped_input


def _build_backbone_gh_rows_for_flight(
    flight_pdf: pd.DataFrame,
    *,
    selected_sensors: list[str],
    all_sensors: list[str],
) -> pd.DataFrame:
    if flight_pdf.empty:
        return pd.DataFrame(columns=[field.name for field in _backbone_gh_schema().fields])
    gh_rows, _ = compute_backbone_gh_by_flight(
        flight_pdf.to_dict(orient="records"),
        selected_sensors=selected_sensors,
        all_sensors=all_sensors,
    )
    normalized_rows = []
    for row in gh_rows:
        normalized_rows.append(
            {
                "tail_id": str(row["tail_id"]),
                "flight_id": str(row["flight_id"]),
                "window_count": int(row["window_count"]),
                "g_f": [[float(value) for value in matrix_row] for matrix_row in row["g_f"].tolist()],
                "h_f": [[float(value) for value in matrix_row] for matrix_row in row["h_f"].tolist()],
            }
        )
    return pd.DataFrame(pandas_records_for_spark(pd.DataFrame(normalized_rows)))


def build_backbone_artifact_tables(
    raw_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    *,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    empty_events_df = pd.DataFrame(columns=["tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_detected", "payload"])
    window_x_df = build_window_features_dataframe(raw_df, empty_events_df, windows_df)
    return build_backbone_artifacts_from_window_x_table(
        window_x_df,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
    )
