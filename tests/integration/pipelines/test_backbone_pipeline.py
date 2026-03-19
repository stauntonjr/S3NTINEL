import runpy
import numpy as np

from libs.backbone import (
    build_backbone_g_spark_table,
    build_backbone_h_spark_table,
    build_backbone_sensor_energy_spark_table,
    select_backbone_sensors_by_energy_spark,
    solve_backbone_weights,
)
from libs.backbone.pipeline import build_backbone_artifacts_from_window_features_table
from libs.io.delta import read_table
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import WINDOW_X_SCHEMA
from libs.testing.seed import seed_sample_dataset
from libs.testing.data import create_sample_events_df, create_sample_raw_table_df, create_sample_windows_df
from libs.windows import build_window_features_spark_table
from pyspark.sql import functions as F


def _build_window_features_pdf(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_pdf = build_window_features_spark_table(raw_df, events_df, windows_df).toPandas()
    return raw_df, events_df, windows_df, window_features_pdf


def test_build_backbone_artifacts_from_window_features_table_produces_backbone_and_energy(spark):
    _, _, _, window_features_df = _build_window_features_pdf(spark)

    backbone_df, energy_df = build_backbone_artifacts_from_window_features_table(
        window_features_df,
        backbone_sensor_count=2,
        backbone_ridge_lambda=0.5,
    )

    assert not backbone_df.empty
    assert not energy_df.empty
    assert set(["backbone_version", "selected_sensors_c", "all_sensors", "weights_b", "lambda_ridge", "training_window_count"]).issubset(backbone_df.columns)
    assert set(["parameter_name", "energy", "support_count", "selected_backbone", "backbone_version"]).issubset(energy_df.columns)
    assert backbone_df.iloc[0]["backbone_version"] == 2
    assert len(backbone_df.iloc[0]["selected_sensors_c"]) <= 2


def test_build_backbone_sensor_energy_spark_table_matches_pandas_builder(spark):
    _, _, _, window_features_pdf = _build_window_features_pdf(spark)
    window_features_sdf = spark.createDataFrame(pandas_records_for_spark(window_features_pdf), schema=WINDOW_X_SCHEMA)

    _, energy_pdf = build_backbone_artifacts_from_window_features_table(
        window_features_pdf,
        backbone_sensor_count=2,
        backbone_ridge_lambda=0.5,
    )
    energy_spark_pdf = (
        build_backbone_sensor_energy_spark_table(window_features_sdf)
        .orderBy("parameter_name")
        .toPandas()[["parameter_name", "energy", "support_count"]]
    )
    energy_pandas_pdf = energy_pdf.sort_values("parameter_name").reset_index(drop=True)[
        ["parameter_name", "energy", "support_count"]
    ]

    assert energy_spark_pdf.to_dict(orient="records") == energy_pandas_pdf.to_dict(orient="records")


def test_build_backbone_g_and_h_spark_tables_emit_backbone_aggregates(spark):
    _, _, _, window_features_pdf = _build_window_features_pdf(spark)
    window_features_sdf = spark.createDataFrame(pandas_records_for_spark(window_features_pdf), schema=WINDOW_X_SCHEMA)

    energy_sdf = build_backbone_sensor_energy_spark_table(window_features_sdf)
    selected_sensors = select_backbone_sensors_by_energy_spark(energy_sdf, k=1)
    g_row = build_backbone_g_spark_table(window_features_sdf, selected_sensors=selected_sensors).collect()[0].asDict()
    h_pdf = build_backbone_h_spark_table(window_features_sdf, selected_sensors=selected_sensors).toPandas()

    assert selected_sensors == ["ENG_TEMP_1"]
    assert g_row["window_count"] >= 1
    assert "g_0_0" in g_row
    assert not h_pdf.empty
    assert set(["parameter_name", "h_vector_c"]).issubset(h_pdf.columns)


def test_backbone_spark_aggregates_reconstruct_canonical_backbone_model(spark):
    _, _, _, window_features_pdf = _build_window_features_pdf(spark)
    window_features_sdf = spark.createDataFrame(pandas_records_for_spark(window_features_pdf), schema=WINDOW_X_SCHEMA)

    expected_backbone_df, _ = build_backbone_artifacts_from_window_features_table(
        window_features_pdf,
        backbone_sensor_count=2,
        backbone_ridge_lambda=0.5,
    )
    expected_row = expected_backbone_df.iloc[0].to_dict()

    energy_sdf = build_backbone_sensor_energy_spark_table(window_features_sdf)
    selected_sensors = select_backbone_sensors_by_energy_spark(energy_sdf, k=2)
    g_row = build_backbone_g_spark_table(window_features_sdf, selected_sensors=selected_sensors).first().asDict()
    h_pdf = build_backbone_h_spark_table(window_features_sdf, selected_sensors=selected_sensors).toPandas()
    all_sensors = h_pdf["parameter_name"].astype(str).tolist()
    g = [
        [float(g_row[f"g_{i}_{j}"]) for j in range(len(selected_sensors))]
        for i in range(len(selected_sensors))
    ]
    h = [
        [float(h_pdf.iloc[col_idx]["h_vector_c"][row_idx]) for col_idx in range(len(h_pdf))]
        for row_idx in range(len(selected_sensors))
    ]
    weights_b = solve_backbone_weights(np.asarray(g), np.asarray(h), ridge_lambda=0.5)
    actual_row = {
        "backbone_version": 2,
        "selected_sensors_c": list(selected_sensors),
        "all_sensors": list(all_sensors),
        "weights_b": [[round(float(value), 6) for value in row] for row in weights_b.tolist()],
        "lambda_ridge": 0.5,
        "training_window_count": int(g_row["window_count"]),
    }
    normalized_expected = {
        "backbone_version": int(expected_row["backbone_version"]),
        "selected_sensors_c": list(expected_row["selected_sensors_c"]),
        "all_sensors": list(expected_row["all_sensors"]),
        "weights_b": [[round(float(value), 6) for value in row] for row in expected_row["weights_b"]],
        "lambda_ridge": float(expected_row["lambda_ridge"]),
        "training_window_count": int(expected_row["training_window_count"]),
    }

    assert actual_row == normalized_expected


def test_backbone_window_features_fixture_keeps_event_type_counts(spark):
    _, _, _, window_features_pdf = _build_window_features_pdf(spark)

    nonempty_event_maps = sum(1 for row in window_features_pdf.to_dict(orient="records") if row.get("event_type_counts"))

    assert nonempty_event_maps > 0


def test_run_backbone_stage_builds_backbone_tables_in_spark(spark, tmp_path, monkeypatch):
    base_dir = tmp_path / "backbone_stage"
    seed_sample_dataset(
        spark=spark,
        base_dir=str(base_dir),
        mode="overwrite",
        table_format="parquet",
        include_intermediate_tables=True,
    )

    monkeypatch.setenv("S3NTINEL_RAW_TABLE_PATH", str(base_dir / "delta" / "raw_telemetry"))
    monkeypatch.setenv("S3NTINEL_EVENTS_TABLE_PATH", str(base_dir / "delta" / "events"))
    monkeypatch.setenv("S3NTINEL_WINDOWS_TABLE_PATH", str(base_dir / "delta" / "windows"))
    monkeypatch.setenv("S3NTINEL_BACKBONE_TABLE_PATH", str(base_dir / "delta" / "backbone"))
    monkeypatch.setenv("S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH", str(base_dir / "delta" / "backbone_sensor_energy"))
    monkeypatch.setenv("S3NTINEL_TABLE_FORMAT", "parquet")
    monkeypatch.setenv("S3NTINEL_FIT_WRITE_MODE", "overwrite")
    monkeypatch.setenv("S3NTINEL_BACKBONE_SENSOR_COUNT", "2")

    runpy.run_module("pipelines.40_backbone_fit", run_name="__main__")

    backbone_df = read_table(spark, str(base_dir / "delta" / "backbone"), fmt="parquet")
    energy_df = read_table(spark, str(base_dir / "delta" / "backbone_sensor_energy"), fmt="parquet")

    assert backbone_df.count() == 1
    assert energy_df.count() > 0


def test_run_backbone_stage_persisted_window_features_keep_event_type_counts(spark, tmp_path, monkeypatch):
    base_dir = tmp_path / "backbone_stage_window_features"
    seed_sample_dataset(
        spark=spark,
        base_dir=str(base_dir),
        mode="overwrite",
        table_format="parquet",
        include_intermediate_tables=True,
    )

    monkeypatch.setenv("S3NTINEL_RAW_TABLE_PATH", str(base_dir / "delta" / "raw_telemetry"))
    monkeypatch.setenv("S3NTINEL_EVENTS_TABLE_PATH", str(base_dir / "delta" / "events"))
    monkeypatch.setenv("S3NTINEL_WINDOWS_TABLE_PATH", str(base_dir / "delta" / "windows"))
    monkeypatch.setenv("S3NTINEL_WINDOW_FEATURES_TABLE_PATH", str(base_dir / "delta" / "window_features"))
    monkeypatch.setenv("S3NTINEL_BACKBONE_TABLE_PATH", str(base_dir / "delta" / "backbone"))
    monkeypatch.setenv("S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH", str(base_dir / "delta" / "backbone_sensor_energy"))
    monkeypatch.setenv("S3NTINEL_TABLE_FORMAT", "parquet")
    monkeypatch.setenv("S3NTINEL_FIT_WRITE_MODE", "overwrite")
    monkeypatch.setenv("S3NTINEL_BACKBONE_SENSOR_COUNT", "2")

    runpy.run_module("pipelines.40_backbone_fit", run_name="__main__")

    window_features_df = read_table(spark, str(base_dir / "delta" / "window_features"), fmt="parquet")
    nonempty_event_type_windows = window_features_df.where(
        F.size(F.coalesce(F.col("event_type_counts"), F.expr("cast(map() as map<string,int>)"))) > 0
    ).count()

    assert nonempty_event_type_windows > 0
