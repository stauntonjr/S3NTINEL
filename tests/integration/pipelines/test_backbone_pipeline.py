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
    assert set(
        [
            "parameter_name",
            "energy",
            "support_count",
            "event_prior",
            "selection_score",
            "selected_backbone",
            "backbone_version",
        ]
    ).issubset(energy_df.columns)
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
    energy_spark_pdf = build_backbone_sensor_energy_spark_table(window_features_sdf).orderBy("parameter_name").toPandas()[
        ["parameter_name", "energy", "support_count", "event_prior", "selection_score"]
    ]
    energy_pandas_pdf = energy_pdf.sort_values("parameter_name").reset_index(drop=True)[
        ["parameter_name", "energy", "support_count", "event_prior", "selection_score"]
    ]

    assert energy_spark_pdf.round(6).to_dict(orient="records") == energy_pandas_pdf.round(6).to_dict(
        orient="records"
    )


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


def test_window_features_use_raw_snapshot_state_not_numeric_event_payload(spark):
    raw_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": "2026-01-01T00:00:00",
                "parameter_name": "TEMP_A",
                "parameter_value": "10.0",
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": "2026-01-01T00:00:05",
                "parameter_name": "TEMP_A",
                "parameter_value": "20.0",
                "date_utc": "2026-01-01",
            },
        ]
    ).selectExpr(
        "tail_id",
        "flight_id",
        "cast(timestamp_utc as timestamp) as timestamp_utc",
        "parameter_name",
        "parameter_value",
        "cast(date_utc as date) as date_utc",
    )
    events_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 1,
                "timestamp_utc": "2026-01-01T00:00:03",
                "parameter_name": "TEMP_A",
                "event_type_detected": "slope_pos",
                "payload": {"value": "11.0", "run_peak_delta": "3.0", "emission_reason": "run_confirm"},
                "date_utc": "2026-01-01",
            }
        ]
    ).selectExpr(
        "tail_id",
        "flight_id",
        "cast(event_seq_id as long) as event_seq_id",
        "cast(timestamp_utc as timestamp) as timestamp_utc",
        "parameter_name",
        "event_type_detected",
        "payload",
        "cast(date_utc as date) as date_utc",
    )
    windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": "2026-01-01T00:00:00",
                "t_end": "2026-01-01T00:00:05",
                "duration_ms": 5000,
                "event_count": 1,
                "date_utc": "2026-01-01",
            }
        ]
    ).selectExpr(
        "tail_id",
        "flight_id",
        "cast(win_id as int) as win_id",
        "cast(t_start as timestamp) as t_start",
        "cast(t_end as timestamp) as t_end",
        "cast(duration_ms as int) as duration_ms",
        "cast(event_count as int) as event_count",
        "cast(date_utc as date) as date_utc",
    )

    row = build_window_features_spark_table(raw_df, events_df, windows_df).first().asDict(recursive=True)

    assert row["continuous_vector_t_end"]["TEMP_A"] == 20.0
    assert row["continuous_event_summary"]["slope_abs_impulse_by_parameter"]["TEMP_A"] == 3.0


def test_window_features_emit_continuous_event_summary_and_empty_maps(spark):
    raw_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": "2026-01-01T00:00:00",
                "parameter_name": "P1",
                "parameter_value": "10.0",
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": "2026-01-01T00:00:00",
                "parameter_name": "P2",
                "parameter_value": "OFF",
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": "2026-01-01T00:00:10",
                "parameter_name": "P1",
                "parameter_value": "20.0",
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": "2026-01-01T00:00:10",
                "parameter_name": "P2",
                "parameter_value": "ON",
                "date_utc": "2026-01-01",
            },
        ]
    ).selectExpr(
        "tail_id",
        "flight_id",
        "cast(timestamp_utc as timestamp) as timestamp_utc",
        "parameter_name",
        "parameter_value",
        "cast(date_utc as date) as date_utc",
    )
    events_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 1,
                "timestamp_utc": "2026-01-01T00:00:01",
                "parameter_name": "P1",
                "event_type_detected": "slope_pos",
                "payload": {"run_peak_delta": "2.0", "emission_reason": "run_confirm"},
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 2,
                "timestamp_utc": "2026-01-01T00:00:02",
                "parameter_name": "P1",
                "event_type_detected": "slope_pos",
                "payload": {"run_peak_delta": "4.0", "emission_reason": "run_strengthen"},
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 3,
                "timestamp_utc": "2026-01-01T00:00:03",
                "parameter_name": "P1",
                "event_type_detected": "slope_neg",
                "payload": {"run_peak_delta": "1.5", "emission_reason": "run_confirm"},
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 4,
                "timestamp_utc": "2026-01-01T00:00:03",
                "parameter_name": "P2",
                "event_type_detected": "switch",
                "payload": {},
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 5,
                "timestamp_utc": "2026-01-01T00:00:04",
                "parameter_name": "P2",
                "event_type_detected": "threshold",
                "payload": {},
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 6,
                "timestamp_utc": "2026-01-01T00:00:05",
                "parameter_name": "P2",
                "event_type_detected": "oscillation",
                "payload": {},
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "event_seq_id": 7,
                "timestamp_utc": "2026-01-01T00:00:06",
                "parameter_name": "P2",
                "event_type_detected": "drift_guard",
                "payload": {},
                "date_utc": "2026-01-01",
            },
        ]
    ).selectExpr(
        "tail_id",
        "flight_id",
        "cast(event_seq_id as long) as event_seq_id",
        "cast(timestamp_utc as timestamp) as timestamp_utc",
        "parameter_name",
        "event_type_detected",
        "payload",
        "cast(date_utc as date) as date_utc",
    )
    windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": "2026-01-01T00:00:00",
                "t_end": "2026-01-01T00:00:07",
                "duration_ms": 7000,
                "event_count": 7,
                "date_utc": "2026-01-01",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": "2026-01-01T00:00:08",
                "t_end": "2026-01-01T00:00:10",
                "duration_ms": 2000,
                "event_count": 0,
                "date_utc": "2026-01-01",
            },
        ]
    ).selectExpr(
        "tail_id",
        "flight_id",
        "cast(win_id as int) as win_id",
        "cast(t_start as timestamp) as t_start",
        "cast(t_end as timestamp) as t_end",
        "cast(duration_ms as int) as duration_ms",
        "cast(event_count as int) as event_count",
        "cast(date_utc as date) as date_utc",
    )

    rows = {
        int(row["win_id"]): row.asDict(recursive=True)
        for row in build_window_features_spark_table(raw_df, events_df, windows_df).collect()
    }

    summary = rows[1]["continuous_event_summary"]
    assert summary["slope_run_count_by_parameter"]["P1"] == 3
    assert summary["slope_reinforcement_count_by_parameter"]["P1"] == 1
    assert summary["slope_signed_impulse_by_parameter"]["P1"] == 4.5
    assert summary["slope_abs_impulse_by_parameter"]["P1"] == 7.5
    assert summary["slope_peak_abs_delta_by_parameter"]["P1"] == 4.0
    assert summary["switch_count_by_parameter"]["P2"] == 1
    assert summary["threshold_count_by_parameter"]["P2"] == 1
    assert summary["oscillation_count_by_parameter"]["P2"] == 1
    assert summary["drift_guard_count_by_parameter"]["P2"] == 1
    assert rows[2]["continuous_event_summary"]["slope_run_count_by_parameter"] == {}
    assert rows[2]["continuous_event_summary"]["threshold_count_by_parameter"] == {}


def test_backbone_sensor_energy_prefers_event_rich_sensor_when_energy_is_tied(spark):
    window_features_sdf = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": None,
                "t_end": None,
                "duration_ms": 100,
                "event_count": 2,
                "date_utc": None,
                "event_type_counts": {},
                "continuous_event_summary": {
                    "slope_run_count_by_parameter": {"s1": 1, "s2": 1},
                    "slope_reinforcement_count_by_parameter": {"s1": 2, "s2": 0},
                    "slope_signed_impulse_by_parameter": {"s1": 5.0, "s2": 0.5},
                    "slope_abs_impulse_by_parameter": {"s1": 5.0, "s2": 0.5},
                    "slope_peak_abs_delta_by_parameter": {"s1": 5.0, "s2": 0.5},
                    "switch_count_by_parameter": {"s1": 1, "s2": 0},
                    "threshold_count_by_parameter": {"s1": 0, "s2": 0},
                    "oscillation_count_by_parameter": {"s1": 0, "s2": 0},
                    "drift_guard_count_by_parameter": {"s1": 0, "s2": 0},
                },
                "continuous_vector_t_end": {"s1": 1.0, "s2": 1.0},
                "continuous_vector_t_end_scaled": {"s1": 1.0, "s2": 1.0},
                "categorical_state_t_end": {},
                "drift_magnitude_profiled": 0.0,
                "phase_label": None,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": None,
                "t_end": None,
                "duration_ms": 100,
                "event_count": 2,
                "date_utc": None,
                "event_type_counts": {},
                "continuous_event_summary": {
                    "slope_run_count_by_parameter": {"s1": 1, "s2": 1},
                    "slope_reinforcement_count_by_parameter": {"s1": 0, "s2": 0},
                    "slope_signed_impulse_by_parameter": {"s1": 2.0, "s2": 0.5},
                    "slope_abs_impulse_by_parameter": {"s1": 2.0, "s2": 0.5},
                    "slope_peak_abs_delta_by_parameter": {"s1": 2.0, "s2": 0.5},
                    "switch_count_by_parameter": {"s1": 0, "s2": 0},
                    "threshold_count_by_parameter": {"s1": 0, "s2": 0},
                    "oscillation_count_by_parameter": {"s1": 0, "s2": 0},
                    "drift_guard_count_by_parameter": {"s1": 0, "s2": 0},
                },
                "continuous_vector_t_end": {"s1": 1.0, "s2": 1.0},
                "continuous_vector_t_end_scaled": {"s1": 1.0, "s2": 1.0},
                "categorical_state_t_end": {},
                "drift_magnitude_profiled": 0.0,
                "phase_label": None,
            },
        ],
        schema=WINDOW_X_SCHEMA,
    )

    energy_df = build_backbone_sensor_energy_spark_table(window_features_sdf)
    energy_rows = [row.asDict() for row in energy_df.orderBy("parameter_name").collect()]

    assert {row["parameter_name"] for row in energy_rows} == {"s1", "s2"}
    assert energy_rows[0]["parameter_name"] == "s1"
    assert energy_rows[0]["event_prior"] > energy_rows[1]["event_prior"]
    assert select_backbone_sensors_by_energy_spark(energy_df, k=1) == ["s1"]


def test_backbone_g_and_h_ignore_continuous_event_summary_when_sensors_are_fixed(spark):
    base_rows = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 1,
            "t_start": None,
            "t_end": None,
            "duration_ms": 100,
            "event_count": 2,
            "date_utc": None,
            "event_type_counts": {},
            "continuous_vector_t_end": {"s1": 1.0, "s2": 2.0},
            "continuous_vector_t_end_scaled": {"s1": 1.0, "s2": 2.0},
            "categorical_state_t_end": {},
            "drift_magnitude_profiled": 0.0,
            "phase_label": None,
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "win_id": 2,
            "t_start": None,
            "t_end": None,
            "duration_ms": 100,
            "event_count": 2,
            "date_utc": None,
            "event_type_counts": {},
            "continuous_vector_t_end": {"s1": 2.0, "s2": 4.0},
            "continuous_vector_t_end_scaled": {"s1": 2.0, "s2": 4.0},
            "categorical_state_t_end": {},
            "drift_magnitude_profiled": 0.0,
            "phase_label": None,
        },
    ]
    variant_a = [
        {
            **row,
            "continuous_event_summary": {
                "slope_run_count_by_parameter": {"s1": 0, "s2": 0},
                "slope_reinforcement_count_by_parameter": {"s1": 0, "s2": 0},
                "slope_signed_impulse_by_parameter": {"s1": 0.0, "s2": 0.0},
                "slope_abs_impulse_by_parameter": {"s1": 0.0, "s2": 0.0},
                "slope_peak_abs_delta_by_parameter": {"s1": 0.0, "s2": 0.0},
                "switch_count_by_parameter": {"s1": 0, "s2": 0},
                "threshold_count_by_parameter": {"s1": 0, "s2": 0},
                "oscillation_count_by_parameter": {"s1": 0, "s2": 0},
                "drift_guard_count_by_parameter": {"s1": 0, "s2": 0},
            },
        }
        for row in base_rows
    ]
    variant_b = [
        {
            **row,
            "continuous_event_summary": {
                "slope_run_count_by_parameter": {"s1": 3, "s2": 1},
                "slope_reinforcement_count_by_parameter": {"s1": 2, "s2": 0},
                "slope_signed_impulse_by_parameter": {"s1": 6.0, "s2": 1.0},
                "slope_abs_impulse_by_parameter": {"s1": 6.0, "s2": 1.0},
                "slope_peak_abs_delta_by_parameter": {"s1": 4.0, "s2": 1.0},
                "switch_count_by_parameter": {"s1": 1, "s2": 0},
                "threshold_count_by_parameter": {"s1": 1, "s2": 0},
                "oscillation_count_by_parameter": {"s1": 0, "s2": 0},
                "drift_guard_count_by_parameter": {"s1": 0, "s2": 0},
            },
        }
        for row in base_rows
    ]

    window_features_a = spark.createDataFrame(variant_a, schema=WINDOW_X_SCHEMA)
    window_features_b = spark.createDataFrame(variant_b, schema=WINDOW_X_SCHEMA)

    g_a = build_backbone_g_spark_table(window_features_a, selected_sensors=["s1"]).first().asDict()
    g_b = build_backbone_g_spark_table(window_features_b, selected_sensors=["s1"]).first().asDict()
    h_a = build_backbone_h_spark_table(window_features_a, selected_sensors=["s1"]).orderBy("parameter_name").collect()
    h_b = build_backbone_h_spark_table(window_features_b, selected_sensors=["s1"]).orderBy("parameter_name").collect()

    assert g_a == g_b
    assert [row.asDict(recursive=True) for row in h_a] == [row.asDict(recursive=True) for row in h_b]


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
    assert {"event_prior", "selection_score"}.issubset(set(energy_df.columns))


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
