from libs.backbone import (
    build_backbone_artifact_tables,
    build_backbone_artifacts_from_window_x_table,
    build_backbone_gh_spark_table,
    build_backbone_sensor_energy_spark_table,
)
from libs.windows import build_window_x_table
from libs.testing.sample_data import create_sample_raw_table_df, create_sample_windows_df


def test_build_backbone_artifact_tables_produces_backbone_and_energy(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    backbone_df, energy_df = build_backbone_artifact_tables(
        raw_df,
        windows_df,
        backbone_sensor_count=2,
        backbone_ridge_lambda=0.5,
    )

    assert not backbone_df.empty
    assert not energy_df.empty
    assert set(["backbone_version", "selected_sensors_c", "all_sensors", "weights_b", "lambda_ridge", "training_window_count"]).issubset(backbone_df.columns)
    assert set(["parameter_name", "energy", "support_count", "selected_backbone", "backbone_version"]).issubset(energy_df.columns)
    assert backbone_df.iloc[0]["backbone_version"] == 2
    assert len(backbone_df.iloc[0]["selected_sensors_c"]) <= 2


def test_build_backbone_artifacts_from_window_x_table_matches_raw_builder(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()
    empty_events_df = raw_df.iloc[0:0].copy()
    empty_events_df = empty_events_df.assign(event_type_detected="", payload=None)
    window_x_df = build_window_x_table(raw_df, empty_events_df, windows_df)

    split_backbone_df, split_energy_df = build_backbone_artifacts_from_window_x_table(
        window_x_df,
        backbone_sensor_count=2,
        backbone_ridge_lambda=0.5,
    )
    mono_backbone_df, mono_energy_df = build_backbone_artifact_tables(
        raw_df,
        windows_df,
        backbone_sensor_count=2,
        backbone_ridge_lambda=0.5,
    )

    assert list(split_backbone_df.columns) == list(mono_backbone_df.columns)
    assert list(split_energy_df.columns) == list(mono_energy_df.columns)
    assert len(split_backbone_df) == len(mono_backbone_df)
    assert len(split_energy_df) == len(mono_energy_df)


def test_build_backbone_sensor_energy_spark_table_matches_pandas_builder(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()
    empty_events_df = raw_df.iloc[0:0].copy()
    empty_events_df = empty_events_df.assign(event_type_detected="", payload=None)
    window_x_pdf = build_window_x_table(raw_df, empty_events_df, windows_df)
    window_x_sdf = spark.createDataFrame(window_x_pdf)

    _, energy_pdf = build_backbone_artifacts_from_window_x_table(
        window_x_pdf,
        backbone_sensor_count=2,
        backbone_ridge_lambda=0.5,
    )
    energy_spark_pdf = (
        build_backbone_sensor_energy_spark_table(window_x_sdf)
        .orderBy("parameter_name")
        .toPandas()[["parameter_name", "energy", "support_count"]]
    )
    energy_pandas_pdf = energy_pdf.sort_values("parameter_name").reset_index(drop=True)[
        ["parameter_name", "energy", "support_count"]
    ]

    assert energy_spark_pdf.to_dict(orient="records") == energy_pandas_pdf.to_dict(orient="records")


def test_build_backbone_gh_spark_table_emits_per_flight_rows(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()
    empty_events_df = raw_df.iloc[0:0].copy()
    empty_events_df = empty_events_df.assign(event_type_detected="", payload=None)
    window_x_pdf = build_window_x_table(raw_df, empty_events_df, windows_df)
    window_x_sdf = spark.createDataFrame(window_x_pdf)

    gh_spark_pdf = build_backbone_gh_spark_table(window_x_sdf, selected_sensors=["ENG_TEMP_1"]).toPandas()

    assert not gh_spark_pdf.empty
    assert set(["tail_id", "flight_id", "window_count", "g_f", "h_f"]).issubset(gh_spark_pdf.columns)
    assert gh_spark_pdf.iloc[0]["tail_id"] == "T001"
    assert gh_spark_pdf.iloc[0]["flight_id"] == "F001"
    assert gh_spark_pdf.iloc[0]["window_count"] >= 1
