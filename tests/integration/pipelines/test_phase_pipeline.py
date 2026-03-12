from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import PHASE_BASELINES_SCHEMA, PHASE_WINDOWS_SCHEMA
from libs.phase import (
    PhaseFeatureConfig,
    PhaseFeatures,
    build_phase_artifact_tables,
    build_phase_features_from_window_features_dataframe,
    build_window_features_spark_dataframe,
    build_window_features_dataframe,
)
from libs.scoring import build_window_scores_raw_spark_table, build_window_scores_raw_table
from libs.testing.data import create_sample_events_df, create_sample_raw_table_df, create_sample_windows_df
import pandas as pd


def test_build_phase_artifact_tables_produces_phase_windows_and_baselines(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    phase_windows_df, phases_df = build_phase_artifact_tables(
        raw_df,
        events_df,
        windows_df,
        phase_count=2,
        backbone_sensor_count=2,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )

    assert not phase_windows_df.empty
    assert set(["tail_id", "flight_id", "win_id", "phase_id_detected", "phase_state_detected", "s_w", "x_c", "backbone_residual_by_parameter"]).issubset(phase_windows_df.columns)
    assert not phases_df.empty
    assert set(["tail_id", "phase_id_detected", "s_w_centroid", "reconstruction_median"]).issubset(phases_df.columns)
    assert all("cooccur" not in item for item in phase_windows_df["selected_event_types"].explode().dropna().tolist())


def test_build_phase_features_from_window_features_dataframe_matches_monolithic_builder(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    window_x_df = build_window_features_dataframe(raw_df, events_df, windows_df)
    split_phase_windows_df, split_phases_df = build_phase_features_from_window_features_dataframe(
        window_x_df,
        phase_count=2,
        backbone_sensor_count=2,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    mono_phase_windows_df, mono_phases_df = build_phase_artifact_tables(
        raw_df,
        events_df,
        windows_df,
        phase_count=2,
        backbone_sensor_count=2,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )

    assert list(split_phase_windows_df.columns) == list(mono_phase_windows_df.columns)
    assert list(split_phases_df.columns) == list(mono_phases_df.columns)
    assert len(split_phase_windows_df) == len(mono_phase_windows_df)
    assert len(split_phases_df) == len(mono_phases_df)


def test_phase_feature_config_and_features_build_from_window_rows(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()
    window_x_df = build_window_features_dataframe(raw_df, events_df, windows_df)

    config = PhaseFeatureConfig.from_window_feature_rows(
        window_x_df.to_dict(orient="records"),
        backbone_sensor_count=2,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    artifacts = PhaseFeatures.from_window_feature_rows(
        window_x_df.to_dict(orient="records"),
        config=config,
        phase_count=2,
    )

    assert config.backbone_model.selected_sensors_c
    assert artifacts.phase_windows
    assert artifacts.phase_baselines


def test_build_window_features_spark_dataframe_matches_pandas_builder(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)

    spark_window_x = build_window_features_spark_dataframe(raw_df, events_df, windows_df).toPandas()
    pandas_window_x = build_window_features_dataframe(raw_df.toPandas(), events_df.toPandas(), windows_df.toPandas())

    assert list(spark_window_x.columns) == list(pandas_window_x.columns)
    assert len(spark_window_x) == len(pandas_window_x)


def test_build_window_features_dataframe_builds_expected_window_vectors(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    window_x_df = build_window_features_dataframe(raw_df, events_df, windows_df)

    assert not window_x_df.empty
    assert {"tail_id", "flight_id", "win_id", "continuous_vector_t_end", "categorical_state_t_end"}.issubset(window_x_df.columns)


def test_build_window_scores_raw_table_uses_phase_artifacts(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    phase_windows_df, phases_df = build_phase_artifact_tables(
        raw_df,
        events_df,
        windows_df,
        phase_count=2,
        backbone_sensor_count=2,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    hierarchy_sensor_map_df = pd.DataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "HYD_PRESS_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0002"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0003"},
        ]
    )
    scores_df = build_window_scores_raw_table(phase_windows_df, phases_df, hierarchy_sensor_map_df)

    assert not scores_df.empty
    assert set(["tail_id", "flight_id", "win_id", "global_score", "severity", "score_component_scores"]).issubset(scores_df.columns)
    assert set(scores_df["dominant_score_component"].tolist()).issubset({"structure", "reconstruction"})
    assert scores_df["subsystem_scores"].apply(lambda item: isinstance(item, dict)).all()


def test_build_window_scores_raw_spark_table_uses_phase_artifacts(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    phase_windows_pdf, phases_pdf = build_phase_artifact_tables(
        raw_df,
        events_df,
        windows_df,
        phase_count=2,
        backbone_sensor_count=2,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    phase_windows_df = spark.createDataFrame(pandas_records_for_spark(phase_windows_pdf), schema=PHASE_WINDOWS_SCHEMA)
    phases_df = spark.createDataFrame(pandas_records_for_spark(phases_pdf), schema=PHASE_BASELINES_SCHEMA)
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "HYD_PRESS_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0002"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0003"},
        ]
    )

    scores_df = build_window_scores_raw_spark_table(
        phase_windows_df,
        phases_df,
        hierarchy_sensor_map_df,
    ).toPandas()

    assert not scores_df.empty
    assert set(["tail_id", "flight_id", "win_id", "global_score", "severity", "score_component_scores"]).issubset(scores_df.columns)
    assert set(scores_df["dominant_score_component"].tolist()).issubset({"structure", "reconstruction"})
    assert scores_df["subsystem_scores"].apply(lambda item: isinstance(item, dict)).all()
