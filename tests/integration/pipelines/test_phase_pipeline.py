from libs.backbone.artifacts import BackboneModel, BackboneSpec
from libs.graph.tables import HierarchySensorMapTable
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import PHASE_BASELINES_SCHEMA, PHASE_WINDOWS_SCHEMA
from libs.phase import (
    PhaseBaselinesTable,
    PhaseDetectionPlan,
    PhaseFeatureConfig,
    PhaseWindowsTable,
    fit_phase_feature_config_from_spark,
    fit_phase_feature_config_with_diagnostics_from_spark,
)
from libs.scoring.artifacts import WindowScoreArtifacts
from libs.scoring import WindowScoresRawTable
from libs.testing.data import create_sample_events_df, create_sample_raw_table_df, create_sample_windows_df
from libs.windows import WindowFeaturesTable
import pandas as pd
from pyspark.sql import functions as F


def _round_nested(value, *, digits: int = 6):
    if isinstance(value, dict):
        return {key: _round_nested(item, digits=digits) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_round_nested(item, digits=digits) for item in value]
    if isinstance(value, float):
        return round(value, digits)
    return value


def _build_window_features_pdf(raw_df, events_df, windows_df) -> pd.DataFrame:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active SparkSession is required to build window features in tests")
    if isinstance(raw_df, pd.DataFrame):
        raw_df = spark.createDataFrame(pandas_records_for_spark(raw_df))
    if isinstance(events_df, pd.DataFrame):
        events_df = spark.createDataFrame(pandas_records_for_spark(events_df))
    if isinstance(windows_df, pd.DataFrame):
        windows_df = spark.createDataFrame(pandas_records_for_spark(windows_df))
    return (
        WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )


def _phase_plan(
    *,
    phase_count: int,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 2,
) -> PhaseDetectionPlan:
    return PhaseDetectionPlan(
        phase_count=phase_count,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )


def _build_phase_artifacts_pdf(raw_df, events_df, windows_df):
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("An active SparkSession is required to build phase artifacts in tests")
    if isinstance(raw_df, pd.DataFrame):
        raw_df = spark.createDataFrame(pandas_records_for_spark(raw_df))
    if isinstance(events_df, pd.DataFrame):
        events_df = spark.createDataFrame(pandas_records_for_spark(events_df))
    if isinstance(windows_df, pd.DataFrame):
        windows_df = spark.createDataFrame(pandas_records_for_spark(windows_df))

    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    backbone_df = _build_backbone_sdf(spark, window_features_sdf.toPandas(), sensor_count=2, ridge_lambda=1.0)
    config = fit_phase_feature_config_from_spark(
        window_features_df=window_features_sdf,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    phase_windows_pdf = (
        _phase_plan(phase_count=2).build_phase_windows(window_features_sdf, phase_config=config).to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )
    phase_baselines_pdf = (
        _phase_plan(phase_count=1)
        .build_phase_baselines(
            spark.createDataFrame(pandas_records_for_spark(phase_windows_pdf), schema=PHASE_WINDOWS_SCHEMA()),
            phase_config=config,
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "phase_id_detected"], kind="stable")
        .reset_index(drop=True)
    )
    return phase_windows_pdf, phase_baselines_pdf


def _build_backbone_sdf(spark, window_features_df: pd.DataFrame, *, sensor_count: int = 2, ridge_lambda: float = 1.0):
    backbone_model, _ = BackboneModel.from_window_feature_rows(
        window_features_df.to_dict(orient="records"),
        spec=BackboneSpec(sensor_count=sensor_count, ridge_lambda=ridge_lambda),
    )
    return spark.createDataFrame([backbone_model.to_row()])


def test_local_phase_artifacts_produce_phase_windows_and_baselines(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    phase_windows_df, phases_df = _build_phase_artifacts_pdf(raw_df, events_df, windows_df)

    assert not phase_windows_df.empty
    assert set(["tail_id", "flight_id", "win_id", "phase_id_detected", "phase_state_detected", "s_w", "x_c", "backbone_residual_by_parameter"]).issubset(phase_windows_df.columns)
    assert not phases_df.empty
    assert set(["tail_id", "phase_id_detected", "s_w_centroid", "reconstruction_median"]).issubset(phases_df.columns)
    assert all("cooccur" not in item for item in phase_windows_df["selected_event_types"].explode().dropna().tolist())


def test_build_window_features_spark_table_builds_expected_window_vectors(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)

    assert not window_features_df.empty
    assert {
        "tail_id",
        "flight_id",
        "win_id",
        "continuous_event_summary",
        "continuous_vector_t_end",
        "categorical_state_t_end",
    }.issubset(window_features_df.columns)


def test_build_window_features_with_diagnostics_spark_table_reports_step_details(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)

    window_features_sdf, diagnostics = WindowFeaturesTable.from_raw_events_windows_with_diagnostics(
        raw_df,
        events_df,
        windows_df,
    )
    window_features_sdf = window_features_sdf.to_dataframe()

    assert window_features_sdf.count() > 0
    assert diagnostics.output_row_count > 0
    assert diagnostics.total_timing_ms >= 0.0
    assert diagnostics.steps
    assert any(step.step_name == "assemble_feature_frame" for step in diagnostics.steps)
    assert all(step.row_count >= 0 for step in diagnostics.steps)


def test_fit_phase_feature_config_from_spark_builds_backbone_weights(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_df = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    backbone_df = _build_backbone_sdf(
        spark,
        _build_window_features_pdf(raw_df, events_df, windows_df),
        sensor_count=2,
        ridge_lambda=1.0,
    )

    config = fit_phase_feature_config_from_spark(
        window_features_df=window_features_df,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )

    assert config["selected_sensors_c"]
    assert config["all_sensors"]
    assert config["weights_b"]


def test_fit_phase_feature_config_with_diagnostics_from_spark_reports_selection_details(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_df = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    backbone_df = _build_backbone_sdf(
        spark,
        _build_window_features_pdf(raw_df, events_df, windows_df),
        sensor_count=2,
        ridge_lambda=1.0,
    )

    config, diagnostics = fit_phase_feature_config_with_diagnostics_from_spark(
        window_features_df=window_features_df,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )

    assert config["phase_selected_sensors"]
    assert diagnostics.event_types.selected_count == len(config["phase_selected_event_types"])
    assert diagnostics.categorical_state_pairs.selected_count == len(config["phase_selected_categorical_state_pairs"])
    assert diagnostics.sensors.selected_count == len(config["phase_selected_sensors"])
    assert diagnostics.event_types.timing_ms >= 0.0
    assert diagnostics.categorical_state_pairs.timing_ms >= 0.0


def test_build_phase_spark_tables_match_python_phase_runtime(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    config = fit_phase_feature_config_from_spark(
        window_features_df=window_features_sdf,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    phase_windows_sdf = _phase_plan(
        phase_count=2,
        phase_smoothing_radius=2,
        phase_transition_penalty=1.5,
        phase_min_dwell_windows=2,
    ).build_phase_windows(
        window_features_sdf,
        phase_config=config,
    ).to_dataframe()
    phase_baselines_sdf = _phase_plan(phase_count=1).build_phase_baselines(phase_windows_sdf, phase_config=config).to_dataframe()

    spark_phase_windows = (
        phase_windows_sdf.toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )
    spark_phase_baselines = (
        phase_baselines_sdf.toPandas()
        .sort_values(["tail_id", "phase_id_detected"], kind="stable")
        .reset_index(drop=True)
    )

    assert not spark_phase_windows.empty
    assert not spark_phase_baselines.empty
    assert set(spark_phase_windows["phase_state_detected"]).issubset({"stable", "transition_region"})
    assert (spark_phase_windows["phase_confidence_detected"] >= 0.0).all()
    assert (spark_phase_baselines["stable_window_count"] >= 0).all()


def test_build_phase_spark_tables_support_single_phase_fast_path(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    config = fit_phase_feature_config_from_spark(
        window_features_df=window_features_sdf,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    spark_phase_windows = (
        _phase_plan(
            phase_count=1,
            phase_smoothing_radius=0,
            phase_transition_penalty=1.5,
            phase_min_dwell_windows=2,
        )
        .build_phase_windows(
            window_features_sdf,
            phase_config=config,
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )
    assert spark_phase_windows["phase_id_detected"].tolist() == [0] * len(spark_phase_windows)
    assert set(spark_phase_windows["phase_state_detected"]).issubset({"stable", "transition_region"})


def test_build_phase_spark_tables_support_zero_smoothing_radius(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    config = fit_phase_feature_config_from_spark(
        window_features_df=window_features_sdf,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    spark_phase_windows = (
        _phase_plan(
            phase_count=2,
            phase_smoothing_radius=0,
            phase_transition_penalty=1.5,
            phase_min_dwell_windows=2,
        )
        .build_phase_windows(
            window_features_sdf,
            phase_config=config,
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )
    assert (spark_phase_windows["phase_confidence_detected"] >= 0.0).all()
    assert (spark_phase_windows["phase_confidence_detected"] <= 1.0).all()


def test_build_phase_spark_tables_support_min_dwell_one_fast_path(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    config = fit_phase_feature_config_from_spark(
        window_features_df=window_features_sdf,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    spark_phase_windows = (
        _phase_plan(
            phase_count=2,
            phase_smoothing_radius=2,
            phase_transition_penalty=1.5,
            phase_min_dwell_windows=1,
        )
        .build_phase_windows(
            window_features_sdf,
            phase_config=config,
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )
    assert len(spark_phase_windows) == window_features_sdf.count()
    assert set(spark_phase_windows["phase_state_detected"]).issubset({"stable", "transition_region"})


def test_fit_cluster_model_falls_back_when_no_windows_are_stable(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe().withColumn(
        "drift_magnitude_profiled",
        F.lit(float("nan")).cast("double"),
    )
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    plan = PhaseDetectionPlan(phase_count=2, phase_min_dwell_windows=2)
    config = plan.fit_phase_feature_config(
        window_features_sdf,
        backbone_df=backbone_df,
    )
    feature_frame = plan.build_feature_frame(window_features_sdf, phase_config=config)
    scaled_df, cluster_model = plan._fit_cluster_model(feature_frame)

    assert scaled_df.count() == window_features_sdf.count()
    assert cluster_model.centroids_df.count() >= 1


def test_phase_detection_plan_build_keeps_phase_config_as_domain_object(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    artifacts = PhaseDetectionPlan(phase_count=2, phase_min_dwell_windows=2).build(
        window_features_sdf,
        backbone_df=backbone_df,
    )

    assert isinstance(artifacts.phase_config, PhaseFeatureConfig)
    assert artifacts.phase_config.phase_selected_sensors
    assert artifacts.phase_windows.to_dataframe().count() == window_features_sdf.count()
    assert artifacts.phase_baselines.to_dataframe().count() >= 1


def test_phase_cooccurrence_metadata_does_not_affect_structure_vectors_or_assignments(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    config_without = fit_phase_feature_config_from_spark(
        window_features_df=window_features_sdf,
        backbone_df=backbone_df,
        phase_detect_sensor_count=2,
        phase_detect_event_type_count=2,
        phase_detect_categorical_state_count=2,
    )
    config_with = dict(config_without)
    config_with["phase_selected_window_cooccurrence_pairs"] = [
        ("ENG_TEMP_1", "HYD_PRESS_1"),
        ("HYD_PRESS_1", "PUMP_STATE"),
    ]

    plan = PhaseDetectionPlan(phase_count=2, phase_min_dwell_windows=2)
    feature_frame_without = plan.build_feature_frame(window_features_sdf, phase_config=config_without)
    feature_frame_with = plan.build_feature_frame(window_features_sdf, phase_config=config_with)
    phase_windows_without = (
        _phase_plan(
            phase_count=2,
            phase_smoothing_radius=2,
            phase_transition_penalty=1.5,
            phase_min_dwell_windows=2,
        )
        .build_phase_windows(
            window_features_sdf,
            phase_config=config_without,
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )
    phase_windows_with = (
        _phase_plan(
            phase_count=2,
            phase_smoothing_radius=2,
            phase_transition_penalty=1.5,
            phase_min_dwell_windows=2,
        )
        .build_phase_windows(
            window_features_sdf,
            phase_config=config_with,
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )

    assert config_without["phase_selected_window_cooccurrence_pairs"] == []
    assert config_with["phase_selected_window_cooccurrence_pairs"] != []
    assert feature_frame_without.feature_names == feature_frame_with.feature_names
    assert feature_frame_without.dataframe.select("tail_id", "flight_id", "win_id", "s_w").toPandas().to_dict(
        orient="records"
    ) == feature_frame_with.dataframe.select("tail_id", "flight_id", "win_id", "s_w").toPandas().to_dict(
        orient="records"
    )
    assert list(
        phase_windows_without[["tail_id", "flight_id", "win_id", "phase_id_detected", "phase_state_detected"]].itertuples(
            index=False, name=None
        )
    ) == list(
        phase_windows_with[["tail_id", "flight_id", "win_id", "phase_id_detected", "phase_state_detected"]].itertuples(
            index=False, name=None
        )
    )


def test_build_window_scores_raw_table_uses_phase_artifacts(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    phase_windows_df, phases_df = _build_phase_artifacts_pdf(raw_df, events_df, windows_df)
    hierarchy_sensor_map_df = pd.DataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "HYD_PRESS_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0002"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0003"},
        ]
    )
    scores_df = WindowScoreArtifacts.from_phase_rows(
        phase_windows_df.to_dict(orient="records"),
        phases_df.to_dict(orient="records"),
        hierarchy_sensor_map_df.to_dict(orient="records"),
    ).to_df()

    assert not scores_df.empty
    assert set(["tail_id", "flight_id", "win_id", "global_score", "severity", "score_component_scores"]).issubset(scores_df.columns)
    assert set(scores_df["dominant_score_component"].tolist()).issubset({"structure", "reconstruction"})
    assert scores_df["subsystem_scores"].apply(lambda item: isinstance(item, dict)).all()


def test_window_scores_raw_table_from_phase_tables_uses_phase_artifacts(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()

    phase_windows_pdf, phases_pdf = _build_phase_artifacts_pdf(raw_df, events_df, windows_df)
    phase_windows_df = spark.createDataFrame(pandas_records_for_spark(phase_windows_pdf), schema=PHASE_WINDOWS_SCHEMA())
    phases_df = spark.createDataFrame(pandas_records_for_spark(phases_pdf), schema=PHASE_BASELINES_SCHEMA())
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "HYD_PRESS_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0002"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0003"},
        ]
    )

    scores_df = WindowScoresRawTable.from_phase_tables(
        PhaseWindowsTable(dataframe=phase_windows_df),
        PhaseBaselinesTable(dataframe=phases_df),
        HierarchySensorMapTable(dataframe=hierarchy_sensor_map_df),
    ).to_dataframe().toPandas()

    assert not scores_df.empty
    assert set(["tail_id", "flight_id", "win_id", "global_score", "severity", "score_component_scores"]).issubset(scores_df.columns)
    assert set(scores_df["dominant_score_component"].tolist()).issubset({"structure", "reconstruction"})
    assert scores_df["subsystem_scores"].apply(lambda item: isinstance(item, dict)).all()

    expected_scores_df = (
        WindowScoreArtifacts.from_phase_rows(
            phase_windows_pdf.to_dict(orient="records"),
            phases_pdf.to_dict(orient="records"),
            hierarchy_sensor_map_df.toPandas().to_dict(orient="records"),
        ).to_df()
        .sort_values(["tail_id", "flight_id", "win_id"], kind="stable")
        .reset_index(drop=True)
    )
    spark_scores_df = scores_df.sort_values(["tail_id", "flight_id", "win_id"], kind="stable").reset_index(drop=True)
    comparison_columns = [
        "tail_id",
        "flight_id",
        "win_id",
        "global_score",
        "severity",
        "dominant_subsystem_id",
        "dominant_score_component",
        "subsystem_scores",
        "score_component_scores",
    ]
    spark_rows = [{col: _round_nested(row[col]) for col in comparison_columns} for row in spark_scores_df.to_dict(orient="records")]
    expected_rows = [{col: _round_nested(row[col]) for col in comparison_columns} for row in expected_scores_df.to_dict(orient="records")]
    assert spark_rows == expected_rows
