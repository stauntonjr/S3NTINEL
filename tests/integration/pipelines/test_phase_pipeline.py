from libs.backbone.artifacts import BackboneModel, BackboneSpec
from libs.graph.tables import HierarchySensorMapTable
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import PHASE_BASELINES_SCHEMA, PHASE_WINDOWS_SCHEMA, WINDOW_X_SCHEMA
from libs.phase import (
    PhaseBaselinesTable,
    PhaseClusterModel,
    PhaseDetectionPlan,
    PhaseFeatureConfig,
    PhaseLabelCentroidsTable,
    PhaseReferenceModelTable,
    PhaseWindowsTable,
    build_phase_centroid_comparison_summary_from_tables,
    fit_phase_feature_config_from_spark,
    fit_phase_feature_config_with_diagnostics_from_spark,
)
from libs.phase.artifacts import phase_output_literals, select_phase_windows
from libs.phase.decode import build_assignment_input, enforce_min_dwell
from libs.phase.fit import build_fit_source
from libs.phase.selectors import (
    select_categorical_state_pairs_from_window_features_spark,
    select_event_types_from_window_features_spark,
)
from libs.phase.frames import PhaseFeatureFrame
from libs.phase.types import PhaseTransitionModel
from libs.scoring import SCORE_COMPONENT_NAMES, WindowScoresRawTable
from libs.testing.data import create_sample_events_df, create_sample_raw_table_df, create_sample_windows_df
from libs.windows import WindowFeaturesTable
from datetime import date, datetime, timezone
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
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 2,
) -> PhaseDetectionPlan:
    return PhaseDetectionPlan(
        phase_count=phase_count,
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
    assert set(
        [
            "tail_id",
            "flight_id",
            "win_id",
            "phase_id_detected",
            "phase_state_detected",
            "transition_from_phase_id_detected",
            "transition_to_phase_id_detected",
            "s_w",
            "x_c",
            "backbone_residual_by_parameter",
        ]
    ).issubset(phase_windows_df.columns)
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
        "continuous_vector_t_start",
        "continuous_vector_t_start_scaled",
        "continuous_vector_t_end",
        "categorical_state_t_start",
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


def test_phase_feature_config_exposes_family_ordered_feature_names():
    config = PhaseFeatureConfig(
        backbone_model=BackboneModel(
            selected_sensors_c=["s0", "s1"],
            all_sensors=["s0", "s1"],
            weights_b=[[1.0, 0.0], [0.0, 1.0]],
            lambda_ridge=1.0,
            training_window_count=2,
            backbone_version=2,
        ),
        phase_selected_sensors=["s0", "s1"],
        phase_selected_event_types=["slope_pos", "transition"],
        phase_selected_categorical_state_pairs=[("press_mode_state", "AUTO"), ("press_mode_state", "MANUAL")],
        phase_selected_window_cooccurrence_pairs=[],
    )

    assert config.level_feature_names == ["parameter_name::s0", "parameter_name::s1"]
    assert config.delta_feature_names[:2] == ["parameter_delta::s0", "parameter_delta::s1"]
    assert config.event_feature_names == ["event_type::slope_pos", "event_type::transition"]
    assert config.categorical_feature_names[:2] == [
        "categorical_start::press_mode_state=AUTO",
        "categorical_start::press_mode_state=MANUAL",
    ]
    assert config.temporal_sensor_feature_names[:2] == [
        "temporal_sensor::s0:delta_mean:w2",
        "temporal_sensor::s1:delta_mean:w2",
    ]
    assert config.temporal_summary_feature_names[0] == "temporal_summary::history_coverage:w2"
    assert config.temporal_summary_feature_names[-3:] == [
        "temporal_summary::delta_energy_short_long_contrast",
        "temporal_summary::drift_rate_short_long_contrast",
        "temporal_summary::event_shift_short_long_contrast",
    ]
    assert config.feature_names == (
        config.level_feature_names
        + config.delta_feature_names
        + config.event_feature_names
        + config.categorical_feature_names
        + config.summary_feature_names
        + config.temporal_feature_names
    )


def test_phase_feature_frame_builds_temporal_family_features_with_history_warmup(spark):
    window_features_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "duration_ms": 10000,
                "event_count": 4,
                "date_utc": date(2025, 1, 1),
                "event_type_counts": {"slope_pos": 2, "transition": 1},
                "continuous_event_summary": {
                    "slope_run_count_by_parameter": {"s0": 2},
                    "slope_reinforcement_count_by_parameter": {"s0": 1},
                    "slope_signed_impulse_by_parameter": {"s0": 3.0},
                    "slope_abs_impulse_by_parameter": {"s0": 3.0},
                    "slope_peak_abs_delta_by_parameter": {"s0": 2.0},
                    "switch_count_by_parameter": {"s0": 1},
                    "threshold_count_by_parameter": {},
                    "oscillation_count_by_parameter": {},
                    "drift_guard_count_by_parameter": {"s0": 1},
                },
                "continuous_vector_t_start": {"s0": 10.0, "s1": 4.0},
                "continuous_vector_t_start_scaled": {"s0": 1.0, "s1": 4.0},
                "continuous_vector_t_end": {"s0": 30.0, "s1": 1.0},
                "continuous_vector_t_end_scaled": {"s0": 3.0, "s1": 1.0},
                "categorical_state_t_start": {"press_mode_state": "AUTO"},
                "categorical_state_t_end": {"press_mode_state": "MANUAL"},
                "drift_magnitude_profiled": 2.0,
                "phase_label": None,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "duration_ms": 10000,
                "event_count": 4,
                "date_utc": date(2025, 1, 1),
                "event_type_counts": {"transition": 2},
                "continuous_event_summary": {
                    "slope_run_count_by_parameter": {"s0": 1},
                    "slope_reinforcement_count_by_parameter": {"s0": 0},
                    "slope_signed_impulse_by_parameter": {"s0": 1.0},
                    "slope_abs_impulse_by_parameter": {"s0": 1.0},
                    "slope_peak_abs_delta_by_parameter": {"s0": 1.0},
                    "switch_count_by_parameter": {},
                    "threshold_count_by_parameter": {"s0": 1},
                    "oscillation_count_by_parameter": {},
                    "drift_guard_count_by_parameter": {},
                },
                "continuous_vector_t_start": {"s0": 30.0, "s1": 1.0},
                "continuous_vector_t_start_scaled": {"s0": 3.0, "s1": 1.0},
                "continuous_vector_t_end": {"s0": 40.0, "s1": 5.0},
                "continuous_vector_t_end_scaled": {"s0": 4.0, "s1": 5.0},
                "categorical_state_t_start": {"press_mode_state": "MANUAL"},
                "categorical_state_t_end": {"press_mode_state": "MANUAL"},
                "drift_magnitude_profiled": 1.0,
                "phase_label": None,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
                "duration_ms": 10000,
                "event_count": 4,
                "date_utc": date(2025, 1, 1),
                "event_type_counts": {"slope_pos": 1, "transition": 1},
                "continuous_event_summary": {
                    "slope_run_count_by_parameter": {"s0": 1},
                    "slope_reinforcement_count_by_parameter": {"s0": 1},
                    "slope_signed_impulse_by_parameter": {"s0": -1.0},
                    "slope_abs_impulse_by_parameter": {"s0": 1.0},
                    "slope_peak_abs_delta_by_parameter": {"s0": 1.0},
                    "switch_count_by_parameter": {"s0": 1},
                    "threshold_count_by_parameter": {},
                    "oscillation_count_by_parameter": {"s0": 1},
                    "drift_guard_count_by_parameter": {},
                },
                "continuous_vector_t_start": {"s0": 40.0, "s1": 5.0},
                "continuous_vector_t_start_scaled": {"s0": 4.0, "s1": 5.0},
                "continuous_vector_t_end": {"s0": 30.0, "s1": 6.0},
                "continuous_vector_t_end_scaled": {"s0": 3.0, "s1": 6.0},
                "categorical_state_t_start": {"press_mode_state": "MANUAL"},
                "categorical_state_t_end": {"press_mode_state": "AUTO"},
                "drift_magnitude_profiled": 4.0,
                "phase_label": None,
            }
        ],
        schema=WINDOW_X_SCHEMA(),
    )
    config = PhaseFeatureConfig(
        backbone_model=BackboneModel(
            selected_sensors_c=["s0", "s1"],
            all_sensors=["s0", "s1"],
            weights_b=[[1.0, 0.0], [0.0, 1.0]],
            lambda_ridge=1.0,
            training_window_count=1,
            backbone_version=2,
        ),
        phase_selected_sensors=["s0", "s1"],
        phase_selected_event_types=["slope_pos", "transition"],
        phase_selected_categorical_state_pairs=[("press_mode_state", "AUTO"), ("press_mode_state", "MANUAL")],
        phase_selected_window_cooccurrence_pairs=[],
    )

    feature_frame = PhaseFeatureFrame.from_window_features_df(window_features_df, phase_config=config)
    rows = (
        feature_frame.dataframe.select("win_id", "s_w", "feature_names")
        .toPandas()
        .sort_values("win_id", kind="stable")
        .reset_index(drop=True)
    )
    row1 = dict(zip(rows.iloc[0]["feature_names"], rows.iloc[0]["s_w"], strict=True))
    row2 = dict(zip(rows.iloc[1]["feature_names"], rows.iloc[1]["s_w"], strict=True))
    row3 = dict(zip(rows.iloc[2]["feature_names"], rows.iloc[2]["s_w"], strict=True))

    assert rows.iloc[0]["feature_names"] == config.feature_names
    assert row1["parameter_name::s0"] == 3.0
    assert row1["parameter_name::s1"] == 1.0
    assert row1["parameter_delta::s0"] == 2.0
    assert row1["parameter_delta::s1"] == -3.0
    assert row1["categorical_start::press_mode_state=AUTO"] == 1.0
    assert row1["categorical_end::press_mode_state=MANUAL"] == 1.0
    assert row1["categorical_changed::press_mode_state=AUTO"] == 1.0
    assert row1["categorical_changed::press_mode_state=MANUAL"] == 1.0
    assert row1["temporal_summary::history_coverage:w2"] == 0.0
    assert row2["temporal_summary::history_coverage:w2"] == 0.5
    assert row3["temporal_summary::history_coverage:w2"] == 1.0
    assert row3["temporal_summary::history_coverage:w4"] == 0.5
    assert round(row2["temporal_sensor::s0:delta_mean:w2"], 6) == 2.0
    assert round(row2["temporal_sensor::s1:delta_mean:w2"], 6) == -3.0
    assert round(row2["temporal_event::slope_pos:rate_mean:w2"], 6) == 0.5
    assert round(row2["temporal_event::transition:rate_mean:w2"], 6) == 0.25
    assert round(row2["temporal_categorical::dwell:press_mode_state=AUTO:w2"], 6) == 0.0
    assert round(row2["temporal_categorical::dwell:press_mode_state=MANUAL:w2"], 6) == 1.0
    assert round(row2["temporal_summary::delta_continuation_fraction:w2"], 6) == 0.5
    assert round(row2["temporal_summary::delta_reversal_fraction:w2"], 6) == 0.5
    assert round(row2["temporal_summary::event_shift:w2"], 6) == 0.375
    assert round(row2["temporal_summary::categorical_transition_rate:w2"], 6) == 1.0
    assert round(row3["temporal_sensor::s0:delta_mean:w2"], 6) == 1.5
    assert round(row3["temporal_sensor::s1:delta_mean:w2"], 6) == 0.5
    assert round(row3["temporal_summary::delta_continuation_fraction:w2"], 6) == 0.5
    assert round(row3["temporal_summary::delta_reversal_fraction:w2"], 6) == 0.5
    assert round(row3["temporal_summary::delta_energy_short_long_contrast"], 6) == 0.0
    assert round(row3["temporal_summary::drift_rate_short_long_contrast"], 6) == 0.0
    assert round(row3["temporal_summary::event_shift_short_long_contrast"], 6) == 0.0


def test_phase_selectors_prefer_concentrated_events_and_changing_states(spark):
    window_features_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "event_type_counts": {"slope_pos": 1, "oscillation": 3, "transition": 1},
                "categorical_state_t_start": {"a_mode": "ON", "z_mode": "AUTO"},
                "categorical_state_t_end": {"a_mode": "OFF", "z_mode": "AUTO"},
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "event_type_counts": {"slope_pos": 1, "oscillation": 3},
                "categorical_state_t_start": {"a_mode": "OFF", "z_mode": "AUTO"},
                "categorical_state_t_end": {"a_mode": "ON", "z_mode": "AUTO"},
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "event_type_counts": {"slope_pos": 1},
                "categorical_state_t_start": {"a_mode": "ON", "z_mode": "AUTO"},
                "categorical_state_t_end": {"a_mode": "OFF", "z_mode": "AUTO"},
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 4,
                "event_type_counts": {"slope_pos": 1},
                "categorical_state_t_start": {"a_mode": "OFF", "z_mode": "AUTO"},
                "categorical_state_t_end": {"a_mode": "ON", "z_mode": "AUTO"},
            },
        ]
    )

    assert select_event_types_from_window_features_spark(window_features_df, k=1) == ["oscillation"]
    assert select_categorical_state_pairs_from_window_features_spark(window_features_df, k=1) == [("a_mode", "OFF")]


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
    assert (spark_phase_baselines["baseline_window_count"] >= 1).all()
    assert (spark_phase_baselines["stable_window_count"] >= 0).all()


def test_enforce_min_dwell_preserves_short_boundary_phase_runs(spark):
    assigned_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 1, "phase_id_detected": 0, "dwell_limit": 3, "phase_costs": [0.1, 1.0, 2.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 2, "phase_id_detected": 0, "dwell_limit": 3, "phase_costs": [0.1, 1.0, 2.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 3, "phase_id_detected": 1, "dwell_limit": 3, "phase_costs": [1.0, 0.1, 2.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 4, "phase_id_detected": 1, "dwell_limit": 3, "phase_costs": [1.0, 0.1, 2.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 5, "phase_id_detected": 1, "dwell_limit": 3, "phase_costs": [1.0, 0.1, 2.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 6, "phase_id_detected": 2, "dwell_limit": 3, "phase_costs": [2.0, 1.0, 0.1]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 7, "phase_id_detected": 2, "dwell_limit": 3, "phase_costs": [2.0, 1.0, 0.1]},
        ]
    )

    rows = (
        enforce_min_dwell(assigned_df, config=_phase_plan(phase_count=3, phase_min_dwell_windows=3))
        .orderBy("phase_row_number")
        .select("phase_id_detected")
        .toPandas()["phase_id_detected"]
        .tolist()
    )

    assert rows == [0, 0, 1, 1, 1, 2, 2]


def test_enforce_min_dwell_still_merges_short_interior_phase_runs(spark):
    assigned_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 1, "phase_id_detected": 0, "dwell_limit": 3, "phase_costs": [0.1, 2.0, 2.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 2, "phase_id_detected": 0, "dwell_limit": 3, "phase_costs": [0.1, 2.0, 2.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 3, "phase_id_detected": 1, "dwell_limit": 3, "phase_costs": [0.2, 0.1, 1.0]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 4, "phase_id_detected": 2, "dwell_limit": 3, "phase_costs": [2.0, 1.0, 0.1]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 5, "phase_id_detected": 2, "dwell_limit": 3, "phase_costs": [2.0, 1.0, 0.1]},
            {"tail_id": "T1", "flight_id": "F1", "phase_row_number": 6, "phase_id_detected": 2, "dwell_limit": 3, "phase_costs": [2.0, 1.0, 0.1]},
        ]
    )

    rows = (
        enforce_min_dwell(assigned_df, config=_phase_plan(phase_count=3, phase_min_dwell_windows=3))
        .orderBy("phase_row_number")
        .select("phase_id_detected")
        .toPandas()["phase_id_detected"]
        .tolist()
    )

    assert rows == [0, 0, 0, 2, 2, 2]


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


def test_build_phase_spark_tables_emit_bounded_confidence_scores(spark):
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


def test_phase_baselines_fall_back_to_stable_windows_when_no_high_confidence_windows_exist(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 2,
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.0,
                "distance_to_centroid_detected": 2.0,
                "drift_magnitude": 1.0,
                "breadth": 1.0,
                "backbone_reconstruction_error": 0.2,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 1.0],
                "s_w": [0.0, 1.0],
                "date_utc": date(2025, 1, 1),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["slope_pos"],
                "selected_categorical_state_pairs": ["press_mode_state=AUTO"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 2,
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.1,
                "distance_to_centroid_detected": 3.0,
                "drift_magnitude": 1.2,
                "breadth": 1.0,
                "backbone_reconstruction_error": 0.3,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 1.0],
                "s_w": [0.2, 1.2],
                "date_utc": date(2025, 1, 1),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["slope_pos"],
                "selected_categorical_state_pairs": ["press_mode_state=AUTO"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
        ],
        schema=PHASE_WINDOWS_SCHEMA(),
    )

    baselines_df = PhaseBaselinesTable.from_phase_windows(
        phase_windows_df,
        phase_config={
            "selected_sensors_c": ["s0", "s1"],
            "all_sensors": ["s0", "s1"],
            "weights_b": [[1.0, 0.0], [0.0, 1.0]],
            "lambda_ridge": 1.0,
            "training_window_count": 2,
            "backbone_version": 2,
            "phase_selected_sensors": ["s0", "s1"],
            "phase_selected_event_types": ["slope_pos"],
            "phase_selected_categorical_state_pairs": [("press_mode_state", "AUTO")],
            "phase_selected_window_cooccurrence_pairs": [],
        },
    ).to_dataframe().toPandas()

    assert not baselines_df.empty
    assert baselines_df["baseline_source_mode"].iloc[0] == "stable"
    assert baselines_df["baseline_window_count"].iloc[0] == 2
    assert baselines_df["stable_window_count"].iloc[0] == 2


def test_phase_baselines_fall_back_to_confident_transition_windows_per_phase(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 2,
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.8,
                "distance_to_centroid_detected": 1.0,
                "drift_magnitude": 1.0,
                "breadth": 1.0,
                "backbone_reconstruction_error": 0.2,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 1.0],
                "s_w": [0.0, 1.0],
                "date_utc": date(2025, 1, 1),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["slope_pos"],
                "selected_categorical_state_pairs": ["press_mode_state=AUTO"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 2,
                "phase_id_detected": 1,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.8,
                "distance_to_centroid_detected": 0.3,
                "drift_magnitude": 1.2,
                "breadth": 1.0,
                "backbone_reconstruction_error": 0.3,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 1.0],
                "s_w": [10.0, 11.0],
                "date_utc": date(2025, 1, 1),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["slope_pos"],
                "selected_categorical_state_pairs": ["press_mode_state=AUTO"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 15, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 2,
                "phase_id_detected": 1,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.7,
                "distance_to_centroid_detected": 0.5,
                "drift_magnitude": 1.4,
                "breadth": 1.0,
                "backbone_reconstruction_error": 0.4,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 1.0],
                "s_w": [12.0, 13.0],
                "date_utc": date(2025, 1, 1),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["slope_pos"],
                "selected_categorical_state_pairs": ["press_mode_state=AUTO"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
        ],
        schema=PHASE_WINDOWS_SCHEMA(),
    )

    baselines_df = (
        PhaseBaselinesTable.from_phase_windows(
            phase_windows_df,
            phase_config={
                "selected_sensors_c": ["s0", "s1"],
                "all_sensors": ["s0", "s1"],
                "weights_b": [[1.0, 0.0], [0.0, 1.0]],
                "lambda_ridge": 1.0,
                "training_window_count": 3,
                "backbone_version": 2,
                "phase_selected_sensors": ["s0", "s1"],
                "phase_selected_event_types": ["slope_pos"],
                "phase_selected_categorical_state_pairs": [("press_mode_state", "AUTO")],
                "phase_selected_window_cooccurrence_pairs": [],
            },
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["phase_id_detected"], kind="stable")
        .reset_index(drop=True)
    )

    assert baselines_df["phase_id_detected"].tolist() == [0, 1]
    assert baselines_df["baseline_source_mode"].tolist() == ["stable_high_confidence", "confident_transition"]
    assert baselines_df["baseline_window_count"].tolist() == [1, 2]
    assert baselines_df["stable_window_count"].tolist() == [1, 0]


def test_phase_label_centroids_use_majority_overlap_truth_labels(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "duration_ms": 10000,
                "event_count": 3,
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 1.0,
                "breadth": 0.2,
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 0.0],
                "s_w": [1.0, 3.0],
                "date_utc": date(2025, 1, 1),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["slope_pos"],
                "selected_categorical_state_pairs": ["press_mode_state=AUTO"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "duration_ms": 10000,
                "event_count": 3,
                "phase_id_detected": 1,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.8,
                "distance_to_centroid_detected": 0.2,
                "drift_magnitude": 1.5,
                "breadth": 0.3,
                "backbone_reconstruction_error": 0.2,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 0.0],
                "s_w": [3.0, 5.0],
                "date_utc": date(2025, 1, 1),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["slope_pos"],
                "selected_categorical_state_pairs": ["press_mode_state=AUTO"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
            {
                "tail_id": "T1",
                "flight_id": "F2",
                "win_id": 1,
                "t_start": datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 2, 0, 0, 10, tzinfo=timezone.utc),
                "duration_ms": 10000,
                "event_count": 2,
                "phase_id_detected": 2,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.4,
                "distance_to_centroid_detected": 0.5,
                "drift_magnitude": 2.0,
                "breadth": 0.4,
                "backbone_reconstruction_error": 0.3,
                "backbone_residual_by_parameter": {},
                "x_c": [0.0, 0.0],
                "s_w": [10.0, 20.0],
                "date_utc": date(2025, 1, 2),
                "feature_names": ["f0", "f1"],
                "selected_sensors_c": ["s0", "s1"],
                "selected_event_types": ["transition"],
                "selected_categorical_state_pairs": ["press_mode_state=MANUAL"],
                "selected_window_cooccurrence_pairs": [],
                "backbone_all_sensors": ["s0", "s1"],
            },
        ],
        schema=PHASE_WINDOWS_SCHEMA(),
    )
    phase_labels_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 3, tzinfo=timezone.utc),
                "phase_label": "climb",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 7, tzinfo=timezone.utc),
                "phase_label": "climb",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 12, tzinfo=timezone.utc),
                "phase_label": "climb",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 17, tzinfo=timezone.utc),
                "phase_label": "climb",
            },
            {
                "tail_id": "T1",
                "flight_id": "F2",
                "timestamp_utc": datetime(2025, 1, 2, 0, 0, 4, tzinfo=timezone.utc),
                "phase_label": "cruise",
            },
        ]
    )

    centroids_df = (
        PhaseLabelCentroidsTable.from_phase_windows_and_labels(
            phase_windows_df=phase_windows_df,
            phase_labels_df=phase_labels_df,
        )
        .to_dataframe()
        .toPandas()
        .sort_values(["tail_id", "phase_label"], kind="stable")
        .reset_index(drop=True)
    )

    assert centroids_df["phase_label"].tolist() == ["climb", "cruise"]
    assert centroids_df["labeled_window_count"].tolist() == [2, 1]
    assert centroids_df["flight_count"].tolist() == [1, 1]
    assert centroids_df["s_w_centroid"].tolist() == [[2.0, 4.0], [10.0, 20.0]]


def test_phase_centroid_comparison_summary_reports_low_drift_nearest_labels():
    phase_windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 1.0,
                "s_w": [1.0, 3.0],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "phase_id_detected": 1,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.8,
                "distance_to_centroid_detected": 0.2,
                "drift_magnitude": 3.0,
                "s_w": [5.0, 7.0, 0.1, 0.0, 0.2, 0.25],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
                "phase_id_detected": 2,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.4,
                "distance_to_centroid_detected": 0.4,
                "drift_magnitude": 9.0,
                "s_w": [10.0, 20.0, 0.0, 0.0, 0.3, 0.0],
            },
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 2, tzinfo=timezone.utc), "phase_label": "climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 8, tzinfo=timezone.utc), "phase_label": "climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 12, tzinfo=timezone.utc), "phase_label": "climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 18, tzinfo=timezone.utc), "phase_label": "climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 24, tzinfo=timezone.utc), "phase_label": "cruise"},
        ]
    )
    phase_baselines_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "phase_name_detected": "phase_0",
                "stable_window_count": 2,
                "feature_names": [
                    "parameter_name::f0",
                    "parameter_delta::f0",
                    "event_type::transition",
                    "categorical_start::mode=AUTO",
                    "summary::event_density_hz",
                    "temporal_summary::history_coverage:w2",
                ],
                "s_w_centroid": [1.0, 3.0, 0.2, 1.0, 0.1, 0.5],
            }
        ]
    )

    summary = build_phase_centroid_comparison_summary_from_tables(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
        phase_baselines_df=phase_baselines_df,
    )

    assert summary["status"] == "ok"
    assert summary["stable_window_label_counts"] == {"climb": 2, "cruise": 1}
    assert summary["truth_label_window_counts"] == {"climb": 2, "cruise": 1}
    assert any(item["window_subset"] == "low_drift_p50" for item in summary["truth_label_centroids"])
    nearest_overall = summary["nearest_truth_centroid_by_detected"][0]
    assert nearest_overall["phase_label"] == "climb"
    nearest_by_subset = {
        item["window_subset"]: item["phase_label"]
        for item in summary["nearest_truth_centroid_by_detected_and_subset"]
    }
    assert nearest_by_subset["all"] == "climb"
    assert nearest_by_subset["low_drift_p50"] == "climb"
    assert summary["feature_family_index_ranges"] == {
        "categorical": [3, 3],
        "delta": [1, 1],
        "event": [2, 2],
        "level": [0, 0],
        "summary": [4, 4],
        "temporal": [5, 5],
    }
    assert "distance_by_feature_family" in summary["distance_matrix"][0]
    assert summary["excluded_transition_window_counts_by_phase_label"] == {}


def test_select_phase_windows_derives_transition_context_from_neighboring_stable_runs(spark):
    enriched_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 1,
                "phase_id_detected": 0,
                "phase_raw_distances": [0.1, 9.0],
                "phase_distance_scales": [1.0, 1.0],
                "phase_costs": [0.1, 2.0],
                "drift_magnitude_profiled": 0.1,
                "drift_threshold": 1.0,
                "breadth": 0.1,
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"s0": 0.0},
                "x_c": [0.0],
                "s_w": [0.0],
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 1,
                "phase_id_detected": 0,
                "phase_raw_distances": [2.0, 3.0],
                "phase_distance_scales": [1.0, 1.0],
                "phase_costs": [0.2, 1.0],
                "drift_magnitude_profiled": 2.0,
                "drift_threshold": 1.0,
                "breadth": 0.2,
                "backbone_reconstruction_error": 0.2,
                "backbone_residual_by_parameter": {"s0": 0.0},
                "x_c": [0.0],
                "s_w": [1.0],
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 15, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 1,
                "phase_id_detected": 1,
                "phase_raw_distances": [8.0, 0.1],
                "phase_distance_scales": [1.0, 1.0],
                "phase_costs": [2.0, 0.1],
                "drift_magnitude_profiled": 0.1,
                "drift_threshold": 1.0,
                "breadth": 0.3,
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"s0": 0.0},
                "x_c": [0.0],
                "s_w": [2.0],
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 4,
                "t_start": datetime(2025, 1, 1, 0, 0, 15, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 1,
                "phase_id_detected": 1,
                "phase_raw_distances": [6.0, 2.0],
                "phase_distance_scales": [1.0, 1.0],
                "phase_costs": [2.0, 0.3],
                "drift_magnitude_profiled": 2.0,
                "drift_threshold": 1.0,
                "breadth": 0.4,
                "backbone_reconstruction_error": 0.2,
                "backbone_residual_by_parameter": {"s0": 0.0},
                "x_c": [0.0],
                "s_w": [3.0],
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 5,
                "t_start": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 25, tzinfo=timezone.utc),
                "duration_ms": 5000,
                "event_count": 1,
                "phase_id_detected": 1,
                "phase_raw_distances": [7.0, 0.1],
                "phase_distance_scales": [1.0, 1.0],
                "phase_costs": [2.0, 0.1],
                "drift_magnitude_profiled": 0.1,
                "drift_threshold": 1.0,
                "breadth": 0.5,
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"s0": 0.0},
                "x_c": [0.0],
                "s_w": [4.0],
                "date_utc": date(2025, 1, 1),
            },
        ]
    )
    phase_config = PhaseFeatureConfig(
        backbone_model=BackboneModel(
            selected_sensors_c=["s0"],
            all_sensors=["s0"],
            weights_b=[[1.0]],
            lambda_ridge=1.0,
            training_window_count=5,
            backbone_version=2,
        ),
        phase_selected_sensors=["s0"],
        phase_selected_event_types=["transition"],
        phase_selected_categorical_state_pairs=[],
        phase_selected_window_cooccurrence_pairs=[],
    )

    phase_windows_df = (
        select_phase_windows(
            enriched_df=enriched_df,
            phase_literals=phase_output_literals(phase_config),
        )
        .toPandas()
        .sort_values(["win_id"], kind="stable")
        .reset_index(drop=True)
    )

    assert phase_windows_df["phase_state_detected"].tolist() == [
        "stable",
        "transition_region",
        "stable",
        "stable",
        "stable",
    ]
    transition_from = [
        None if pd.isna(value) else float(value)
        for value in phase_windows_df["transition_from_phase_id_detected"].tolist()
    ]
    transition_to = [
        None if pd.isna(value) else float(value)
        for value in phase_windows_df["transition_to_phase_id_detected"].tolist()
    ]
    assert transition_from == [None, 0.0, None, None, None]
    assert transition_to == [None, 1.0, None, None, None]


def test_phase_centroid_comparison_summary_excludes_transition_truth_windows_from_truth_centroids():
    phase_windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "s_w": [1.0, 1.0],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "t_end": datetime(2025, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.3,
                "distance_to_centroid_detected": 0.5,
                "drift_magnitude": 5.0,
                "s_w": [9.0, 9.0],
            },
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc), "phase_label": "climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 6, tzinfo=timezone.utc), "phase_label": "climb"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": datetime(2025, 1, 1, 0, 0, 9, tzinfo=timezone.utc), "phase_label": "cruise"},
        ]
    )
    phase_baselines_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "phase_name_detected": "phase_0",
                "stable_window_count": 1,
                "feature_names": ["parameter_name::f0", "parameter_delta::f0"],
                "s_w_centroid": [1.0, 1.0],
            }
        ]
    )

    summary = build_phase_centroid_comparison_summary_from_tables(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
        phase_baselines_df=phase_baselines_df,
    )

    climb_centroid = next(
        item
        for item in summary["truth_label_centroids"]
        if item["phase_label"] == "climb" and item["window_subset"] == "all"
    )
    assert climb_centroid["window_count"] == 1
    assert climb_centroid["s_w_centroid"] == [1.0, 1.0]
    assert summary["excluded_transition_window_counts_by_phase_label"] == {"climb": 1}


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
    assert cluster_model.fit_source_stats_df is not None
    assert cluster_model.seed_bucket_counts_df is not None
    assert cluster_model.fit_source_stats_df.collect()[0]["fit_source_window_count"] == window_features_sdf.count()
    assert cluster_model.seed_bucket_counts_df.count() >= 1


def test_fit_cluster_model_uses_all_windows_for_fit_source_when_only_some_windows_are_stable(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    window_count = window_features_sdf.count()
    window_features_sdf = (
        window_features_sdf.orderBy("tail_id", "flight_id", "win_id")
        .withColumn(
            "drift_magnitude_profiled",
            F.when(F.col("win_id") == F.lit(0), F.lit(0.0)).otherwise(F.lit(10.0)).cast("double"),
        )
    )
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    plan = PhaseDetectionPlan(phase_count=2, phase_min_dwell_windows=2)
    config = plan.fit_phase_feature_config(
        window_features_sdf,
        backbone_df=backbone_df,
    )
    feature_frame = plan.build_feature_frame(window_features_sdf, phase_config=config)
    _, cluster_model = plan._fit_cluster_model(feature_frame)

    assert cluster_model.fit_source_stats_df is not None
    assert cluster_model.seed_bucket_counts_df is not None
    fit_source_row = cluster_model.fit_source_stats_df.collect()[0]
    feature_stats_row = cluster_model.feature_stats_df.collect()[0]
    seed_bucket_rows = cluster_model.seed_bucket_counts_df.orderBy("phase_id_detected").collect()
    centroid_rows = cluster_model.centroids_df.orderBy("phase_id_detected").collect()
    transition_support_rows = cluster_model.transition_model.support_df.orderBy("phase_id_detected").collect()

    assert fit_source_row["fit_source_window_count"] == window_count
    assert feature_stats_row["effective_phase_count"] == 2
    assert [row["phase_id_detected"] for row in seed_bucket_rows] == [0, 1]
    assert [row["phase_id_detected"] for row in centroid_rows] == [0, 1]
    assert [row["phase_id_detected"] for row in transition_support_rows] == [0, 1]
    assert cluster_model.transition_model.canonical_order_source == "seed_bucket"
    assert cluster_model.transition_model.policy_name == "monotone_progress_band"
    assert cluster_model.transition_model.progress_support_source == "seed_progress_mass_position_span"
    assert transition_support_rows[0]["phase_progress_start"] <= transition_support_rows[0]["phase_progress_end"]
    assert transition_support_rows[1]["phase_progress_start"] <= transition_support_rows[1]["phase_progress_end"]


def test_build_fit_source_uses_progress_mass_seed_buckets_when_change_is_concentrated(spark):
    scaled_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": idx,
                "phase_row_number": idx + 1,
                "flight_window_count": 10,
                "effective_phase_count": 4,
                "s_w_scaled": [value],
            }
            for idx, value in enumerate([0.0, 0.0, 6.389, 12.778, 12.778, 12.778, 12.778, 12.778, 12.778, 12.778])
        ]
    )

    rows = (
        build_fit_source(scaled_df)
        .orderBy("phase_row_number")
        .select("phase_row_number", "seed_phase_id")
        .collect()
    )

    seed_counts = {}
    for row in rows:
        seed_counts[int(row["seed_phase_id"])] = seed_counts.get(int(row["seed_phase_id"]), 0) + 1

    assert [int(row["seed_phase_id"]) for row in rows] == [0, 0, 1, 1, 2, 2, 2, 3, 3, 3]
    assert seed_counts == {0: 2, 1: 2, 2: 3, 3: 3}


def test_build_assignment_input_uses_progress_support_penalty_without_hint_smoothing(spark):
    feature_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 0,
                "phase_row_number": 1,
                "t_end": datetime(2025, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
                "effective_phase_count": 2,
                "dwell_limit": 2,
                "drift_threshold": 0.5,
                "flight_window_count": 4,
                "s_w_scaled": [0.0],
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "phase_row_number": 4,
                "t_end": datetime(2025, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
                "effective_phase_count": 2,
                "dwell_limit": 2,
                "drift_threshold": 0.5,
                "flight_window_count": 4,
                "s_w_scaled": [0.0],
            },
        ]
    )
    centroids_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "phase_id_detected": 0, "s_w_centroid": [-1.0], "fit_window_count": 2},
            {"tail_id": "T1", "flight_id": "F1", "phase_id_detected": 1, "s_w_centroid": [1.0], "fit_window_count": 2},
        ]
    )
    distance_scales_df = spark.createDataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "phase_id_detected": 0, "distance_scale": 1.0},
            {"tail_id": "T1", "flight_id": "F1", "phase_id_detected": 1, "distance_scale": 1.0},
        ]
    )
    transition_support_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "phase_id_detected": 0,
                "phase_progress_start": 0.0,
                "phase_progress_end": 0.3,
                "phase_progress_center": 0.1,
                "phase_progress_half_width": 0.15,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "phase_id_detected": 1,
                "phase_progress_start": 0.7,
                "phase_progress_end": 1.0,
                "phase_progress_center": 0.9,
                "phase_progress_half_width": 0.15,
            },
        ]
    )
    feature_stats_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "flight_window_count": 4,
                "stable_window_count_raw": 4,
                "effective_phase_count": 2,
            }
        ]
    )

    cluster_model = PhaseClusterModel(
        feature_stats_df=feature_stats_df,
        centroids_df=centroids_df,
        distance_scales_df=distance_scales_df,
        transition_model=PhaseTransitionModel(support_df=transition_support_df),
    )

    assignment_input_df = build_assignment_input(
        feature_df,
        cluster_model=cluster_model,
    )
    assignment_rows = assignment_input_df.orderBy("win_id").select("raw_phase_id").collect()

    assert [row["raw_phase_id"] for row in assignment_rows] == [0, 1]


def test_phase_detection_diagnostics_report_transition_support_and_assignment_counts(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_sdf = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)

    plan = PhaseDetectionPlan(phase_count=2, phase_min_dwell_windows=2)
    config = plan.fit_phase_feature_config(
        window_features_sdf,
        backbone_df=backbone_df,
    )
    detection_run = plan.run_detection(window_features_sdf, phase_config=config)

    flight_diagnostics = (detection_run.diagnostics or {})["phase_fit_flights"][0]

    assert flight_diagnostics["canonical_phase_order_source"] == "seed_bucket"
    assert flight_diagnostics["transition_policy_name"] == "monotone_progress_band"
    assert flight_diagnostics["progress_support_source"] == "seed_progress_mass_position_span"
    assert flight_diagnostics["phase_progress_support_by_phase_id"]
    assert "hint_assignment_counts_by_phase_id" not in flight_diagnostics
    assert "pre_decode_assignment_counts_by_phase_id" not in flight_diagnostics
    assert flight_diagnostics["raw_assignment_counts_by_phase_id"]


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
    assert artifacts.reference_model is not None
    assert artifacts.reference_model.to_dataframe().count() >= 1


def test_phase_reference_model_applies_fitted_model_to_target_flight(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_df = create_sample_events_df(spark)
    windows_df = create_sample_windows_df(spark)
    window_features_df = WindowFeaturesTable.from_raw_events_and_windows(raw_df, events_df, windows_df).to_dataframe()
    local_window_features_df = _build_window_features_pdf(raw_df, events_df, windows_df)
    backbone_df = _build_backbone_sdf(spark, local_window_features_df, sensor_count=2, ridge_lambda=1.0)
    plan = PhaseDetectionPlan(phase_count=2, phase_min_dwell_windows=2)
    config = plan.fit_phase_feature_config(window_features_df, backbone_df=backbone_df)
    fitted_run = plan.run_detection(window_features_df, phase_config=config)
    reference_model = PhaseReferenceModelTable.from_detection_run(fitted_run)
    reference_model.validate_schema()
    target_window_features_df = window_features_df.withColumn("flight_id", F.lit("F_TARGET"))

    inferred_run = plan.run_reference_inference(
        target_window_features_df,
        reference_model=reference_model,
    )
    inferred_rows = inferred_run.phase_windows.to_dataframe().orderBy("win_id").collect()
    fitted_phase_ids = [row["phase_id_detected"] for row in fitted_run.phase_windows.to_dataframe().orderBy("win_id").collect()]

    assert inferred_rows
    assert {row["flight_id"] for row in inferred_rows} == {"F_TARGET"}
    assert [row["phase_id_detected"] for row in inferred_rows] == fitted_phase_ids
    assert (inferred_run.diagnostics or {})["phase_reference_inference"] is True


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

    phase_windows_pdf, phases_pdf = _build_phase_artifacts_pdf(raw_df, events_df, windows_df)
    phase_windows_df = spark.createDataFrame(pandas_records_for_spark(phase_windows_pdf), schema=PHASE_WINDOWS_SCHEMA())
    phases_df = spark.createDataFrame(pandas_records_for_spark(phases_pdf), schema=PHASE_BASELINES_SCHEMA())
    hierarchy_sensor_map_df = pd.DataFrame(
        [
            {"parameter_name": "ENG_TEMP_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0001"},
            {"parameter_name": "HYD_PRESS_1", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0001", "module_id": "MOD_0002"},
            {"parameter_name": "PUMP_STATE", "system_id": "SYS_0001", "subsystem_id": "SUBSYS_0002", "module_id": "MOD_0003"},
        ]
    )
    scores_df = (
        WindowScoresRawTable.from_phase_dataframes(
            phase_windows_df,
            phases_df,
            spark.createDataFrame(pandas_records_for_spark(hierarchy_sensor_map_df)),
        )
        .to_dataframe()
        .toPandas()
    )

    assert not scores_df.empty
    assert set(["tail_id", "flight_id", "win_id", "global_score", "severity", "score_component_scores"]).issubset(scores_df.columns)
    assert set(scores_df["dominant_score_component"].tolist()).issubset(set(SCORE_COMPONENT_NAMES))
    assert scores_df["subsystem_scores"].apply(lambda item: isinstance(item, dict) and not item).all()


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
    dataframe_scores_df = (
        WindowScoresRawTable.from_phase_dataframes(
            phase_windows_df,
            phases_df,
            hierarchy_sensor_map_df,
        )
        .to_dataframe()
        .toPandas()
    )

    assert not scores_df.empty
    assert set(["tail_id", "flight_id", "win_id", "global_score", "severity", "score_component_scores"]).issubset(scores_df.columns)
    assert set(scores_df["dominant_score_component"].tolist()).issubset(set(SCORE_COMPONENT_NAMES))
    assert scores_df["subsystem_scores"].apply(lambda item: isinstance(item, dict) and not item).all()

    expected_scores_df = dataframe_scores_df.sort_values(["tail_id", "flight_id", "win_id"], kind="stable").reset_index(drop=True)
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
