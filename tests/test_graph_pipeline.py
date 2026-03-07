from libs.graph import (
    build_event_graph_spark_table,
    build_graph_fusion_from_component_tables,
    build_graph_artifact_tables,
    build_graph_artifacts_from_window_x_table,
    build_lag_graph_spark_table,
    build_precision_graph_from_window_x_spark_table,
    build_transition_graph_spark_table,
)
from libs.testing.sample_data import create_sample_events_df, create_sample_raw_table_df, create_sample_windows_df
from libs.windows import build_window_x_table


def test_build_graph_artifact_tables_produces_graph_families_and_hierarchy(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()
    backbone_df = spark.createDataFrame(
        [
            {
                "backbone_version": 2,
                "selected_sensors_c": ["ENG_TEMP_1"],
                "all_sensors": ["ENG_TEMP_1", "HYD_PRESS_1"],
                "weights_b": [[1.0, 0.0]],
                "lambda_ridge": 1.0,
                "training_window_count": 2,
            }
        ]
    ).toPandas()

    precision_df, event_df, lag_df, transition_df, fused_df, hierarchy_df = build_graph_artifact_tables(
        raw_df,
        events_df,
        windows_df,
        backbone_df,
        min_abs_partial_corr=0.0,
        min_event_count=1,
        min_lag_count=1,
        min_fused_edge_weight=0.0,
    )

    assert set(["sensor_u", "sensor_v", "edge_family"]).issubset(event_df.columns)
    assert set(["sensor_u", "sensor_v", "mean_lag_seconds", "edge_family"]).issubset(lag_df.columns)
    assert set(["sensor_u", "sensor_v", "precedence_count", "precedence_weight", "edge_family"]).issubset(transition_df.columns)
    assert set(["sensor_u", "sensor_v", "fused_weight", "edge_family"]).issubset(fused_df.columns)
    assert set(["parameter_name", "system_id", "subsystem_id", "module_id"]).issubset(hierarchy_df.columns)
    assert len(hierarchy_df) >= 1


def test_build_graph_artifacts_from_window_x_table_matches_raw_builder(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_df = create_sample_events_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()
    window_x_df = build_window_x_table(
        raw_df,
        raw_df.iloc[0:0].assign(event_type_detected="", payload=None),
        windows_df,
    )
    backbone_df = spark.createDataFrame(
        [
            {
                "backbone_version": 2,
                "selected_sensors_c": ["ENG_TEMP_1"],
                "all_sensors": ["ENG_TEMP_1", "HYD_PRESS_1"],
                "weights_b": [[1.0, 0.0]],
                "lambda_ridge": 1.0,
                "training_window_count": 2,
            }
        ]
    ).toPandas()

    split = build_graph_artifacts_from_window_x_table(
        window_x_df,
        events_df,
        windows_df,
        backbone_df,
        min_abs_partial_corr=0.0,
        min_event_count=1,
        min_lag_count=1,
        min_fused_edge_weight=0.0,
    )
    mono = build_graph_artifact_tables(
        raw_df,
        events_df,
        windows_df,
        backbone_df,
        min_abs_partial_corr=0.0,
        min_event_count=1,
        min_lag_count=1,
        min_fused_edge_weight=0.0,
    )

    for split_df, mono_df in zip(split, mono, strict=False):
        assert list(split_df.columns) == list(mono_df.columns)
        assert len(split_df) == len(mono_df)


def test_spark_graph_tables_feed_fusion_helper(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    events_sdf = create_sample_events_df(spark)
    windows_sdf = create_sample_windows_df(spark)
    window_x_df = build_window_x_table(
        raw_df,
        raw_df.iloc[0:0].assign(event_type_detected="", payload=None),
        windows_sdf.toPandas(),
    )
    backbone_df = spark.createDataFrame(
        [
            {
                "backbone_version": 2,
                "selected_sensors_c": ["ENG_TEMP_1"],
                "all_sensors": ["ENG_TEMP_1", "HYD_PRESS_1"],
                "weights_b": [[1.0, 0.0]],
                "lambda_ridge": 1.0,
                "training_window_count": 2,
            }
        ]
    ).toPandas()

    event_pdf = build_event_graph_spark_table(events_sdf, windows_sdf, min_count=1, min_npmi=0.0, top_k_per_sensor=8).toPandas()
    lag_pdf = build_lag_graph_spark_table(
        events_sdf,
        tau_max_seconds=30.0,
        min_count=1,
        max_mean_lag_seconds=None,
        top_k_outgoing=8,
    ).toPandas()
    transition_pdf = build_transition_graph_spark_table(events_sdf, min_count=1).toPandas()
    precision_df = build_precision_graph_from_window_x_spark_table(
        spark.createDataFrame(window_x_df),
        selected_sensors=["ENG_TEMP_1"],
        ridge_lambda=1.0,
        min_abs_partial_corr=0.0,
    )
    fused_df, hierarchy_df = build_graph_fusion_from_component_tables(
        precision_df,
        event_pdf,
        lag_pdf,
        backbone_df,
        min_fused_edge_weight=0.0,
    )

    assert set(["sensor_u", "sensor_v", "cooccur_count", "event_weight", "edge_family"]).issubset(event_pdf.columns)
    assert set(["sensor_u", "sensor_v", "lag_count", "lag_weight", "mean_lag_seconds", "edge_family"]).issubset(lag_pdf.columns)
    assert set(["sensor_u", "sensor_v", "precedence_count", "precedence_weight", "edge_family"]).issubset(transition_pdf.columns)
    assert set(["sensor_u", "sensor_v", "partial_corr", "precision_weight", "edge_family"]).issubset(precision_df.columns)
    assert set(["sensor_u", "sensor_v", "fused_weight", "edge_family"]).issubset(fused_df.columns)
    assert set(["parameter_name", "system_id", "subsystem_id", "module_id"]).issubset(hierarchy_df.columns)


def test_build_precision_graph_from_window_x_spark_table_matches_pandas_builder(spark):
    raw_df = create_sample_raw_table_df(spark).toPandas()
    windows_df = create_sample_windows_df(spark).toPandas()
    window_x_df = build_window_x_table(
        raw_df,
        raw_df.iloc[0:0].assign(event_type_detected="", payload=None),
        windows_df,
    )

    spark_precision_df = build_precision_graph_from_window_x_spark_table(
        spark.createDataFrame(window_x_df),
        selected_sensors=["ENG_TEMP_1"],
        ridge_lambda=1.0,
        min_abs_partial_corr=0.0,
    )
    pandas_precision_df, _, _, _, _, _ = build_graph_artifacts_from_window_x_table(
        window_x_df,
        create_sample_events_df(spark).toPandas(),
        windows_df,
        spark.createDataFrame(
            [
                {
                    "backbone_version": 2,
                    "selected_sensors_c": ["ENG_TEMP_1"],
                    "all_sensors": ["ENG_TEMP_1", "HYD_PRESS_1"],
                    "weights_b": [[1.0, 0.0]],
                    "lambda_ridge": 1.0,
                    "training_window_count": 2,
                }
            ]
        ).toPandas(),
        min_abs_partial_corr=0.0,
    )

    assert list(spark_precision_df.columns) == list(pandas_precision_df.columns)
    assert len(spark_precision_df) == len(pandas_precision_df)


def test_hierarchy_assignment_requires_mutual_local_support():
    from libs.graph.pipeline import _assign_hierarchy

    fused_df = __import__("pandas").DataFrame(
        [
            {"sensor_u": "A", "sensor_v": "B", "fused_weight": 0.9},
            {"sensor_u": "A", "sensor_v": "C", "fused_weight": 0.8},
            {"sensor_u": "B", "sensor_v": "C", "fused_weight": 0.1},
            {"sensor_u": "D", "sensor_v": "E", "fused_weight": 0.85},
        ]
    )
    hierarchy_df = _assign_hierarchy(
        fused_df,
        ["A", "B", "C", "D", "E"],
        min_edge_weight=0.2,
        top_k_per_sensor=1,
    )
    by_parameter = {row["parameter_name"]: row["module_id"] for row in hierarchy_df.to_dict(orient="records")}

    assert by_parameter["A"] == by_parameter["B"]
    assert by_parameter["C"] != by_parameter["A"]
    assert by_parameter["D"] == by_parameter["E"]
    assert by_parameter["A"] != by_parameter["D"]


def test_hierarchy_assignment_respects_explicit_weight_thresholds():
    from libs.graph.pipeline import _assign_hierarchy

    fused_df = __import__("pandas").DataFrame(
        [
            {"sensor_u": "A", "sensor_v": "B", "fused_weight": 0.95},
            {"sensor_u": "C", "sensor_v": "D", "fused_weight": 0.94},
            {"sensor_u": "B", "sensor_v": "C", "fused_weight": 0.72},
        ]
    )

    permissive_df = _assign_hierarchy(
        fused_df,
        ["A", "B", "C", "D"],
        min_edge_weight=0.7,
        top_k_per_sensor=1,
        subsystem_min_edge_weight=0.7,
        system_min_edge_weight=0.3,
    )
    strict_df = _assign_hierarchy(
        fused_df,
        ["A", "B", "C", "D"],
        min_edge_weight=0.7,
        top_k_per_sensor=1,
        subsystem_min_edge_weight=0.8,
        system_min_edge_weight=0.7,
    )

    permissive = {row["parameter_name"]: row for row in permissive_df.to_dict(orient="records")}
    strict = {row["parameter_name"]: row for row in strict_df.to_dict(orient="records")}

    assert permissive["B"]["subsystem_id"] == permissive["C"]["subsystem_id"]
    assert strict["B"]["subsystem_id"] != strict["C"]["subsystem_id"]
