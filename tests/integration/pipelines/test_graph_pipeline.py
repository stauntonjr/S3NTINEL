from datetime import datetime, timedelta, timezone

import pytest

from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.schemas import WINDOW_X_SCHEMA
from libs.graph import (
    build_event_graph_spark_table,
    build_fused_graph_spark_table,
    build_graph_components_with_diagnostics_spark_table,
    build_hierarchy_from_fused_spark_table,
    build_lag_candidate_pairs_spark_table,
    build_lag_graph_spark_table,
    build_lag_profile_spark_table,
    build_precision_graph_from_window_features_spark_table,
    build_transition_graph_spark_table,
    collapse_lag_profile_spark_table,
    LagBandSpec,
)
from libs.testing.data import create_sample_events_df, create_sample_raw_table_df, create_sample_windows_df
from libs.windows import build_window_features_spark_table


def _build_window_features_pdf_with_events(spark):
    raw_df = create_sample_raw_table_df(spark)
    events_sdf = create_sample_events_df(spark)
    windows_sdf = create_sample_windows_df(spark)
    window_features_pdf = build_window_features_spark_table(raw_df, events_sdf, windows_sdf).toPandas()
    return raw_df, events_sdf, windows_sdf, window_features_pdf


def _round_nested(value, *, digits: int = 6):
    if isinstance(value, dict):
        return {key: _round_nested(item, digits=digits) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_round_nested(item, digits=digits) for item in value]
    if isinstance(value, float):
        return round(value, digits)
    return value


def _precision_pandas_to_spark_table(spark, precision_df):
    schema = """
parameter_name_u string,
parameter_name_v string,
partial_corr double,
precision_weight double,
edge_family string
"""
    if precision_df.empty:
        return spark.createDataFrame([], schema=schema)
    return spark.createDataFrame(pandas_records_for_spark(precision_df), schema=schema)


def test_spark_graph_tables_produce_graph_families_and_hierarchy(spark):
    _, events_sdf, windows_df, window_features_df = _build_window_features_pdf_with_events(spark)
    backbone_sdf = spark.createDataFrame(
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
    )
    window_features_sdf = spark.createDataFrame(pandas_records_for_spark(window_features_df), schema=WINDOW_X_SCHEMA)

    precision_sdf, event_sdf, lag_sdf, transition_sdf, fused_sdf, parameter_universe_sdf, _ = (
        build_graph_components_with_diagnostics_spark_table(
            window_features_sdf,
            events_sdf,
            windows_df,
            backbone_sdf,
            min_abs_partial_corr=0.0,
            min_event_count=1,
            min_lag_count=1,
        )
    )
    hierarchy_df = build_hierarchy_from_fused_spark_table(
        fused_sdf,
        parameter_names=[row["parameter_name"] for row in parameter_universe_sdf.collect()],
        min_fused_edge_weight=0.0,
        hierarchy_top_k_per_parameter_name=3,
    )

    assert set(["parameter_name_u", "parameter_name_v", "edge_family"]).issubset(event_sdf.columns)
    assert set(["parameter_name_u", "parameter_name_v", "mean_lag_seconds", "edge_family"]).issubset(lag_sdf.columns)
    assert set(["parameter_name_u", "parameter_name_v", "precedence_count", "precedence_weight", "edge_family"]).issubset(transition_sdf.columns)
    assert set(["parameter_name_u", "parameter_name_v", "fused_weight", "edge_family"]).issubset(fused_sdf.columns)
    assert set(["parameter_name", "system_id", "subsystem_id", "module_id"]).issubset(hierarchy_df.columns)
    assert len(hierarchy_df) >= 1


def test_build_graph_components_with_diagnostics_spark_table_reports_component_details(spark):
    _, events_sdf, windows_sdf, window_features_pdf = _build_window_features_pdf_with_events(spark)
    window_features_sdf = spark.createDataFrame(pandas_records_for_spark(window_features_pdf), schema=WINDOW_X_SCHEMA)
    backbone_sdf = spark.createDataFrame(
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
    )

    precision_sdf, event_sdf, lag_sdf, transition_sdf, fused_sdf, parameter_universe_sdf, diagnostics = (
        build_graph_components_with_diagnostics_spark_table(
            window_features_sdf,
            events_sdf,
            windows_sdf,
            backbone_sdf,
            min_abs_partial_corr=0.0,
            min_event_count=1,
            min_lag_count=1,
        )
    )

    assert precision_sdf.count() >= 0
    assert event_sdf.count() >= 0
    assert lag_sdf.count() >= 0
    assert transition_sdf.count() >= 0
    assert fused_sdf.count() >= 0
    assert parameter_universe_sdf.count() > 0
    assert diagnostics.total_timing_ms >= 0.0
    assert diagnostics.steps
    assert any(step.step_name == "lag_profile_build" for step in diagnostics.steps)
    assert any(step.step_name == "lag_graph_build" for step in diagnostics.steps)


def test_spark_graph_tables_feed_fusion_helper(spark):
    _, events_sdf, windows_sdf, window_features_df = _build_window_features_pdf_with_events(spark)
    backbone_sdf = spark.createDataFrame(
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
    )

    event_sdf = build_event_graph_spark_table(events_sdf, windows_sdf, min_count=1, min_npmi=0.0, top_k_per_parameter_name=8)
    lag_sdf = build_lag_graph_spark_table(
        events_sdf,
        tau_max_seconds=30.0,
        min_count=1,
        max_mean_lag_seconds=None,
        top_k_outgoing=8,
    )
    transition_pdf = build_transition_graph_spark_table(events_sdf, min_count=1).toPandas()
    precision_df = build_precision_graph_from_window_features_spark_table(
        spark.createDataFrame(pandas_records_for_spark(window_features_df), schema=WINDOW_X_SCHEMA),
        selected_sensors=["ENG_TEMP_1"],
        ridge_lambda=1.0,
        min_abs_partial_corr=0.0,
    )
    fused_sdf = build_fused_graph_spark_table(
        _precision_pandas_to_spark_table(spark, precision_df),
        event_sdf,
        lag_sdf,
        alpha=1.0,
        beta=1.0,
        gamma=1.0,
    )
    hierarchy_df = build_hierarchy_from_fused_spark_table(
        fused_sdf,
        parameter_names=["ENG_TEMP_1", "HYD_PRESS_1", "PUMP_STATE"],
        min_fused_edge_weight=0.0,
        hierarchy_top_k_per_parameter_name=3,
    )
    event_pdf = event_sdf.toPandas()
    lag_pdf = lag_sdf.toPandas()
    fused_df = fused_sdf.toPandas()
    assert set(["parameter_name_u", "parameter_name_v", "cooccur_count", "event_weight", "edge_family"]).issubset(event_pdf.columns)
    assert set(["parameter_name_u", "parameter_name_v", "lag_count", "lag_weight", "mean_lag_seconds", "edge_family"]).issubset(lag_pdf.columns)
    assert set(["parameter_name_u", "parameter_name_v", "precedence_count", "precedence_weight", "edge_family"]).issubset(transition_pdf.columns)
    assert set(["parameter_name_u", "parameter_name_v", "partial_corr", "precision_weight", "edge_family"]).issubset(precision_df.columns)
    assert set(["parameter_name_u", "parameter_name_v", "fused_weight", "edge_family"]).issubset(fused_df.columns)
    assert set(["parameter_name", "system_id", "subsystem_id", "module_id"]).issubset(hierarchy_df.columns)
    assert len(fused_df) >= 1
    assert len(hierarchy_df) >= 1


def test_build_precision_graph_from_window_features_spark_table_produces_expected_columns(spark):
    _, events_sdf, windows_df, window_features_df = _build_window_features_pdf_with_events(spark)

    spark_precision_df = build_precision_graph_from_window_features_spark_table(
        spark.createDataFrame(pandas_records_for_spark(window_features_df), schema=WINDOW_X_SCHEMA),
        selected_sensors=["ENG_TEMP_1"],
        ridge_lambda=1.0,
        min_abs_partial_corr=0.0,
    )
    assert set(spark_precision_df.columns) == {
        "parameter_name_u",
        "parameter_name_v",
        "partial_corr",
        "precision_weight",
        "edge_family",
    }
    assert len(spark_precision_df) >= 0


def test_graph_window_features_fixture_keeps_event_type_counts(spark):
    _, _, _, window_features_df = _build_window_features_pdf_with_events(spark)

    nonempty_event_maps = sum(1 for row in window_features_df.to_dict(orient="records") if row.get("event_type_counts"))

    assert nonempty_event_maps > 0


def test_graph_builders_require_event_seq_id(spark):
    events_sdf = create_sample_events_df(spark).drop("event_seq_id")
    windows_sdf = create_sample_windows_df(spark)

    with pytest.raises(ValueError, match="event_seq_id"):
        build_event_graph_spark_table(events_sdf, windows_sdf, min_count=1, min_npmi=0.0, top_k_per_parameter_name=8).count()
    with pytest.raises(ValueError, match="event_seq_id"):
        build_lag_graph_spark_table(
            events_sdf,
            tau_max_seconds=30.0,
            min_count=1,
            max_mean_lag_seconds=None,
            top_k_outgoing=8,
        ).count()
    with pytest.raises(ValueError, match="event_seq_id"):
        build_transition_graph_spark_table(events_sdf, min_count=1).count()


def test_lag_graph_spark_table_keeps_nearest_prior_per_parameter_and_same_timestamp_order(spark):
    base = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    event_schema = """
tail_id string,
flight_id string,
event_seq_id long,
win_id int,
timestamp_utc timestamp,
parameter_name string,
event_type_detected string,
anomaly_type_detected string,
anomaly_score_detected double,
payload map<string,string>,
date_utc date
"""
    rows = [
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 1, "win_id": 1, "timestamp_utc": base, "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 2, "win_id": 1, "timestamp_utc": base, "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 3, "win_id": 1, "timestamp_utc": base + timedelta(seconds=5), "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 4, "win_id": 1, "timestamp_utc": base + timedelta(seconds=6), "parameter_name": "C", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 5, "win_id": 1, "timestamp_utc": base + timedelta(seconds=10), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 6, "win_id": 1, "timestamp_utc": base + timedelta(seconds=11), "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 7, "win_id": 1, "timestamp_utc": base + timedelta(seconds=21), "parameter_name": "C", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 8, "win_id": 1, "timestamp_utc": base + timedelta(seconds=21), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
    ]
    events_sdf = spark.createDataFrame(rows, schema=event_schema)

    lag_pdf = build_lag_graph_spark_table(
        events_sdf,
        tau_max_seconds=10.0,
        min_count=1,
        max_mean_lag_seconds=None,
        top_k_outgoing=0,
    ).toPandas()

    actual = lag_pdf.sort_values(["parameter_name_u", "parameter_name_v"], kind="stable").reset_index(drop=True)
    expected = [
        {"parameter_name_u": "A", "parameter_name_v": "B", "lag_count": 3, "lag_weight": 0.3, "mean_lag_seconds": 5.0, "edge_family": "lag_directed"},
        {"parameter_name_u": "A", "parameter_name_v": "C", "lag_count": 2, "lag_weight": 0.18, "mean_lag_seconds": 5.5, "edge_family": "lag_directed"},
        {"parameter_name_u": "B", "parameter_name_v": "A", "lag_count": 2, "lag_weight": 0.466667, "mean_lag_seconds": 3.0, "edge_family": "lag_directed"},
        {"parameter_name_u": "B", "parameter_name_v": "C", "lag_count": 1, "lag_weight": 0.133333, "mean_lag_seconds": 6.0, "edge_family": "lag_directed"},
        {"parameter_name_u": "C", "parameter_name_v": "A", "lag_count": 1, "lag_weight": 0.166667, "mean_lag_seconds": 5.0, "edge_family": "lag_directed"},
        {"parameter_name_u": "C", "parameter_name_v": "B", "lag_count": 2, "lag_weight": 0.533333, "mean_lag_seconds": 2.0, "edge_family": "lag_directed"},
    ]
    expected_rows = [{key: _round_nested(value) for key, value in row.items()} for row in expected]
    actual_rows = [
        {key: _round_nested(value) for key, value in row.items()}
        for row in actual.to_dict(orient="records")
    ]

    assert actual_rows == expected_rows


def test_lag_graph_spark_table_applies_filters_and_top_k_by_source(spark):
    base = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    event_schema = """
tail_id string,
flight_id string,
event_seq_id long,
win_id int,
timestamp_utc timestamp,
parameter_name string,
event_type_detected string,
anomaly_type_detected string,
anomaly_score_detected double,
payload map<string,string>,
date_utc date
"""
    rows = [
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 1, "win_id": 1, "timestamp_utc": base, "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 2, "win_id": 1, "timestamp_utc": base + timedelta(seconds=1), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 3, "win_id": 1, "timestamp_utc": base + timedelta(seconds=2), "parameter_name": "C", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 4, "win_id": 1, "timestamp_utc": base + timedelta(seconds=3), "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 5, "win_id": 1, "timestamp_utc": base + timedelta(seconds=4), "parameter_name": "C", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 6, "win_id": 1, "timestamp_utc": base + timedelta(seconds=20), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
    ]
    events_sdf = spark.createDataFrame(rows, schema=event_schema)

    min_count_pdf = build_lag_graph_spark_table(
        events_sdf,
        tau_max_seconds=10.0,
        min_count=2,
        max_mean_lag_seconds=None,
        top_k_outgoing=0,
    ).toPandas()
    assert set(zip(min_count_pdf["parameter_name_u"], min_count_pdf["parameter_name_v"])) == {("A", "C"), ("B", "C")}

    top_k_pdf = build_lag_graph_spark_table(
        events_sdf,
        tau_max_seconds=10.0,
        min_count=1,
        max_mean_lag_seconds=None,
        top_k_outgoing=1,
    ).toPandas()
    assert top_k_pdf["parameter_name_u"].value_counts().max() == 1

    max_mean_pdf = build_lag_graph_spark_table(
        events_sdf,
        tau_max_seconds=10.0,
        min_count=1,
        max_mean_lag_seconds=1.5,
        top_k_outgoing=0,
    ).toPandas()
    assert all(float(value) <= 1.5 for value in max_mean_pdf["mean_lag_seconds"].tolist())


def test_lag_profile_spark_table_assigns_one_band_and_collapses_to_legacy_graph(spark):
    base = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    event_schema = """
tail_id string,
flight_id string,
event_seq_id long,
win_id int,
timestamp_utc timestamp,
parameter_name string,
event_type_detected string,
anomaly_type_detected string,
anomaly_score_detected double,
payload map<string,string>,
date_utc date
"""
    rows = [
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 1, "win_id": 1, "timestamp_utc": base, "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 2, "win_id": 1, "timestamp_utc": base + timedelta(seconds=1), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 3, "win_id": 1, "timestamp_utc": base + timedelta(seconds=5), "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 4, "win_id": 1, "timestamp_utc": base + timedelta(seconds=11), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 5, "win_id": 1, "timestamp_utc": base + timedelta(seconds=20), "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 6, "win_id": 1, "timestamp_utc": base + timedelta(seconds=33), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T002", "flight_id": "F002", "event_seq_id": 1, "win_id": 1, "timestamp_utc": base, "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T002", "flight_id": "F002", "event_seq_id": 2, "win_id": 1, "timestamp_utc": base + timedelta(seconds=1), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
    ]
    events_sdf = spark.createDataFrame(rows, schema=event_schema)
    band_specs = (
        LagBandSpec(name="quick", lower_seconds=0.0, upper_seconds=2.0, combine_weight=1.0),
        LagBandSpec(name="medium", lower_seconds=2.0, upper_seconds=10.0, combine_weight=0.8),
        LagBandSpec(name="slow", lower_seconds=10.0, upper_seconds=30.0, combine_weight=0.5),
    )

    profile_pdf = build_lag_profile_spark_table(
        events_sdf,
        tau_max_seconds=30.0,
        bands=band_specs,
    ).toPandas()
    collapsed_pdf = collapse_lag_profile_spark_table(
        spark.createDataFrame(profile_pdf),
        tau_max_seconds=30.0,
        bands=band_specs,
        min_count=1,
        max_mean_lag_seconds=None,
        top_k_outgoing=0,
    ).toPandas()

    pair_profile_pdf = profile_pdf[
        (profile_pdf["parameter_name_u"] == "A") & (profile_pdf["parameter_name_v"] == "B")
    ]
    profile_rows = pair_profile_pdf.sort_values(["lag_band"], kind="stable").to_dict(orient="records")
    assert [(row["lag_band"], int(row["lag_count"])) for row in profile_rows] == [
        ("medium", 1),
        ("quick", 2),
        ("slow", 1),
    ]
    support_by_band = {row["lag_band"]: int(row["support_flight_count"]) for row in profile_rows}
    assert support_by_band == {"quick": 2, "medium": 1, "slow": 1}

    collapsed_row = collapsed_pdf[
        (collapsed_pdf["parameter_name_u"] == "A") & (collapsed_pdf["parameter_name_v"] == "B")
    ].to_dict(orient="records")
    assert len(collapsed_row) == 1
    collapsed = collapsed_row[0]
    assert collapsed["parameter_name_u"] == "A"
    assert collapsed["parameter_name_v"] == "B"
    assert int(collapsed["lag_count"]) == 4
    assert _round_nested(collapsed["mean_lag_seconds"]) == _round_nested((1.0 + 6.0 + 13.0 + 1.0) / 4.0)
    expected_weight = (
        pair_profile_pdf.loc[pair_profile_pdf["lag_band"] == "quick", "lag_weight"].sum() * 1.0
        + pair_profile_pdf.loc[pair_profile_pdf["lag_band"] == "medium", "lag_weight"].sum() * 0.8
        + pair_profile_pdf.loc[pair_profile_pdf["lag_band"] == "slow", "lag_weight"].sum() * 0.5
    )
    assert _round_nested(float(collapsed["lag_weight"])) == _round_nested(float(expected_weight))


def test_lag_profile_spark_table_candidate_pruning_keeps_supported_edges(spark):
    base = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    event_schema = """
tail_id string,
flight_id string,
event_seq_id long,
win_id int,
timestamp_utc timestamp,
parameter_name string,
event_type_detected string,
anomaly_type_detected string,
anomaly_score_detected double,
payload map<string,string>,
date_utc date
"""
    window_schema = """
tail_id string,
flight_id string,
win_id int,
t_start timestamp,
t_end timestamp,
date_utc date
"""
    events_rows = [
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 1, "win_id": 1, "timestamp_utc": base, "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 2, "win_id": 1, "timestamp_utc": base + timedelta(seconds=1), "parameter_name": "C", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 3, "win_id": 1, "timestamp_utc": base + timedelta(seconds=2), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 4, "win_id": 2, "timestamp_utc": base + timedelta(seconds=10), "parameter_name": "D", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 5, "win_id": 2, "timestamp_utc": base + timedelta(seconds=11), "parameter_name": "E", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 6, "win_id": 3, "timestamp_utc": base + timedelta(seconds=13), "parameter_name": "F", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
    ]
    window_rows = [
        {"tail_id": "T001", "flight_id": "F001", "win_id": 1, "t_start": base, "t_end": base + timedelta(seconds=2), "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "win_id": 2, "t_start": base + timedelta(seconds=10), "t_end": base + timedelta(seconds=11), "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "win_id": 3, "t_start": base + timedelta(seconds=13), "t_end": base + timedelta(seconds=13), "date_utc": base.date()},
    ]
    events_sdf = spark.createDataFrame(events_rows, schema=event_schema)
    windows_sdf = spark.createDataFrame(window_rows, schema=window_schema)

    event_sdf = build_event_graph_spark_table(events_sdf, windows_sdf, min_count=1, min_npmi=0.0, top_k_per_parameter_name=8)
    transition_sdf = build_transition_graph_spark_table(events_sdf, min_count=1)
    candidate_pairs_sdf = build_lag_candidate_pairs_spark_table(event_sdf, transition_sdf)
    lag_profile_pdf = build_lag_profile_spark_table(
        events_sdf,
        tau_max_seconds=10.0,
        candidate_pairs_df=candidate_pairs_sdf,
    ).toPandas()

    actual_pairs = set(zip(lag_profile_pdf["parameter_name_u"], lag_profile_pdf["parameter_name_v"]))
    assert ("A", "B") in actual_pairs
    assert ("D", "F") not in actual_pairs


def test_transition_graph_spark_table_follows_event_seq_id_order_and_row_normalizes(spark):
    base = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    event_schema = """
tail_id string,
flight_id string,
event_seq_id long,
win_id int,
timestamp_utc timestamp,
parameter_name string,
event_type_detected string,
anomaly_type_detected string,
anomaly_score_detected double,
payload map<string,string>,
date_utc date
"""
    rows = [
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 1, "win_id": 1, "timestamp_utc": base, "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 2, "win_id": 1, "timestamp_utc": base, "parameter_name": "A", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 3, "win_id": 1, "timestamp_utc": base + timedelta(seconds=1), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 4, "win_id": 1, "timestamp_utc": base + timedelta(seconds=2), "parameter_name": "C", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
        {"tail_id": "T001", "flight_id": "F001", "event_seq_id": 5, "win_id": 1, "timestamp_utc": base + timedelta(seconds=3), "parameter_name": "B", "event_type_detected": "e", "anomaly_type_detected": "", "anomaly_score_detected": 0.0, "payload": {}, "date_utc": base.date()},
    ]
    events_sdf = spark.createDataFrame(rows, schema=event_schema)

    transition_pdf = build_transition_graph_spark_table(events_sdf, min_count=1).toPandas()
    actual_rows = sorted(
        [{key: _round_nested(value) for key, value in row.items()} for row in transition_pdf.to_dict(orient="records")],
        key=lambda row: (row["parameter_name_u"], row["parameter_name_v"]),
    )
    expected_rows = sorted(
        [
            {"parameter_name_u": "A", "parameter_name_v": "B", "precedence_count": 1, "precedence_weight": 1.0, "edge_family": "transition"},
            {"parameter_name_u": "B", "parameter_name_v": "C", "precedence_count": 1, "precedence_weight": 1.0, "edge_family": "transition"},
            {"parameter_name_u": "C", "parameter_name_v": "B", "precedence_count": 1, "precedence_weight": 1.0, "edge_family": "transition"},
        ],
        key=lambda row: (row["parameter_name_u"], row["parameter_name_v"]),
    )

    assert actual_rows == expected_rows


def test_hierarchy_assignment_requires_mutual_local_support():
    from libs.graph.pipeline import _assign_hierarchy

    fused_df = __import__("pandas").DataFrame(
        [
            {"parameter_name_u": "A", "parameter_name_v": "B", "fused_weight": 0.9},
            {"parameter_name_u": "A", "parameter_name_v": "C", "fused_weight": 0.8},
            {"parameter_name_u": "B", "parameter_name_v": "C", "fused_weight": 0.1},
            {"parameter_name_u": "D", "parameter_name_v": "E", "fused_weight": 0.85},
        ]
    )
    hierarchy_df = _assign_hierarchy(
        fused_df,
        ["A", "B", "C", "D", "E"],
        min_edge_weight=0.2,
        top_k_per_parameter_name=1,
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
            {"parameter_name_u": "A", "parameter_name_v": "B", "fused_weight": 0.95},
            {"parameter_name_u": "C", "parameter_name_v": "D", "fused_weight": 0.94},
            {"parameter_name_u": "B", "parameter_name_v": "C", "fused_weight": 0.72},
        ]
    )

    permissive_df = _assign_hierarchy(
        fused_df,
        ["A", "B", "C", "D"],
        min_edge_weight=0.7,
        top_k_per_parameter_name=1,
        subsystem_min_edge_weight=0.7,
        system_min_edge_weight=0.3,
    )
    strict_df = _assign_hierarchy(
        fused_df,
        ["A", "B", "C", "D"],
        min_edge_weight=0.7,
        top_k_per_parameter_name=1,
        subsystem_min_edge_weight=0.8,
        system_min_edge_weight=0.7,
    )

    permissive = {row["parameter_name"]: row for row in permissive_df.to_dict(orient="records")}
    strict = {row["parameter_name"]: row for row in strict_df.to_dict(orient="records")}

    assert permissive["B"]["subsystem_id"] == permissive["C"]["subsystem_id"]
    assert strict["B"]["subsystem_id"] != strict["C"]["subsystem_id"]
