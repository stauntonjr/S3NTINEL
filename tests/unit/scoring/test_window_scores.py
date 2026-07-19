from __future__ import annotations

from datetime import date, datetime

from libs.io.schemas.scoring import WINDOW_SCORES_RAW_SCHEMA
from libs.scoring import (
    ACCUMULATION_VIOLATION_CHANNEL,
    EVENT_DISCORDANCE_CHANNEL,
    RECONSTRUCTION_ERROR_CHANNEL,
    REGIME_DEVIATION_CHANNEL,
    RESPONSE_VIOLATION_CHANNEL,
    SCORE_COMPONENT_NAMES,
    WindowScoresCalibratedTable,
    WindowScoresRawTable,
)
from libs.scoring.channels import score_component_scores_with_updates
from pyspark.sql import types as T


def _score_row(
    *,
    win_id: int,
    phase_id_detected: int,
    global_score: float,
    severity: str,
    flight_id: str = "F1",
    date_utc: date = date(2025, 1, 1),
) -> dict[str, object]:
    return {
        "tail_id": "T1",
        "flight_id": flight_id,
        "win_id": int(win_id),
        "phase_state_detected": "stable",
        "phase_id_detected": int(phase_id_detected),
        "phase_confidence_detected": 0.9,
        "distance_to_centroid_detected": 0.1,
        "drift_magnitude": 0.1,
        "breadth": 0.2,
        "global_score": float(global_score),
        "p_value": 1.0,
        "severity": str(severity),
        "dominant_subsystem_id": None,
        "dominant_module_id": None,
        "dominant_score_component": REGIME_DEVIATION_CHANNEL,
        "subsystem_scores": {},
        "score_component_scores": score_component_scores_with_updates(
            {
                REGIME_DEVIATION_CHANNEL: float(global_score),
                RECONSTRUCTION_ERROR_CHANNEL: 0.0,
            }
        ),
        "parameter_score_evidence": [],
        "date_utc": date_utc,
    }


def test_window_scores_raw_table_builds_scores_from_phase_dataframes(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"p1": 0.2, "p2": 0.1},
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "s_w": [0.1, 0.0],
                "backbone_reconstruction_error": 0.2,
                "backbone_residual_by_parameter": {"p1": 0.3, "p2": 0.1},
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.8,
                "distance_to_centroid_detected": 0.2,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "s_w": [5.0, 0.0],
                "backbone_reconstruction_error": 2.0,
                "backbone_residual_by_parameter": {"p1": 2.0, "p2": 0.2},
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.1,
                "distance_to_centroid_detected": 5.0,
                "drift_magnitude": 1.2,
                "breadth": 0.8,
                "date_utc": date(2025, 1, 1),
            },
        ]
    )
    phase_baselines_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "s_w_centroid": [0.0, 0.0],
                "reconstruction_median": 0.1,
                "reconstruction_mad": 0.1,
                "distance_median": 0.1,
                "distance_mad": 0.1,
            }
        ]
    )
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "p1", "subsystem_id": "SUB1", "module_id": "MOD1"},
            {"parameter_name": "p2", "subsystem_id": "SUB2", "module_id": "MOD2"},
        ]
    )

    scores_df = WindowScoresRawTable.from_phase_dataframes(
        phase_windows_df,
        phase_baselines_df,
        hierarchy_sensor_map_df,
    ).to_dataframe()
    scored = {int(item["win_id"]): item for item in scores_df.collect()}

    assert len(scored) == 3
    assert scored[3]["global_score"] > scored[1]["global_score"]
    assert scored[3]["severity"] in {"low", "medium", "high"}
    assert set(scored[3]["score_component_scores"].keys()) == set(SCORE_COMPONENT_NAMES)
    evidence = {item["parameter_name"]: item for item in scored[3]["parameter_score_evidence"]}
    assert set(evidence) == {"p1", "p2"}
    assert evidence["p1"]["residual_weight"] == 2.0
    assert evidence["p1"]["residual_share"] > evidence["p2"]["residual_share"]
    assert evidence["p1"]["global_evidence_rank"] == 1
    assert "reconstruction_error" in evidence["p1"]["candidate_channels"]


def test_window_scores_raw_table_joins_hierarchy_support_from_phase_dataframes(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"p1": 0.2, "p2": 0.1},
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "s_w": [5.0, 0.0],
                "backbone_reconstruction_error": 2.0,
                "backbone_residual_by_parameter": {"p1": 2.0, "p2": 0.2},
                "phase_id_detected": 0,
                "phase_state_detected": "transition_region",
                "phase_confidence_detected": 0.1,
                "distance_to_centroid_detected": 5.0,
                "drift_magnitude": 1.2,
                "breadth": 0.8,
                "date_utc": date(2025, 1, 1),
            },
        ]
    )
    phase_baselines_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "s_w_centroid": [0.0, 0.0],
                "reconstruction_median": 0.1,
                "reconstruction_mad": 0.1,
                "distance_median": 0.1,
                "distance_mad": 0.1,
            }
        ]
    )
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "p1", "subsystem_id": "SUB1", "module_id": "MOD1"},
            {"parameter_name": "p2", "subsystem_id": "SUB2", "module_id": "MOD2"},
        ]
    )

    scores_df = WindowScoresRawTable.from_phase_dataframes(
        phase_windows_df,
        phase_baselines_df,
        hierarchy_sensor_map_df,
    ).to_dataframe()
    by_win = {int(item["win_id"]): item for item in scores_df.collect()}

    assert by_win[2]["global_score"] > by_win[1]["global_score"]
    assert by_win[2]["dominant_subsystem_id"] == "SUB1"
    assert by_win[2]["dominant_module_id"] == "MOD1"
    assert set(by_win[2]["score_component_scores"].keys()) == set(SCORE_COMPONENT_NAMES)


def test_window_score_calibration_emits_warm_windows_by_severity_and_rarity(spark):
    rows = [
        _score_row(win_id=win_id, phase_id_detected=0, global_score=float(21 - win_id), severity="low")
        for win_id in range(1, 21)
    ]
    rows[0]["severity"] = "normal"
    rows[1]["severity"] = "low"
    rows[2]["severity"] = "high"
    rows[3]["severity"] = "low"
    rows.extend(
        [
            _score_row(
                win_id=101,
                phase_id_detected=1,
                global_score=9.0,
                severity="high",
                flight_id="F2",
            ),
            _score_row(
                win_id=102,
                phase_id_detected=1,
                global_score=8.0,
                severity="high",
                flight_id="F2",
            ),
        ]
    )

    calibrated = WindowScoresCalibratedTable.from_scores(
        spark.createDataFrame(rows, schema=WINDOW_SCORES_RAW_SCHEMA()),
        min_warm=3,
    ).to_dataframe()
    records = {
        (str(row["flight_id"]), int(row["win_id"])): row
        for row in calibrated.select("flight_id", "win_id", "p_value", "warm", "emit_ready").collect()
    }

    assert records[("F1", 1)]["warm"] is True
    assert records[("F1", 1)]["emit_ready"] is False
    assert float(records[("F1", 1)]["p_value"]) == 0.05

    assert records[("F1", 2)]["warm"] is True
    assert records[("F1", 2)]["emit_ready"] is True
    assert float(records[("F1", 2)]["p_value"]) == 0.10

    assert records[("F1", 3)]["warm"] is True
    assert records[("F1", 3)]["emit_ready"] is True
    assert float(records[("F1", 3)]["p_value"]) == 0.15

    assert records[("F1", 4)]["warm"] is True
    assert records[("F1", 4)]["emit_ready"] is False
    assert float(records[("F1", 4)]["p_value"]) == 0.20

    assert records[("F2", 101)]["warm"] is False
    assert records[("F2", 101)]["emit_ready"] is False
    assert records[("F2", 101)]["p_value"] is None


def test_window_scores_raw_table_prefers_concentrated_module_support_for_dominant_subsystem(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.0,
                "breadth": 0.1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {
                    "p_broad_a": 2.0,
                    "p_broad_b": 2.0,
                    "p_narrow": 3.0,
                },
                "date_utc": date(2025, 1, 1),
            }
        ]
    )
    phase_baselines_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "s_w_centroid": [0.0, 0.0],
                "reconstruction_median": 0.1,
                "reconstruction_mad": 0.1,
                "distance_median": 0.0,
                "distance_mad": 0.1,
            }
        ]
    )
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "p_broad_a", "subsystem_id": "SUB_BROAD", "module_id": "MOD_BROAD_A"},
            {"parameter_name": "p_broad_b", "subsystem_id": "SUB_BROAD", "module_id": "MOD_BROAD_B"},
            {"parameter_name": "p_narrow", "subsystem_id": "SUB_NARROW", "module_id": "MOD_NARROW"},
        ]
    )

    scores_df = WindowScoresRawTable.from_phase_dataframes(
        phase_windows_df,
        phase_baselines_df,
        hierarchy_sensor_map_df,
    ).to_dataframe()
    row = scores_df.select("dominant_subsystem_id", "dominant_module_id").collect()[0]

    assert row["dominant_subsystem_id"] == "SUB_NARROW"
    assert row["dominant_module_id"] == "MOD_NARROW"


def test_window_scores_raw_table_prefers_concentrated_parameter_support_within_module(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.0,
                "breadth": 0.1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {
                    "p_diffuse_a": 1.8,
                    "p_diffuse_b": 1.8,
                    "p_diffuse_c": 1.8,
                    "p_sharp": 2.1,
                },
                "date_utc": date(2025, 1, 1),
            }
        ]
    )
    phase_baselines_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "s_w_centroid": [0.0, 0.0],
                "reconstruction_median": 0.1,
                "reconstruction_mad": 0.1,
                "distance_median": 0.0,
                "distance_mad": 0.1,
            }
        ]
    )
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "p_diffuse_a", "subsystem_id": "SUB_DIFFUSE", "module_id": "MOD_DIFFUSE"},
            {"parameter_name": "p_diffuse_b", "subsystem_id": "SUB_DIFFUSE", "module_id": "MOD_DIFFUSE"},
            {"parameter_name": "p_diffuse_c", "subsystem_id": "SUB_DIFFUSE", "module_id": "MOD_DIFFUSE"},
            {"parameter_name": "p_sharp", "subsystem_id": "SUB_SHARP", "module_id": "MOD_SHARP"},
        ]
    )

    scores_df = WindowScoresRawTable.from_phase_dataframes(
        phase_windows_df,
        phase_baselines_df,
        hierarchy_sensor_map_df,
    ).to_dataframe()
    row = scores_df.select("dominant_subsystem_id", "dominant_module_id").collect()[0]

    assert row["dominant_subsystem_id"] == "SUB_SHARP"
    assert row["dominant_module_id"] == "MOD_SHARP"


def test_window_scores_raw_table_aligns_null_win_id_events_by_timestamp(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.0,
                "breadth": 0.1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"p1": 0.0},
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.0,
                "breadth": 0.1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"p1": 0.0},
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "phase_state_detected": "stable",
                "phase_id_detected": 0,
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.0,
                "breadth": 0.1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"p1": 2.0},
                "date_utc": date(2025, 1, 1),
            },
        ]
    )
    phase_baselines_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "s_w_centroid": [0.0, 0.0],
                "reconstruction_median": 0.1,
                "reconstruction_mad": 0.1,
                "distance_median": 0.0,
                "distance_mad": 0.1,
            }
        ]
    )
    hierarchy_sensor_map_df = spark.createDataFrame([{"parameter_name": "p1", "subsystem_id": "SUB1"}])
    windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "date_utc": date(2025, 1, 1),
                "t_start": datetime(2025, 1, 1, 0, 0, 0),
                "t_end": datetime(2025, 1, 1, 0, 0, 1),
                "duration_ms": 1000,
                "event_count": 0,
                "real_event_count": 0,
                "event_type_counts": {},
                "close_reason": "budget_threshold",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "date_utc": date(2025, 1, 1),
                "t_start": datetime(2025, 1, 1, 0, 0, 1),
                "t_end": datetime(2025, 1, 1, 0, 0, 2),
                "duration_ms": 1000,
                "event_count": 0,
                "real_event_count": 0,
                "event_type_counts": {},
                "close_reason": "budget_threshold",
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "date_utc": date(2025, 1, 1),
                "t_start": datetime(2025, 1, 1, 0, 0, 2),
                "t_end": datetime(2025, 1, 1, 0, 0, 3),
                "duration_ms": 1000,
                "event_count": 2,
                "real_event_count": 2,
                "event_type_counts": {"slope_pos": 2},
                "close_reason": "budget_threshold",
            },
        ]
    )
    events_df = spark.createDataFrame(
        [
            {
                "event_seq_id": 1,
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": None,
                "date_utc": date(2025, 1, 1),
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 2, 250000),
                "parameter_name": "p1",
                "event_type_detected": "slope_pos",
            },
            {
                "event_seq_id": 2,
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": None,
                "date_utc": date(2025, 1, 1),
                "timestamp_utc": datetime(2025, 1, 1, 0, 0, 2, 500000),
                "parameter_name": "p1",
                "event_type_detected": "slope_pos",
            },
        ]
        ,
        schema=T.StructType(
            [
                T.StructField("event_seq_id", T.IntegerType(), False),
                T.StructField("tail_id", T.StringType(), False),
                T.StructField("flight_id", T.StringType(), False),
                T.StructField("win_id", T.IntegerType(), True),
                T.StructField("date_utc", T.DateType(), True),
                T.StructField("timestamp_utc", T.TimestampType(), True),
                T.StructField("parameter_name", T.StringType(), True),
                T.StructField("event_type_detected", T.StringType(), True),
            ]
        ),
    )
    parameter_behavior_profile_df = spark.createDataFrame(
        [
            {
                "parameter_name": "p1",
                "regulated_score_profiled": 0.8,
                "tracking_score_profiled": 0.9,
                "inertial_score_profiled": 0.2,
                "accumulative_score_profiled": 0.0,
                "persistent_run_strength_profiled": 0.0,
                "run_reinforcement_score_profiled": 0.0,
                "discrete_state_score_profiled": 0.0,
                "excursion_rate_profiled": 0.0,
                "excursion_return_ratio_profiled": 0.0,
                "bound_occupancy_profiled": 0.0,
                "saturation_rate_profiled": 0.0,
                "monotone_accumulation_score_profiled": 0.0,
                "reset_drop_rate_profiled": 0.0,
                "oscillation_score_profiled": 0.4,
                "tracking_error_score_profiled": 0.8,
                "tracking_recovery_score_profiled": 0.6,
                "lagged_response_score_profiled": 0.7,
                "transition_rate_profiled": 0.0,
                "dominant_state_ratio_profiled": 0.0,
                "state_chatter_rate_profiled": 0.0,
            }
        ]
    )
    parameter_event_profile_df = spark.createDataFrame(
        [
            {
                "parameter_name": "p1",
                "drift_score_profiled": 0.1,
                "repeatability_score_profiled": 0.2,
                "smoothness_score_profiled": 0.3,
                "recommended_emit_threshold": False,
                "recommended_emit_oscillation": False,
            }
        ]
    )

    scores_df = WindowScoresRawTable.from_phase_dataframes(
        phase_windows_df,
        phase_baselines_df,
        hierarchy_sensor_map_df,
        windows_df=windows_df,
        events_df=events_df,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        parameter_event_profile_df=parameter_event_profile_df,
    ).to_dataframe()
    scores = {
        int(row["win_id"]): row["score_component_scores"]
        for row in scores_df.select("win_id", "score_component_scores").collect()
    }

    assert float(scores[3][EVENT_DISCORDANCE_CHANNEL]) > 0.0
    assert float(scores[3][RESPONSE_VIOLATION_CHANNEL]) > 0.0


def test_window_scores_raw_table_activates_accumulation_violation_for_accumulative_residual_windows(spark):
    phase_windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "s_w": [0.0, 0.0],
                "backbone_reconstruction_error": 0.1,
                "backbone_residual_by_parameter": {"p_ctx": 0.1},
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.9,
                "distance_to_centroid_detected": 0.1,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "s_w": [0.1, 0.0],
                "backbone_reconstruction_error": 0.2,
                "backbone_residual_by_parameter": {"p_ctx": 0.2},
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.8,
                "distance_to_centroid_detected": 0.2,
                "drift_magnitude": 0.1,
                "breadth": 0.2,
                "date_utc": date(2025, 1, 1),
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "s_w": [0.1, 0.1],
                "backbone_reconstruction_error": 2.0,
                "backbone_residual_by_parameter": {"p_accum": 2.0, "p_ctx": 0.1},
                "phase_id_detected": 0,
                "phase_state_detected": "stable",
                "phase_confidence_detected": 0.7,
                "distance_to_centroid_detected": 0.3,
                "drift_magnitude": 0.9,
                "breadth": 0.4,
                "date_utc": date(2025, 1, 1),
            },
        ]
    )
    phase_baselines_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "phase_id_detected": 0,
                "s_w_centroid": [0.0, 0.0],
                "reconstruction_median": 0.1,
                "reconstruction_mad": 0.1,
                "distance_median": 0.1,
                "distance_mad": 0.1,
            }
        ]
    )
    hierarchy_sensor_map_df = spark.createDataFrame(
        [
            {"parameter_name": "p_accum", "subsystem_id": "SUB1", "module_id": "MOD1"},
            {"parameter_name": "p_ctx", "subsystem_id": "SUB2", "module_id": "MOD2"},
        ]
    )
    windows_df = spark.createDataFrame(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": win_id,
                "date_utc": date(2025, 1, 1),
                "t_start": datetime(2025, 1, 1, 0, 0, win_id - 1),
                "t_end": datetime(2025, 1, 1, 0, 0, win_id),
                "duration_ms": 1000,
                "event_count": 0,
                "real_event_count": 0,
                "event_type_counts": {},
                "close_reason": "budget_threshold",
            }
            for win_id in (1, 2, 3)
        ],
        schema=T.StructType(
            [
                T.StructField("tail_id", T.StringType(), False),
                T.StructField("flight_id", T.StringType(), False),
                T.StructField("win_id", T.IntegerType(), False),
                T.StructField("date_utc", T.DateType(), True),
                T.StructField("t_start", T.TimestampType(), True),
                T.StructField("t_end", T.TimestampType(), True),
                T.StructField("duration_ms", T.LongType(), True),
                T.StructField("event_count", T.IntegerType(), True),
                T.StructField("real_event_count", T.IntegerType(), True),
                T.StructField("event_type_counts", T.MapType(T.StringType(), T.IntegerType(), True), True),
                T.StructField("close_reason", T.StringType(), True),
            ]
        ),
    )
    events_df = spark.createDataFrame(
        [],
        schema=T.StructType(
            [
                T.StructField("event_seq_id", T.IntegerType(), True),
                T.StructField("tail_id", T.StringType(), True),
                T.StructField("flight_id", T.StringType(), True),
                T.StructField("win_id", T.IntegerType(), True),
                T.StructField("date_utc", T.DateType(), True),
                T.StructField("timestamp_utc", T.TimestampType(), True),
                T.StructField("parameter_name", T.StringType(), True),
                T.StructField("event_type_detected", T.StringType(), True),
            ]
        ),
    )
    parameter_behavior_profile_df = spark.createDataFrame(
        [
            {
                "parameter_name": "p_accum",
                "persistent_run_strength_profiled": 0.9,
                "run_reinforcement_score_profiled": 0.8,
                "regulated_score_profiled": 0.1,
                "tracking_score_profiled": 0.1,
                "inertial_score_profiled": 0.1,
                "accumulative_score_profiled": 0.95,
                "discrete_state_score_profiled": 0.0,
                "excursion_rate_profiled": 0.0,
                "excursion_return_ratio_profiled": 0.0,
                "bound_occupancy_profiled": 0.0,
                "saturation_rate_profiled": 0.0,
                "monotone_accumulation_score_profiled": 0.9,
                "reset_drop_rate_profiled": 0.4,
                "oscillation_score_profiled": 0.0,
                "tracking_error_score_profiled": 0.0,
                "tracking_recovery_score_profiled": 0.0,
                "lagged_response_score_profiled": 0.0,
                "transition_rate_profiled": 0.0,
                "dominant_state_ratio_profiled": 0.0,
                "state_chatter_rate_profiled": 0.0,
            },
            {
                "parameter_name": "p_ctx",
                "persistent_run_strength_profiled": 0.1,
                "run_reinforcement_score_profiled": 0.1,
                "regulated_score_profiled": 0.2,
                "tracking_score_profiled": 0.2,
                "inertial_score_profiled": 0.2,
                "accumulative_score_profiled": 0.0,
                "discrete_state_score_profiled": 0.0,
                "excursion_rate_profiled": 0.0,
                "excursion_return_ratio_profiled": 0.0,
                "bound_occupancy_profiled": 0.0,
                "saturation_rate_profiled": 0.0,
                "monotone_accumulation_score_profiled": 0.0,
                "reset_drop_rate_profiled": 0.0,
                "oscillation_score_profiled": 0.0,
                "tracking_error_score_profiled": 0.0,
                "tracking_recovery_score_profiled": 0.0,
                "lagged_response_score_profiled": 0.0,
                "transition_rate_profiled": 0.0,
                "dominant_state_ratio_profiled": 0.0,
                "state_chatter_rate_profiled": 0.0,
            },
        ]
    )
    parameter_event_profile_df = spark.createDataFrame(
        [
            {
                "parameter_name": "p_accum",
                "drift_score_profiled": 0.9,
                "repeatability_score_profiled": 0.1,
                "smoothness_score_profiled": 0.2,
                "recommended_emit_threshold": False,
                "recommended_emit_oscillation": False,
            },
            {
                "parameter_name": "p_ctx",
                "drift_score_profiled": 0.0,
                "repeatability_score_profiled": 0.7,
                "smoothness_score_profiled": 0.7,
                "recommended_emit_threshold": False,
                "recommended_emit_oscillation": False,
            },
        ]
    )

    scores_df = WindowScoresRawTable.from_phase_dataframes(
        phase_windows_df,
        phase_baselines_df,
        hierarchy_sensor_map_df,
        windows_df=windows_df,
        events_df=events_df,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        parameter_event_profile_df=parameter_event_profile_df,
    ).to_dataframe()
    scores = {
        int(row["win_id"]): row["score_component_scores"]
        for row in scores_df.select("win_id", "score_component_scores").collect()
    }

    assert float(scores[3][ACCUMULATION_VIOLATION_CHANNEL]) > 0.0
