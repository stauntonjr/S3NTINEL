from __future__ import annotations

import pandas as pd

import libs.simulation.reporting as reporting
from libs.simulation.fault.examples import build_misbehavior_program_spec, build_misbehavior_window_spec


def test_build_misbehavior_attribution_summary_uses_full_hierarchy_views(monkeypatch):
    captured: dict[str, pd.DataFrame] = {}

    def fake_validate_attribution_against_misbehavior_truth(**kwargs):
        captured["hierarchy_sensor_map_df"] = kwargs["hierarchy_sensor_map_df"]
        captured["hierarchy_label_df"] = kwargs["hierarchy_label_df"]
        return {"status": "ok"}

    monkeypatch.setattr(reporting, "validate_attribution_against_misbehavior_truth", fake_validate_attribution_against_misbehavior_truth)

    hierarchy_rows = pd.DataFrame(
        [
            {
                "parameter_name": "p1",
                "system_id": "SYS_1",
                "subsystem_id": "SUB_1",
                "module_id": "MOD_1",
            }
        ]
    )

    class FakeTables:
        def pandas(self, view):
            if view is reporting.HIERARCHY_SENSOR_MAP_VIEW:
                return hierarchy_rows.copy()
            if view is reporting.HIERARCHY_LABEL_VIEW:
                return hierarchy_rows.copy()
            return pd.DataFrame()

    summary = reporting._build_misbehavior_attribution_summary(FakeTables())

    assert summary == {"status": "ok"}
    assert list(captured["hierarchy_sensor_map_df"].columns) == ["parameter_name", "system_id", "subsystem_id", "module_id"]
    assert list(captured["hierarchy_label_df"].columns) == ["parameter_name", "system_id", "subsystem_id", "module_id"]


def test_hierarchy_edge_evidence_summary_normalizes_to_expected_coupling_direction():
    evidence_rows = pd.DataFrame(
        [
            {
                "parameter_name_u": "actuator_position_pct",
                "parameter_name_v": "outflow_cmd_pct",
                "module_id": "MOD_0001",
                "lag_count_u_to_v": 2,
                "lag_weight_u_to_v": 0.25,
                "mean_lag_seconds_u_to_v": 1.5,
                "lag_count_v_to_u": None,
                "lag_weight_v_to_u": float("nan"),
                "mean_lag_seconds_v_to_u": float("nan"),
            }
        ]
    )

    class FakeTables:
        def pandas(self, view):
            assert view is reporting.HIERARCHY_EDGE_EVIDENCE_VIEW
            return evidence_rows.copy()

    summary = reporting._build_hierarchy_edge_evidence_summary(
        FakeTables(),
        expected_coupling_signatures=(
            {
                "coupling_id": "coupling-1",
                "parameter_name_u": "outflow_cmd_pct",
                "parameter_name_v": "actuator_position_pct",
            },
        ),
    )

    assert summary["status"] == "ok"
    assert summary["expected_coupling_retained_edge_count"] == 1
    assert summary["expected_coupling_matches"] == [
        {
            "coupling_id": "coupling-1",
            "parameter_name_u": "outflow_cmd_pct",
            "parameter_name_v": "actuator_position_pct",
            "retained_in_hierarchy": True,
            "module_id": "MOD_0001",
            "lag_count_u_to_v": None,
            "lag_weight_u_to_v": None,
            "mean_lag_seconds_u_to_v": None,
            "lag_count_v_to_u": 2,
            "lag_weight_v_to_u": 0.25,
            "mean_lag_seconds_v_to_u": 1.5,
        }
    ]


def test_build_simulation_benchmark_audit_summary_classifies_recoverability_tiers():
    class FakeFlight:
        metadata = {"flight_name": "power_chain"}
        misbehavior_program_spec = build_misbehavior_program_spec(
            windows=(
                build_misbehavior_window_spec(
                    module_id="MOD_A",
                    parameter_name="P_A",
                    start_step=0,
                    end_step_exclusive=1,
                    context={"violation_type": "lag_increase"},
                    metadata={
                        "fault_window_id": "FW1",
                        "fault_family_label": "regulated",
                        "benchmark_recoverability_target": "subsystem_recoverable",
                    },
                ),
                build_misbehavior_window_spec(
                    module_id="MOD_B",
                    parameter_name="P_B",
                    start_step=1,
                    end_step_exclusive=2,
                    context={"violation_type": "shared_disturbance"},
                    metadata={
                        "fault_window_id": "FW2",
                        "fault_family_label": "regulated",
                        "benchmark_recoverability_target": "parameter_visible_only",
                    },
                ),
                build_misbehavior_window_spec(
                    module_id="MOD_C",
                    parameter_name="P_C",
                    start_step=2,
                    end_step_exclusive=3,
                    context={"violation_type": "forbidden_transition"},
                    metadata={
                        "fault_window_id": "FW3",
                        "fault_family_label": "discrete_state",
                        "benchmark_recoverability_target": "detection_only",
                    },
                ),
            )
        )

    score_summary = {
        "status": "ok",
        "fault_windows": [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "fault_window_id": "FW1",
                "fault_family_label": "regulated",
                "fault_type": "lag_increase",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_A",
                "parameter_name": "P_A",
                "detected_window_count": 1,
                "emit_ready_window_count": 1,
                "detection_latency_seconds": 2.0,
                "emit_ready_latency_seconds": 3.0,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "fault_window_id": "FW2",
                "fault_family_label": "regulated",
                "fault_type": "shared_disturbance",
                "subsystem_id": "SUB_B",
                "module_id": "MOD_B",
                "parameter_name": "P_B",
                "detected_window_count": 1,
                "emit_ready_window_count": 0,
                "detection_latency_seconds": 1.0,
                "emit_ready_latency_seconds": None,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "fault_window_id": "FW3",
                "fault_family_label": "discrete_state",
                "fault_type": "forbidden_transition",
                "subsystem_id": "SUB_C",
                "module_id": "MOD_C",
                "parameter_name": "P_C",
                "detected_window_count": 0,
                "emit_ready_window_count": 0,
                "detection_latency_seconds": None,
                "emit_ready_latency_seconds": None,
            },
        ],
    }
    attribution_summary = {
        "status": "ok",
        "fault_windows": [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "fault_window_id": "FW1",
                "dominant_score_component": "event_discordance",
                "telemetry_parameter_match": True,
                "telemetry_selected_parameter_match": True,
                "event_parameter_match": True,
                "dominant_subsystem_match": False,
                "dominant_module_match": False,
                "top_subsystem_candidate_present": True,
                "top_module_candidate_present": True,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "fault_window_id": "FW2",
                "dominant_score_component": "reconstruction_error",
                "telemetry_parameter_match": True,
                "telemetry_selected_parameter_match": False,
                "event_parameter_match": False,
                "dominant_subsystem_match": False,
                "dominant_module_match": False,
                "top_subsystem_candidate_present": False,
                "top_module_candidate_present": False,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "fault_window_id": "FW3",
                "dominant_score_component": "",
                "telemetry_parameter_match": False,
                "telemetry_selected_parameter_match": False,
                "event_parameter_match": False,
                "dominant_subsystem_match": False,
                "dominant_module_match": False,
                "top_subsystem_candidate_present": False,
                "top_module_candidate_present": False,
            },
        ],
    }

    summary = reporting._build_simulation_benchmark_audit_summary(
        flight=FakeFlight(),
        fault_score_summary=score_summary,
        fault_attribution_summary=attribution_summary,
    )

    assert summary["status"] == "ok"
    assert summary["flight_name"] == "power_chain"
    assert summary["observed_recoverability_strength_tier_count"] == {
        "module_recoverable": 1,
        "subsystem_recoverable": 0,
        "parameter_visible_only": 1,
        "detection_only": 0,
        "undetected": 1,
    }
    assert summary["benchmark_review_priority_count"] == {
        "critical": 1,
        "high": 1,
        "low": 1,
    }
    assert summary["declared_benchmark_tier_count"] == {
        "detection_only": 1,
        "parameter_visible_only": 1,
        "subsystem_recoverable": 1,
    }
    assert summary["benchmark_tier_alignment_status_count"] == {
        "missed_target": 1,
        "met_target": 1,
        "exceeded_target": 1,
    }
    assert list(summary["benchmark_tier_scorecards"]) == [
        "detection_only",
        "parameter_visible_only",
        "module_recoverable",
        "subsystem_recoverable",
    ]
    assert summary["benchmark_tier_scorecards"]["detection_only"] == {
        "fault_window_count": 1,
        "detected_fault_window_count": 0,
        "emit_ready_fault_window_count": 0,
        "detected_fault_window_rate": 0.0,
        "emit_ready_fault_window_rate": 0.0,
        "telemetry_parameter_match_rate": 0.0,
        "telemetry_selected_parameter_match_rate": 0.0,
        "event_parameter_match_rate": 0.0,
        "dominant_subsystem_match_rate": 0.0,
        "dominant_module_match_rate": 0.0,
        "top_subsystem_candidate_present_rate": 0.0,
        "top_module_candidate_present_rate": 0.0,
        "module_recoverable_exact_rate": 0.0,
        "subsystem_or_better_rate": 0.0,
        "parameter_or_better_rate": 0.0,
        "observed_recoverability_strength_tier_count": {
            "undetected": 1,
            "detection_only": 0,
            "parameter_visible_only": 0,
            "subsystem_recoverable": 0,
            "module_recoverable": 0,
        },
        "benchmark_tier_alignment_status_count": {
            "missed_target": 1,
            "undeclared": 0,
            "met_target": 0,
            "exceeded_target": 0,
        },
        "dominant_score_component_count": {"unassigned": 1},
        "benchmark_tier_met_or_exceeded_rate": 0.0,
    }
    assert summary["benchmark_tier_scorecards"]["parameter_visible_only"]["parameter_or_better_rate"] == 1.0
    assert summary["benchmark_tier_scorecards"]["parameter_visible_only"]["benchmark_tier_met_or_exceeded_rate"] == 1.0
    assert summary["benchmark_tier_scorecards"]["module_recoverable"]["fault_window_count"] == 0
    assert summary["benchmark_tier_scorecards"]["module_recoverable"]["detected_fault_window_rate"] is None
    assert summary["benchmark_tier_scorecards"]["subsystem_recoverable"]["subsystem_or_better_rate"] == 1.0
    assert summary["benchmark_tier_scorecards"]["subsystem_recoverable"]["top_module_candidate_present_rate"] == 1.0
    assert summary["top_review_candidates"][0]["fault_type"] == "forbidden_transition"
    assert [row["observed_recoverability_strength_tier"] for row in summary["fault_window_audit_cases"]] == [
        "undetected",
        "parameter_visible_only",
        "module_recoverable",
    ]


def test_build_benchmark_scope_validation_summary_filters_denominators_by_declared_tier():
    summary = reporting._build_benchmark_scope_validation_summary(
        simulation_benchmark_audit_summary={
            "status": "ok",
            "fault_window_audit_cases": [
                {
                    "fault_window_id": "FW1",
                    "declared_benchmark_tier": "subsystem_recoverable",
                    "detected": True,
                    "emit_ready": True,
                    "detection_latency_seconds": 2.0,
                    "emit_ready_latency_seconds": 3.0,
                    "dominant_score_component": "event_discordance",
                    "telemetry_parameter_match": True,
                    "telemetry_selected_parameter_match": True,
                    "event_parameter_match": True,
                    "dominant_subsystem_match": False,
                    "dominant_module_match": False,
                    "top_subsystem_candidate_present": True,
                    "top_module_candidate_present": True,
                },
                {
                    "fault_window_id": "FW2",
                    "declared_benchmark_tier": "parameter_visible_only",
                    "detected": True,
                    "emit_ready": False,
                    "detection_latency_seconds": 1.0,
                    "emit_ready_latency_seconds": None,
                    "dominant_score_component": "reconstruction_error",
                    "telemetry_parameter_match": True,
                    "telemetry_selected_parameter_match": False,
                    "event_parameter_match": False,
                    "dominant_subsystem_match": False,
                    "dominant_module_match": False,
                    "top_subsystem_candidate_present": False,
                    "top_module_candidate_present": False,
                },
                {
                    "fault_window_id": "FW3",
                    "declared_benchmark_tier": "detection_only",
                    "detected": False,
                    "emit_ready": False,
                    "detection_latency_seconds": None,
                    "emit_ready_latency_seconds": None,
                    "dominant_score_component": "",
                    "telemetry_parameter_match": False,
                    "telemetry_selected_parameter_match": False,
                    "event_parameter_match": False,
                    "dominant_subsystem_match": False,
                    "dominant_module_match": False,
                    "top_subsystem_candidate_present": False,
                    "top_module_candidate_present": False,
                },
            ],
        }
    )

    detection = summary["score_validation_by_benchmark_scope"]["detection"]
    parameter = summary["attribution_validation_by_benchmark_scope"]["parameter"]
    module = summary["attribution_validation_by_benchmark_scope"]["module"]
    subsystem = summary["attribution_validation_by_benchmark_scope"]["subsystem"]

    assert summary["benchmark_scope_order"] == ["detection", "parameter", "module", "subsystem"]
    assert detection["eligible_fault_window_count"] == 3
    assert detection["detected_fault_window_rate"] == 2.0 / 3.0
    assert detection["emit_ready_fault_window_rate"] == 1.0 / 3.0
    assert detection["median_detection_latency_seconds"] == 1.5
    assert parameter["eligible_fault_window_count"] == 2
    assert parameter["excluded_declared_benchmark_tier_count"] == {"detection_only": 1}
    assert parameter["telemetry_parameter_match_rate"] == 1.0
    assert parameter["telemetry_selected_parameter_match_rate"] == 0.5
    assert parameter["event_parameter_match_rate"] == 0.5
    assert parameter["top_subsystem_candidate_present_rate"] == 0.5
    assert parameter["top_module_candidate_present_rate"] == 0.5
    assert module["eligible_fault_window_count"] == 0
    assert module["dominant_module_match_rate"] is None
    assert subsystem["eligible_fault_window_count"] == 1
    assert subsystem["top_subsystem_candidate_present_rate"] == 1.0
    assert subsystem["top_module_candidate_present_rate"] == 1.0
    assert summary["recommended_objective_metric_paths"]["module"] == [
        "attribution_validation_by_benchmark_scope.module.dominant_module_match_rate",
        "attribution_validation_by_benchmark_scope.module.top_module_candidate_present_rate",
    ]


def test_build_benchmark_tier_validation_summary_emits_tier_scorecards_and_eligible_window_ledger():
    summary = reporting._build_benchmark_tier_validation_summary(
        simulation_benchmark_audit_summary={
            "status": "ok",
            "fault_window_audit_cases": [
                {
                    "tail_id": "T1",
                    "flight_id": "F1",
                    "fault_window_id": "FW_D",
                    "fault_family_label": "inertial",
                    "fault_type": "timing_lag",
                    "declared_benchmark_tier": "detection_only",
                    "observed_recoverability_strength_tier": "undetected",
                    "declared_target_alignment_status": "missed_target",
                    "detected": False,
                    "emit_ready": False,
                    "detection_latency_seconds": None,
                    "emit_ready_latency_seconds": None,
                    "dominant_score_component": "unassigned",
                    "telemetry_parameter_match": False,
                    "telemetry_selected_parameter_match": False,
                    "event_parameter_match": False,
                    "dominant_subsystem_match": False,
                    "dominant_module_match": False,
                    "top_subsystem_candidate_present": False,
                    "top_module_candidate_present": False,
                },
                {
                    "tail_id": "T1",
                    "flight_id": "F1",
                    "fault_window_id": "FW_P",
                    "fault_family_label": "regulated",
                    "fault_type": "saturation",
                    "declared_benchmark_tier": "parameter_visible_only",
                    "observed_recoverability_strength_tier": "detection_only",
                    "declared_target_alignment_status": "missed_target",
                    "detected": True,
                    "emit_ready": True,
                    "detection_latency_seconds": 1.0,
                    "emit_ready_latency_seconds": 2.0,
                    "dominant_score_component": "reconstruction_error",
                    "telemetry_parameter_match": False,
                    "telemetry_selected_parameter_match": False,
                    "event_parameter_match": False,
                    "dominant_subsystem_match": False,
                    "dominant_module_match": False,
                    "top_subsystem_candidate_present": False,
                    "top_module_candidate_present": False,
                },
                {
                    "tail_id": "T1",
                    "flight_id": "F1",
                    "fault_window_id": "FW_S",
                    "fault_family_label": "regulated",
                    "fault_type": "bias",
                    "declared_benchmark_tier": "subsystem_recoverable",
                    "observed_recoverability_strength_tier": "detection_only",
                    "declared_target_alignment_status": "missed_target",
                    "detected": True,
                    "emit_ready": True,
                    "detection_latency_seconds": 1.5,
                    "emit_ready_latency_seconds": 2.5,
                    "dominant_score_component": "reconstruction_error",
                    "telemetry_parameter_match": True,
                    "telemetry_selected_parameter_match": True,
                    "event_parameter_match": False,
                    "dominant_subsystem_match": False,
                    "dominant_module_match": False,
                    "top_subsystem_candidate_present": False,
                    "top_module_candidate_present": True,
                },
                {
                    "tail_id": "T1",
                    "flight_id": "F1",
                    "fault_window_id": "FW_M",
                    "fault_family_label": "accumulative",
                    "fault_type": "drift",
                    "declared_benchmark_tier": "module_recoverable",
                    "observed_recoverability_strength_tier": "subsystem_recoverable",
                    "declared_target_alignment_status": "missed_target",
                    "detected": True,
                    "emit_ready": True,
                    "detection_latency_seconds": 0.5,
                    "emit_ready_latency_seconds": 1.0,
                    "dominant_score_component": "reconstruction_error",
                    "telemetry_parameter_match": True,
                    "telemetry_selected_parameter_match": True,
                    "event_parameter_match": False,
                    "dominant_subsystem_match": True,
                    "dominant_module_match": False,
                    "top_subsystem_candidate_present": True,
                    "top_module_candidate_present": False,
                },
            ],
        },
        fault_attribution_summary={
            "status": "ok",
            "fault_windows": [
                {
                    "tail_id": "T1",
                    "flight_id": "F1",
                    "fault_window_id": "FW_P",
                    "reconstruction_failure_bucket": "missing_truth_local_candidate",
                    "top_ranked_selected_parameter_name": "P_PARAM",
                    "top_ranked_selected_parameter_rank": 1,
                    "top_ranked_selected_parameter_support": 0.2,
                    "telemetry_selected_attributed_parameter_names": ["P_PARAM"],
                    "top_subsystem_candidate_ids_detected": ["SUB_P"],
                    "top_module_candidate_ids_detected": ["MOD_P"],
                    "matched_attribution_window_count": 1,
                    "overlapping_window_count": 2,
                },
                {
                    "tail_id": "T1",
                    "flight_id": "F1",
                    "fault_window_id": "FW_S",
                    "reconstruction_failure_bucket": "shared_source_won",
                    "top_ranked_selected_parameter_name": "S_PARAM",
                    "top_ranked_selected_parameter_rank": 1,
                    "top_ranked_selected_parameter_support": 0.4,
                    "telemetry_selected_attributed_parameter_names": ["S_PARAM"],
                    "top_subsystem_candidate_ids_detected": ["SUB_S"],
                    "top_subsystem_candidate_truth_ids": [],
                    "top_module_candidate_ids_detected": ["MOD_S"],
                    "top_module_candidate_truth_ids": ["MOD_S"],
                    "matched_attribution_window_count": 2,
                    "overlapping_window_count": 3,
                },
                {
                    "tail_id": "T1",
                    "flight_id": "F1",
                    "fault_window_id": "FW_M",
                    "reconstruction_failure_bucket": "truth_module_present_but_lost",
                    "top_ranked_selected_parameter_name": "M_PARAM",
                    "top_ranked_selected_parameter_rank": 1,
                    "top_ranked_selected_parameter_support": 0.8,
                    "telemetry_selected_attributed_parameter_names": ["M_PARAM"],
                    "top_subsystem_candidate_ids_detected": ["SUB_M"],
                    "top_subsystem_candidate_truth_ids": ["SUB_M"],
                    "top_module_candidate_ids_detected": ["MOD_M"],
                    "top_module_candidate_truth_ids": ["MOD_M"],
                    "matched_attribution_window_count": 3,
                    "overlapping_window_count": 4,
                },
            ],
        },
    )

    assert summary["status"] == "ok"
    assert list(summary["score_validation_by_benchmark_tier"]) == [
        "detection_only",
        "parameter_visible_only",
        "module_recoverable",
        "subsystem_recoverable",
    ]
    assert summary["score_validation_by_benchmark_tier"]["subsystem_recoverable"]["emit_ready_fault_window_rate"] == 1.0
    assert summary["attribution_validation_by_benchmark_tier"]["module_recoverable"]["top_module_candidate_present_rate"] == 0.0
    assert summary["eligible_composite_fault_window_count"] == 3
    assert summary["eligible_composite_declared_benchmark_tier_count"] == {
        "module_recoverable": 1,
        "parameter_visible_only": 1,
        "subsystem_recoverable": 1,
    }
    assert summary["eligible_composite_first_failed_benchmark_scope_count"] == {
        "parameter": 1,
        "subsystem": 1,
        "module": 1,
    }
    assert summary["eligible_composite_candidate_rollup_consistency_violation_count"] == 1
    assert summary["eligible_composite_top_subsystem_truth_mapping_gap_count"] == 2
    assert summary["eligible_composite_top_module_truth_mapping_gap_count"] == 1
    assert summary["eligible_composite_failure_summary_by_fault_family"][0]["fault_family_label"] == "regulated"
    ledger = summary["eligible_composite_window_failure_ledger"]
    assert [row["fault_window_id"] for row in ledger] == ["FW_P", "FW_S", "FW_M"]
    assert ledger[0]["first_failed_benchmark_scope"] == "parameter"
    assert ledger[1]["first_failed_benchmark_scope"] == "subsystem"
    assert ledger[1]["reconstruction_failure_bucket"] == "shared_source_won"
    assert ledger[1]["candidate_rollup_consistency_violation"] is True
    assert ledger[1]["top_subsystem_truth_mapping_gap"] is True
    assert ledger[1]["top_module_truth_mapping_gap"] is False
    assert ledger[1]["top_subsystem_candidate_truth_ids"] == []
    assert ledger[1]["top_module_candidate_truth_ids"] == ["MOD_S"]
    assert ledger[2]["first_failed_benchmark_scope"] == "module"
