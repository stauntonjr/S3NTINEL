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
    assert [row["declared_target_alignment_status"] for row in summary["fault_window_audit_cases"]] == [
        "missed_target",
        "met_target",
        "exceeded_target",
    ]
