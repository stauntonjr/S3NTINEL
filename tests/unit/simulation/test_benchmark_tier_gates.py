from __future__ import annotations

import json
from pathlib import Path

from libs.simulation.benchmark_tier_gates import (
    BENCHMARK_DECISION_LEDGER_MARKDOWN_FILENAME,
    BENCHMARK_DECISION_LEDGER_SUMMARY_FILENAME,
    BENCHMARK_TIER_GATE_MARKDOWN_FILENAME,
    BENCHMARK_TIER_GATE_SUMMARY_FILENAME,
    build_benchmark_tier_gate_run_summary,
    build_benchmark_tier_gate_suite_summary,
    ordered_benchmark_tier_gate_specs,
    write_benchmark_decision_ledger_report,
    write_benchmark_tier_gate_suite_report,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def test_ordered_benchmark_tier_gate_specs_follow_ladder_order():
    specs = ordered_benchmark_tier_gate_specs()

    assert [spec.gate_name for spec in specs] == [
        "module_tier_drift",
        "subsystem_tier_bias",
    ]
    assert [spec.declared_benchmark_tier for spec in specs] == [
        "module_recoverable",
        "subsystem_recoverable",
    ]


def test_ordered_benchmark_tier_gate_specs_follow_parameter_suite_order():
    specs = ordered_benchmark_tier_gate_specs("parameter")

    assert [spec.gate_name for spec in specs] == [
        "parameter_tier_regulated_saturation",
        "parameter_tier_accumulative_drift",
        "parameter_tier_discrete_state_chatter",
        "parameter_tier_coupling_timing_jitter",
    ]
    assert [spec.declared_benchmark_tier for spec in specs] == [
        "parameter_visible_only",
        "parameter_visible_only",
        "parameter_visible_only",
        "parameter_visible_only",
    ]


def test_benchmark_tier_gate_suite_summary_writes_grouped_payload(tmp_path: Path):
    specs = ordered_benchmark_tier_gate_specs()
    module_run_dir = tmp_path / "runs" / "20260412T200000Z_power_pressurization_hierarchy_smoke_localization_focus_drift"
    subsystem_run_dir = tmp_path / "runs" / "20260412T200100Z_power_pressurization_hierarchy_smoke_localization_focus_bias"

    _write_json(
        module_run_dir / "reports" / "simulation_benchmark_audit_summary.json",
        {
            "fault_window_audit_cases": [
                {
                    "declared_target_alignment_status": "met_target",
                    "observed_recoverability_strength_tier": "module_recoverable",
                    "recommended_review_action": "keep_as_module_localization_benchmark",
                }
            ]
        },
    )
    _write_json(
        module_run_dir / "reports" / "score_validation_summary.json",
        {
            "detected_fault_window_rate": 1.0,
            "emit_ready_fault_window_rate": 1.0,
        },
    )
    _write_json(
        module_run_dir / "reports" / "attribution_validation_summary.json",
        {
            "telemetry_parameter_match_rate": 1.0,
            "telemetry_selected_parameter_match_rate": 1.0,
            "dominant_subsystem_match_rate": 0.0,
            "dominant_module_match_rate": 0.0,
            "top_subsystem_candidate_present_rate": 1.0,
            "top_module_candidate_present_rate": 1.0,
        },
    )
    _write_json(
        subsystem_run_dir / "reports" / "simulation_benchmark_audit_summary.json",
        {
            "fault_window_audit_cases": [
                {
                    "declared_target_alignment_status": "met_target",
                    "observed_recoverability_strength_tier": "subsystem_recoverable",
                    "recommended_review_action": "use_as_subsystem_benchmark_or_improve_module_separation",
                }
            ]
        },
    )
    _write_json(
        subsystem_run_dir / "reports" / "score_validation_summary.json",
        {
            "detected_fault_window_rate": 1.0,
            "emit_ready_fault_window_rate": 1.0,
        },
    )
    _write_json(
        subsystem_run_dir / "reports" / "attribution_validation_summary.json",
        {
            "telemetry_parameter_match_rate": 1.0,
            "telemetry_selected_parameter_match_rate": 0.0,
            "dominant_subsystem_match_rate": 1.0,
            "dominant_module_match_rate": 0.0,
            "top_subsystem_candidate_present_rate": 1.0,
            "top_module_candidate_present_rate": 0.0,
        },
    )

    gate_results = (
        build_benchmark_tier_gate_run_summary(
            gate_spec=specs[0],
            run_dir=module_run_dir,
            run_status="success",
        ),
        build_benchmark_tier_gate_run_summary(
            gate_spec=specs[1],
            run_dir=subsystem_run_dir,
            run_status="success",
        ),
    )
    summary = build_benchmark_tier_gate_suite_summary(
        suite_dir=tmp_path,
        gate_results=gate_results,
        suite_key="localization",
        gate_specs=specs,
    )
    payload = write_benchmark_tier_gate_suite_report(
        suite_dir=tmp_path,
        summary=summary,
    )

    assert payload["all_gates_met_or_exceeded"] is True
    assert payload["met_or_exceeded_gate_count"] == 2
    assert payload["gate_alignment_status_count"] == {"met_target": 2}
    assert [result["gate_name"] for result in payload["gate_results"]] == [
        "module_tier_drift",
        "subsystem_tier_bias",
    ]
    assert payload["suite_key"] == "localization"
    assert payload["gate_results"][1]["dominant_subsystem_match_rate"] == 1.0
    assert (tmp_path / "reports" / BENCHMARK_TIER_GATE_SUMMARY_FILENAME).exists()
    assert (tmp_path / "reports" / BENCHMARK_TIER_GATE_MARKDOWN_FILENAME).exists()


def test_benchmark_tier_gate_suite_writes_cross_run_decision_ledger(tmp_path: Path):
    specs = ordered_benchmark_tier_gate_specs()
    gate_results = tuple(
        build_benchmark_tier_gate_run_summary(
            gate_spec=spec,
            run_dir=None,
            run_status="success",
        )
        for spec in specs
    )
    summary = build_benchmark_tier_gate_suite_summary(
        suite_dir=tmp_path,
        gate_results=gate_results,
        suite_key="localization",
        gate_specs=specs,
    )
    composite_run_dir = tmp_path / "composite"
    _write_json(
        composite_run_dir / "reports" / "simulation_benchmark_audit_summary.json",
        {
            "fault_window_audit_cases": [
                {
                    "fault_window_id": "FW_DRIFT",
                    "fault_type": "drift",
                    "declared_benchmark_tier": "module_recoverable",
                    "observed_recoverability_strength_tier": "parameter_visible_only",
                    "declared_target_alignment_status": "missed_target",
                }
            ]
        },
    )
    _write_json(
        composite_run_dir / "reports" / "benchmark_tier_validation_summary.json",
        {
            "eligible_composite_window_failure_ledger": [
                {"fault_window_id": "FW_DRIFT", "first_failed_benchmark_scope": "module"}
            ]
        },
    )

    payload = write_benchmark_decision_ledger_report(
        suite_dir=tmp_path,
        summary=summary,
        composite_run_dir=composite_run_dir,
    )

    assert payload["entries"][0]["dedicated_references"][0]["gate_name"] == "module_tier_drift"
    assert payload["entries"][0]["recommended_decision"] == "review_truth_scope_or_structural_observability"
    assert (tmp_path / "reports" / BENCHMARK_DECISION_LEDGER_SUMMARY_FILENAME).exists()
    assert (tmp_path / "reports" / BENCHMARK_DECISION_LEDGER_MARKDOWN_FILENAME).exists()


def test_benchmark_tier_gate_suite_summary_writes_parameter_suite_payload(tmp_path: Path):
    specs = ordered_benchmark_tier_gate_specs("parameter")
    regulated_run_dir = tmp_path / "runs" / "20260412T200000Z_power_pressurization_hierarchy_smoke_parameter_focus_regulated"
    accum_run_dir = tmp_path / "runs" / "20260412T200100Z_power_pressurization_hierarchy_smoke_parameter_focus_accumulative"
    discrete_run_dir = tmp_path / "runs" / "20260412T200200Z_power_pressurization_hierarchy_smoke_parameter_focus_discrete"
    coupling_run_dir = tmp_path / "runs" / "20260412T200300Z_power_pressurization_hierarchy_smoke_parameter_focus_coupling"

    for run_dir, observed_tier in (
        (regulated_run_dir, "parameter_visible_only"),
        (accum_run_dir, "module_recoverable"),
        (discrete_run_dir, "parameter_visible_only"),
        (coupling_run_dir, "parameter_visible_only"),
    ):
        _write_json(
            run_dir / "reports" / "simulation_benchmark_audit_summary.json",
            {
                "fault_window_audit_cases": [
                    {
                        "declared_target_alignment_status": "met_target" if observed_tier == "parameter_visible_only" else "exceeded_target",
                        "observed_recoverability_strength_tier": observed_tier,
                        "recommended_review_action": "keep_parameter_tier_gate",
                    }
                ]
            },
        )
        _write_json(
            run_dir / "reports" / "score_validation_summary.json",
            {
                "detected_fault_window_rate": 1.0,
                "emit_ready_fault_window_rate": 1.0,
            },
        )
        _write_json(
            run_dir / "reports" / "attribution_validation_summary.json",
            {
                "telemetry_parameter_match_rate": 1.0,
                "telemetry_selected_parameter_match_rate": 0.75,
                "dominant_subsystem_match_rate": 0.0,
                "dominant_module_match_rate": 0.0,
                "top_subsystem_candidate_present_rate": 0.0,
                "top_module_candidate_present_rate": 0.0,
            },
        )

    gate_results = tuple(
        build_benchmark_tier_gate_run_summary(
            gate_spec=spec,
            run_dir=run_dir,
            run_status="success",
        )
        for spec, run_dir in zip(specs, (regulated_run_dir, accum_run_dir, discrete_run_dir, coupling_run_dir), strict=True)
    )
    summary = build_benchmark_tier_gate_suite_summary(
        suite_dir=tmp_path,
        gate_results=gate_results,
        suite_key="parameter",
        gate_specs=specs,
    )
    payload = write_benchmark_tier_gate_suite_report(
        suite_dir=tmp_path,
        summary=summary,
    )

    assert payload["suite_key"] == "parameter"
    assert payload["suite_name"] == "parameter_benchmark_tier_gates"
    assert payload["all_gates_met_or_exceeded"] is True
    assert payload["met_or_exceeded_gate_count"] == 4
    assert payload["gate_alignment_status_count"] == {"exceeded_target": 1, "met_target": 3}
    assert [result["gate_name"] for result in payload["gate_results"]] == [
        "parameter_tier_regulated_saturation",
        "parameter_tier_accumulative_drift",
        "parameter_tier_discrete_state_chatter",
        "parameter_tier_coupling_timing_jitter",
    ]
