from __future__ import annotations

from libs.simulation.benchmark_decision_ledger import (
    BenchmarkDecisionReference,
    build_benchmark_decision_ledger,
    render_benchmark_decision_ledger_markdown,
)


def test_benchmark_decision_ledger_selects_review_paths_from_composite_and_clean_evidence():
    ledger = build_benchmark_decision_ledger(
        composite_run_dir="data/simulation_runs/composite",
        reference_suite_dir="data/simulation_gate_runs/localization",
        simulation_benchmark_audit_summary={
            "fault_window_audit_cases": [
                {
                    "fault_window_id": "FW_TIMING",
                    "fault_type": "timing_jitter",
                    "declared_benchmark_tier": "module_recoverable",
                    "observed_recoverability_strength_tier": "detection_only",
                    "declared_target_alignment_status": "missed_target",
                },
                {
                    "fault_window_id": "FW_BIAS",
                    "fault_type": "bias",
                    "declared_benchmark_tier": "module_recoverable",
                    "observed_recoverability_strength_tier": "parameter_visible_only",
                    "declared_target_alignment_status": "missed_target",
                },
                {
                    "fault_window_id": "FW_DRIFT",
                    "fault_type": "drift",
                    "declared_benchmark_tier": "module_recoverable",
                    "observed_recoverability_strength_tier": "parameter_visible_only",
                    "declared_target_alignment_status": "missed_target",
                },
                {
                    "fault_window_id": "FW_CHATTER",
                    "fault_type": "state_chatter",
                    "declared_benchmark_tier": "module_recoverable",
                    "observed_recoverability_strength_tier": "module_recoverable",
                    "declared_target_alignment_status": "met_target",
                },
            ]
        },
        benchmark_tier_validation_summary={
            "eligible_composite_window_failure_ledger": [
                {"fault_window_id": "FW_BIAS", "first_failed_benchmark_scope": "module"},
                {"fault_window_id": "FW_DRIFT", "first_failed_benchmark_scope": "module"},
                {"fault_window_id": "FW_TIMING", "first_failed_benchmark_scope": "detection"},
            ]
        },
        references=(
            BenchmarkDecisionReference(
                fault_type="bias",
                gate_name="subsystem_tier_bias",
                flight_name="bias_smoke",
                declared_benchmark_tier="subsystem_recoverable",
                run_status="success",
                declared_target_alignment_status="met_target",
                observed_recoverability_strength_tier="subsystem_recoverable",
                recommended_review_action=None,
                run_dir="data/simulation_gate_runs/bias",
            ),
            BenchmarkDecisionReference(
                fault_type="drift",
                gate_name="module_tier_drift",
                flight_name="drift_smoke",
                declared_benchmark_tier="module_recoverable",
                run_status="success",
                declared_target_alignment_status="met_target",
                observed_recoverability_strength_tier="module_recoverable",
                recommended_review_action=None,
                run_dir="data/simulation_gate_runs/drift",
            ),
        ),
    )

    payload = ledger.to_payload()

    assert [entry["fault_window_id"] for entry in payload["entries"]] == [
        "FW_BIAS",
        "FW_CHATTER",
        "FW_DRIFT",
        "FW_TIMING",
    ]
    decisions = {entry["fault_window_id"]: entry["recommended_decision"] for entry in payload["entries"]}
    assert decisions == {
        "FW_BIAS": "review_lower_target_or_module_separation",
        "FW_CHATTER": "retain_target",
        "FW_DRIFT": "formulate_downstream_model_hypothesis",
        "FW_TIMING": "review_signal_observability_and_fault_design",
    }
    assert payload["requires_human_review_count"] == 3
    assert "### FW_DRIFT" in render_benchmark_decision_ledger_markdown(ledger)
