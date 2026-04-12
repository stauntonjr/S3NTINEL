from libs.simulation.flight import build_named_flight_spec
from libs.simulation.flight.examples import build_legacy_power_pressurization_hierarchy_reference_flight_spec
from libs.simulation.validation_harness import _flatten_numeric_metric_records, _summarize_misbehavior_program


def test_flatten_numeric_metric_records_includes_event_family_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "event_family_metrics": {
                "slope_pos": {
                    "f1": 0.12,
                    "precision": 0.25,
                },
                "transition": {
                    "f1": 1.0,
                },
            }
        },
        category="validation",
        scope_name="overall",
        subscope_name="event_validation",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("event_family_metrics.slope_pos.f1", 0.12),
        ("event_family_metrics.slope_pos.precision", 0.25),
        ("event_family_metrics.transition.f1", 1.0),
    ]


def test_flatten_numeric_metric_records_includes_slope_label_contract_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "slope_label_contract_metrics": {
                "families": {
                    "slope_pos": {
                        "repeated_same_run_label_fraction": 0.66,
                        "median_repeated_label_spacing_seconds": 2.0,
                    }
                }
            }
        },
        category="validation",
        scope_name="overall",
        subscope_name="event_validation",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("slope_label_contract_metrics.families.slope_pos.median_repeated_label_spacing_seconds", 2.0),
        ("slope_label_contract_metrics.families.slope_pos.repeated_same_run_label_fraction", 0.66),
    ]


def test_flatten_numeric_metric_records_includes_slope_run_capture_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "slope_run_capture_metrics": {
                "slope_pos": {
                    "run_recall": 0.5,
                    "detections_outside_truth_runs_count": 3,
                }
            }
        },
        category="validation",
        scope_name="overall",
        subscope_name="event_validation",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("slope_run_capture_metrics.slope_pos.detections_outside_truth_runs_count", 3),
        ("slope_run_capture_metrics.slope_pos.run_recall", 0.5),
    ]


def test_flatten_numeric_metric_records_includes_window_policy_profile_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "edge_stability": {
                "mean_boundary_jaccard": 0.72,
            },
            "selected_balance_penalty": 0.18,
            "closure_mix": {
                "budget_threshold_rate": 0.22,
            },
            "downstream_cost_proxy": {
                "pair_cost_proxy": 24.0,
                "same_window_pair_expansion_proxy": 15.0,
            },
            "truth_phase_window_supply": {
                "cruise_majority_window_count": 6,
                "target_phase_real_event_count": {
                    "mean": 0.5,
                },
                "target_phase_duration_ms": {
                    "mean": 4200.0,
                    "p95": 5000.0,
                },
            },
        },
        category="validation",
        scope_name="overall",
        subscope_name="window_policy_profile",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("closure_mix.budget_threshold_rate", 0.22),
        ("downstream_cost_proxy.pair_cost_proxy", 24.0),
        ("downstream_cost_proxy.same_window_pair_expansion_proxy", 15.0),
        ("edge_stability.mean_boundary_jaccard", 0.72),
        ("selected_balance_penalty", 0.18),
        ("truth_phase_window_supply.cruise_majority_window_count", 6),
        ("truth_phase_window_supply.target_phase_duration_ms.mean", 4200.0),
        ("truth_phase_window_supply.target_phase_duration_ms.p95", 5000.0),
        ("truth_phase_window_supply.target_phase_real_event_count.mean", 0.5),
    ]


def test_flatten_numeric_metric_records_includes_score_diagnostic_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "raw_score_validation": {
                "window_count": 307,
                "window_count_by_truth_overlap_bucket": {
                    "strict_overlap": 2,
                    "soft_overlap": 14,
                },
                "truth_window_recall_by_top_k_raw_score": {
                    "any_overlap": {
                        "top_5": 0.5,
                    }
                },
            },
            "calibrated_score_validation": {
                "truth_window_recall_by_top_k_calibrated_rarity": {
                    "strict_overlap": {
                        "top_10": 0.25,
                    }
                }
            },
            "emission_validation": {
                "blocked_candidate_window_count_by_p_value_threshold": {
                    "p_le_0p05": 3,
                },
                "emit_ready_rate_by_top_k_calibrated_rarity": {
                    "top_10": 0.9,
                },
            },
        },
        category="validation",
        scope_name="overall",
        subscope_name="score_validation",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("calibrated_score_validation.truth_window_recall_by_top_k_calibrated_rarity.strict_overlap.top_10", 0.25),
        ("emission_validation.blocked_candidate_window_count_by_p_value_threshold.p_le_0p05", 3),
        ("emission_validation.emit_ready_rate_by_top_k_calibrated_rarity.top_10", 0.9),
        ("raw_score_validation.truth_window_recall_by_top_k_raw_score.any_overlap.top_5", 0.5),
        ("raw_score_validation.window_count", 307),
        ("raw_score_validation.window_count_by_truth_overlap_bucket.soft_overlap", 14),
        ("raw_score_validation.window_count_by_truth_overlap_bucket.strict_overlap", 2),
    ]


def test_flatten_numeric_metric_records_includes_parameter_localization_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "channel_localization_validation": {
                "dominant_subsystem_match_rate_by_score_component": {
                    "reconstruction_error": 0.5,
                },
                "top_module_candidate_present_rate_by_score_component": {
                    "reconstruction_error": 0.75,
                },
                "telemetry_selected_parameter_match_rate_by_score_component": {
                    "event_discordance": 0.75,
                },
            },
            "module_localization_validation": {
                "dominant_module_match_rate": 0.5,
                "top_module_candidate_present_rate": 0.75,
                "truth_module_present_count_by_source": {
                    "telemetry": 3,
                },
            },
            "parameter_localization_validation": {
                "exact_parameter_match_rate_by_source": {
                    "telemetry": 0.8,
                    "telemetry_selected": 0.6,
                    "event": 0.25,
                    "any": 0.9,
                },
                "truth_subsystem_present_count_by_source": {
                    "telemetry": 4,
                },
            },
            "reconstruction_localization_validation": {
                "reconstruction_truth_window_count": 4,
                "reconstruction_failure_count": 3,
                "failure_count_by_bucket": {
                    "missing_truth_local_candidate": 1,
                    "shared_source_won": 2,
                },
                "top_ranked_selected_parameter_in_truth_subsystem_rate": 0.25,
            },
        },
        category="validation",
        scope_name="overall",
        subscope_name="attribution_validation",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("channel_localization_validation.dominant_subsystem_match_rate_by_score_component.reconstruction_error", 0.5),
        ("channel_localization_validation.telemetry_selected_parameter_match_rate_by_score_component.event_discordance", 0.75),
        ("channel_localization_validation.top_module_candidate_present_rate_by_score_component.reconstruction_error", 0.75),
        ("module_localization_validation.dominant_module_match_rate", 0.5),
        ("module_localization_validation.top_module_candidate_present_rate", 0.75),
        ("module_localization_validation.truth_module_present_count_by_source.telemetry", 3),
        ("parameter_localization_validation.exact_parameter_match_rate_by_source.any", 0.9),
        ("parameter_localization_validation.exact_parameter_match_rate_by_source.event", 0.25),
        ("parameter_localization_validation.exact_parameter_match_rate_by_source.telemetry", 0.8),
        ("parameter_localization_validation.exact_parameter_match_rate_by_source.telemetry_selected", 0.6),
        ("parameter_localization_validation.truth_subsystem_present_count_by_source.telemetry", 4),
        ("reconstruction_localization_validation.failure_count_by_bucket.missing_truth_local_candidate", 1),
        ("reconstruction_localization_validation.failure_count_by_bucket.shared_source_won", 2),
        ("reconstruction_localization_validation.reconstruction_failure_count", 3),
        ("reconstruction_localization_validation.reconstruction_truth_window_count", 4),
        ("reconstruction_localization_validation.top_ranked_selected_parameter_in_truth_subsystem_rate", 0.25),
    ]


def test_flatten_numeric_metric_records_includes_simulation_benchmark_audit_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "fault_window_count": 18,
            "detected_fault_window_count": 15,
            "declared_benchmark_tier_count": {
                "module_recoverable": 9,
                "subsystem_recoverable": 3,
            },
            "benchmark_tier_alignment_status_count": {
                "missed_target": 6,
                "met_target": 5,
            },
            "observed_recoverability_strength_tier_count": {
                "module_recoverable": 3,
                "undetected": 2,
            },
            "observed_recoverability_strength_tier_rate": {
                "module_recoverable": 1.0 / 6.0,
                "parameter_visible_only": 0.5,
            },
            "benchmark_review_priority_count": {
                "critical": 4,
                "high": 6,
            },
            "dominant_score_component_count": {
                "event_discordance": 4,
                "reconstruction_error": 10,
            },
            "benchmark_tier_scorecards": {
                "detection_only": {
                    "fault_window_count": 2,
                    "detected_fault_window_rate": 0.5,
                },
                "module_recoverable": {
                    "fault_window_count": 4,
                    "dominant_module_match_rate": 0.25,
                },
            },
        },
        category="validation",
        scope_name="overall",
        subscope_name="simulation_benchmark_audit",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("benchmark_review_priority_count.critical", 4),
        ("benchmark_review_priority_count.high", 6),
        ("benchmark_tier_alignment_status_count.met_target", 5),
        ("benchmark_tier_alignment_status_count.missed_target", 6),
        ("benchmark_tier_scorecards.detection_only.detected_fault_window_rate", 0.5),
        ("benchmark_tier_scorecards.detection_only.fault_window_count", 2),
        ("benchmark_tier_scorecards.module_recoverable.dominant_module_match_rate", 0.25),
        ("benchmark_tier_scorecards.module_recoverable.fault_window_count", 4),
        ("declared_benchmark_tier_count.module_recoverable", 9),
        ("declared_benchmark_tier_count.subsystem_recoverable", 3),
        ("detected_fault_window_count", 15),
        ("dominant_score_component_count.event_discordance", 4),
        ("dominant_score_component_count.reconstruction_error", 10),
        ("fault_window_count", 18),
        ("observed_recoverability_strength_tier_count.module_recoverable", 3),
        ("observed_recoverability_strength_tier_count.undetected", 2),
        ("observed_recoverability_strength_tier_rate.module_recoverable", 1.0 / 6.0),
        ("observed_recoverability_strength_tier_rate.parameter_visible_only", 0.5),
    ]


def test_flatten_numeric_metric_records_includes_benchmark_scope_validation_metric_paths():
    records = _flatten_numeric_metric_records(
        {
            "score_validation_by_benchmark_scope": {
                "detection": {
                    "eligible_fault_window_count": 12,
                    "detected_fault_window_rate": 0.75,
                }
            },
            "attribution_validation_by_benchmark_scope": {
                "module": {
                    "eligible_fault_window_count": 4,
                    "dominant_module_match_rate": 0.25,
                }
            },
        },
        category="validation",
        scope_name="overall",
        subscope_name="benchmark_scope_validation",
    )

    assert [(record.metric_path, record.value) for record in records] == [
        ("attribution_validation_by_benchmark_scope.module.dominant_module_match_rate", 0.25),
        ("attribution_validation_by_benchmark_scope.module.eligible_fault_window_count", 4),
        ("score_validation_by_benchmark_scope.detection.detected_fault_window_rate", 0.75),
        ("score_validation_by_benchmark_scope.detection.eligible_fault_window_count", 12),
    ]


def test_summarize_misbehavior_program_includes_declared_benchmark_tiers():
    flight = build_legacy_power_pressurization_hierarchy_reference_flight_spec()

    summary = _summarize_misbehavior_program(flight=flight, dt_seconds=1.0)

    assert summary["window_count"] == 4
    assert summary["declared_benchmark_window_count"] == 4
    assert summary["declared_benchmark_tier_count"] == {
        "module_recoverable": 4,
    }
    assert [row["declared_benchmark_tier"] for row in summary["benchmark_windows"]] == [
        "module_recoverable",
        "module_recoverable",
        "module_recoverable",
        "module_recoverable",
    ]


def test_summarize_misbehavior_program_includes_declared_benchmark_tiers_for_current_composite_builder():
    flight = build_named_flight_spec("power_pressurization_hierarchy_composite")

    summary = _summarize_misbehavior_program(flight=flight, dt_seconds=0.5)

    assert summary["window_count"] == 23
    assert summary["declared_benchmark_window_count"] == 23
    assert summary["declared_benchmark_tier_count"] == {
        "subsystem_recoverable": 9,
        "module_recoverable": 14,
    }
