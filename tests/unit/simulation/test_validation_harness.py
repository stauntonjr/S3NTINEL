from libs.simulation.validation_harness import _flatten_numeric_metric_records


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
