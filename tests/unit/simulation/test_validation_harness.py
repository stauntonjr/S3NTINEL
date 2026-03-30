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
