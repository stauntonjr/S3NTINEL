from libs.events.calibration import _candidate_score


def test_candidate_score_ignores_legacy_closure_balance_targets():
    summary_a = {
        "slope_event_share": 0.75,
        "total_event_count": 100,
        "window_summary": {
            "closure_mix": {
                "event_threshold": 0.0,
                "budget_threshold": 0.99,
                "end_of_stream": 0.01,
            },
            "pair_cost_proxy": 400.0,
            "same_window_pair_expansion_proxy": 320.0,
            "p95_event_count": 1.0,
        },
    }
    summary_b = {
        "slope_event_share": 0.75,
        "total_event_count": 100,
        "window_summary": {
            "closure_mix": {
                "event_threshold": 0.75,
                "budget_threshold": 0.25,
                "end_of_stream": 0.01,
            },
            "pair_cost_proxy": 400.0,
            "same_window_pair_expansion_proxy": 320.0,
            "p95_event_count": 1.0,
        },
    }

    assert _candidate_score(summary_a) == _candidate_score(summary_b)


def test_candidate_score_penalizes_overloaded_windows():
    bounded = {
        "slope_event_share": 0.75,
        "total_event_count": 100,
        "window_summary": {
            "closure_mix": {
                "end_of_stream": 0.01,
            },
            "pair_cost_proxy": 400.0,
            "same_window_pair_expansion_proxy": 320.0,
            "p95_event_count": 1.0,
        },
    }
    overloaded = {
        "slope_event_share": 0.75,
        "total_event_count": 100,
        "window_summary": {
            "closure_mix": {
                "end_of_stream": 0.01,
            },
            "pair_cost_proxy": 400.0,
            "same_window_pair_expansion_proxy": 320.0,
            "p95_event_count": 3.0,
        },
    }

    assert _candidate_score(overloaded) > _candidate_score(bounded)
