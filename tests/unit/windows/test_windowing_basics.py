from datetime import datetime, timedelta, timezone

import pandas as pd

from libs.windows import (
    Window,
    WindowPolicy,
    WindowPolicyEvaluationSpec,
    WindowPolicyProfile,
    WindowPolicyProfileTable,
    WindowPolicyProfileSpec,
    WindowsTable,
    WindowSensorBuffer,
    build_window_truth_phase_coverage_summary,
    build_window_policy_profile_evaluation_report_spark,
)
from libs.windows.policy_profile import compute_window_policy_penalty


def test_window_policy_closes_by_budget_or_event_count():
    policy = WindowPolicy(max_ms=200, event_threshold=20, min_ms=50, inactivity_timeout_ms=0)

    assert not policy.should_close(duration_ms=50, event_count=1)
    assert policy.should_close(duration_ms=250, event_count=1)
    assert policy.should_close(duration_ms=10, event_count=20)
    assert policy.close_reason(duration_ms=250, event_count=30) == "event_threshold"
    assert policy.close_reason(duration_ms=100, event_count=20) == "event_threshold"
    assert policy.close_reason(duration_ms=250, event_count=5) == "budget_threshold"


def test_default_window_max_ms_uses_10_samples_over_min_sampling_rate():
    assert WindowPolicy.max_ms_from_min_sampling_rate(1.0) == 10000
    assert WindowPolicy.default().max_ms == 10000


def test_compute_window_policy_penalty_penalizes_window_overload():
    bounded = compute_window_policy_penalty(
        pair_cost_proxy=400.0,
        same_window_pair_expansion_proxy=320.0,
        sampled_event_count=100,
        p95_event_count=1.0,
        end_of_stream_rate=0.01,
    )
    overloaded = compute_window_policy_penalty(
        pair_cost_proxy=400.0,
        same_window_pair_expansion_proxy=320.0,
        sampled_event_count=100,
        p95_event_count=3.0,
        end_of_stream_rate=0.01,
    )

    assert overloaded > bounded


def test_window_policy_warning_reasons_use_aggregate_policy_penalty():
    profile = WindowPolicyProfile(
        spec=WindowPolicyProfileSpec(
            min_sampling_rate_hz=1.0,
            configured_max_ms=5000,
            configured_event_threshold=10,
            min_ms=25,
            inactivity_timeout_ms=0,
        )
    )

    warning_reasons = profile._warning_reasons(
        selected_profile_row={"balance_penalty": 2.7},
        configured_profile_row={"balance_penalty": 3.1},
        edge_stability={"mean_boundary_jaccard": None},
        evaluation_spec=WindowPolicyEvaluationSpec(),
    )

    assert warning_reasons == []


def test_window_ingest_event_updates_local_state():
    window = Window.open(datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
    event = {
        "tail_id": "T1",
        "flight_id": "F1",
        "parameter_name": "pump_state",
        "timestamp_utc": datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
        "event_type_detected": "transition",
        "payload": {"to": "ON"},
    }

    window.ingest_event(event)

    assert window.event_count == 1
    assert window.duration_ms == 5000
    assert window.sensor_buffer.snapshot() == {"pump_state": "ON"}
    assert window.event_type_counts == {"transition": 1}


def test_window_sensor_buffer_updates_and_snapshots():
    buffer = WindowSensorBuffer()
    event = {
        "parameter_name": "pump_state",
        "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        "event_type_detected": "transition",
        "payload": {"to": "ON"},
    }

    buffer.ingest_event(event)

    assert buffer.snapshot() == {"pump_state": "ON"}


def test_build_windows_table_emits_expected_budget_threshold_windows(spark):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": 1,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": "1.0"},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": 2,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": "2.0"},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        },
    ]
    events_df = spark.createDataFrame(events)
    observed = [
        row.asDict(recursive=True)
        for row in WindowsTable.from_events(
            events_df,
            max_ms=10000,
            event_threshold=20,
            min_ms=50,
            inactivity_timeout_ms=0,
        )
        .to_dataframe()
        .orderBy("win_id")
        .collect()
    ]

    assert len(observed) == 3
    assert observed[0]["close_reason"] == "budget_threshold"
    assert observed[0]["duration_ms"] == 9500
    assert observed[0]["t_end"] == observed[0]["t_start"] + timedelta(milliseconds=9500)
    assert observed[0]["event_count"] == 1
    assert observed[0]["real_event_count"] == 1
    assert observed[0]["quiet_credit_end"] == 19.0
    assert observed[0]["closure_budget_end"] == 20.0
    assert observed[0]["sensor_count"] == 1
    assert observed[0]["event_type_counts"] == {"slope_pos": 1}
    assert observed[0]["zoh_snapshot"] == {"p1": "1.0"}
    assert observed[1]["close_reason"] == "budget_threshold"
    assert observed[1]["duration_ms"] == 10000
    assert observed[1]["event_count"] == 0
    assert observed[1]["real_event_count"] == 0
    assert observed[1]["quiet_credit_end"] == 20.0
    assert observed[1]["closure_budget_end"] == 20.0
    assert observed[1]["sensor_count"] == 0
    assert observed[1]["event_type_counts"] == {}
    assert observed[1]["zoh_snapshot"] == {}
    assert observed[2]["close_reason"] == "end_of_stream"
    assert observed[2]["duration_ms"] == 500
    assert observed[2]["event_count"] == 1
    assert observed[2]["real_event_count"] == 1
    assert observed[2]["sensor_count"] == 1
    assert observed[2]["event_type_counts"] == {"slope_pos": 1}
    assert observed[2]["zoh_snapshot"] == {"p1": "2.0"}


def test_build_window_policy_profile_table_emits_ranked_selected_candidate(spark):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": index + 1,
            "parameter_name": f"p{(index % 2) + 1}",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(milliseconds=200 * index),
            "event_type_detected": "slope_pos" if index % 2 == 0 else "transition",
            "payload": {"value": str(index)},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        }
        for index in range(8)
    ]
    profile_df = WindowPolicyProfileTable.from_events(
        spark.createDataFrame(events),
        spec=WindowPolicyProfileSpec(
            min_sampling_rate_hz=1.0,
            configured_max_ms=2000,
            configured_event_threshold=4,
            min_ms=50,
            inactivity_timeout_ms=0,
            gap_quantiles=(0.5,),
            event_threshold_multipliers=(1.0,),
            max_profile_flights=1,
        ),
    ).to_dataframe()
    rows = [row.asDict() for row in profile_df.orderBy("candidate_rank").collect()]

    assert rows
    assert rows[0]["candidate_rank"] == 1
    assert rows[0]["is_selected"] is True
    assert sum(1 for row in rows if row["is_selected"]) == 1
    assert all(int(row["max_ms"]) >= 50 for row in rows)
    assert all(int(row["event_threshold"]) >= 2 for row in rows)


def test_window_policy_profile_falls_back_to_configured_policy_when_candidate_profiling_fails(spark, monkeypatch):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": index + 1,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(milliseconds=200 * index),
            "event_type_detected": "slope_pos",
            "payload": {"value": str(index)},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        }
        for index in range(4)
    ]
    profile = WindowPolicyProfile(
        spec=WindowPolicyProfileSpec(
            min_sampling_rate_hz=1.0,
            configured_max_ms=2000,
            configured_event_threshold=4,
            min_ms=50,
            inactivity_timeout_ms=0,
            gap_quantiles=(0.5,),
            event_threshold_multipliers=(1.0,),
            max_profile_flights=1,
        )
    )
    monkeypatch.setattr(
        WindowPolicyProfile,
        "_evaluate_candidate_with_coverage",
        lambda self, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    rows = [row.asDict() for row in profile.build_dataframe(spark.createDataFrame(events)).collect()]

    assert len(rows) == 1
    assert rows[0]["candidate_rank"] == 1
    assert rows[0]["is_selected"] is True
    assert rows[0]["max_ms"] == 2000
    assert rows[0]["event_threshold"] == 4
    assert rows[0]["sampled_event_count"] == 4
    assert rows[0]["sampled_flight_count"] == 1


def test_window_policy_profile_resolves_selected_policy_over_fallback(spark):
    profile_df = spark.createDataFrame(
        [
            {
                "profile_id": "WINDOW_POLICY_PROFILE_V1",
                "profile_scope": "global",
                "candidate_rank": 1,
                "is_selected": True,
                "max_ms": 1200,
                "event_threshold": 6,
                "min_ms": 50,
                "inactivity_timeout_ms": 0,
                "objective_score": 1.0,
                "balance_penalty": 0.1,
                "predicted_window_count": 10,
                "mean_duration_ms": 400.0,
                "p95_duration_ms": 800.0,
                "mean_event_count": 5.0,
                "p95_event_count": 6.0,
                "mean_sensor_count": 2.0,
                "mean_event_type_count": 1.5,
                "event_threshold_close_rate": 0.7,
                "budget_threshold_close_rate": 0.3,
                "max_ms_close_rate": 0.3,
                "pair_cost_proxy": 250.0,
                "mean_quiet_credit_end": 1.2,
                "p95_quiet_credit_end": 2.0,
                "mean_closure_budget_end": 6.2,
                "p95_closure_budget_end": 7.0,
                "sampled_event_count": 50,
                "sampled_flight_count": 2,
            }
        ]
    )

    resolved, source = WindowPolicyProfile.resolve_selected_policy(
        profile_df,
        fallback_policy=WindowPolicy(max_ms=10000, event_threshold=20, min_ms=50, inactivity_timeout_ms=0),
    )

    assert source == "profile"
    assert resolved.max_ms == 1200
    assert resolved.event_threshold == 6


def test_build_window_policy_profile_evaluation_report_skips_when_evaluation_fails(spark, monkeypatch):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": event_index + 1,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            + timedelta(milliseconds=200 * event_index),
            "event_type_detected": "slope_pos",
            "payload": {"value": str(event_index)},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        }
        for event_index in range(4)
    ]
    profile_spec = WindowPolicyProfileSpec(
        min_sampling_rate_hz=1.0,
        configured_max_ms=1000,
        configured_event_threshold=2,
        min_ms=50,
        inactivity_timeout_ms=0,
        gap_quantiles=(0.5,),
        event_threshold_multipliers=(1.0,),
        max_profile_flights=1,
    )
    profile_df = WindowPolicyProfileTable.from_events(spark.createDataFrame(events), spec=profile_spec).to_dataframe()
    monkeypatch.setattr(
        WindowPolicyProfile,
        "build_evaluation_report",
        lambda self, *args, **kwargs: (_ for _ in ()).throw(RuntimeError("evaluation failed")),
    )

    report = build_window_policy_profile_evaluation_report_spark(
        spark.createDataFrame(events),
        profile_df=profile_df,
        profile_spec=profile_spec,
    )

    assert report["status"] == "skipped"
    assert report["selected_policy"]["policy_source"] in {"profile", "configured"}
    if report["selected_policy"]["policy_source"] == "profile":
        profile_row = report["selected_policy"]["profile_row"]
        assert profile_row is not None
        assert report["selected_policy"]["resolved_policy"]["max_ms"] == profile_row["max_ms"]
        assert report["selected_policy"]["resolved_policy"]["event_threshold"] == profile_row["event_threshold"]
    else:
        assert report["selected_policy"]["resolved_policy"]["max_ms"] == 1000
        assert report["selected_policy"]["resolved_policy"]["event_threshold"] == 2
    assert report["closure_mix"]["status"] == "skipped"


def test_build_window_policy_profile_evaluation_report_emits_selected_summary_and_stability(spark):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": event_index + 1,
            "parameter_name": f"p{(event_index % 2) + 1}",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            + timedelta(milliseconds=200 * event_index),
            "event_type_detected": "slope_pos" if event_index % 2 == 0 else "transition",
            "payload": {"value": str(event_index)},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        }
        for event_index in range(4)
    ]
    profile_spec = WindowPolicyProfileSpec(
        min_sampling_rate_hz=1.0,
        configured_max_ms=1000,
        configured_event_threshold=2,
        min_ms=50,
        inactivity_timeout_ms=0,
        gap_quantiles=(0.5,),
        event_threshold_multipliers=(1.0,),
        max_profile_flights=1,
    )
    events_df = spark.createDataFrame(events)
    profile_df = WindowPolicyProfileTable.from_events(events_df, spec=profile_spec).to_dataframe()

    report = build_window_policy_profile_evaluation_report_spark(
        events_df,
        profile_df=profile_df,
        profile_spec=profile_spec,
        evaluation_spec=WindowPolicyEvaluationSpec(
            candidate_frontier_size=2,
            stability_sample_count=1,
            max_stability_flights=1,
        ),
    )

    selected_profile_row = report["selected_policy"]["profile_row"]
    assert report["status"] in {"ok", "warning"}
    assert selected_profile_row is not None
    assert report["selected_policy"]["policy_source"] == "profile"
    assert report["selected_policy"]["resolved_policy"]["max_ms"] == selected_profile_row["max_ms"]
    assert report["selected_policy"]["resolved_policy"]["event_threshold"] == selected_profile_row["event_threshold"]
    assert "max_ms_close_rate" not in selected_profile_row
    assert "event_threshold_plus_max_ms_close_rate" not in selected_profile_row
    assert len(report["candidate_frontier"]) <= 3
    assert [row["candidate_rank"] for row in report["candidate_frontier"]] == sorted(
        row["candidate_rank"] for row in report["candidate_frontier"]
    )
    assert all("max_ms_close_rate" not in row for row in report["candidate_frontier"])
    delta = report["selection_delta_vs_configured"]
    assert delta["max_ms"]["absolute_delta"] == (
        report["selected_policy"]["resolved_policy"]["max_ms"] - report["selected_policy"]["configured_policy"]["max_ms"]
    )
    assert delta["event_threshold"]["absolute_delta"] == (
        report["selected_policy"]["resolved_policy"]["event_threshold"]
        - report["selected_policy"]["configured_policy"]["event_threshold"]
    )
    closure_mix = report["closure_mix"]
    assert abs(sum(closure_mix["rates"].values()) - 1.0) < 1e-9
    assert "event_threshold+max_ms" not in closure_mix["rates"]
    assert "event_threshold+max_ms" not in closure_mix["counts"]
    assert "pair_cost_proxy" in report["downstream_cost_proxy"]
    assert report["edge_stability"]["status"] in {"ok", "skipped"}
    assert "samples" in report["edge_stability"]


def test_build_window_truth_phase_coverage_summary_reports_target_phase_supply():
    windows_df = pd.DataFrame.from_records(
        [
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 1,
                "t_start": "2026-01-01T00:00:00Z",
                "t_end": "2026-01-01T00:00:04Z",
                "duration_ms": 4000,
                "close_reason": "budget_threshold",
                "real_event_count": 0,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 2,
                "t_start": "2026-01-01T00:00:05Z",
                "t_end": "2026-01-01T00:00:09Z",
                "duration_ms": 4000,
                "close_reason": "budget_threshold",
                "real_event_count": 0,
            },
            {
                "tail_id": "T1",
                "flight_id": "F1",
                "win_id": 3,
                "t_start": "2026-01-01T00:00:10Z",
                "t_end": "2026-01-01T00:00:11Z",
                "duration_ms": 1000,
                "close_reason": "event_threshold",
                "real_event_count": 8,
            },
        ]
    )
    phase_labels_df = pd.DataFrame.from_records(
        [
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": "2026-01-01T00:00:00Z", "phase_label": "cruise"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": "2026-01-01T00:00:02Z", "phase_label": "cruise"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": "2026-01-01T00:00:05Z", "phase_label": "cruise"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": "2026-01-01T00:00:08Z", "phase_label": "cruise"},
            {"tail_id": "T1", "flight_id": "F1", "timestamp_utc": "2026-01-01T00:00:10Z", "phase_label": "takeoff_climb"},
        ]
    )

    summary = build_window_truth_phase_coverage_summary(
        windows_df=windows_df,
        phase_labels_df=phase_labels_df,
    )

    assert summary["status"] == "ok"
    assert summary["majority_window_counts_by_phase_label"] == {"cruise": 2, "takeoff_climb": 1}
    assert summary["cruise_majority_window_count"] == 2
    assert summary["target_phase_close_reason_rates"] == {"budget_threshold": 1.0}
    assert summary["target_phase_duration_ms"]["mean"] == 4000.0
    assert summary["target_phase_real_event_count"]["mean"] == 0.0


def test_build_windows_table_inserts_budget_threshold_windows_for_long_event_gaps(spark):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": 1,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": "1.0"},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        },
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": 2,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 20, tzinfo=timezone.utc),
            "event_type_detected": "slope_pos",
            "payload": {"value": "2.0"},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        },
    ]
    observed = [
        row.asDict(recursive=True)
        for row in WindowsTable.from_events(
            spark.createDataFrame(events),
            max_ms=10000,
            event_threshold=20,
            min_ms=50,
            inactivity_timeout_ms=0,
        )
        .to_dataframe()
        .orderBy("win_id")
        .collect()
    ]

    assert len(observed) == 3
    assert observed[0]["close_reason"] == "budget_threshold"
    assert observed[0]["duration_ms"] == 9500
    assert observed[0]["t_end"] == observed[0]["t_start"] + timedelta(milliseconds=9500)
    assert observed[0]["event_count"] == 1
    assert observed[1]["quiet_credit_end"] == 20.0
    assert observed[1]["close_reason"] == "budget_threshold"
    assert observed[1]["event_count"] == 0
    assert observed[1]["duration_ms"] == 10000
    assert observed[2]["close_reason"] == "end_of_stream"
    assert observed[2]["duration_ms"] == 500


def test_build_windows_table_closes_short_event_rich_gaps_by_combined_budget(spark):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": index + 1,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(milliseconds=400 * index),
            "event_type_detected": "transition",
            "payload": {"value": str(index)},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        }
        for index in range(5)
    ]
    observed = [
        row.asDict(recursive=True)
        for row in WindowsTable.from_events(
            spark.createDataFrame(events),
            max_ms=4000,
            event_threshold=8,
            min_ms=50,
            inactivity_timeout_ms=0,
        )
        .to_dataframe()
        .orderBy("win_id")
        .collect()
    ]

    assert len(observed) == 1
    assert observed[0]["close_reason"] == "budget_threshold"
    assert observed[0]["event_count"] == 5
    assert observed[0]["duration_ms"] == 1600


def test_build_windows_table_keeps_event_rich_threshold_closure_with_continuous_budget(spark):
    events = [
        {
            "tail_id": "T1",
            "flight_id": "F1",
            "event_seq_id": index + 1,
            "parameter_name": "p1",
            "timestamp_utc": datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc) + timedelta(milliseconds=200 * index),
            "event_type_detected": "transition",
            "payload": {"value": str(index)},
            "date_utc": datetime(2026, 1, 1, tzinfo=timezone.utc).date(),
        }
        for index in range(4)
    ]
    observed = [
        row.asDict(recursive=True)
        for row in WindowsTable.from_events(
            spark.createDataFrame(events),
            max_ms=5000,
            event_threshold=4,
            min_ms=50,
            inactivity_timeout_ms=0,
        )
        .to_dataframe()
        .orderBy("win_id")
        .collect()
    ]

    assert len(observed) == 1
    assert observed[0]["close_reason"] == "event_threshold"
    assert observed[0]["event_count"] == 4
