from __future__ import annotations

import argparse

from libs.tuning import build_benchmark_dir, validate_benchmark_args


def test_build_benchmark_dir_normalizes_flight_name():
    path = build_benchmark_dir(base_dir="/tmp/perf", flight_name="power/chain", mode="full")

    assert path.parent.as_posix() == "/tmp/perf"
    assert path.name.endswith("_power_chain_full_performance_profile")


def test_validate_benchmark_args_rejects_invalid_objective_replay_combinations():
    args = argparse.Namespace(
        search_stage=None,
        search_strategy="grid",
        search_budget=None,
        search_seed=0,
        variants=[],
        variant_set="quick",
        mode="full",
        replay_source_run_dir=None,
        replay_target_stage=None,
        evaluation_tier=None,
        objective_name="sim_full_default_v1",
        objective_preset=None,
        objective_spec_path=None,
        objective_overrides=[],
    )

    try:
        validate_benchmark_args(args)
    except SystemExit as exc:
        assert str(exc) == "--objective-name requires --replay-source-run-dir"
    else:
        raise AssertionError("expected validate_benchmark_args to reject missing replay source")
