"""Shared benchmark invocation helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from libs.tuning.benchmark_search import resolve_search_spec


def build_benchmark_dir(*, base_dir: str, flight_name: str, mode: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_flight_name = str(flight_name).replace("/", "_")
    return Path(base_dir) / f"{timestamp}_{safe_flight_name}_{mode}_performance_profile"


def validate_benchmark_args(args: Any) -> None:
    if args.search_stage is None and (args.search_budget is not None or int(args.search_seed) != 0 or str(args.search_strategy) != "grid"):
        raise SystemExit("--search-strategy, --search-budget, and --search-seed require --search-stage")
    if args.search_stage is not None and args.variants:
        raise SystemExit("--search-stage is mutually exclusive with --variant")
    if args.search_stage is not None and str(args.variant_set) != "quick":
        raise SystemExit("--search-stage is mutually exclusive with --variant-set")
    if args.search_stage is not None:
        search_spec = resolve_search_spec(str(args.search_stage))
        if str(args.mode) != str(search_spec.mode):
            raise SystemExit(f"--search-stage {args.search_stage!r} requires --mode {search_spec.mode!r}")
    if args.replay_target_stage is not None and args.replay_source_run_dir is None:
        raise SystemExit("--replay-target-stage requires --replay-source-run-dir")
    if args.evaluation_tier is not None and args.replay_source_run_dir is None:
        raise SystemExit("--evaluation-tier requires --replay-source-run-dir")
    if args.objective_name is not None and args.replay_source_run_dir is None:
        raise SystemExit("--objective-name requires --replay-source-run-dir")
    if args.objective_preset is not None and args.replay_source_run_dir is None:
        raise SystemExit("--objective-preset requires --replay-source-run-dir")
    if args.objective_spec_path is not None and args.replay_source_run_dir is None:
        raise SystemExit("--objective-spec-path requires --replay-source-run-dir")
    if args.objective_overrides and args.replay_source_run_dir is None:
        raise SystemExit("--objective-override requires --replay-source-run-dir")
    if args.objective_name is not None and args.objective_preset is not None:
        raise SystemExit("--objective-name and --objective-preset are mutually exclusive")
    if args.objective_preset is not None and args.objective_spec_path is not None:
        raise SystemExit("--objective-preset and --objective-spec-path are mutually exclusive")
    if args.objective_name is not None and args.objective_spec_path is not None:
        raise SystemExit("--objective-name and --objective-spec-path are mutually exclusive")
