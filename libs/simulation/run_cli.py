"""CLI entrypoint for persisted simulation pipeline runs."""

from __future__ import annotations

import argparse

from libs.simulation.cli import add_backbone_args, add_event_args, add_profile_args, add_source_args, add_window_args
from libs.simulation.run_context import PipelineRunConfig
from libs.simulation.runner import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the simulation pipeline into a persisted artifact bundle")
    add_source_args(parser)
    add_profile_args(parser)
    add_event_args(parser)
    add_window_args(parser)
    add_backbone_args(parser)
    parser.add_argument("--base-dir", default="data/simulation_runs", help="Base directory for simulation pipeline runs")
    parser.add_argument(
        "--mode",
        default="full",
        choices=("profile", "event", "structural", "full", "reference_inference"),
    )
    parser.add_argument("--start-stage", default=None, help="Optional stage script name to start from, e.g. 20_events_extract.py")
    parser.add_argument("--end-stage", default=None, help="Optional stage script name to stop at, e.g. 60_fit_hierarchy.py")
    parser.add_argument("--replay-run-dir", default=None, help="Existing run directory to replay into using persisted upstream artifacts")
    parser.add_argument("--format", default="parquet", choices=("parquet", "delta"), help="Persisted table format")
    parser.add_argument("--write-mode", default="overwrite", choices=("overwrite", "append", "merge"))
    parser.add_argument("--min-warm", default=1, type=int, help="Conformal minimum warm size")
    parser.add_argument("--phase-count", type=int, default=4, help="Detected phase count")
    return parser.parse_args()


def main() -> None:
    run_pipeline(PipelineRunConfig.from_args(parse_args()))
