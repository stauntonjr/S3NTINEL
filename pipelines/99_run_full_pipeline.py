# File: pipelines/99_run_full_pipeline.py
"""Run grouped fitting + inference pipelines under one parent MLflow run."""

import argparse

from libs.perf import log_memory_usage
from pipelines._pipeline_runner import StageGroupSpec, run_stage_group


GROUPED_STAGE_SCRIPTS = [
    "97_run_fitting_pipeline.py",
    "98_run_inference_pipeline.py",
]
FULL_STAGE_GROUP = StageGroupSpec(
    run_name="s3ntinel.full_pipeline",
    pipeline_mode="full",
    stage_scripts=tuple(GROUPED_STAGE_SCRIPTS),
    summary_artifact_path="reports/pipeline_run_summary.json",
    manifest_artifact_path="reports/stages/99_run_full_pipeline_manifest.json",
    logger_name=__name__,
)


@log_memory_usage(label="99_run_full_pipeline")
def run(
    *,
    start_stage_script: str | None = None,
    end_stage_script: str | None = None,
    replay_run_dir: str | None = None,
) -> None:
    run_stage_group(
        spec=FULL_STAGE_GROUP,
        start_stage_script=start_stage_script,
        end_stage_script=end_stage_script,
        replay_run_dir=replay_run_dir,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run grouped fitting and inference runners")
    parser.add_argument(
        "--start-stage",
        default=None,
        help="Optional grouped stage script name to start from, e.g. 98_run_inference_pipeline.py",
    )
    parser.add_argument(
        "--end-stage",
        default=None,
        help="Optional grouped stage script name to stop at, e.g. 97_run_fitting_pipeline.py",
    )
    parser.add_argument(
        "--replay-run-dir",
        default=None,
        help="Existing run directory with persisted grouped-stage replayable inputs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        start_stage_script=(None if args.start_stage is None else str(args.start_stage)),
        end_stage_script=(None if args.end_stage is None else str(args.end_stage)),
        replay_run_dir=(None if args.replay_run_dir is None else str(args.replay_run_dir)),
    )


if __name__ == "__main__":
    main()
