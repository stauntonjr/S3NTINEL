"""Run fitting stages under one parent MLflow run."""

import argparse
import os

from libs.perf import log_memory_usage
from pipelines._pipeline_runner import StageGroupSpec, run_stage_group


FITTING_STAGE_SCRIPTS = [
    "00_ingest_raw.py",
    "10_parameter_profiles_fit.py",
    "12_behavior_profiles_fit.py",
    "15_event_profiles_fit.py",
    "20_events_extract.py",
    "25_window_policy_profile.py",
    "30_windows_adaptive.py",
    "40_backbone_fit.py",
    "50_build_graph.py",
    "60_fit_hierarchy.py",
]
FITTING_STAGE_GROUP = StageGroupSpec(
    run_name="s3ntinel.fitting_pipeline",
    pipeline_mode="fitting:v2",
    stage_scripts=tuple(FITTING_STAGE_SCRIPTS),
    summary_artifact_path="reports/fitting_pipeline_run_summary.json",
    manifest_artifact_path="reports/stages/97_run_fitting_pipeline_manifest.json",
    logger_name=__name__,
)


@log_memory_usage(label="97_run_fitting_pipeline")
def run(
    *,
    start_stage_script: str | None = None,
    end_stage_script: str | None = None,
    replay_run_dir: str | None = None,
) -> None:
    os.environ.setdefault("S3NTINEL_WRITE_MODE", "overwrite")
    os.environ.setdefault("S3NTINEL_FIT_WRITE_MODE", "overwrite")
    run_stage_group(
        spec=FITTING_STAGE_GROUP,
        start_stage_script=start_stage_script,
        end_stage_script=end_stage_script,
        replay_run_dir=replay_run_dir,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run grouped fitting stages")
    parser.add_argument("--start-stage", default=None, help="Optional stage script name to start from")
    parser.add_argument("--end-stage", default=None, help="Optional stage script name to stop at")
    parser.add_argument(
        "--replay-run-dir",
        default=None,
        help="Existing run directory with persisted replayable inputs for later-stage resumes",
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
