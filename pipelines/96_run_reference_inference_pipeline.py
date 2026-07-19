"""Run target-flight inference against pre-positioned reference artifacts."""

import argparse
import os

from libs.perf import log_memory_usage
from pipelines._pipeline_runner import StageGroupSpec, run_stage_group


REFERENCE_INFERENCE_STAGE_SCRIPTS = (
    "20_events_extract.py",
    "30_windows_adaptive.py",
    "35_window_features_apply.py",
    "70_phase_fit.py",
    "72_phase_label_centroids.py",
    "80_window_scores_raw.py",
    "85_window_scores_calibrate.py",
    "90_anomaly_attribution.py",
    "95_emit_explorer_bundle.py",
)
REFERENCE_INFERENCE_STAGE_GROUP = StageGroupSpec(
    run_name="s3ntinel.reference_inference_pipeline",
    pipeline_mode="reference_inference:v2",
    stage_scripts=REFERENCE_INFERENCE_STAGE_SCRIPTS,
    summary_artifact_path="reports/reference_inference_pipeline_run_summary.json",
    manifest_artifact_path="reports/stages/96_run_reference_inference_pipeline_manifest.json",
    logger_name=__name__,
)


@log_memory_usage(label="96_run_reference_inference_pipeline")
def run(
    *,
    start_stage_script: str | None = None,
    end_stage_script: str | None = None,
    replay_run_dir: str | None = None,
) -> None:
    os.environ.setdefault("S3NTINEL_WRITE_MODE", "overwrite")
    previous_phase_mode = os.environ.get("S3NTINEL_PHASE_EXECUTION_MODE")
    os.environ["S3NTINEL_PHASE_EXECUTION_MODE"] = "apply_reference"
    try:
        run_stage_group(
            spec=REFERENCE_INFERENCE_STAGE_GROUP,
            start_stage_script=start_stage_script,
            end_stage_script=end_stage_script,
            replay_run_dir=replay_run_dir,
        )
    finally:
        if previous_phase_mode is None:
            os.environ.pop("S3NTINEL_PHASE_EXECUTION_MODE", None)
        else:
            os.environ["S3NTINEL_PHASE_EXECUTION_MODE"] = previous_phase_mode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run inference against pre-positioned reference artifacts")
    parser.add_argument("--start-stage", default=None)
    parser.add_argument("--end-stage", default=None)
    parser.add_argument("--replay-run-dir", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        start_stage_script=args.start_stage,
        end_stage_script=args.end_stage,
        replay_run_dir=args.replay_run_dir,
    )


if __name__ == "__main__":
    main()
