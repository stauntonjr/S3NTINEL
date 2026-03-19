"""Run inference stages under one parent MLflow run."""

import os

from libs.perf import log_memory_usage
from pipelines._pipeline_runner import run_stage_group

INFERENCE_STAGE_SCRIPTS = [
    "20_events_extract.py",
    "30_windows_adaptive.py",
    "50_phase_fit.py",
    "60_window_scores_raw.py",
    "70_window_scores_calibrate.py",
    "80_anomaly_attribution.py",
]


@log_memory_usage(label="92_run_inference_pipeline")
def run() -> None:
    os.environ.setdefault("S3NTINEL_WRITE_MODE", "overwrite")
    run_stage_group(
        run_name="s3ntinel.inference_pipeline",
        pipeline_mode="inference:v2",
        stage_scripts=list(INFERENCE_STAGE_SCRIPTS),
        summary_artifact_path="reports/inference_pipeline_run_summary.json",
        logger_name=__name__,
    )


if __name__ == "__main__":
    run()
