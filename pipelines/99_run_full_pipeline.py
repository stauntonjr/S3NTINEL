# File: pipelines/99_run_full_pipeline.py
"""Run grouped fitting + inference pipelines under one parent MLflow run."""

from libs.perf import log_memory_usage
from pipelines._pipeline_runner import run_stage_group


GROUPED_STAGE_SCRIPTS = [
    "97_run_fitting_pipeline.py",
    "98_run_inference_pipeline.py",
]


@log_memory_usage(label="99_run_full_pipeline")
def run() -> None:
    run_stage_group(
        run_name="s3ntinel.full_pipeline",
        pipeline_mode="full",
        stage_scripts=GROUPED_STAGE_SCRIPTS,
        summary_artifact_path="reports/pipeline_run_summary.json",
        logger_name=__name__,
    )


if __name__ == "__main__":
    run()
