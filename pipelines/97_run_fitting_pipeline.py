"""Run fitting stages under one parent MLflow run."""

import os

from libs.perf import log_memory_usage
from pipelines._pipeline_runner import run_stage_group


FITTING_STAGE_SCRIPTS = [
    "00_ingest_raw.py",
    "10_parameter_profiles_fit.py",
    "40_backbone_fit.py",
    "50_build_graph.py",
    "60_fit_hierarchy.py",
]


@log_memory_usage(label="97_run_fitting_pipeline")
def run() -> None:
    os.environ.setdefault("S3NTINEL_WRITE_MODE", "overwrite")
    os.environ.setdefault("S3NTINEL_FIT_WRITE_MODE", "overwrite")
    run_stage_group(
        run_name="s3ntinel.fitting_pipeline",
        pipeline_mode="fitting:v2",
        stage_scripts=list(FITTING_STAGE_SCRIPTS),
        summary_artifact_path="reports/fitting_pipeline_run_summary.json",
        logger_name=__name__,
    )


if __name__ == "__main__":
    run()
