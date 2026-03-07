"""Run fitting stages under one parent MLflow run."""

import os

from pipelines._pipeline_runner import run_stage_group


FITTING_STAGE_SCRIPTS = [
    "00_ingest_raw.py",
    "10_backbone_fit.py",
    "11_graph_fit.py",
]


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
