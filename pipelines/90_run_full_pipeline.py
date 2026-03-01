# File: pipelines/90_run_full_pipeline.py
"""Run all pipeline stages under one parent MLflow run."""

from __future__ import annotations

import runpy
import time
from pathlib import Path

from libs.perf import (
    active_run_id,
    get_logger,
    log_dict_artifact_if_active,
    log_metric_if_active,
    pipeline_run_context,
)


LOGGER = get_logger(__name__)

STAGE_SCRIPTS = [
    "00_ingest_raw.py",
    "10_cur_backbone_fit.py",
    "20_events_extract.py",
    "30_windows_adaptive.py",
    "40_signatures_build.py",
    "50_phase_detect.py",
    "60_anomaly_score.py",
    "70_conformal_calibrate.py",
    "80_emit_anomalies.py",
]


def run() -> None:
    run_name = "s3ntinel.full_pipeline"
    run_start = time.perf_counter()
    with pipeline_run_context(
        run_name=run_name,
        logger=LOGGER,
        tags={
            "project": "S3NTINEL",
            "pipeline_mode": "full",
        },
    ):
        parent_run_id = active_run_id()
        LOGGER.info("pipeline_parent_run=%s run_id=%s", run_name, parent_run_id)

        pipeline_dir = Path(__file__).resolve().parent
        stage_results: list[dict[str, object]] = []
        failure: Exception | None = None

        for stage_script in STAGE_SCRIPTS:
            stage_path = pipeline_dir / stage_script
            stage_start = time.perf_counter()
            LOGGER.info("stage_start script=%s", stage_script)
            try:
                runpy.run_path(str(stage_path), run_name="__main__")
                elapsed_ms = (time.perf_counter() - stage_start) * 1000.0
                stage_results.append(
                    {
                        "stage_script": stage_script,
                        "status": "success",
                        "elapsed_ms": elapsed_ms,
                    }
                )
                LOGGER.info("stage_end script=%s elapsed_ms=%.3f", stage_script, elapsed_ms)
            except Exception as exc:
                elapsed_ms = (time.perf_counter() - stage_start) * 1000.0
                stage_results.append(
                    {
                        "stage_script": stage_script,
                        "status": "failed",
                        "elapsed_ms": elapsed_ms,
                        "error": str(exc),
                    }
                )
                LOGGER.exception("stage_failed script=%s elapsed_ms=%.3f", stage_script, elapsed_ms)
                failure = exc
                break

        total_elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        completed_count = sum(1 for item in stage_results if item["status"] == "success")
        failed_count = sum(1 for item in stage_results if item["status"] == "failed")

        summary = {
            "project": "S3NTINEL",
            "run_name": run_name,
            "parent_run_id": parent_run_id,
            "status": "failed" if failure else "success",
            "total_elapsed_ms": total_elapsed_ms,
            "stage_count": len(STAGE_SCRIPTS),
            "completed_stage_count": completed_count,
            "failed_stage_count": failed_count,
            "stages": stage_results,
        }

        log_metric_if_active("pipeline_total_elapsed_ms", total_elapsed_ms)
        log_metric_if_active("pipeline_completed_stage_count", float(completed_count))
        log_metric_if_active("pipeline_failed_stage_count", float(failed_count))
        log_dict_artifact_if_active(summary, "reports/pipeline_run_summary.json")

        LOGGER.info(
            "pipeline_summary status=%s completed=%s failed=%s total_elapsed_ms=%.3f",
            summary["status"],
            completed_count,
            failed_count,
            total_elapsed_ms,
        )

        if failure is not None:
            raise failure


if __name__ == "__main__":
    run()
