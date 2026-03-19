"""Shared stage-group runner for MLflow-tracked pipeline orchestration."""

from __future__ import annotations

import runpy
import time
from pathlib import Path

from libs.perf import (
    active_run_id,
    capture_memory_snapshot,
    get_logger,
    log_dict_artifact_if_active,
    log_memory_usage,
    log_metric_if_active,
    pipeline_run_context,
)


@log_memory_usage(label="pipeline_stage_group")
def run_stage_group(
    *,
    run_name: str,
    pipeline_mode: str,
    stage_scripts: list[str],
    summary_artifact_path: str,
    logger_name: str,
) -> None:
    logger = get_logger(logger_name)
    run_start = time.perf_counter()

    with pipeline_run_context(
        run_name=run_name,
        logger=logger,
        tags={
            "project": "S3NTINEL",
            "pipeline_mode": pipeline_mode,
        },
    ):
        parent_run_id = active_run_id()
        logger.info("pipeline_parent_run=%s run_id=%s", run_name, parent_run_id)

        pipeline_dir = Path(__file__).resolve().parent
        stage_results: list[dict[str, object]] = []
        failure: Exception | None = None

        for stage_script in stage_scripts:
            stage_path = pipeline_dir / stage_script
            stage_start = time.perf_counter()
            logger.info("stage_start script=%s", stage_script)
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
                logger.info("stage_end script=%s elapsed_ms=%.3f", stage_script, elapsed_ms)
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
                logger.exception("stage_failed script=%s elapsed_ms=%.3f", stage_script, elapsed_ms)
                failure = exc
                break

        total_elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        completed_count = sum(1 for item in stage_results if item["status"] == "success")
        failed_count = sum(1 for item in stage_results if item["status"] == "failed")

        summary = {
            "project": "S3NTINEL",
            "run_name": run_name,
            "pipeline_mode": pipeline_mode,
            "parent_run_id": parent_run_id,
            "status": "failed" if failure else "success",
            "total_elapsed_ms": total_elapsed_ms,
            "stage_count": len(stage_scripts),
            "completed_stage_count": completed_count,
            "failed_stage_count": failed_count,
            "stages": stage_results,
        }
        summary["memory_snapshot_end"] = capture_memory_snapshot(
            label=run_name,
            event="pipeline_summary",
            started_at=run_start,
            status=str(summary["status"]),
            include_spark=True,
        )

        log_metric_if_active("pipeline_total_elapsed_ms", total_elapsed_ms)
        log_metric_if_active("pipeline_completed_stage_count", float(completed_count))
        log_metric_if_active("pipeline_failed_stage_count", float(failed_count))
        log_dict_artifact_if_active(summary, summary_artifact_path)

        logger.info(
            "pipeline_summary mode=%s status=%s completed=%s failed=%s total_elapsed_ms=%.3f",
            pipeline_mode,
            summary["status"],
            completed_count,
            failed_count,
            total_elapsed_ms,
        )

        if failure is not None:
            raise failure
