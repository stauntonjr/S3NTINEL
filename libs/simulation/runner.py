"""Library entrypoint for persisted simulation pipeline runs."""

from __future__ import annotations

import time
from datetime import datetime, timezone
import json

from libs.io.delta import get_spark, write_table
from libs.perf import get_logger
from pipelines._pipeline_runner import run_stage_group
from libs.simulation.cli import resolve_flight
from libs.simulation.full_run_report import write_full_run_report as _write_full_run_report
from libs.simulation.run_context import (
    PipelineRunConfig,
    PipelineRunResult,
    RunPaths,
    build_manifest as _build_manifest,
    restore_env as _restore_env,
    run_mode as _run_mode,
    set_run_env as _set_run_env,
    tee_console as _tee_console,
    write_manifest as _write_manifest,
)
from libs.simulation.validation_harness import write_validation_harness_report as _write_validation_harness_report
from libs.simulation.seed_bundle import write_seed_tables as _write_seed_tables_impl
from libs.simulation.reporting import (
    build_fault_attribution_summary_from_misbehavior as _build_fault_attribution_summary_from_misbehavior,
    build_fault_score_summary_from_misbehavior as _build_fault_score_summary_from_misbehavior,
    write_validation_reports as _write_validation_reports,
)
from libs.tuning import write_objective_evaluation_report as _write_objective_evaluation_report

LOGGER_NAME = "s3ntinel.run_sim_pipeline"


def _write_seed_tables(**kwargs):
    return _write_seed_tables_impl(write_table_fn=write_table, **kwargs)


def _load_existing_seed_counts(paths: RunPaths) -> dict[str, int]:
    if not paths.manifest_path.exists():
        return {}
    try:
        payload = json.loads(paths.manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {
        str(key): int(value)
        for key, value in dict(payload.get("seed_counts") or {}).items()
    }


def run_pipeline(config: PipelineRunConfig) -> PipelineRunResult:
    flight = (
        resolve_flight(config.flight_name, sim_seed=config.sim_seed)
        if config.sim_seed is not None
        else resolve_flight(config.flight_name)
    )
    config = config.with_flight_defaults(flight=flight)
    paths = RunPaths(run_dir=config.build_run_dir())
    paths.run_dir.mkdir(parents=True, exist_ok=True)
    previous_env = _set_run_env(paths, config)

    logger = None
    run_start = time.perf_counter()
    start_utc = datetime.now(timezone.utc)
    status = "success"
    error_message: str | None = None
    seed_counts: dict[str, int] = {}
    summary_artifact_path: str | None = None
    validation_payloads: dict[str, object] | None = None

    try:
        with _tee_console(paths.log_path):
            logger = get_logger(LOGGER_NAME)
            spark = get_spark(LOGGER_NAME)
            run_name, pipeline_mode, stage_scripts, summary_artifact_path = _run_mode(config)
            should_write_seed_tables = bool(stage_scripts) and (
                config.replay_run_dir is None or stage_scripts[0] == "00_ingest_raw.py"
            )
            if should_write_seed_tables:
                seed_counts = _write_seed_tables(spark=spark, paths=paths, config=config, flight=flight)
            else:
                seed_counts = _load_existing_seed_counts(paths)
            logger.info(
                "sim_run_start flight=%s mode=%s run_dir=%s format=%s replay_run_dir=%s start_stage=%s end_stage=%s",
                config.flight_name,
                config.mode,
                paths.run_dir,
                config.table_format,
                config.replay_run_dir,
                config.start_stage,
                config.end_stage,
            )
            run_stage_group(
                run_name=run_name,
                pipeline_mode=pipeline_mode,
                stage_scripts=stage_scripts,
                summary_artifact_path=summary_artifact_path,
                logger_name=LOGGER_NAME,
                start_stage_script=config.start_stage,
                end_stage_script=config.end_stage,
                replay_run_dir=config.replay_run_dir,
            )
            validation_payloads = _write_validation_reports(
                spark=spark,
                paths=paths,
                flight=flight,
                table_format=config.table_format,
            )
            logger.info("sim_run_complete flight=%s mode=%s run_dir=%s", config.flight_name, config.mode, paths.run_dir)
    except Exception as exc:
        status = "failed"
        error_message = f"{exc.__class__.__name__}: {exc}"
        if logger is not None:
            logger.exception("sim_run_failed flight=%s mode=%s", config.flight_name, config.mode)
        raise
    finally:
        end_utc = datetime.now(timezone.utc)
        elapsed_ms = (time.perf_counter() - run_start) * 1000.0
        manifest = _build_manifest(
            paths=paths,
            config=config,
            flight=flight,
            status=status,
            error_message=error_message,
            start_utc=start_utc,
            end_utc=end_utc,
            elapsed_ms=elapsed_ms,
            seed_counts=seed_counts,
        )
        _write_manifest(paths.manifest_path, manifest)
        full_run_report = _write_full_run_report(
            paths=paths,
            manifest=manifest,
            summary_artifact_path=summary_artifact_path,
            validation_payloads=validation_payloads,
        )
        harness_report = _write_validation_harness_report(
            paths=paths,
            manifest=manifest,
            full_run_report=full_run_report,
            flight=flight,
        )
        _write_objective_evaluation_report(
            run_dir=paths.run_dir,
            harness_report=harness_report,
        )
        _restore_env(previous_env)
    return PipelineRunResult(paths=paths, status=status, seed_counts=seed_counts)
