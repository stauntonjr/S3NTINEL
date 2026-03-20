"""Benchmark semantics-preserving pipeline tuning variants on canonical simulation runs.

TODO:
- add an explicit dataset-size scale sweep mode; the current script compares tuning variants
  on fixed workloads and does not measure how wall time, memory, disk, and stage timings
  scale with dataset size.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

from libs.perf import get_logger
from scripts.sim_common import add_event_args, add_source_args, add_window_args


LOGGER_NAME = "s3ntinel.profile_pipeline_performance"
SUMMARY_NAME_BY_MODE = {
    "profile": "profile_pipeline_run_summary.json",
    "structural": "structural_pipeline_run_summary.json",
    "full": "pipeline_run_summary.json",
}
TIMED_STAGE_SCRIPTS = (
    "20_events_extract.py",
    "25_window_policy_profile.py",
    "30_windows_adaptive.py",
    "50_build_graph.py",
    "70_phase_fit.py",
)


@dataclass(frozen=True)
class BenchmarkVariant:
    name: str
    description: str
    env_overrides: dict[str, str]


@dataclass(frozen=True)
class BenchmarkResult:
    name: str
    description: str
    repeat_index: int
    status: str
    env_overrides: dict[str, str]
    run_dir: str | None
    manifest_path: str | None
    elapsed_ms: float | None
    stage_elapsed_ms: dict[str, float]
    return_code: int
    error: str | None = None


SAFE_SMALL_SEGMENT_ENV = {
    "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "25000",
    "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "600000",
    "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "25000",
    "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "600000",
    "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "2500",
    "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "1200000",
}


VARIANT_SET_BY_NAME = {
    "quick": (
        BenchmarkVariant(
            name="baseline",
            description="Canonical settings with no extra tuning overrides.",
            env_overrides={},
        ),
        BenchmarkVariant(
            name="all_small_segments",
            description="Moderately smaller event/window/phase segments that stay in a safer range for the window stage.",
            env_overrides=dict(SAFE_SMALL_SEGMENT_ENV),
        ),
        BenchmarkVariant(
            name="all_large_segments",
            description="Larger event/window/phase segments to reduce carry-over overhead and shuffle fan-out.",
            env_overrides={
                "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "10000",
                "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "3600000",
            },
        ),
    ),
    "detailed": (
        BenchmarkVariant(
            name="baseline",
            description="Canonical settings with no extra tuning overrides.",
            env_overrides={},
        ),
        BenchmarkVariant(
            name="event_small_segments",
            description="Moderately smaller per-parameter event segments only.",
            env_overrides={
                "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "25000",
                "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "600000",
            },
        ),
        BenchmarkVariant(
            name="window_small_segments",
            description="Moderately smaller per-flight window segments only.",
            env_overrides={
                "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "25000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "600000",
            },
        ),
        BenchmarkVariant(
            name="phase_small_segments",
            description="Moderately smaller per-flight phase segments only.",
            env_overrides={
                "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "2500",
                "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "1200000",
            },
        ),
        BenchmarkVariant(
            name="all_small_segments",
            description="Moderately smaller event/window/phase segments together.",
            env_overrides=dict(SAFE_SMALL_SEGMENT_ENV),
        ),
        BenchmarkVariant(
            name="all_large_segments",
            description="Larger event/window/phase segments together.",
            env_overrides={
                "S3NTINEL_EVENT_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_EVENT_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_ROWS": "100000",
                "S3NTINEL_WINDOW_SEGMENT_MAX_SPAN_MS": "1800000",
                "S3NTINEL_PHASE_SEGMENT_MAX_ROWS": "10000",
                "S3NTINEL_PHASE_SEGMENT_MAX_SPAN_MS": "3600000",
            },
        ),
    ),
}
VARIANT_BY_NAME = {
    variant.name: variant
    for variant_group in VARIANT_SET_BY_NAME.values()
    for variant in variant_group
}


def _parse_env_assignment(raw_value: str) -> tuple[str, str]:
    key, separator, value = str(raw_value).partition("=")
    if not separator or not key.strip():
        raise argparse.ArgumentTypeError("environment overrides must use KEY=VALUE syntax")
    return key.strip(), value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark semantics-preserving pipeline tuning variants against the canonical simulation runner"
    )
    add_source_args(parser)
    add_event_args(parser)
    add_window_args(parser)
    parser.add_argument("--base-dir", default="data/performance_profiles", help="Base directory for benchmark bundles")
    parser.add_argument("--mode", default="full", choices=("profile", "structural", "full"))
    parser.add_argument("--format", default="parquet", choices=("parquet", "delta"))
    parser.add_argument("--write-mode", default="overwrite", choices=("overwrite", "append", "merge"))
    parser.add_argument("--min-warm", default=1, type=int)
    parser.add_argument("--phase-count", default=3, type=int)
    parser.add_argument("--backbone-parameter-count", default=8, type=int)
    parser.add_argument("--backbone-ridge-lambda", default=1.0, type=float)
    parser.add_argument("--variant-set", default="quick", choices=tuple(sorted(VARIANT_SET_BY_NAME)))
    parser.add_argument(
        "--variant",
        dest="variants",
        action="append",
        default=[],
        choices=tuple(sorted(VARIANT_BY_NAME)),
        help="Run only the named variant; may be repeated",
    )
    parser.add_argument("--repeat", default=1, type=int, help="Repeat each variant this many times")
    parser.add_argument("--spark-profile", default=None, help="Optional S3NTINEL_SPARK_PROFILE override for child runs")
    parser.add_argument(
        "--env",
        dest="extra_env",
        action="append",
        default=[],
        type=_parse_env_assignment,
        help="Extra semantics-preserving environment override in KEY=VALUE form",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Deprecated compatibility flag; benchmark runs now continue after variant failures by default.",
    )
    parser.add_argument(
        "--fail-on-variant-error",
        action="store_true",
        help="Exit non-zero when any variant fails instead of recording the failure in the summary and continuing.",
    )
    return parser.parse_args()


def _build_benchmark_dir(*, base_dir: str, flight_name: str, mode: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_flight_name = str(flight_name).replace("/", "_")
    return Path(base_dir) / f"{timestamp}_{safe_flight_name}_{mode}_performance_profile"


def _build_run_command(args: argparse.Namespace, *, run_base_dir: Path) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "scripts.run_sim_pipeline",
        "--flight-name",
        str(args.flight_name),
        "--tail-id",
        str(args.tail_id),
        "--flight-id",
        str(args.flight_id),
        "--base-dir",
        str(run_base_dir),
        "--mode",
        str(args.mode),
        "--format",
        str(args.format),
        "--write-mode",
        str(args.write_mode),
        "--min-warm",
        str(args.min_warm),
        "--delta-threshold",
        str(args.delta_threshold),
        "--slope-source",
        str(args.slope_source),
        "--ema-alpha",
        str(args.ema_alpha),
        "--window-max-ms",
        str(args.window_max_ms),
        "--window-event-threshold",
        str(args.window_event_threshold),
        "--window-min-ms",
        str(args.window_min_ms),
        "--window-inactivity-timeout-ms",
        str(args.window_inactivity_timeout_ms),
        "--window-strategy",
        str(args.window_strategy),
        "--phase-count",
        str(args.phase_count),
        "--backbone-parameter-count",
        str(args.backbone_parameter_count),
        "--backbone-ridge-lambda",
        str(args.backbone_ridge_lambda),
    ]
    if args.n_steps is not None:
        command.extend(("--n-steps", str(args.n_steps)))
    if args.dt_seconds is not None:
        command.extend(("--dt-seconds", str(args.dt_seconds)))
    return command


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_child_run_dir(run_base_dir: Path) -> Path | None:
    candidates = [path for path in run_base_dir.iterdir() if path.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _stage_elapsed_ms(summary_payload: dict[str, object]) -> dict[str, float]:
    stages = summary_payload.get("stages", [])
    if not isinstance(stages, list):
        return {}
    stage_timings: dict[str, float] = {}
    for stage_entry in stages:
        if not isinstance(stage_entry, dict):
            continue
        stage_script = stage_entry.get("stage_script")
        elapsed_ms = stage_entry.get("elapsed_ms")
        if isinstance(stage_script, str) and isinstance(elapsed_ms, (int, float)):
            stage_timings[stage_script] = float(elapsed_ms)
    return stage_timings


def _build_summary_payload(
    *,
    benchmark_dir: Path,
    args: argparse.Namespace,
    results: list[BenchmarkResult],
) -> dict[str, object]:
    successful_results = [result for result in results if result.status == "success" and result.elapsed_ms is not None]
    failed_results = [result for result in results if result.status != "success"]
    fastest_result = min(successful_results, key=lambda result: float(result.elapsed_ms)) if successful_results else None
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmark_dir": str(benchmark_dir),
        "flight_name": str(args.flight_name),
        "mode": str(args.mode),
        "variant_set": str(args.variant_set),
        "repeat": int(args.repeat),
        "status": ("success" if not failed_results else "partial_failure" if successful_results else "failed"),
        "successful_result_count": len(successful_results),
        "failed_result_count": len(failed_results),
        "spark_profile": args.spark_profile,
        "extra_env": {key: value for key, value in args.extra_env},
        "results": [asdict(result) for result in results],
        "fastest_result": (asdict(fastest_result) if fastest_result is not None else None),
    }


def _build_markdown_summary(*, args: argparse.Namespace, results: list[BenchmarkResult]) -> str:
    lines = [
        "# Pipeline Performance Profile",
        "",
        f"- flight: `{args.flight_name}`",
        f"- mode: `{args.mode}`",
        f"- variant set: `{args.variant_set}`",
        f"- repeats: `{args.repeat}`",
        "",
        "| variant | repeat | status | total_s | events_s | windows_s | graph_s | phase_s | run_dir |",
        "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for result in results:
        total_seconds = (float(result.elapsed_ms) / 1000.0) if result.elapsed_ms is not None else None
        stage_seconds = {
            stage_script: (result.stage_elapsed_ms.get(stage_script, 0.0) / 1000.0)
            for stage_script in TIMED_STAGE_SCRIPTS
        }
        lines.append(
            "| "
            + " | ".join(
                (
                    result.name,
                    str(result.repeat_index),
                    result.status,
                    (f"{total_seconds:.1f}" if total_seconds is not None else "n/a"),
                    f"{stage_seconds['20_events_extract.py']:.1f}" if stage_seconds["20_events_extract.py"] else "n/a",
                    f"{stage_seconds['30_windows_adaptive.py']:.1f}" if stage_seconds["30_windows_adaptive.py"] else "n/a",
                    f"{stage_seconds['50_build_graph.py']:.1f}" if stage_seconds["50_build_graph.py"] else "n/a",
                    f"{stage_seconds['70_phase_fit.py']:.1f}" if stage_seconds["70_phase_fit.py"] else "n/a",
                    (result.run_dir or "n/a"),
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _run_variant(
    *,
    args: argparse.Namespace,
    benchmark_dir: Path,
    variant: BenchmarkVariant,
    repeat_index: int,
) -> BenchmarkResult:
    logger = get_logger(LOGGER_NAME)
    run_base_dir = benchmark_dir / "runs" / variant.name / f"repeat_{repeat_index}"
    run_base_dir.mkdir(parents=True, exist_ok=True)
    command = _build_run_command(args, run_base_dir=run_base_dir)
    child_env = dict(os.environ)
    if args.spark_profile:
        child_env["S3NTINEL_SPARK_PROFILE"] = str(args.spark_profile)
    for key, value in args.extra_env:
        child_env[str(key)] = str(value)
    for key, value in variant.env_overrides.items():
        child_env[str(key)] = str(value)

    logger.info(
        "benchmark_variant_start variant=%s repeat=%s mode=%s run_base_dir=%s",
        variant.name,
        repeat_index,
        args.mode,
        run_base_dir,
    )
    completed = subprocess.run(command, cwd=Path(__file__).resolve().parents[1], env=child_env, check=False)
    run_dir = _find_child_run_dir(run_base_dir)
    manifest_path = (run_dir / "reports" / "run_manifest.json") if run_dir is not None else None
    summary_path = (
        run_dir / "reports" / SUMMARY_NAME_BY_MODE[str(args.mode)]
        if run_dir is not None
        else None
    )
    manifest_payload = _load_json(manifest_path) if manifest_path is not None and manifest_path.exists() else {}
    summary_payload = _load_json(summary_path) if summary_path is not None and summary_path.exists() else {}
    status = str(manifest_payload.get("status", "failed" if completed.returncode else "success"))
    error = manifest_payload.get("error")
    if error is not None:
        error = str(error)
    elif completed.returncode:
        error = f"benchmark child exited with code {completed.returncode}"
    return BenchmarkResult(
        name=variant.name,
        description=variant.description,
        repeat_index=repeat_index,
        status=status,
        env_overrides=dict(variant.env_overrides),
        run_dir=(str(run_dir) if run_dir is not None else None),
        manifest_path=(str(manifest_path) if manifest_path is not None else None),
        elapsed_ms=(
            float(manifest_payload["timing"]["elapsed_ms"])
            if isinstance(manifest_payload.get("timing"), dict)
            and isinstance(manifest_payload["timing"].get("elapsed_ms"), (int, float))
            else None
        ),
        stage_elapsed_ms=_stage_elapsed_ms(summary_payload),
        return_code=int(completed.returncode),
        error=error,
    )


def main() -> None:
    args = parse_args()
    benchmark_dir = _build_benchmark_dir(base_dir=str(args.base_dir), flight_name=str(args.flight_name), mode=str(args.mode))
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger(LOGGER_NAME)
    variants = (
        tuple(VARIANT_BY_NAME[name] for name in args.variants)
        if args.variants
        else VARIANT_SET_BY_NAME[str(args.variant_set)]
    )
    results: list[BenchmarkResult] = []

    for variant in variants:
        for repeat_index in range(1, max(int(args.repeat), 1) + 1):
            result = _run_variant(
                args=args,
                benchmark_dir=benchmark_dir,
                variant=variant,
                repeat_index=repeat_index,
            )
            results.append(result)
            logger.info(
                "benchmark_variant_end variant=%s repeat=%s status=%s elapsed_ms=%s run_dir=%s",
                result.name,
                result.repeat_index,
                result.status,
                result.elapsed_ms,
                result.run_dir,
            )

    reports_dir = benchmark_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    summary_payload = _build_summary_payload(benchmark_dir=benchmark_dir, args=args, results=results)
    (reports_dir / "performance_profile_summary.json").write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "performance_profile_summary.md").write_text(
        _build_markdown_summary(args=args, results=results),
        encoding="utf-8",
    )

    any_success = any(result.status == "success" for result in results)
    any_failure = any(result.status != "success" for result in results)
    if args.fail_on_variant_error and any_failure:
        raise SystemExit(1)
    if not any_success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
