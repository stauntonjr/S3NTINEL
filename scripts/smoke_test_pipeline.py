# File: scripts/smoke_test_pipeline.py
"""Run an end-to-end local smoke test for stages 00->80 using sample data."""

from __future__ import annotations

import argparse
import json
import os
import runpy
from pathlib import Path
from typing import TYPE_CHECKING

from libs.io.delta import get_spark, read_table
from libs.testing.sample_data import seed_sample_dataset
from libs.testing.window_diagnostics import close_reason_tv_distance, compute_numeric_deltas, compute_window_diagnostics

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run S3NTINEL local smoke test")
    parser.add_argument("--base-dir", default="data/smoke", help="Base directory for smoke-test input/output data")
    parser.add_argument("--format", default="parquet", choices=["parquet", "delta"], help="Table format")
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"], help="Spark write mode")
    parser.add_argument("--min-warm", default=1, type=int, help="Conformal minimum warm size override for smoke tests")
    parser.add_argument(
        "--compare-window-strategies",
        action="store_true",
        help="Run stage 30 in stream_parity mode and generate diagnostics report vs bucketed windows",
    )
    parser.add_argument(
        "--window-report-path",
        default=None,
        help="Optional JSON path for window strategy comparison report (defaults under <base-dir>/reports)",
    )
    parser.add_argument(
        "--guard-max-window-count-delta",
        default=None,
        type=float,
        help="Optional max allowed absolute delta for window_count between bucketed and stream_parity",
    )
    parser.add_argument(
        "--guard-max-event-count-avg-delta",
        default=None,
        type=float,
        help="Optional max allowed absolute delta for event_count_avg between bucketed and stream_parity",
    )
    parser.add_argument(
        "--guard-max-sensor-count-avg-delta",
        default=None,
        type=float,
        help="Optional max allowed absolute delta for sensor_count_avg between bucketed and stream_parity",
    )
    parser.add_argument(
        "--guard-max-close-reason-tv-distance",
        default=None,
        type=float,
        help="Optional max allowed total-variation distance between close_reason distributions",
    )
    parser.add_argument(
        "--guard-profile",
        default="off",
        choices=["off", "conservative", "strict"],
        help="Convenience threshold preset for window regression guard",
    )
    return parser.parse_args()


def resolve_guard_thresholds(args: argparse.Namespace) -> tuple[float | None, float | None, float | None, float | None]:
    profile_defaults = {
        "off": (None, None, None, None),
        "conservative": (2.0, 1.0, 1.0, 0.35),
        "strict": (0.0, 0.0, 0.0, 0.05),
    }
    (
        default_window_delta,
        default_event_avg_delta,
        default_sensor_avg_delta,
        default_close_reason_tv_distance,
    ) = profile_defaults[str(args.guard_profile)]

    window_delta = (
        args.guard_max_window_count_delta
        if args.guard_max_window_count_delta is not None
        else default_window_delta
    )
    event_avg_delta = (
        args.guard_max_event_count_avg_delta
        if args.guard_max_event_count_avg_delta is not None
        else default_event_avg_delta
    )
    sensor_avg_delta = (
        args.guard_max_sensor_count_avg_delta
        if args.guard_max_sensor_count_avg_delta is not None
        else default_sensor_avg_delta
    )
    close_reason_tv_distance = (
        args.guard_max_close_reason_tv_distance
        if args.guard_max_close_reason_tv_distance is not None
        else default_close_reason_tv_distance
    )
    return window_delta, event_avg_delta, sensor_avg_delta, close_reason_tv_distance


def set_env_paths(base_dir: str, table_format: str, write_mode: str, min_warm: int) -> None:
    base = Path(base_dir)
    os.environ["S3NTINEL_RAW_INPUT_PATH"] = str(base / "input" / "raw_telemetry")
    os.environ["S3NTINEL_RAW_TABLE_PATH"] = str(base / "delta" / "raw_telemetry")
    os.environ["S3NTINEL_EVENTS_TABLE_PATH"] = str(base / "delta" / "events")
    os.environ["S3NTINEL_WINDOWS_TABLE_PATH"] = str(base / "delta" / "windows")
    os.environ["S3NTINEL_SIGNATURES_TABLE_PATH"] = str(base / "delta" / "signatures")
    os.environ["S3NTINEL_PHASE_WINDOWS_TABLE_PATH"] = str(base / "delta" / "phase_windows")
    os.environ["S3NTINEL_PHASES_TABLE_PATH"] = str(base / "delta" / "phases")
    os.environ["S3NTINEL_SCORES_TABLE_PATH"] = str(base / "delta" / "scores")
    os.environ["S3NTINEL_CALIBRATED_TABLE_PATH"] = str(base / "delta" / "calibrated")
    os.environ["S3NTINEL_ANOMALIES_TABLE_PATH"] = str(base / "delta" / "anomalies")
    os.environ["S3NTINEL_TABLE_FORMAT"] = table_format
    os.environ["S3NTINEL_RAW_OUTPUT_FORMAT"] = table_format
    os.environ["S3NTINEL_WRITE_MODE"] = write_mode
    os.environ["S3NTINEL_MIN_WARM"] = str(min_warm)


def run_stages() -> None:
    pipeline_dir = Path(__file__).resolve().parent.parent / "pipelines"
    stage_scripts = [
        "00_ingest_raw.py",
        "20_events_extract.py",
        "30_windows_adaptive.py",
        "40_signatures_build.py",
        "50_phase_detect.py",
        "60_anomaly_score.py",
        "70_conformal_calibrate.py",
        "80_emit_anomalies.py",
    ]

    for stage_script in stage_scripts:
        stage_path = pipeline_dir / stage_script
        print(f"[smoke] running {stage_script}")
        runpy.run_path(str(stage_path), run_name="__main__")


def print_row_counts(spark: "SparkSession", table_format: str) -> None:
    table_paths = {
        "raw_telemetry": os.environ["S3NTINEL_RAW_TABLE_PATH"],
        "events": os.environ["S3NTINEL_EVENTS_TABLE_PATH"],
        "windows": os.environ["S3NTINEL_WINDOWS_TABLE_PATH"],
        "signatures": os.environ["S3NTINEL_SIGNATURES_TABLE_PATH"],
        "phase_windows": os.environ["S3NTINEL_PHASE_WINDOWS_TABLE_PATH"],
        "phases": os.environ["S3NTINEL_PHASES_TABLE_PATH"],
        "scores": os.environ["S3NTINEL_SCORES_TABLE_PATH"],
        "calibrated": os.environ["S3NTINEL_CALIBRATED_TABLE_PATH"],
        "anomalies": os.environ["S3NTINEL_ANOMALIES_TABLE_PATH"],
    }

    print("\n[smoke] row counts")
    for name, path in table_paths.items():
        path_obj = Path(path)
        if not path_obj.exists():
            print(f"- {name}: 0 rows (path missing)")
            continue
        try:
            df = read_table(spark, path=path, fmt=table_format)
            print(f"- {name}: {df.count()} rows")
        except Exception as exc:
            print(f"- {name}: 0 rows (unreadable: {exc.__class__.__name__})")


def _print_window_report(report: dict[str, object]) -> None:
    print("\n[smoke] window strategy comparison")
    bucketed = report.get("bucketed", {})
    parity = report.get("stream_parity", {})
    deltas = report.get("deltas", {})
    print(f"- bucketed.window_count: {bucketed.get('window_count')}")
    print(f"- stream_parity.window_count: {parity.get('window_count')}")
    print(f"- delta.window_count: {deltas.get('window_count')}")
    print(f"- bucketed.event_count_avg: {bucketed.get('event_count_avg')}")
    print(f"- stream_parity.event_count_avg: {parity.get('event_count_avg')}")
    print(f"- delta.event_count_avg: {deltas.get('event_count_avg')}")
    print(f"- bucketed.duration_ms_avg: {bucketed.get('duration_ms_avg')}")
    print(f"- stream_parity.duration_ms_avg: {parity.get('duration_ms_avg')}")
    print(f"- delta.duration_ms_avg: {deltas.get('duration_ms_avg')}")
    print(f"- bucketed.sensor_count_avg: {bucketed.get('sensor_count_avg')}")
    print(f"- stream_parity.sensor_count_avg: {parity.get('sensor_count_avg')}")
    print(f"- delta.sensor_count_avg: {deltas.get('sensor_count_avg')}")
    print(f"- bucketed.close_reason_counts: {bucketed.get('close_reason_counts')}")
    print(f"- stream_parity.close_reason_counts: {parity.get('close_reason_counts')}")
    print(f"- close_reason_tv_distance: {report.get('close_reason_tv_distance')}")
    print(f"- report_path: {report.get('report_path')}")


def compare_window_strategies(
    spark: "SparkSession",
    base_dir: str,
    table_format: str,
    report_path: str | None,
) -> dict[str, object]:
    pipeline_dir = Path(__file__).resolve().parent.parent / "pipelines"
    stage_path = pipeline_dir / "30_windows_adaptive.py"

    original_windows_path = os.environ["S3NTINEL_WINDOWS_TABLE_PATH"]
    original_strategy = os.environ.get("S3NTINEL_WINDOW_STRATEGY", "")

    parity_windows_path = str(Path(base_dir) / "delta" / "windows_parity")
    os.environ["S3NTINEL_WINDOWS_TABLE_PATH"] = parity_windows_path
    os.environ["S3NTINEL_WINDOW_STRATEGY"] = "stream_parity"
    runpy.run_path(str(stage_path), run_name="__main__")

    bucketed_diag = compute_window_diagnostics(
        spark=spark,
        table_format=table_format,
        windows_path=original_windows_path,
        events_path=os.environ["S3NTINEL_EVENTS_TABLE_PATH"],
    )
    parity_diag = compute_window_diagnostics(
        spark=spark,
        table_format=table_format,
        windows_path=parity_windows_path,
        events_path=os.environ["S3NTINEL_EVENTS_TABLE_PATH"],
    )
    deltas = compute_numeric_deltas(bucketed_diag, parity_diag)

    if report_path:
        report_target = Path(report_path)
    else:
        report_target = Path(base_dir) / "reports" / "window_strategy_compare.json"
    report_target.parent.mkdir(parents=True, exist_ok=True)

    report: dict[str, object] = {
        "bucketed": bucketed_diag,
        "stream_parity": parity_diag,
        "deltas": deltas,
        "bucketed_path": original_windows_path,
        "stream_parity_path": parity_windows_path,
        "report_path": str(report_target),
    }

    bucketed_reason_counts = bucketed_diag.get("close_reason_counts", {})
    parity_reason_counts = parity_diag.get("close_reason_counts", {})
    if isinstance(bucketed_reason_counts, dict) and isinstance(parity_reason_counts, dict):
        report["close_reason_tv_distance"] = close_reason_tv_distance(
            bucketed_counts={str(k): int(v) for k, v in bucketed_reason_counts.items()},
            parity_counts={str(k): int(v) for k, v in parity_reason_counts.items()},
        )
    else:
        report["close_reason_tv_distance"] = None

    report_target.write_text(json.dumps(report, indent=2), encoding="utf-8")
    _print_window_report(report)

    os.environ["S3NTINEL_WINDOWS_TABLE_PATH"] = original_windows_path
    if original_strategy:
        os.environ["S3NTINEL_WINDOW_STRATEGY"] = original_strategy
    elif "S3NTINEL_WINDOW_STRATEGY" in os.environ:
        del os.environ["S3NTINEL_WINDOW_STRATEGY"]

    return report


def apply_window_regression_guard(report: dict[str, object], args: argparse.Namespace) -> None:
    deltas = report.get("deltas", {})
    if not isinstance(deltas, dict):
        return

    window_delta, event_avg_delta, sensor_avg_delta, close_reason_threshold = resolve_guard_thresholds(args)

    checks: list[tuple[str, float | None]] = [
        ("window_count", window_delta),
        ("event_count_avg", event_avg_delta),
        ("sensor_count_avg", sensor_avg_delta),
    ]
    guard_active = any(threshold is not None for _, threshold in checks)
    if guard_active:
        print("\n[smoke] window regression guard thresholds")
        for key, threshold in checks:
            print(f"- {key}: {threshold}")
        print(f"- close_reason_tv_distance: {close_reason_threshold}")

    failures: list[str] = []
    for key, threshold in checks:
        if threshold is None:
            continue
        value = deltas.get(key)
        if value is None:
            failures.append(f"{key}: missing delta")
            continue
        abs_value = abs(float(value))
        if abs_value > float(threshold):
            failures.append(f"{key}: |{abs_value:.6f}| > {float(threshold):.6f}")

    close_reason_tv_distance = report.get("close_reason_tv_distance")
    if close_reason_threshold is not None:
        if close_reason_tv_distance is None:
            failures.append("close_reason_tv_distance: missing")
        else:
            abs_value = abs(float(close_reason_tv_distance))
            if abs_value > float(close_reason_threshold):
                failures.append(
                    f"close_reason_tv_distance: |{abs_value:.6f}| > {float(close_reason_threshold):.6f}"
                )

    if failures:
        print("\n[smoke] window regression guard FAILED")
        for item in failures:
            print(f"- {item}")
        raise SystemExit(2)

    if guard_active:
        print("\n[smoke] window regression guard PASSED")


def main() -> None:
    args = parse_args()
    set_env_paths(base_dir=args.base_dir, table_format=args.format, write_mode=args.write_mode, min_warm=args.min_warm)

    spark = get_spark("s3ntinel.smoke_test_pipeline")
    seed_sample_dataset(
        spark=spark,
        base_dir=args.base_dir,
        mode="overwrite",
        table_format=args.format,
    )

    run_stages()

    if args.compare_window_strategies:
        report = compare_window_strategies(
            spark=spark,
            base_dir=args.base_dir,
            table_format=args.format,
            report_path=args.window_report_path,
        )
        apply_window_regression_guard(report, args)

    print_row_counts(spark, table_format=args.format)
    print("\n[smoke] completed successfully")


if __name__ == "__main__":
    main()
