# File: scripts/smoke_test_pipeline.py
"""Run an end-to-end local smoke test for stages 00->80 using sample data."""

from __future__ import annotations

import argparse
import json
import os
import runpy
import sys
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
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=["overwrite", "append", "merge"],
        help="Spark write mode (stage 80 supports merge upsert)",
    )
    parser.add_argument("--min-warm", default=1, type=int, help="Conformal minimum warm size override for smoke tests")
    parser.add_argument("--tail-count", default=1, type=int, help="Synthetic seed tail count")
    parser.add_argument("--flights-per-tail", default=1, type=int, help="Synthetic seed flights per tail")
    parser.add_argument("--sensor-count", default=3, type=int, help="Synthetic seed sensor count per timestamp")
    parser.add_argument("--timestamp-count", default=12, type=int, help="Synthetic seed timestamp count per flight")
    parser.add_argument("--step-ms", default=100, type=int, help="Synthetic seed timestamp step in milliseconds")
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
    os.environ["S3NTINEL_SUBSYSTEM_MAP_TABLE_PATH"] = str(base / "delta" / "sensor_subsystem_map")
    os.environ["S3NTINEL_SCORES_TABLE_PATH"] = str(base / "delta" / "scores")
    os.environ["S3NTINEL_CALIBRATED_TABLE_PATH"] = str(base / "delta" / "calibrated")
    os.environ["S3NTINEL_ANOMALIES_TABLE_PATH"] = str(base / "delta" / "anomalies")
    os.environ["S3NTINEL_TABLE_FORMAT"] = table_format
    os.environ["S3NTINEL_RAW_OUTPUT_FORMAT"] = table_format
    os.environ["S3NTINEL_WRITE_MODE"] = "overwrite" if write_mode == "merge" else write_mode
    os.environ["S3NTINEL_MIN_WARM"] = str(min_warm)


def seed_subsystem_map_for_smoke(spark: "SparkSession", table_format: str) -> None:
    raw_path = os.environ["S3NTINEL_RAW_TABLE_PATH"]
    output_path = os.environ["S3NTINEL_SUBSYSTEM_MAP_TABLE_PATH"]
    raw_df = read_table(spark, path=raw_path, fmt=table_format)
    sensors = sorted(
        [str(row["sensor"]) for row in raw_df.select("sensor").where("sensor is not null").distinct().collect()]
    )
    if not sensors:
        raise RuntimeError("smoke subsystem-map seed failed: no sensors found in raw table")
    rows = [{"sensor": sensor, "subsystem_id": f"SUBSYS_{index + 1:04d}"} for index, sensor in enumerate(sensors)]
    spark.createDataFrame(rows).write.format(table_format).mode("overwrite").save(output_path)


def run_stages(stage_80_write_mode: str) -> None:
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
        if stage_script == "80_emit_anomalies.py":
            os.environ["S3NTINEL_WRITE_MODE"] = stage_80_write_mode
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


def assert_anomaly_payload_quality(spark: "SparkSession", table_format: str, write_mode: str) -> None:
    from pyspark.sql import functions as F

    anomalies_path = os.environ["S3NTINEL_ANOMALIES_TABLE_PATH"]
    anomalies_df = read_table(spark, path=anomalies_path, fmt=table_format)
    row_count = anomalies_df.count()
    if row_count <= 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: no anomaly rows emitted")

    duplicate_key_rows = (
        anomalies_df.groupBy("tail_id", "flight_id", "win_id").agg(F.count(F.lit(1)).alias("n")).where(F.col("n") > F.lit(1)).count()
    )
    if duplicate_key_rows > 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: duplicate anomaly merge keys found")

    panel_rows = anomalies_df.where(F.col("panel_context").isNotNull()).count()
    if panel_rows <= 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: panel_context is null for all rows")

    top_sensor_rows = anomalies_df.where(F.expr("exists(subsystems, s -> size(s.top_sensors) > 0)")).count()
    if top_sensor_rows <= 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: no subsystem top_sensors populated")

    if str(write_mode).lower() == "merge":
        stage_80_path = Path(__file__).resolve().parent.parent / "pipelines" / "80_emit_anomalies.py"
        os.environ["S3NTINEL_WRITE_MODE"] = "merge"
        runpy.run_path(str(stage_80_path), run_name="__main__")
        anomalies_df_post = read_table(spark, path=anomalies_path, fmt=table_format)
        post_count = anomalies_df_post.count()
        if post_count != row_count:
            raise SystemExit(
                f"[smoke] anomaly quality assertion failed: merge mode idempotence violated (before={row_count}, after={post_count})"
            )
        duplicate_key_rows_post = (
            anomalies_df_post.groupBy("tail_id", "flight_id", "win_id")
            .agg(F.count(F.lit(1)).alias("n"))
            .where(F.col("n") > F.lit(1))
            .count()
        )
        if duplicate_key_rows_post > 0:
            raise SystemExit("[smoke] anomaly quality assertion failed: duplicate merge keys after merge rerun")

    print(
        "[smoke] anomaly quality assertions passed "
        f"(rows={row_count}, panel_rows={panel_rows}, top_sensor_rows={top_sensor_rows}, write_mode={write_mode})"
    )


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
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    if args.write_mode == "merge" and args.format != "delta":
        raise SystemExit("[smoke] --write-mode merge requires --format delta")

    set_env_paths(base_dir=args.base_dir, table_format=args.format, write_mode=args.write_mode, min_warm=args.min_warm)

    spark = get_spark("s3ntinel.smoke_test_pipeline")
    if args.write_mode == "merge":
        try:
            delta_probe_path = str(Path(args.base_dir) / "_delta_probe")
            spark.range(1).write.format("delta").mode("overwrite").save(delta_probe_path)
            spark.read.format("delta").load(delta_probe_path).count()
        except Exception as exc:
            raise SystemExit(
                "[smoke] --write-mode merge requires a Spark session with Delta format support. "
                "Install/configure Delta runtime and Spark Delta extensions/catalog before running merge smoke checks."
            ) from exc

    seed_sample_dataset(
        spark=spark,
        base_dir=args.base_dir,
        mode="overwrite",
        table_format=args.format,
        tail_count=int(args.tail_count),
        flights_per_tail=int(args.flights_per_tail),
        sensor_count=int(args.sensor_count),
        timestamp_count=int(args.timestamp_count),
        step_ms=int(args.step_ms),
        include_intermediate_tables=False,
    )
    seed_subsystem_map_for_smoke(spark=spark, table_format=args.format)

    run_stages(stage_80_write_mode=args.write_mode)

    if args.compare_window_strategies:
        report = compare_window_strategies(
            spark=spark,
            base_dir=args.base_dir,
            table_format=args.format,
            report_path=args.window_report_path,
        )
        apply_window_regression_guard(report, args)

    print_row_counts(spark, table_format=args.format)
    assert_anomaly_payload_quality(spark=spark, table_format=args.format, write_mode=args.write_mode)
    print("\n[smoke] completed successfully")


if __name__ == "__main__":
    main()
