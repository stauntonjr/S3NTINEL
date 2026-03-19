"""Run downstream jobs for each partition row in a synthetic fleet manifest."""

from __future__ import annotations

import argparse
import os
import runpy
import subprocess
from pathlib import Path
from typing import Any

from libs.io.delta import get_spark, read_table


PIPELINE_STAGE_SCRIPTS = [
    "00_ingest_raw.py",
    "20_events_extract.py",
    "30_windows_adaptive.py",
    "70_phase_fit.py",
    "80_window_scores_raw.py",
    "85_window_scores_calibrate.py",
    "90_anomaly_attribution.py",
]

GROUPED_PIPELINE_SCRIPTS = [
    "97_run_fitting_pipeline.py",
    "98_run_inference_pipeline.py",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run downstream jobs for each partition manifest row")
    parser.add_argument("--partition-manifest-path", required=True, help="Path to partition manifest table")
    parser.add_argument("--manifest-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--tail-id", default=None, help="Optional filter for tail_id")
    parser.add_argument("--flight-id", default=None, help="Optional filter for flight_id")
    parser.add_argument("--limit", default=0, type=int, help="Optional max manifest rows to process (0 = all)")

    parser.add_argument("--job", default="pipeline", choices=["pipeline", "grouped", "custom"])
    parser.add_argument(
        "--command",
        action="append",
        default=[],
        help=(
            "Custom command template (repeatable) with placeholders: "
            "{tail_id}, {flight_id}, {output_path}, {run_dir}. Required when --job custom"
        ),
    )

    parser.add_argument("--jobs-base-dir", default="data/fleet_jobs", help="Base directory for per-flight pipeline outputs")
    parser.add_argument("--table-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument("--min-warm", default=1, type=int)

    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _rows_from_manifest(
    manifest_path: str,
    manifest_format: str,
    tail_id: str | None,
    flight_id: str | None,
    limit: int,
) -> list[dict[str, str]]:
    spark = get_spark("s3ntinel.run_partition_manifest_jobs")
    df = read_table(spark, manifest_path, fmt=manifest_format).select("tail_id", "flight_id", "output_path")

    if tail_id:
        df = df.where(df.tail_id == str(tail_id))
    if flight_id:
        df = df.where(df.flight_id == str(flight_id))
    if int(limit) > 0:
        df = df.limit(int(limit))

    rows = []
    for row in df.collect():
        if row["tail_id"] is None or row["flight_id"] is None or row["output_path"] is None:
            continue
        rows.append(
            {
                "tail_id": str(row["tail_id"]),
                "flight_id": str(row["flight_id"]),
                "output_path": str(row["output_path"]),
            }
        )
    return rows


def _set_pipeline_env(
    run_dir: Path,
    raw_input_path: str,
    table_format: str,
    write_mode: str,
    min_warm: int,
) -> None:
    os.environ["S3NTINEL_RAW_INPUT_PATH"] = str(raw_input_path)
    os.environ["S3NTINEL_RAW_TABLE_PATH"] = str(run_dir / "delta" / "raw_telemetry")
    os.environ["S3NTINEL_EVENTS_TABLE_PATH"] = str(run_dir / "delta" / "events")
    os.environ["S3NTINEL_WINDOWS_TABLE_PATH"] = str(run_dir / "delta" / "windows")
    os.environ["S3NTINEL_PHASE_WINDOWS_TABLE_PATH"] = str(run_dir / "delta" / "phase_windows")
    os.environ["S3NTINEL_PHASE_BASELINES_TABLE_PATH"] = str(run_dir / "delta" / "phase_baselines")
    os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH"] = str(run_dir / "delta" / "hierarchy_sensor_map")
    os.environ["S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH"] = str(run_dir / "delta" / "window_scores_raw")
    os.environ["S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH"] = str(run_dir / "delta" / "window_scores_calibrated")
    os.environ["S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH"] = str(run_dir / "delta" / "anomaly_window_attribution")
    os.environ["S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH"] = str(run_dir / "delta" / "anomaly_telemetry_attribution")
    os.environ["S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH"] = str(run_dir / "delta" / "anomaly_event_attribution")
    os.environ["S3NTINEL_TABLE_FORMAT"] = str(table_format)
    os.environ["S3NTINEL_RAW_OUTPUT_FORMAT"] = str(table_format)
    os.environ["S3NTINEL_WRITE_MODE"] = str(write_mode)
    os.environ["S3NTINEL_MIN_WARM"] = str(int(min_warm))


def _run_pipeline_for_row(row: dict[str, str], args: argparse.Namespace) -> None:
    pipeline_dir = Path(__file__).resolve().parent.parent / "pipelines"
    run_dir = Path(args.jobs_base_dir) / f"tail_id={row['tail_id']}" / f"flight_id={row['flight_id']}"
    run_dir.mkdir(parents=True, exist_ok=True)

    _set_pipeline_env(
        run_dir=run_dir,
        raw_input_path=row["output_path"],
        table_format=args.table_format,
        write_mode=args.write_mode,
        min_warm=args.min_warm,
    )

    for stage_script in PIPELINE_STAGE_SCRIPTS:
        stage_path = pipeline_dir / stage_script
        print(f"[manifest-jobs] tail={row['tail_id']} flight={row['flight_id']} stage={stage_script}")
        if args.dry_run:
            continue
        runpy.run_path(str(stage_path), run_name="__main__")


def _run_grouped_pipeline_for_row(row: dict[str, str], args: argparse.Namespace) -> None:
    pipeline_dir = Path(__file__).resolve().parent.parent / "pipelines"
    run_dir = Path(args.jobs_base_dir) / f"tail_id={row['tail_id']}" / f"flight_id={row['flight_id']}"
    run_dir.mkdir(parents=True, exist_ok=True)

    _set_pipeline_env(
        run_dir=run_dir,
        raw_input_path=row["output_path"],
        table_format=args.table_format,
        write_mode=args.write_mode,
        min_warm=args.min_warm,
    )

    for grouped_script in GROUPED_PIPELINE_SCRIPTS:
        grouped_path = pipeline_dir / grouped_script
        print(f"[manifest-jobs] tail={row['tail_id']} flight={row['flight_id']} grouped={grouped_script}")
        if args.dry_run:
            continue
        runpy.run_path(str(grouped_path), run_name="__main__")


def _run_custom_for_row(row: dict[str, str], args: argparse.Namespace) -> None:
    if not args.command:
        raise ValueError("--command is required when --job custom")

    run_dir = Path(args.jobs_base_dir) / f"tail_id={row['tail_id']}" / f"flight_id={row['flight_id']}"
    run_dir.mkdir(parents=True, exist_ok=True)

    placeholders: dict[str, Any] = {
        "tail_id": row["tail_id"],
        "flight_id": row["flight_id"],
        "output_path": row["output_path"],
        "run_dir": str(run_dir),
    }

    for template in args.command:
        command = str(template).format(**placeholders)
        print(f"[manifest-jobs] tail={row['tail_id']} flight={row['flight_id']} command={command}")
        if args.dry_run:
            continue
        subprocess.run(command, shell=True, check=True)


def main() -> None:
    args = parse_args()
    rows = _rows_from_manifest(
        manifest_path=args.partition_manifest_path,
        manifest_format=args.manifest_format,
        tail_id=args.tail_id,
        flight_id=args.flight_id,
        limit=args.limit,
    )
    if not rows:
        raise ValueError("no manifest rows selected")

    completed = 0
    failures = 0
    for row in rows:
        try:
            if args.job == "pipeline":
                _run_pipeline_for_row(row, args)
            elif args.job == "grouped":
                _run_grouped_pipeline_for_row(row, args)
            else:
                _run_custom_for_row(row, args)
            completed += 1
        except Exception as exc:
            failures += 1
            print(
                "[manifest-jobs] FAILED "
                f"tail={row['tail_id']} flight={row['flight_id']} error={exc.__class__.__name__}: {exc}"
            )
            if not args.continue_on_error:
                raise

    print("[manifest-jobs] summary")
    print(f"- selected_rows: {len(rows)}")
    print(f"- completed_rows: {completed}")
    print(f"- failed_rows: {failures}")


if __name__ == "__main__":
    main()
