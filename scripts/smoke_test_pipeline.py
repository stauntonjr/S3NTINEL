# File: scripts/smoke_test_pipeline.py
"""Run an end-to-end local smoke test for the active V2 pipeline using sample data."""

from __future__ import annotations

import argparse
import json
import os
import runpy
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from libs.graph import validate_hierarchy_recovery
from libs.io.delta import get_spark, read_table
from libs.io.schemas import ACTIVE_V2_TABLES
from libs.phase import validate_detected_phases_from_tables
from libs.testing.seed import seed_sample_dataset

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
    return parser.parse_args()


def set_env_paths(base_dir: str, table_format: str, write_mode: str, min_warm: int) -> None:
    base = Path(base_dir)
    os.environ["S3NTINEL_RAW_INPUT_PATH"] = str(base / "input" / "raw_telemetry")
    os.environ["S3NTINEL_RAW_TABLE_PATH"] = str(base / "delta" / "raw_telemetry")
    os.environ["S3NTINEL_PARAMETER_DATATYPE_PROFILE_TABLE_PATH"] = str(base / "delta" / "parameter_datatype_profile")
    os.environ["S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH"] = str(base / "delta" / "continuous_scaling_profile")
    os.environ["S3NTINEL_PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_TABLE_PATH"] = str(base / "delta" / "parameter_behavior_primitive_profile")
    os.environ["S3NTINEL_PARAMETER_BEHAVIOR_PROFILE_TABLE_PATH"] = str(base / "delta" / "parameter_behavior_profile")
    os.environ["S3NTINEL_EVENTS_TABLE_PATH"] = str(base / "delta" / "events")
    os.environ["S3NTINEL_WINDOW_POLICY_PROFILE_TABLE_PATH"] = str(base / "delta" / "window_policy_profile")
    os.environ["S3NTINEL_WINDOWS_TABLE_PATH"] = str(base / "delta" / "windows")
    os.environ["S3NTINEL_PHASE_LABELS_TABLE_PATH"] = str(base / "delta" / "phase_labels")
    os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_LABEL_TABLE_PATH"] = str(base / "delta" / "hierarchy_sensor_map_label")
    os.environ["S3NTINEL_BACKBONE_TABLE_PATH"] = str(base / "delta" / "backbone")
    os.environ["S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH"] = str(base / "delta" / "backbone_sensor_energy")
    os.environ["S3NTINEL_PRECISION_GRAPH_TABLE_PATH"] = str(base / "delta" / "precision_graph")
    os.environ["S3NTINEL_EVENT_GRAPH_TABLE_PATH"] = str(base / "delta" / "event_graph")
    os.environ["S3NTINEL_LAG_PROFILE_TABLE_PATH"] = str(base / "delta" / "lag_profile")
    os.environ["S3NTINEL_LAG_GRAPH_TABLE_PATH"] = str(base / "delta" / "lag_graph")
    os.environ["S3NTINEL_TRANSITION_GRAPH_TABLE_PATH"] = str(base / "delta" / "transition_graph")
    os.environ["S3NTINEL_FUSED_GRAPH_TABLE_PATH"] = str(base / "delta" / "fused_graph")
    os.environ["S3NTINEL_PHASE_WINDOWS_TABLE_PATH"] = str(base / "delta" / "phase_windows")
    os.environ["S3NTINEL_PHASE_BASELINES_TABLE_PATH"] = str(base / "delta" / "phase_baselines")
    os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH"] = str(base / "delta" / "hierarchy_sensor_map")
    os.environ["S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH"] = str(base / "delta" / "window_scores_raw")
    os.environ["S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH"] = str(base / "delta" / "window_scores_calibrated")
    os.environ["S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH"] = str(base / "delta" / "anomaly_window_attribution")
    os.environ["S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH"] = str(base / "delta" / "anomaly_telemetry_attribution")
    os.environ["S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH"] = str(base / "delta" / "anomaly_event_attribution")
    os.environ["S3NTINEL_TABLE_FORMAT"] = table_format
    os.environ["S3NTINEL_RAW_OUTPUT_FORMAT"] = table_format
    os.environ["S3NTINEL_WRITE_MODE"] = "overwrite" if write_mode == "merge" else write_mode
    os.environ["S3NTINEL_MIN_WARM"] = str(min_warm)


def run_stages(stage_80_write_mode: str) -> None:
    pipeline_dir = Path(__file__).resolve().parent.parent / "pipelines"
    stage_scripts = [
        "00_ingest_raw.py",
        "10_parameter_profiles_fit.py",
        "20_events_extract.py",
        "25_window_policy_profile.py",
        "30_windows_adaptive.py",
        "40_backbone_fit.py",
        "50_build_graph.py",
        "70_phase_fit.py",
        "80_window_scores_raw.py",
        "85_window_scores_calibrate.py",
        "90_anomaly_attribution.py",
    ]

    for stage_script in stage_scripts:
        if stage_script == "90_anomaly_attribution.py":
            os.environ["S3NTINEL_WRITE_MODE"] = stage_80_write_mode
        stage_path = pipeline_dir / stage_script
        print(f"[smoke] running {stage_script}")
        runpy.run_path(str(stage_path), run_name="__main__")


def print_row_counts(spark: "SparkSession", table_format: str) -> None:
    table_paths = {
        "raw_telemetry": os.environ["S3NTINEL_RAW_TABLE_PATH"],
        "parameter_datatype_profile": os.environ["S3NTINEL_PARAMETER_DATATYPE_PROFILE_TABLE_PATH"],
        "continuous_scaling_profile": os.environ["S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH"],
        "parameter_behavior_primitive_profile": os.environ["S3NTINEL_PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_TABLE_PATH"],
        "parameter_behavior_profile": os.environ["S3NTINEL_PARAMETER_BEHAVIOR_PROFILE_TABLE_PATH"],
        "events": os.environ["S3NTINEL_EVENTS_TABLE_PATH"],
        "window_policy_profile": os.environ["S3NTINEL_WINDOW_POLICY_PROFILE_TABLE_PATH"],
        "windows": os.environ["S3NTINEL_WINDOWS_TABLE_PATH"],
        "phase_labels": os.environ["S3NTINEL_PHASE_LABELS_TABLE_PATH"],
        "hierarchy_sensor_map_label": os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_LABEL_TABLE_PATH"],
        "backbone": os.environ["S3NTINEL_BACKBONE_TABLE_PATH"],
        "backbone_sensor_energy": os.environ["S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH"],
        "precision_graph": os.environ["S3NTINEL_PRECISION_GRAPH_TABLE_PATH"],
        "event_graph": os.environ["S3NTINEL_EVENT_GRAPH_TABLE_PATH"],
        "lag_profile": os.environ["S3NTINEL_LAG_PROFILE_TABLE_PATH"],
        "lag_graph": os.environ["S3NTINEL_LAG_GRAPH_TABLE_PATH"],
        "transition_graph": os.environ["S3NTINEL_TRANSITION_GRAPH_TABLE_PATH"],
        "fused_graph": os.environ["S3NTINEL_FUSED_GRAPH_TABLE_PATH"],
        "hierarchy_sensor_map": os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH"],
        "phase_windows": os.environ["S3NTINEL_PHASE_WINDOWS_TABLE_PATH"],
        "phase_baselines": os.environ["S3NTINEL_PHASE_BASELINES_TABLE_PATH"],
        "window_scores_raw": os.environ["S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH"],
        "window_scores_calibrated": os.environ["S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH"],
        "anomaly_window_attribution": os.environ["S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH"],
        "anomaly_telemetry_attribution": os.environ["S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH"],
        "anomaly_event_attribution": os.environ["S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH"],
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

    anomaly_window_attribution_path = os.environ["S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH"]
    anomaly_window_attribution_df = read_table(spark, path=anomaly_window_attribution_path, fmt=table_format)
    row_count = anomaly_window_attribution_df.count()
    if row_count <= 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: no anomaly rows emitted")

    duplicate_key_rows = (
        anomaly_window_attribution_df.groupBy("tail_id", "flight_id", "win_id").agg(F.count(F.lit(1)).alias("n")).where(F.col("n") > F.lit(1)).count()
    )
    if duplicate_key_rows > 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: duplicate anomaly merge keys found")

    panel_rows = anomaly_window_attribution_df.where(F.col("panel_context").isNotNull()).count()
    if panel_rows <= 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: panel_context is null for all rows")

    top_sensor_rows = anomaly_window_attribution_df.where(F.expr("exists(subsystems, s -> size(s.top_sensors) > 0)")).count()
    if top_sensor_rows <= 0:
        raise SystemExit("[smoke] anomaly quality assertion failed: no subsystem top_sensors populated")

    if str(write_mode).lower() == "merge":
        stage_80_path = Path(__file__).resolve().parent.parent / "pipelines" / "90_anomaly_attribution.py"
        os.environ["S3NTINEL_WRITE_MODE"] = "merge"
        runpy.run_path(str(stage_80_path), run_name="__main__")
        anomaly_window_attribution_df_post = read_table(spark, path=anomaly_window_attribution_path, fmt=table_format)
        post_count = anomaly_window_attribution_df_post.count()
        if post_count != row_count:
            raise SystemExit(
                f"[smoke] anomaly quality assertion failed: merge mode idempotence violated (before={row_count}, after={post_count})"
            )
        duplicate_key_rows_post = (
            anomaly_window_attribution_df_post.groupBy("tail_id", "flight_id", "win_id")
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


def assert_active_v2_table_contracts(spark: "SparkSession", table_format: str) -> None:
    path_by_table = {
        "events": os.environ["S3NTINEL_EVENTS_TABLE_PATH"],
        "window_policy_profile": os.environ["S3NTINEL_WINDOW_POLICY_PROFILE_TABLE_PATH"],
        "parameter_datatype_profile": os.environ["S3NTINEL_PARAMETER_DATATYPE_PROFILE_TABLE_PATH"],
        "continuous_scaling_profile": os.environ["S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH"],
        "parameter_behavior_primitive_profile": os.environ["S3NTINEL_PARAMETER_BEHAVIOR_PRIMITIVE_PROFILE_TABLE_PATH"],
        "parameter_behavior_profile": os.environ["S3NTINEL_PARAMETER_BEHAVIOR_PROFILE_TABLE_PATH"],
        "windows": os.environ["S3NTINEL_WINDOWS_TABLE_PATH"],
        "backbone": os.environ["S3NTINEL_BACKBONE_TABLE_PATH"],
        "backbone_sensor_energy": os.environ["S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH"],
        "precision_graph": os.environ["S3NTINEL_PRECISION_GRAPH_TABLE_PATH"],
        "event_graph": os.environ["S3NTINEL_EVENT_GRAPH_TABLE_PATH"],
        "lag_profile": os.environ["S3NTINEL_LAG_PROFILE_TABLE_PATH"],
        "lag_graph": os.environ["S3NTINEL_LAG_GRAPH_TABLE_PATH"],
        "transition_graph": os.environ["S3NTINEL_TRANSITION_GRAPH_TABLE_PATH"],
        "fused_graph": os.environ["S3NTINEL_FUSED_GRAPH_TABLE_PATH"],
        "hierarchy_sensor_map": os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH"],
        "phase_windows": os.environ["S3NTINEL_PHASE_WINDOWS_TABLE_PATH"],
        "phase_baselines": os.environ["S3NTINEL_PHASE_BASELINES_TABLE_PATH"],
        "window_scores_raw": os.environ["S3NTINEL_WINDOW_SCORES_RAW_TABLE_PATH"],
        "window_scores_calibrated": os.environ["S3NTINEL_WINDOW_SCORES_CALIBRATED_TABLE_PATH"],
        "anomaly_window_attribution": os.environ["S3NTINEL_ANOMALY_WINDOW_ATTRIBUTION_TABLE_PATH"],
        "anomaly_telemetry_attribution": os.environ["S3NTINEL_ANOMALY_TELEMETRY_ATTRIBUTION_TABLE_PATH"],
        "anomaly_event_attribution": os.environ["S3NTINEL_ANOMALY_EVENT_ATTRIBUTION_TABLE_PATH"],
    }
    for table_name, required_columns in ACTIVE_V2_TABLES.items():
        df = read_table(spark, path=path_by_table[table_name], fmt=table_format)
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            raise SystemExit(
                f"[smoke] active contract assertion failed: {table_name} missing columns {missing_columns}"
            )
    print("[smoke] active V2 table contract assertions passed")


def write_quality_report(spark: "SparkSession", base_dir: str, table_format: str) -> None:
    report: dict[str, object] = {}

    phase_labels_path = Path(os.environ["S3NTINEL_PHASE_LABELS_TABLE_PATH"])
    if phase_labels_path.exists():
        phase_windows_pdf = read_table(
            spark,
            path=os.environ["S3NTINEL_PHASE_WINDOWS_TABLE_PATH"],
            fmt=table_format,
        ).toPandas()
        phase_labels_pdf = read_table(
            spark,
            path=os.environ["S3NTINEL_PHASE_LABELS_TABLE_PATH"],
            fmt=table_format,
        ).toPandas()
        windows_pdf = read_table(
            spark,
            path=os.environ["S3NTINEL_WINDOWS_TABLE_PATH"],
            fmt=table_format,
        ).toPandas()
        report["phase_detection"] = validate_detected_phases_from_tables(
            phase_windows_df=phase_windows_pdf,
            phase_labels_df=phase_labels_pdf,
            windows_df=windows_pdf,
        )

    hierarchy_label_path = Path(os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_LABEL_TABLE_PATH"])
    if hierarchy_label_path.exists():
        hierarchy_sensor_map_pdf = read_table(
            spark,
            path=os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH"],
            fmt=table_format,
        ).toPandas()
        hierarchy_label_pdf = read_table(
            spark,
            path=os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_LABEL_TABLE_PATH"],
            fmt=table_format,
        ).toPandas()
        report["hierarchy_recovery"] = validate_hierarchy_recovery(
            hierarchy_sensor_map_df=hierarchy_sensor_map_pdf,
            hierarchy_label_df=hierarchy_label_pdf,
        )

    report_path = Path(base_dir) / "reports" / "smoke_quality_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n[smoke] quality_report: {report_path}")
    phase_detection = report.get("phase_detection")
    if isinstance(phase_detection, dict):
        print(f"- phase_detection.overall_accuracy: {phase_detection.get('overall_accuracy')}")
    hierarchy_recovery = report.get("hierarchy_recovery")
    if isinstance(hierarchy_recovery, dict):
        print(f"- hierarchy_recovery.system_exact_match: {hierarchy_recovery.get('system_exact_match')}")
        print(f"- hierarchy_recovery.subsystem_exact_match: {hierarchy_recovery.get('subsystem_exact_match')}")
        print(f"- hierarchy_recovery.module_exact_match: {hierarchy_recovery.get('module_exact_match')}")
        subsystem_partition = hierarchy_recovery.get("subsystem_partition")
        if isinstance(subsystem_partition, dict):
            print(f"- hierarchy_recovery.subsystem_partition.same_cluster_pair_f1: {subsystem_partition.get('same_cluster_pair_f1')}")
            print(
                f"- hierarchy_recovery.subsystem_partition.adjusted_rand_index: "
                f"{subsystem_partition.get('adjusted_rand_index')}"
            )


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
        include_intermediate_tables=True,
    )

    run_stages(stage_80_write_mode=args.write_mode)

    print_row_counts(spark, table_format=args.format)
    assert_active_v2_table_contracts(spark=spark, table_format=args.format)
    assert_anomaly_payload_quality(spark=spark, table_format=args.format, write_mode=args.write_mode)
    write_quality_report(spark=spark, base_dir=args.base_dir, table_format=args.format)
    print("\n[smoke] completed successfully")


if __name__ == "__main__":
    main()
