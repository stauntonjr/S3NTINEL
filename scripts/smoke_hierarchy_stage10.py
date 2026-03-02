"""Run stage-10 hierarchy build smoke check and print hierarchy diagnostics."""

from __future__ import annotations

import argparse
import json
import os
import runpy
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from libs.io.delta import get_spark, read_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke-check stage-10 hierarchy outputs")
    parser.add_argument("--skip-run-stage10", action="store_true", help="Skip running stage-10 and inspect existing outputs")
    parser.add_argument("--raw-table-path", default=None, help="Optional override for stage-10 input raw table path")
    parser.add_argument("--table-format", default="parquet", choices=["parquet", "delta"], help="Table format for read/write")
    parser.add_argument(
        "--hierarchy-nodes-path",
        default=None,
        help="Hierarchy nodes table path (defaults to env or data/delta/hierarchy_nodes)",
    )
    parser.add_argument(
        "--hierarchy-sensor-map-path",
        default=None,
        help="Hierarchy sensor map path (defaults to env or data/delta/sensor_hierarchy_map)",
    )
    parser.add_argument("--sample-path-count", type=int, default=10, help="How many sample hierarchy paths to print")
    parser.add_argument("--output-json", default=None, help="Optional path to write summary JSON")
    return parser.parse_args()


def _print_path_samples(sensor_map_df, sample_count: int) -> list[str]:
    rows = (
        sensor_map_df.select("sensor", "system_id", "subsystem_id", "module_id")
        .where("sensor is not null and system_id is not null and subsystem_id is not null and module_id is not null")
        .orderBy("system_id", "subsystem_id", "module_id", "sensor")
        .limit(max(int(sample_count), 1))
        .collect()
    )
    out: list[str] = []
    for row in rows:
        sensor = str(row["sensor"])
        system_id = str(row["system_id"])
        subsystem_id = str(row["subsystem_id"])
        module_id = str(row["module_id"])
        out.append(f"GLOBAL -> {system_id} -> {subsystem_id} -> {module_id} -> SENSOR::{sensor}")
    return out


def _normalize_sensor_map_columns(sensor_map_df):
    from pyspark.sql import functions as F

    columns = set(sensor_map_df.columns)
    if "sensor" in columns:
        sensor_col = F.col("sensor")
    elif "parameter_name" in columns:
        sensor_col = F.col("parameter_name")
    else:
        raise ValueError("Hierarchy sensor map must contain either 'sensor' or 'parameter_name' column")

    required = {"system_id", "subsystem_id", "module_id"}
    missing = [name for name in sorted(required) if name not in columns]
    if missing:
        raise ValueError(f"Hierarchy sensor map missing required columns: {', '.join(missing)}")

    return sensor_map_df.select(
        sensor_col.cast("string").alias("sensor"),
        F.col("system_id").cast("string").alias("system_id"),
        F.col("subsystem_id").cast("string").alias("subsystem_id"),
        F.col("module_id").cast("string").alias("module_id"),
    )


def main() -> None:
    args = parse_args()

    if not args.skip_run_stage10:
        os.environ["S3NTINEL_TABLE_FORMAT"] = str(args.table_format)
        if args.raw_table_path:
            os.environ["S3NTINEL_RAW_TABLE_PATH"] = str(args.raw_table_path)
        runpy.run_module("pipelines.10_cur_backbone_fit", run_name="__main__")

    hierarchy_nodes_path = args.hierarchy_nodes_path or os.getenv(
        "S3NTINEL_HIERARCHY_NODES_TABLE_PATH", "data/delta/hierarchy_nodes"
    )
    hierarchy_sensor_map_path = args.hierarchy_sensor_map_path or os.getenv(
        "S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH", "data/delta/sensor_hierarchy_map"
    )

    spark = get_spark("s3ntinel.smoke_hierarchy_stage10")
    nodes_df = read_table(spark, hierarchy_nodes_path, fmt=args.table_format)
    sensor_map_df = _normalize_sensor_map_columns(read_table(spark, hierarchy_sensor_map_path, fmt=args.table_format))

    node_counts = {
        "global": int(nodes_df.where("node_type = 'global'").count()),
        "system": int(nodes_df.where("node_type = 'system'").count()),
        "subsystem": int(nodes_df.where("node_type = 'subsystem'").count()),
        "module": int(nodes_df.where("node_type = 'module'").count()),
        "sensor": int(nodes_df.where("node_type = 'sensor'").count()),
    }

    map_counts = {
        "sensor_rows": int(sensor_map_df.count()),
        "systems": int(sensor_map_df.select("system_id").distinct().count()),
        "subsystems": int(sensor_map_df.select("subsystem_id").distinct().count()),
        "modules": int(sensor_map_df.select("module_id").distinct().count()),
    }

    null_rows = int(
        sensor_map_df.where("sensor is null or system_id is null or subsystem_id is null or module_id is null").count()
    )

    sample_paths = _print_path_samples(sensor_map_df, sample_count=args.sample_path_count)

    summary = {
        "hierarchy_nodes_path": hierarchy_nodes_path,
        "hierarchy_sensor_map_path": hierarchy_sensor_map_path,
        "table_format": args.table_format,
        "node_counts": node_counts,
        "map_counts": map_counts,
        "sensor_map_null_rows": null_rows,
        "sample_paths": sample_paths,
    }

    print("Hierarchy smoke summary")
    print(f"- hierarchy_nodes_path: {hierarchy_nodes_path}")
    print(f"- hierarchy_sensor_map_path: {hierarchy_sensor_map_path}")
    print(f"- node_counts: {node_counts}")
    print(f"- map_counts: {map_counts}")
    print(f"- sensor_map_null_rows: {null_rows}")
    print("- sample_paths:")
    if sample_paths:
        for path in sample_paths:
            print(f"  - {path}")
    else:
        print("  - none")

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(f"- output_json: {output_path}")


if __name__ == "__main__":
    main()
