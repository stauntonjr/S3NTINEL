"""Generate synthetic hierarchy artifacts for simulation-only correlation injection."""

from __future__ import annotations

import argparse
from pathlib import Path

from libs.io.delta import get_spark, read_table
from libs.profiling.hierarchy_synth import HierarchyShape, synthesize_hierarchy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic hierarchy profile artifacts")
    parser.add_argument("--profile-parameter-profile-path", required=True, help="Path to parameter_profile table")
    parser.add_argument("--profile-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--output-dir", default="data/profile_hierarchy")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--hierarchy-profile-id", default="HIER_SYNTH_V1")
    parser.add_argument("--system-count", type=int, default=3)
    parser.add_argument("--subsystems-per-system", type=int, default=2)
    parser.add_argument("--modules-per-subsystem", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-profile-params", type=int, default=1000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("s3ntinel.generate_synthetic_hierarchy_profile")

    profile_df = read_table(spark, args.profile_parameter_profile_path, fmt=args.profile_format)
    parameter_names = [
        str(row["parameter_name"])
        for row in profile_df.select("parameter_name").distinct().limit(int(args.max_profile_params)).collect()
        if row["parameter_name"] is not None
    ]
    if not parameter_names:
        raise ValueError("parameter_profile has no parameter_name rows")

    node_rows, edge_rows, sensor_map_rows = synthesize_hierarchy(
        parameter_names=parameter_names,
        hierarchy_profile_id=str(args.hierarchy_profile_id),
        shape=HierarchyShape(
            system_count=max(int(args.system_count), 1),
            subsystems_per_system=max(int(args.subsystems_per_system), 1),
            modules_per_subsystem=max(int(args.modules_per_subsystem), 1),
        ),
        seed=int(args.seed),
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    nodes_path = str(output_dir / "hierarchy_nodes")
    edges_path = str(output_dir / "hierarchy_edges")
    sensor_map_path = str(output_dir / "sensor_hierarchy_map")

    spark.createDataFrame(node_rows).write.format(args.output_format).mode("overwrite").save(nodes_path)
    spark.createDataFrame(edge_rows).write.format(args.output_format).mode("overwrite").save(edges_path)
    spark.createDataFrame(sensor_map_rows).write.format(args.output_format).mode("overwrite").save(sensor_map_path)

    print("Synthetic hierarchy artifacts written:")
    print(f"- hierarchy_profile_id: {args.hierarchy_profile_id}")
    print(f"- hierarchy_nodes: {nodes_path}")
    print(f"- hierarchy_edges: {edges_path}")
    print(f"- sensor_hierarchy_map: {sensor_map_path}")
    print("- hierarchy_source: synthetic_injected")
    print("- caution: keep synthetic hierarchy artifacts isolated from discovered hierarchy outputs")


if __name__ == "__main__":
    main()
