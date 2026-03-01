# File: scripts/profile_telemetry.py
"""Profile telemetry parameters and build channel routing outputs."""

from __future__ import annotations

import argparse
from pathlib import Path

from libs.io.delta import get_spark, read_parquet, read_table
from libs.profiling.profile import build_categorical_distribution, build_parameter_profile
from libs.profiling.routing import build_channel_routing


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile telemetry and build channel routing")
    parser.add_argument("--input-path", required=True, help="Input telemetry path (parquet or delta)")
    parser.add_argument("--input-format", default="parquet", choices=["parquet", "delta"], help="Input format")
    parser.add_argument("--output-dir", default="data/profile", help="Output directory for profile artifacts")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "delta"], help="Output table format")
    parser.add_argument("--top-k", default=10, type=int, help="Top categorical values to retain per parameter")
    parser.add_argument("--numeric-ratio-threshold", default=0.8, type=float, help="Threshold for numeric type detection")
    parser.add_argument("--categorical-cardinality-max", default=200, type=int, help="Max cardinality treated as categorical")
    parser.add_argument("--high-refresh-hz", default=50.0, type=float, help="High-refresh routing threshold")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("s3ntinel.profile_telemetry")

    if args.input_format == "parquet":
        raw_df = read_parquet(spark, args.input_path)
    else:
        raw_df = read_table(spark, args.input_path, fmt=args.input_format)

    profile_df = build_parameter_profile(
        raw_input_df=raw_df,
        numeric_ratio_threshold=args.numeric_ratio_threshold,
        categorical_cardinality_max=args.categorical_cardinality_max,
    )
    category_df = build_categorical_distribution(raw_input_df=raw_df, top_k=args.top_k)
    routing_df = build_channel_routing(profile_df=profile_df, high_refresh_hz=args.high_refresh_hz)

    output_dir = Path(args.output_dir)
    profile_path = str(output_dir / "parameter_profile")
    category_path = str(output_dir / "categorical_distribution")
    routing_path = str(output_dir / "channel_routing")

    profile_df.write.format(args.output_format).mode("overwrite").save(profile_path)
    category_df.write.format(args.output_format).mode("overwrite").save(category_path)
    routing_df.write.format(args.output_format).mode("overwrite").save(routing_path)

    print("Profiling outputs written:")
    print(f"- parameter_profile: {profile_path}")
    print(f"- categorical_distribution: {category_path}")
    print(f"- channel_routing: {routing_path}")
    print(f"- profiled_parameters: {profile_df.count()}")


if __name__ == "__main__":
    main()
