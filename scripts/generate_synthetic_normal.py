# File: scripts/generate_synthetic_normal.py
"""Generate synthetic normal telemetry from defaults or sampled profile characteristics."""

from __future__ import annotations

import argparse

from libs.io.delta import get_spark, read_table
from libs.profiling.synthetic import ParameterSpec, default_parameter_specs, generate_synthetic_normal_telemetry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic normal telemetry")
    parser.add_argument("--output-path", default="data/synthetic/raw_telemetry", help="Output parquet path")
    parser.add_argument("--duration-seconds", default=300, type=int, help="Synthetic duration in seconds")
    parser.add_argument("--tail-id", default="SYN_T001", help="Synthetic tail id")
    parser.add_argument("--flight-id", default="SYN_F001", help="Synthetic flight id")
    parser.add_argument("--start-ts", default="2026-01-01T00:00:00+00:00", help="Synthetic start timestamp ISO8601")
    parser.add_argument("--seed", default=42, type=int, help="Random seed")
    parser.add_argument("--profile-path", default=None, help="Optional parameter profile path (parquet/delta)")
    parser.add_argument("--profile-format", default="parquet", choices=["parquet", "delta"], help="Profile table format")
    parser.add_argument("--max-profile-params", default=100, type=int, help="Max parameters loaded from profile")
    return parser.parse_args()


def _specs_from_profile(profile_df: "DataFrame", max_params: int) -> list[ParameterSpec]:
    rows = profile_df.limit(int(max_params)).collect()
    specs: list[ParameterSpec] = []
    for row in rows:
        dtype = str(row["detected_type"])
        rate_hz = float(row["sampling_rate_hz"] or 1.0)
        missing_rate = float(row["missing_rate"] or 0.0)
        parameter_name = str(row["parameter_name"])

        if dtype == "numeric":
            specs.append(
                ParameterSpec(
                    parameter_name=parameter_name,
                    detected_type="numeric",
                    sampling_rate_hz=max(rate_hz, 0.5),
                    mean=float(row["num_mean"] or 0.0),
                    std=max(float(row["num_std"] or 1.0), 1e-6),
                    min_value=float(row["num_min"]) if row["num_min"] is not None else None,
                    max_value=float(row["num_max"]) if row["num_max"] is not None else None,
                    missing_rate=missing_rate,
                )
            )
        elif dtype in {"binary", "categorical"}:
            categories = ("ON", "OFF") if dtype == "binary" else ("STATE_A", "STATE_B", "STATE_C")
            specs.append(
                ParameterSpec(
                    parameter_name=parameter_name,
                    detected_type=dtype,
                    sampling_rate_hz=max(rate_hz, 0.5),
                    categories=categories,
                    missing_rate=missing_rate,
                )
            )

    return specs if specs else default_parameter_specs()


def main() -> None:
    args = parse_args()
    spark = get_spark("s3ntinel.generate_synthetic_normal")

    specs = default_parameter_specs()
    if args.profile_path:
        profile_df = read_table(spark, args.profile_path, fmt=args.profile_format)
        specs = _specs_from_profile(profile_df=profile_df, max_params=args.max_profile_params)

    synthetic_df = generate_synthetic_normal_telemetry(
        spark=spark,
        duration_seconds=args.duration_seconds,
        tail_id=args.tail_id,
        flight_id=args.flight_id,
        start_ts=args.start_ts,
        specs=specs,
        seed=args.seed,
    )
    synthetic_df.write.mode("overwrite").parquet(args.output_path)

    print("Synthetic normal telemetry generated:")
    print(f"- output_path: {args.output_path}")
    print(f"- rows: {synthetic_df.count()}")
    print(f"- parameters: {len(specs)}")


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


if __name__ == "__main__":
    main()
