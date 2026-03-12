# File: scripts/generate_sample_data.py
"""Generate deterministic sample parquet/Delta data for pipeline smoke tests."""

from __future__ import annotations

import argparse

from libs.io.delta import get_spark
from libs.testing.seed import seed_sample_dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate S3NTINEL sample test data")
    parser.add_argument("--base-dir", default="data", help="Base directory for generated input/delta data")
    parser.add_argument("--mode", default="overwrite", choices=["overwrite", "append"], help="Spark write mode")
    parser.add_argument("--format", default="delta", choices=["delta", "parquet"], help="Table format for intermediate outputs")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("s3ntinel.generate_sample_data")
    paths = seed_sample_dataset(
        spark=spark,
        base_dir=args.base_dir,
        mode=args.mode,
        table_format=args.format,
    )
    print("Sample dataset generated:")
    for key, value in paths.items():
        print(f"- {key}: {value}")


if __name__ == "__main__":
    main()
