"""Run grouped fitting+inference jobs for the XL 9x9 evaluation dataset manifest."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run grouped jobs for XL 9x9 evaluation manifest")
    parser.add_argument("--base-dir", default="data/evaluation/xl_9x9")
    parser.add_argument("--manifest-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--table-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--jobs-base-dir", default=None, help="Override output jobs base dir")
    parser.add_argument("--tail-id", default=None)
    parser.add_argument("--flight-id", default=None)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--min-warm", type=int, default=1)
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)
    manifest_path = base_dir / "synthetic" / "_partition_manifest"
    jobs_base_dir = Path(args.jobs_base_dir) if args.jobs_base_dir else (base_dir / "jobs_grouped")

    command = [
        sys.executable,
        "-m",
        "scripts.run_partition_manifest_jobs",
        "--partition-manifest-path",
        str(manifest_path),
        "--manifest-format",
        args.manifest_format,
        "--job",
        "grouped",
        "--jobs-base-dir",
        str(jobs_base_dir),
        "--table-format",
        args.table_format,
        "--write-mode",
        "overwrite",
        "--min-warm",
        str(args.min_warm),
        "--limit",
        str(args.limit),
    ]

    if args.tail_id:
        command.extend(["--tail-id", args.tail_id])
    if args.flight_id:
        command.extend(["--flight-id", args.flight_id])
    if args.continue_on_error:
        command.append("--continue-on-error")
    if args.dry_run:
        command.append("--dry-run")

    subprocess.run(command, check=True)


if __name__ == "__main__":
    main()
