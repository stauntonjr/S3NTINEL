"""Run a small local sweep of graph/hierarchy configurations against the smoke pipeline."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


DEFAULT_CONFIGS = [
    {
        "name": "baseline",
        "env": {},
    },
    {
        "name": "conservative",
        "env": {
            "S3NTINEL_V2_EVENT_GRAPH_TOP_K_PER_SENSOR": "3",
            "S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING": "2",
            "S3NTINEL_V2_GRAPH_ALPHA": "1.0",
            "S3NTINEL_V2_GRAPH_BETA": "1.0",
            "S3NTINEL_V2_GRAPH_GAMMA": "1.0",
            "S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR": "2",
            "S3NTINEL_V2_HIERARCHY_SUBSYSTEM_MIN_EDGE_WEIGHT": "0.8",
            "S3NTINEL_V2_HIERARCHY_SYSTEM_MIN_EDGE_WEIGHT": "0.6",
        },
    },
    {
        "name": "event_lag_heavy",
        "env": {
            "S3NTINEL_V2_EVENT_GRAPH_TOP_K_PER_SENSOR": "5",
            "S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING": "3",
            "S3NTINEL_V2_GRAPH_ALPHA": "0.75",
            "S3NTINEL_V2_GRAPH_BETA": "1.25",
            "S3NTINEL_V2_GRAPH_GAMMA": "1.25",
            "S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR": "2",
            "S3NTINEL_V2_HIERARCHY_SUBSYSTEM_MIN_EDGE_WEIGHT": "0.8",
            "S3NTINEL_V2_HIERARCHY_SYSTEM_MIN_EDGE_WEIGHT": "0.6",
        },
    },
    {
        "name": "precision_heavy",
        "env": {
            "S3NTINEL_V2_EVENT_GRAPH_TOP_K_PER_SENSOR": "5",
            "S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING": "3",
            "S3NTINEL_V2_GRAPH_ALPHA": "1.5",
            "S3NTINEL_V2_GRAPH_BETA": "0.75",
            "S3NTINEL_V2_GRAPH_GAMMA": "1.0",
            "S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR": "3",
            "S3NTINEL_V2_HIERARCHY_SUBSYSTEM_MIN_EDGE_WEIGHT": "0.7",
            "S3NTINEL_V2_HIERARCHY_SYSTEM_MIN_EDGE_WEIGHT": "0.5",
        },
    },
    {
        "name": "loose_rollup",
        "env": {
            "S3NTINEL_V2_EVENT_GRAPH_TOP_K_PER_SENSOR": "5",
            "S3NTINEL_V2_LAG_GRAPH_TOP_K_OUTGOING": "3",
            "S3NTINEL_V2_GRAPH_ALPHA": "1.5",
            "S3NTINEL_V2_GRAPH_BETA": "0.75",
            "S3NTINEL_V2_GRAPH_GAMMA": "1.0",
            "S3NTINEL_V2_HIERARCHY_TOP_K_PER_SENSOR": "3",
            "S3NTINEL_V2_HIERARCHY_SUBSYSTEM_MIN_EDGE_WEIGHT": "0.5",
            "S3NTINEL_V2_HIERARCHY_SYSTEM_MIN_EDGE_WEIGHT": "0.3",
        },
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep graph/hierarchy smoke configurations")
    parser.add_argument("--base-dir", default="data/smoke_sweep", help="Base directory for sweep outputs")
    parser.add_argument("--format", default="parquet", choices=["parquet", "delta"], help="Table format")
    parser.add_argument("--min-warm", default=1, type=int, help="Conformal min warm override")
    parser.add_argument("--tail-count", default=1, type=int, help="Synthetic smoke tail count")
    parser.add_argument("--flights-per-tail", default=1, type=int, help="Synthetic smoke flights per tail")
    parser.add_argument("--sensor-count", default=3, type=int, help="Synthetic smoke sensor count")
    parser.add_argument("--timestamp-count", default=12, type=int, help="Synthetic smoke timestamp count")
    parser.add_argument("--step-ms", default=100, type=int, help="Synthetic smoke timestep")
    parser.add_argument("--config", action="append", default=None, help="Optional config names to run")
    parser.add_argument(
        "--conda-env",
        default=None,
        help="Optional conda environment to use for each smoke run (for example: sentinel-spark35)",
    )
    parser.add_argument(
        "--python-executable",
        default=None,
        help="Optional explicit Python executable for each smoke run",
    )
    return parser.parse_args()


def _selected_configs(names: list[str] | None) -> list[dict[str, object]]:
    if not names:
        return list(DEFAULT_CONFIGS)
    wanted = set(names)
    configs = [config for config in DEFAULT_CONFIGS if str(config["name"]) in wanted]
    missing = sorted(wanted - {str(config["name"]) for config in configs})
    if missing:
        raise SystemExit(f"unknown config names: {', '.join(missing)}")
    return configs


def _run_one(
    *,
    config: dict[str, object],
    base_dir: Path,
    table_format: str,
    min_warm: int,
    tail_count: int,
    flights_per_tail: int,
    sensor_count: int,
    timestamp_count: int,
    step_ms: int,
    conda_env: str | None,
    python_executable: str | None,
) -> dict[str, object]:
    name = str(config["name"])
    run_dir = base_dir / name
    run_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["S3NTINEL_TABLE_FORMAT"] = table_format
    for key, value in dict(config["env"]).items():
        env[str(key)] = str(value)

    smoke_args = [
        "-m",
        "scripts.smoke_test_pipeline",
        "--base-dir",
        str(run_dir),
        "--format",
        table_format,
        "--min-warm",
        str(min_warm),
        "--tail-count",
        str(tail_count),
        "--flights-per-tail",
        str(flights_per_tail),
        "--sensor-count",
        str(sensor_count),
        "--timestamp-count",
        str(timestamp_count),
        "--step-ms",
        str(step_ms),
    ]
    if conda_env:
        cmd = ["conda", "run", "-n", conda_env, "python", *smoke_args]
    else:
        cmd = [python_executable or sys.executable, *smoke_args]
    subprocess.run(cmd, env=env, check=True)

    report_path = run_dir / "reports" / "smoke_quality_report.json"
    report = json.loads(report_path.read_text())
    phase = report.get("phase_detection", {})
    hierarchy = report.get("hierarchy_recovery", {})
    subsystem_partition = hierarchy.get("subsystem_partition", {})
    system_partition = hierarchy.get("system_partition", {})
    module_partition = hierarchy.get("module_partition", {})
    return {
        "name": name,
        "base_dir": str(run_dir),
        "phase_accuracy": phase.get("overall_accuracy"),
        "system_exact_match": hierarchy.get("system_exact_match"),
        "subsystem_exact_match": hierarchy.get("subsystem_exact_match"),
        "module_exact_match": hierarchy.get("module_exact_match"),
        "system_ari": system_partition.get("adjusted_rand_index"),
        "subsystem_ari": subsystem_partition.get("adjusted_rand_index"),
        "module_ari": module_partition.get("adjusted_rand_index"),
        "subsystem_pair_f1": subsystem_partition.get("same_cluster_pair_f1"),
        "env": dict(config["env"]),
    }


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, object]] = []
    for config in _selected_configs(args.config):
        result = _run_one(
            config=config,
            base_dir=base_dir,
            table_format=args.format,
            min_warm=args.min_warm,
            tail_count=args.tail_count,
            flights_per_tail=args.flights_per_tail,
            sensor_count=args.sensor_count,
            timestamp_count=args.timestamp_count,
            step_ms=args.step_ms,
            conda_env=args.conda_env,
            python_executable=args.python_executable,
        )
        results.append(result)
        print(
            json.dumps(
                {
                    "name": result["name"],
                    "phase_accuracy": result["phase_accuracy"],
                    "system_ari": result["system_ari"],
                    "subsystem_ari": result["subsystem_ari"],
                    "module_ari": result["module_ari"],
                    "subsystem_pair_f1": result["subsystem_pair_f1"],
                    "system_exact_match": result["system_exact_match"],
                }
            )
        )

    results = sorted(
        results,
        key=lambda item: (
            -(float(item["subsystem_ari"]) if item["subsystem_ari"] is not None else -1.0),
            -(float(item["system_ari"]) if item["system_ari"] is not None else -1.0),
            -(float(item["module_ari"]) if item["module_ari"] is not None else -1.0),
            -(float(item["subsystem_pair_f1"]) if item["subsystem_pair_f1"] is not None else -1.0),
            str(item["name"]),
        ),
    )
    summary_path = base_dir / "graph_hierarchy_sweep_summary.json"
    summary_path.write_text(json.dumps(results, indent=2))
    print(f"\nsummary: {summary_path}")


if __name__ == "__main__":
    main()
