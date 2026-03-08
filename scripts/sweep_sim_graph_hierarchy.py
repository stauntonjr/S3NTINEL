"""Sweep graph/hierarchy configurations against the simulation evaluation runner."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from libs.graph import (
    build_graph_fusion_from_component_tables,
    retain_event_graph_top_k,
    retain_lag_graph_top_k,
)


DEFAULT_CONFIGS = [
    {
        "name": "baseline",
        "args": {},
    },
    {
        "name": "conservative",
        "args": {
            "graph_event_top_k_per_sensor": 3,
            "graph_lag_top_k_outgoing": 2,
            "graph_alpha": 1.0,
            "graph_beta": 1.0,
            "graph_gamma": 1.0,
            "graph_hierarchy_top_k_per_sensor": 2,
            "graph_hierarchy_subsystem_min_edge_weight": 0.8,
            "graph_hierarchy_system_min_edge_weight": 0.6,
        },
    },
    {
        "name": "event_lag_heavy",
        "args": {
            "graph_event_top_k_per_sensor": 5,
            "graph_lag_top_k_outgoing": 3,
            "graph_alpha": 0.75,
            "graph_beta": 1.25,
            "graph_gamma": 1.25,
            "graph_hierarchy_top_k_per_sensor": 2,
            "graph_hierarchy_subsystem_min_edge_weight": 0.8,
            "graph_hierarchy_system_min_edge_weight": 0.6,
        },
    },
    {
        "name": "precision_heavy",
        "args": {
            "graph_event_top_k_per_sensor": 5,
            "graph_lag_top_k_outgoing": 3,
            "graph_alpha": 1.5,
            "graph_beta": 0.75,
            "graph_gamma": 1.0,
            "graph_hierarchy_top_k_per_sensor": 3,
            "graph_hierarchy_subsystem_min_edge_weight": 0.7,
            "graph_hierarchy_system_min_edge_weight": 0.5,
        },
    },
    {
        "name": "loose_rollup",
        "args": {
            "graph_event_top_k_per_sensor": 5,
            "graph_lag_top_k_outgoing": 3,
            "graph_alpha": 1.5,
            "graph_beta": 0.75,
            "graph_gamma": 1.0,
            "graph_hierarchy_top_k_per_sensor": 3,
            "graph_hierarchy_subsystem_min_edge_weight": 0.5,
            "graph_hierarchy_system_min_edge_weight": 0.3,
        },
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sweep graph/hierarchy configurations against the simulation runner")
    parser.add_argument("--base-dir", default="data/sim_sweep", help="Base directory for sweep outputs")
    parser.add_argument("--tail-count", default=3, type=int, help="Tail count for simulation runs")
    parser.add_argument("--flights-per-tail", default=3, type=int, help="Flights per tail for simulation runs")
    parser.add_argument("--phase-count", default=99, type=int, help="Phase count passed to simulation runner")
    parser.add_argument("--seed", default=7, type=int, help="Simulation seed")
    parser.add_argument("--config", action="append", default=None, help="Optional config names to run")
    parser.add_argument(
        "--python-executable",
        default=None,
        help="Optional explicit Python executable to use for each run",
    )
    parser.add_argument(
        "--conda-env",
        default=None,
        help="Optional conda environment to use for each run (for example: sentinel-spark35)",
    )
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Ignore cached per-config reports and rerun the full simulation evaluation",
    )
    parser.add_argument(
        "--graph-cache-json",
        default=None,
        help="Optional graph cache JSON to reuse for hierarchy-only sweeps",
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


def _report_metrics(report: dict[str, Any]) -> dict[str, Any]:
    hierarchy_levels = dict(report.get("graph_v2", {}).get("hierarchy_recovery", {}).get("levels", {}))
    phase_eval = dict(report.get("phase_detection", {}).get("evaluation", {}))

    def _level_metric(level: str, metric: str) -> Any:
        return dict(hierarchy_levels.get(level, {})).get(metric)

    return {
        "phase_accuracy": phase_eval.get("overall_accuracy"),
        "system_exact_match": _level_metric("system_id", "exact_match_ratio"),
        "system_nmi": _level_metric("system_id", "nmi"),
        "subsystem_exact_match": _level_metric("subsystem_id", "exact_match_ratio"),
        "subsystem_nmi": _level_metric("subsystem_id", "nmi"),
        "module_exact_match": _level_metric("module_id", "exact_match_ratio"),
        "module_nmi": _level_metric("module_id", "nmi"),
    }


def _normalized_mutual_information(labels_true: list[str], labels_pred: list[str]) -> float | None:
    if len(labels_true) != len(labels_pred) or not labels_true:
        return None
    n = len(labels_true)
    true_counts: Counter[str] = Counter(str(item) for item in labels_true)
    pred_counts: Counter[str] = Counter(str(item) for item in labels_pred)
    joint_counts: Counter[tuple[str, str]] = Counter(zip((str(item) for item in labels_true), (str(item) for item in labels_pred), strict=False))

    mi = 0.0
    for (true_label, pred_label), joint_count in joint_counts.items():
        p_xy = float(joint_count) / float(n)
        p_x = float(true_counts[true_label]) / float(n)
        p_y = float(pred_counts[pred_label]) / float(n)
        if p_xy <= 0.0 or p_x <= 0.0 or p_y <= 0.0:
            continue
        mi += p_xy * np.log(p_xy / (p_x * p_y))

    h_true = 0.0
    for count in true_counts.values():
        p = float(count) / float(n)
        if p > 0.0:
            h_true -= p * np.log(p)

    h_pred = 0.0
    for count in pred_counts.values():
        p = float(count) / float(n)
        if p > 0.0:
            h_pred -= p * np.log(p)

    if h_true <= 0.0 or h_pred <= 0.0:
        return 1.0 if labels_true == labels_pred else 0.0
    return float(mi / np.sqrt(h_true * h_pred))


def _hierarchy_recovery_metrics(
    *,
    hierarchy_label_df: pd.DataFrame,
    hierarchy_pred_df: pd.DataFrame,
) -> dict[str, Any]:
    if hierarchy_label_df.empty or hierarchy_pred_df.empty:
        return {"parameter_count_compared": 0, "levels": {}}

    label_df = hierarchy_label_df.copy()
    pred_df = hierarchy_pred_df.copy()
    label_key = "parameter_name" if "parameter_name" in label_df.columns else "sensor"
    pred_key = "parameter_name" if "parameter_name" in pred_df.columns else "sensor"
    label_df["parameter_name"] = label_df[label_key].astype(str)
    pred_df["parameter_name"] = pred_df[pred_key].astype(str)
    merged = label_df.merge(
        pred_df[["parameter_name", "system_id", "subsystem_id", "module_id"]],
        on="parameter_name",
        how="inner",
        suffixes=("_label", "_detected"),
    )
    levels: dict[str, Any] = {}
    for level in ["system_id", "subsystem_id", "module_id"]:
        true_col = f"{level}_label"
        pred_col = f"{level}_detected"
        labels_true = [str(item) for item in merged[true_col].fillna("").tolist()]
        labels_pred = [str(item) for item in merged[pred_col].fillna("").tolist()]
        exact = 0.0
        if labels_true:
            exact = float(sum(1 for left, right in zip(labels_true, labels_pred, strict=False) if left == right)) / float(len(labels_true))
        levels[level] = {
            "nmi": _normalized_mutual_information(labels_true, labels_pred),
            "exact_match_ratio": exact,
            "label_cluster_count": int(len(set(labels_true) - {""})),
            "detected_cluster_count": int(len(set(labels_pred) - {""})),
        }
    return {
        "parameter_count_compared": int(len(merged)),
        "levels": levels,
    }


def _default_graph_cache_path(base_dir: Path) -> Path:
    return base_dir / "graph_artifact_cache.json"


def _run_from_graph_cache(
    *,
    config: dict[str, object],
    graph_cache_path: Path,
    base_dir: Path,
) -> dict[str, object]:
    name = str(config["name"])
    run_dir = base_dir / name
    run_dir.mkdir(parents=True, exist_ok=True)

    graph_cache = json.loads(graph_cache_path.read_text())
    backbone_df = pd.DataFrame(graph_cache.get("backbone", []))
    hierarchy_label_df = pd.DataFrame(graph_cache.get("hierarchy_labels", []))
    precision_df = pd.DataFrame(graph_cache.get("precision_graph", []))
    event_df = pd.DataFrame(graph_cache.get("event_graph", []))
    lag_df = pd.DataFrame(graph_cache.get("lag_graph", []))

    args_map = dict(config.get("args", {}))
    event_top_k = int(args_map.get("graph_event_top_k_per_sensor", 6))
    lag_top_k = int(args_map.get("graph_lag_top_k_outgoing", 4))
    filtered_event_df = retain_event_graph_top_k(event_df, top_k_per_sensor=event_top_k)
    filtered_lag_df = retain_lag_graph_top_k(lag_df, top_k_outgoing=lag_top_k)
    fused_df, hierarchy_df = build_graph_fusion_from_component_tables(
        precision_df,
        filtered_event_df,
        filtered_lag_df,
        backbone_df,
        alpha=float(args_map.get("graph_alpha", 1.0)),
        beta=float(args_map.get("graph_beta", 1.0)),
        gamma=float(args_map.get("graph_gamma", 1.0)),
        min_fused_edge_weight=float(args_map.get("graph_min_fused_edge_weight", 0.05)),
        hierarchy_top_k_per_sensor=max(int(args_map.get("graph_hierarchy_top_k_per_sensor", 3)), 1),
        hierarchy_subsystem_min_edge_weight=float(args_map.get("graph_hierarchy_subsystem_min_edge_weight", 0.7)),
        hierarchy_system_min_edge_weight=float(args_map.get("graph_hierarchy_system_min_edge_weight", 0.5)),
    )
    hierarchy_recovery = _hierarchy_recovery_metrics(
        hierarchy_label_df=hierarchy_label_df,
        hierarchy_pred_df=hierarchy_df,
    )
    level_metrics = hierarchy_recovery.get("levels", {})
    result = {
        "name": name,
        "base_dir": str(run_dir),
        "phase_accuracy": graph_cache.get("phase_accuracy"),
        "system_exact_match": dict(level_metrics.get("system_id", {})).get("exact_match_ratio"),
        "system_nmi": dict(level_metrics.get("system_id", {})).get("nmi"),
        "subsystem_exact_match": dict(level_metrics.get("subsystem_id", {})).get("exact_match_ratio"),
        "subsystem_nmi": dict(level_metrics.get("subsystem_id", {})).get("nmi"),
        "module_exact_match": dict(level_metrics.get("module_id", {})).get("exact_match_ratio"),
        "module_nmi": dict(level_metrics.get("module_id", {})).get("nmi"),
        "args": args_map,
    }
    graph_only_report = {
        "cache_source": str(graph_cache_path),
        "config": args_map,
        "phase_accuracy": result["phase_accuracy"],
        "graph_v2": {
            "precision_graph": {
                "edge_count": int(len(precision_df)),
            },
            "event_graph": {
                "edge_count": int(len(filtered_event_df)),
            },
            "lag_graph": {
                "edge_count": int(len(filtered_lag_df)),
            },
            "fused_graph": {
                "edge_count": int(len(fused_df)),
            },
            "hierarchy_sensor_map": json.loads(hierarchy_df.to_json(orient="records")),
            "hierarchy_recovery": hierarchy_recovery,
        },
    }
    (run_dir / "graph_hierarchy_cache_report.json").write_text(json.dumps(graph_only_report, indent=2))
    print(f"\n[graph-cache] config={name} cache={graph_cache_path}", flush=True)
    print(
        "[done] "
        f"config={name} "
        "elapsed_seconds=0.0 "
        f"subsystem_exact_match={result['subsystem_exact_match']} "
        f"system_exact_match={result['system_exact_match']} "
        f"module_exact_match={result['module_exact_match']} "
        f"phase_accuracy={result['phase_accuracy']}",
        flush=True,
    )
    return result


def _run_one(
    *,
    config: dict[str, object],
    base_dir: Path,
    tail_count: int,
    flights_per_tail: int,
    phase_count: int,
    seed: int,
    conda_env: str | None,
    python_executable: str | None,
    force_rerun: bool,
    graph_cache_path: Path | None,
) -> dict[str, object]:
    name = str(config["name"])
    run_dir = base_dir / name
    run_dir.mkdir(parents=True, exist_ok=True)
    output_json = run_dir / "sim_detection_eval_report.json"
    cached = output_json.exists() and not force_rerun

    if graph_cache_path is not None and graph_cache_path.exists() and not force_rerun:
        return _run_from_graph_cache(
            config=config,
            graph_cache_path=graph_cache_path,
            base_dir=base_dir,
        )

    if cached:
        report = json.loads(output_json.read_text())
        metrics = _report_metrics(report)
        result = {
            "name": name,
            "base_dir": str(run_dir),
            **metrics,
            "args": dict(config["args"]),
        }
        print(f"\n[cache-hit] config={name} report={output_json}", flush=True)
        print(
            "[done] "
            f"config={name} "
            "elapsed_seconds=0.0 "
            f"subsystem_exact_match={result['subsystem_exact_match']} "
            f"system_exact_match={result['system_exact_match']} "
            f"module_exact_match={result['module_exact_match']} "
            f"phase_accuracy={result['phase_accuracy']}",
            flush=True,
        )
        return result

    env = os.environ.copy()
    cmd_args = [
        "-m",
        "scripts.run_sim_detection_eval",
        "--tail-count",
        str(tail_count),
        "--flights-per-tail",
        str(flights_per_tail),
        "--phase-count",
        str(phase_count),
        "--seed",
        str(seed),
        "--output-json",
        str(output_json),
        "--skip-cooccurrence",
    ]
    if graph_cache_path is not None and str(config["name"]) == "baseline":
        cmd_args.extend(["--graph-cache-json", str(graph_cache_path)])
    for key, value in dict(config["args"]).items():
        cmd_args.extend([f"--{str(key).replace('_', '-')}", str(value)])

    active_conda_env = str(os.environ.get("CONDA_DEFAULT_ENV", "") or "")
    conda_executable = os.environ.get("CONDA_EXE") or shutil.which("conda")
    if conda_env and active_conda_env == conda_env:
        cmd = [python_executable or sys.executable, *cmd_args]
    elif conda_env and conda_executable:
        cmd = [str(conda_executable), "run", "-n", conda_env, "python", *cmd_args]
    elif conda_env:
        raise SystemExit(
            "conda environment requested but no conda executable was found; "
            "either run from the target env directly or set CONDA_EXE"
        )
    else:
        cmd = [python_executable or sys.executable, *cmd_args]

    print(f"\n[start] config={name}", flush=True)
    print(f"[cmd] {' '.join(cmd)}", flush=True)
    started_at = time.perf_counter()
    subprocess.run(cmd, env=env, check=True)
    elapsed_seconds = time.perf_counter() - started_at

    report = json.loads(output_json.read_text())
    metrics = _report_metrics(report)
    result = {
        "name": name,
        "base_dir": str(run_dir),
        **metrics,
        "args": dict(config["args"]),
    }
    print(
        "[done] "
        f"config={name} "
        f"elapsed_seconds={elapsed_seconds:.1f} "
        f"subsystem_exact_match={result['subsystem_exact_match']} "
        f"system_exact_match={result['system_exact_match']} "
        f"module_exact_match={result['module_exact_match']} "
        f"phase_accuracy={result['phase_accuracy']}",
        flush=True,
    )
    return result


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    graph_cache_path = Path(str(args.graph_cache_json)) if args.graph_cache_json else _default_graph_cache_path(base_dir)

    results: list[dict[str, object]] = []
    for config in _selected_configs(args.config):
        result = _run_one(
            config=config,
            base_dir=base_dir,
            tail_count=args.tail_count,
            flights_per_tail=args.flights_per_tail,
            phase_count=args.phase_count,
            seed=args.seed,
            conda_env=args.conda_env,
            python_executable=args.python_executable,
            force_rerun=args.force_rerun,
            graph_cache_path=graph_cache_path,
        )
        results.append(result)
        print(
            json.dumps(
                {
                    "name": result["name"],
                    "phase_accuracy": result["phase_accuracy"],
                    "system_exact_match": result["system_exact_match"],
                    "subsystem_exact_match": result["subsystem_exact_match"],
                    "module_exact_match": result["module_exact_match"],
                    "system_nmi": result["system_nmi"],
                    "subsystem_nmi": result["subsystem_nmi"],
                    "module_nmi": result["module_nmi"],
                }
            )
        )

    results = sorted(
        results,
        key=lambda item: (
            -(float(item["subsystem_exact_match"]) if item["subsystem_exact_match"] is not None else -1.0),
            -(float(item["subsystem_nmi"]) if item["subsystem_nmi"] is not None else -1.0),
            -(float(item["system_exact_match"]) if item["system_exact_match"] is not None else -1.0),
            -(float(item["module_exact_match"]) if item["module_exact_match"] is not None else -1.0),
            str(item["name"]),
        ),
    )
    summary_path = base_dir / "sim_graph_hierarchy_sweep_summary.json"
    summary_path.write_text(json.dumps(results, indent=2))
    print(f"\nsummary: {summary_path}")


if __name__ == "__main__":
    main()
