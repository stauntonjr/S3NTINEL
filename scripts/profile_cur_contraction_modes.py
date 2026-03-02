"""Profile CUR U-contraction modes and emit complexity + runtime report."""

from __future__ import annotations

import argparse
import json
import os
import runpy
import time
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile CUR contraction modes in stage-10")
    parser.add_argument("--raw-table-path", required=True, help="Input raw telemetry table path")
    parser.add_argument("--table-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument(
        "--modes",
        default="core_w,pivot_restricted_a,full_a",
        help="Comma-separated contraction modes",
    )
    parser.add_argument("--repeats", type=int, default=2, help="Number of repeated runs per mode")
    parser.add_argument("--work-dir", default="data/cur_profile", help="Output root for per-run artifacts")
    parser.add_argument("--output-json", default="reports/cur_contraction_profile.json")
    parser.add_argument(
        "--disable-broadcast-joins",
        action="store_true",
        help="Set spark.sql.autoBroadcastJoinThreshold=-1 during profiling runs",
    )
    parser.add_argument(
        "--driver-memory",
        default=None,
        help="Optional Spark driver memory value (for example: 8g)",
    )
    return parser.parse_args()


def _parse_modes(value: str) -> list[str]:
    out: list[str] = []
    for item in str(value).split(","):
        mode = item.strip().lower()
        if mode:
            out.append(mode)
    return out


def _set_stage10_env(run_root: Path, args: argparse.Namespace, mode: str) -> dict[str, str | None]:
    keys = [
        "S3NTINEL_TABLE_FORMAT",
        "S3NTINEL_RAW_TABLE_PATH",
        "S3NTINEL_CUR_U_CONTRACTION_MODE",
        "S3NTINEL_CUR_NORMALIZATION_TABLE_PATH",
        "S3NTINEL_CUR_COLUMN_SKETCH_TABLE_PATH",
        "S3NTINEL_CUR_COLUMN_LEVERAGE_TABLE_PATH",
        "S3NTINEL_CUR_ROW_SKETCH_TABLE_PATH",
        "S3NTINEL_CUR_SENSOR_SAMPLE_TABLE_PATH",
        "S3NTINEL_CUR_ROW_SAMPLE_TABLE_PATH",
        "S3NTINEL_CUR_C_MATRIX_TABLE_PATH",
        "S3NTINEL_CUR_R_MATRIX_TABLE_PATH",
        "S3NTINEL_CUR_W_MATRIX_TABLE_PATH",
        "S3NTINEL_CUR_U_MATRIX_TABLE_PATH",
        "S3NTINEL_CUR_GRAPH_TABLE_PATH",
        "S3NTINEL_PRECISION_GRAPH_TABLE_PATH",
        "S3NTINEL_EVENT_GRAPH_TABLE_PATH",
        "S3NTINEL_FUSED_GRAPH_TABLE_PATH",
        "S3NTINEL_SUBSYSTEM_MAP_TABLE_PATH",
        "S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH",
        "S3NTINEL_HIERARCHY_NODES_TABLE_PATH",
        "S3NTINEL_HIERARCHY_EDGES_TABLE_PATH",
        "S3NTINEL_FIT_GRAPH_REPORT_PATH",
        "PYSPARK_SUBMIT_ARGS",
    ]
    prior = {key: os.environ.get(key) for key in keys}

    os.environ["S3NTINEL_TABLE_FORMAT"] = str(args.table_format)
    os.environ["S3NTINEL_RAW_TABLE_PATH"] = str(args.raw_table_path)
    os.environ["S3NTINEL_CUR_U_CONTRACTION_MODE"] = str(mode)

    os.environ["S3NTINEL_CUR_NORMALIZATION_TABLE_PATH"] = str(run_root / "cur_normalization_profile")
    os.environ["S3NTINEL_CUR_COLUMN_SKETCH_TABLE_PATH"] = str(run_root / "cur_column_sketch")
    os.environ["S3NTINEL_CUR_COLUMN_LEVERAGE_TABLE_PATH"] = str(run_root / "cur_column_leverage")
    os.environ["S3NTINEL_CUR_ROW_SKETCH_TABLE_PATH"] = str(run_root / "cur_row_sketch")
    os.environ["S3NTINEL_CUR_SENSOR_SAMPLE_TABLE_PATH"] = str(run_root / "cur_sensor_sample")
    os.environ["S3NTINEL_CUR_ROW_SAMPLE_TABLE_PATH"] = str(run_root / "cur_row_sample")
    os.environ["S3NTINEL_CUR_C_MATRIX_TABLE_PATH"] = str(run_root / "cur_c_matrix")
    os.environ["S3NTINEL_CUR_R_MATRIX_TABLE_PATH"] = str(run_root / "cur_r_matrix")
    os.environ["S3NTINEL_CUR_W_MATRIX_TABLE_PATH"] = str(run_root / "cur_w_matrix")
    os.environ["S3NTINEL_CUR_U_MATRIX_TABLE_PATH"] = str(run_root / "cur_u_matrix")

    os.environ["S3NTINEL_CUR_GRAPH_TABLE_PATH"] = str(run_root / "cur_sensor_graph")
    os.environ["S3NTINEL_PRECISION_GRAPH_TABLE_PATH"] = str(run_root / "precision_sensor_graph")
    os.environ["S3NTINEL_EVENT_GRAPH_TABLE_PATH"] = str(run_root / "event_cooccurrence_graph")
    os.environ["S3NTINEL_FUSED_GRAPH_TABLE_PATH"] = str(run_root / "fused_sensor_graph")
    os.environ["S3NTINEL_SUBSYSTEM_MAP_TABLE_PATH"] = str(run_root / "sensor_subsystem_map")
    os.environ["S3NTINEL_HIERARCHY_SENSOR_MAP_TABLE_PATH"] = str(run_root / "sensor_hierarchy_map")
    os.environ["S3NTINEL_HIERARCHY_NODES_TABLE_PATH"] = str(run_root / "hierarchy_nodes")
    os.environ["S3NTINEL_HIERARCHY_EDGES_TABLE_PATH"] = str(run_root / "hierarchy_edges")

    os.environ["S3NTINEL_FIT_GRAPH_REPORT_PATH"] = str(run_root / "fitting_graph_report.json")

    spark_args: list[str] = []
    if bool(args.disable_broadcast_joins):
        spark_args.extend(["--conf", "spark.sql.autoBroadcastJoinThreshold=-1"])
    if args.driver_memory:
        spark_args.extend(["--driver-memory", str(args.driver_memory)])
    if spark_args:
        os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join([*spark_args, "pyspark-shell"])

    return prior


def _restore_env(prior: dict[str, str | None]) -> None:
    for key, value in prior.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _load_report(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _aggregate_metric(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "std": None}
    return {
        "mean": float(round(mean(values), 6)),
        "std": float(round(pstdev(values) if len(values) > 1 else 0.0, 6)),
    }


def _complexity_notes() -> dict[str, str]:
    return {
        "variables": "m=rows, n=columns, c=sampled columns, r=sampled rows, nnz_A=nonzeros in A, nnz_AJ=nonzeros in A[:,J]",
        "core_w": "Contraction uses C+*W*R+; sparse join cost roughly O(nnz(C+) + nnz(W) + nnz(R+) + join/shuffle). Lowest IO; most approximate.",
        "pivot_restricted_a": "Contraction uses C+*A[:,J]*R+; sparse join cost O(nnz(C+) + nnz(AJ) + nnz(R+) + join/shuffle). Better fidelity than core_w at moderate extra cost.",
        "full_a": "Contraction uses C+*A*R+; sparse join cost O(nnz(C+) + nnz(A) + nnz(R+) + join/shuffle). Highest fidelity and highest shuffle/compute cost.",
        "svd": "C+ and R+ each require truncated SVD; practical cost depends on rank k and dimensions, roughly dominated by O(iterations * nnz(matrix) * k).",
    }


def main() -> None:
    args = parse_args()
    modes = _parse_modes(args.modes)
    repeats = max(int(args.repeats), 1)
    if not modes:
        raise ValueError("no modes provided")

    work_root = Path(args.work_dir)
    work_root.mkdir(parents=True, exist_ok=True)

    runs_by_mode: dict[str, list[dict[str, Any]]] = {mode: [] for mode in modes}

    for mode in modes:
        for repeat_idx in range(1, repeats + 1):
            run_root = work_root / f"mode={mode}" / f"rep={repeat_idx}"
            run_root.mkdir(parents=True, exist_ok=True)

            prior = _set_stage10_env(run_root=run_root, args=args, mode=mode)
            t0 = time.perf_counter()
            try:
                runpy.run_module("pipelines.10_cur_backbone_fit", run_name="__main__")
            finally:
                elapsed = time.perf_counter() - t0
                _restore_env(prior)

            report = _load_report(run_root / "fitting_graph_report.json")
            cur_m = report.get("cur_matrices", {}) if isinstance(report.get("cur_matrices"), dict) else {}
            u_core = cur_m.get("u_core", {}) if isinstance(cur_m.get("u_core"), dict) else {}

            row = {
                "mode_requested": mode,
                "mode_effective": cur_m.get("u_contraction_mode"),
                "repeat": repeat_idx,
                "elapsed_seconds": float(round(elapsed, 6)),
                "c_nnz": int(cur_m.get("c_nnz", 0) or 0),
                "r_nnz": int(cur_m.get("r_nnz", 0) or 0),
                "w_nnz": int(cur_m.get("w_nnz", 0) or 0),
                "u_nnz": int(cur_m.get("u_nnz", 0) or 0),
                "sampled_sensor_count": int(cur_m.get("sampled_sensor_count", 0) or 0),
                "sampled_row_count": int(cur_m.get("sampled_row_count", 0) or 0),
                "u_core": u_core,
                "run_root": str(run_root),
            }
            runs_by_mode[mode].append(row)
            print(
                "run_complete "
                + f"mode={mode} rep={repeat_idx} elapsed_s={row['elapsed_seconds']} u_nnz={row['u_nnz']}"
            )

    summary: dict[str, Any] = {}
    for mode, runs in runs_by_mode.items():
        elapsed_values = [float(r["elapsed_seconds"]) for r in runs]
        u_nnz_values = [float(r["u_nnz"]) for r in runs]
        summary[mode] = {
            "run_count": len(runs),
            "elapsed_seconds": _aggregate_metric(elapsed_values),
            "u_nnz": _aggregate_metric(u_nnz_values),
            "effective_modes": sorted({str(r.get("mode_effective")) for r in runs}),
        }

    payload = {
        "raw_table_path": args.raw_table_path,
        "table_format": args.table_format,
        "modes": modes,
        "repeats": repeats,
        "complexity": _complexity_notes(),
        "runs": runs_by_mode,
        "summary": summary,
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("CUR contraction profiling complete")
    print(f"- output_json: {output_path}")
    for mode in modes:
        row = summary.get(mode, {})
        elapsed = row.get("elapsed_seconds", {}).get("mean")
        print(f"  mode={mode} mean_elapsed_s={elapsed}")


if __name__ == "__main__":
    main()
