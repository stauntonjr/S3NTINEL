"""A/B evaluate CUR sampling modes (deterministic vs weighted) across seeds."""

from __future__ import annotations

import argparse
import json
import os
import runpy
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

from libs.io.delta import get_spark, read_table


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A/B evaluate deterministic vs weighted CUR sampling")
    parser.add_argument("--raw-table-path", required=True, help="Input raw telemetry table path")
    parser.add_argument("--table-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--hierarchy-sensor-map-path", required=True, help="Hierarchy sensor map path")
    parser.add_argument("--hierarchy-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--modes", default="deterministic,weighted", help="Comma-separated sampling modes")
    parser.add_argument("--seeds", default="11,23,37", help="Comma-separated sampling seeds")
    parser.add_argument("--work-dir", default="data/ab_sampling", help="Output root for per-run artifacts")
    parser.add_argument("--output-json", default="reports/cur_sampling_ab_report.json")

    parser.add_argument("--cur-max-core-cells", type=int, default=1000000)
    parser.add_argument("--cur-min-core-rows", type=int, default=1)
    parser.add_argument("--cur-min-core-cols", type=int, default=1)
    parser.add_argument("--ab-pivots-k", type=int, default=None, help="Optional stage-10 override for CUR pivots_k")
    parser.add_argument("--ab-row-samples-k", type=int, default=None, help="Optional stage-10 override for CUR row_samples_k")
    parser.add_argument(
        "--ab-cur-graph-max-sensors",
        type=int,
        default=None,
        help="Optional stage-10 override for graph sensor candidate cap",
    )
    return parser.parse_args()


def _parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _parse_int_csv_list(value: str) -> list[int]:
    out: list[int] = []
    for item in _parse_csv_list(value):
        out.append(int(item))
    return out


def _safe_ratio(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return float(num) / float(den)


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return float(round(value, 6))


def _collect_hierarchy_label_map(hierarchy_rows: list[dict[str, Any]]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in hierarchy_rows:
        parameter_name = str(row.get("parameter_name") or "")
        if not parameter_name:
            continue
        out[parameter_name] = {
            "module_id": str(row.get("module_id") or ""),
            "subsystem_id": str(row.get("subsystem_id") or ""),
        }
    return out


def _sampling_recovery_metrics(
    sampled_sensors: list[str],
    label_map: dict[str, dict[str, str]],
) -> dict[str, Any]:
    sensors = [sensor for sensor in sampled_sensors if sensor in label_map]
    sampled_count = len(sensors)

    modules_all = {str(v.get("module_id") or "") for v in label_map.values() if str(v.get("module_id") or "")}
    subsystems_all = {
        str(v.get("subsystem_id") or "") for v in label_map.values() if str(v.get("subsystem_id") or "")
    }
    modules_sampled = {str(label_map[sensor].get("module_id") or "") for sensor in sensors if str(label_map[sensor].get("module_id") or "")}
    subsystems_sampled = {
        str(label_map[sensor].get("subsystem_id") or "") for sensor in sensors if str(label_map[sensor].get("subsystem_id") or "")
    }

    pair_count = 0
    module_pair_count = 0
    subsystem_pair_count = 0
    for i in range(sampled_count):
        left = sensors[i]
        for j in range(i + 1, sampled_count):
            right = sensors[j]
            pair_count += 1
            same_module = (
                str(label_map[left].get("module_id") or "")
                and str(label_map[left].get("module_id") or "") == str(label_map[right].get("module_id") or "")
            )
            same_subsystem = (
                str(label_map[left].get("subsystem_id") or "")
                and str(label_map[left].get("subsystem_id") or "") == str(label_map[right].get("subsystem_id") or "")
            )
            if same_module:
                module_pair_count += 1
            if same_subsystem:
                subsystem_pair_count += 1

    return {
        "sampled_sensor_count": sampled_count,
        "sampled_pair_count": pair_count,
        "module_pair_count": module_pair_count,
        "subsystem_pair_count": subsystem_pair_count,
        "pair_precision_module": _round_or_none(_safe_ratio(module_pair_count, pair_count)),
        "pair_precision_subsystem": _round_or_none(_safe_ratio(subsystem_pair_count, pair_count)),
        "module_coverage_ratio": _round_or_none(_safe_ratio(len(modules_sampled), len(modules_all))),
        "subsystem_coverage_ratio": _round_or_none(_safe_ratio(len(subsystems_sampled), len(subsystems_all))),
    }


def _set_stage10_env(run_root: Path, args: argparse.Namespace, mode: str, seed: int) -> dict[str, str | None]:
    keys = [
        "S3NTINEL_TABLE_FORMAT",
        "S3NTINEL_RAW_TABLE_PATH",
        "S3NTINEL_CUR_SAMPLING_MODE",
        "S3NTINEL_CUR_SAMPLING_SEED",
        "S3NTINEL_CUR_MAX_CORE_CELLS",
        "S3NTINEL_CUR_MIN_CORE_ROWS",
        "S3NTINEL_CUR_MIN_CORE_COLS",
        "S3NTINEL_CUR_PIVOTS_K",
        "S3NTINEL_CUR_ROW_SAMPLES_K",
        "S3NTINEL_CUR_GRAPH_MAX_SENSORS",
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
        "S3NTINEL_EVENT_GRAPH_TABLE_PATH",
        "S3NTINEL_FUSED_GRAPH_TABLE_PATH",
        "S3NTINEL_FIT_GRAPH_REPORT_PATH",
    ]
    prior = {key: os.environ.get(key) for key in keys}

    os.environ["S3NTINEL_TABLE_FORMAT"] = str(args.table_format)
    os.environ["S3NTINEL_RAW_TABLE_PATH"] = str(args.raw_table_path)
    os.environ["S3NTINEL_CUR_SAMPLING_MODE"] = mode
    os.environ["S3NTINEL_CUR_SAMPLING_SEED"] = str(int(seed))
    os.environ["S3NTINEL_CUR_MAX_CORE_CELLS"] = str(int(args.cur_max_core_cells))
    os.environ["S3NTINEL_CUR_MIN_CORE_ROWS"] = str(int(args.cur_min_core_rows))
    os.environ["S3NTINEL_CUR_MIN_CORE_COLS"] = str(int(args.cur_min_core_cols))
    if args.ab_pivots_k is not None:
        os.environ["S3NTINEL_CUR_PIVOTS_K"] = str(max(int(args.ab_pivots_k), 1))
    if args.ab_row_samples_k is not None:
        os.environ["S3NTINEL_CUR_ROW_SAMPLES_K"] = str(max(int(args.ab_row_samples_k), 1))
    if args.ab_cur_graph_max_sensors is not None:
        os.environ["S3NTINEL_CUR_GRAPH_MAX_SENSORS"] = str(max(int(args.ab_cur_graph_max_sensors), 2))

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
    os.environ["S3NTINEL_EVENT_GRAPH_TABLE_PATH"] = str(run_root / "event_cooccurrence_graph")
    os.environ["S3NTINEL_FUSED_GRAPH_TABLE_PATH"] = str(run_root / "fused_sensor_graph")
    os.environ["S3NTINEL_FIT_GRAPH_REPORT_PATH"] = str(run_root / "fitting_graph_report.json")
    return prior


def _restore_env(prior: dict[str, str | None]) -> None:
    for key, value in prior.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _aggregate_mode(runs: list[dict[str, Any]]) -> dict[str, Any]:
    numeric_keys = [
        "sampled_sensor_count",
        "sampled_pair_count",
        "module_pair_count",
        "subsystem_pair_count",
        "pair_precision_module",
        "pair_precision_subsystem",
        "module_coverage_ratio",
        "subsystem_coverage_ratio",
    ]

    aggregates: dict[str, Any] = {}
    for key in numeric_keys:
        values = [float(run[key]) for run in runs if run.get(key) is not None]
        if not values:
            aggregates[key] = {"mean": None, "std": None}
            continue
        aggregates[key] = {
            "mean": _round_or_none(mean(values)),
            "std": _round_or_none(pstdev(values) if len(values) > 1 else 0.0),
        }
    return aggregates


def main() -> None:
    args = parse_args()

    modes = _parse_csv_list(args.modes)
    seeds = _parse_int_csv_list(args.seeds)
    if not modes:
        raise ValueError("no sampling modes provided")
    if not seeds:
        raise ValueError("no seeds provided")

    spark = get_spark("s3ntinel.evaluate_cur_sampling_ab")
    hierarchy_rows = [
        row.asDict(recursive=True)
        for row in read_table(spark, path=args.hierarchy_sensor_map_path, fmt=args.hierarchy_format).collect()
    ]
    label_map = _collect_hierarchy_label_map(hierarchy_rows)
    if not label_map:
        raise ValueError("hierarchy sensor map is empty")

    work_root = Path(args.work_dir)
    work_root.mkdir(parents=True, exist_ok=True)

    per_mode_runs: dict[str, list[dict[str, Any]]] = {}
    for mode in modes:
        mode_runs: list[dict[str, Any]] = []
        for seed in seeds:
            run_root = work_root / f"mode={mode}" / f"seed={seed}"
            run_root.mkdir(parents=True, exist_ok=True)

            prior = _set_stage10_env(run_root=run_root, args=args, mode=mode, seed=seed)
            try:
                runpy.run_module("pipelines.10_cur_backbone_fit", run_name="__main__")
            finally:
                _restore_env(prior)

            sensor_sample_path = str(run_root / "cur_sensor_sample")
            sampled_sensors = [
                str(row["sensor"])
                for row in read_table(spark, path=sensor_sample_path, fmt=args.table_format).select("sensor").collect()
                if row["sensor"] is not None
            ]
            report_path = run_root / "fitting_graph_report.json"
            report_payload = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
            u_core = report_payload.get("cur_matrices", {}).get("u_core", {})

            run_metrics = {
                "mode": mode,
                "seed": int(seed),
                "run_root": str(run_root),
                **_sampling_recovery_metrics(sampled_sensors=sampled_sensors, label_map=label_map),
                "u_guardrail_applied": bool(u_core.get("guardrail_applied", False)),
                "u_effective_core_cells": int(u_core.get("effective_core_cells", 0)),
            }
            mode_runs.append(run_metrics)
            print(
                "run_complete "
                + f"mode={mode} seed={seed} sampled={run_metrics['sampled_sensor_count']} "
                + f"p_module={run_metrics['pair_precision_module']} p_subsystem={run_metrics['pair_precision_subsystem']}"
            )

        per_mode_runs[mode] = mode_runs

    summary = {
        mode: {
            "run_count": len(runs),
            "aggregates": _aggregate_mode(runs),
        }
        for mode, runs in per_mode_runs.items()
    }

    output_payload = {
        "raw_table_path": args.raw_table_path,
        "table_format": args.table_format,
        "hierarchy_sensor_map_path": args.hierarchy_sensor_map_path,
        "ab_overrides": {
            "pivots_k": args.ab_pivots_k,
            "row_samples_k": args.ab_row_samples_k,
            "cur_graph_max_sensors": args.ab_cur_graph_max_sensors,
            "cur_max_core_cells": args.cur_max_core_cells,
            "cur_min_core_rows": args.cur_min_core_rows,
            "cur_min_core_cols": args.cur_min_core_cols,
        },
        "modes": modes,
        "seeds": seeds,
        "runs": per_mode_runs,
        "summary": summary,
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print("A/B sampling evaluation complete")
    print(f"- output_json: {output_path}")
    for mode in modes:
        agg = summary.get(mode, {}).get("aggregates", {})
        p_module = agg.get("pair_precision_module", {}).get("mean")
        p_sub = agg.get("pair_precision_subsystem", {}).get("mean")
        cov_module = agg.get("module_coverage_ratio", {}).get("mean")
        cov_sub = agg.get("subsystem_coverage_ratio", {}).get("mean")
        print(
            "  "
            + f"mode={mode} pair_precision_module_mean={p_module} pair_precision_subsystem_mean={p_sub} "
            + f"module_coverage_mean={cov_module} subsystem_coverage_mean={cov_sub}"
        )


if __name__ == "__main__":
    main()
