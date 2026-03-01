"""Evaluate hierarchy recovery difficulty across easy/medium/hard synthetic correlation presets."""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from libs.io.delta import get_spark, read_table
from libs.profiling.fleet_profile import (
    NumericVarianceConfig,
    expand_parameter_profile_rows,
    get_hierarchy_variance_preset,
    make_fleet_ids,
)
from libs.profiling.synthetic import ParameterSpec, generate_synthetic_normal_telemetry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate hierarchy recovery quality for correlation presets")
    parser.add_argument("--base-parameter-profile-path", required=True, help="Path to base parameter_profile table")
    parser.add_argument("--hierarchy-sensor-map-path", required=True, help="Path to synthetic sensor_hierarchy_map table")
    parser.add_argument("--profile-format", default="parquet", choices=["parquet", "delta"])

    parser.add_argument("--presets", default="easy,medium,hard", help="Comma-separated preset list")
    parser.add_argument("--tail-count", type=int, default=2)
    parser.add_argument("--flights-per-tail", type=int, default=2)
    parser.add_argument("--tail-id-prefix", default="FLEET_T")
    parser.add_argument("--flight-id-prefix", default="FL")

    parser.add_argument("--duration-seconds", type=int, default=180)
    parser.add_argument("--start-ts", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-profile-params", type=int, default=200)
    parser.add_argument("--max-corr-sensors", type=int, default=32)
    parser.add_argument(
        "--telemetry-partition-manifest-path",
        default=None,
        help=(
            "Optional partition manifest path for existing telemetry mode. "
            "If it contains '{preset}', the token is replaced per preset."
        ),
    )
    parser.add_argument(
        "--telemetry-manifest-format",
        default="parquet",
        choices=["parquet", "delta"],
        help="Format for telemetry partition manifest table in existing telemetry mode.",
    )
    parser.add_argument(
        "--telemetry-format",
        default="parquet",
        choices=["parquet", "delta"],
        help="Default format for telemetry output paths when manifest row lacks output_format.",
    )

    parser.add_argument("--output-json", default="reports/hierarchy_recovery_metrics.json")
    return parser.parse_args()


def _dedupe_parameter_rows(rows: list[dict[str, Any]], max_profile_params: int) -> list[dict[str, Any]]:
    by_parameter: dict[str, dict[str, Any]] = {}
    for row in rows:
        parameter_name = str(row.get("parameter_name") or "")
        if not parameter_name:
            continue
        if parameter_name not in by_parameter:
            by_parameter[parameter_name] = row
    ordered = [by_parameter[name] for name in sorted(by_parameter.keys())]
    return ordered[: int(max_profile_params)]


def _scope_rows(rows: list[dict[str, Any]], tail_id: str, flight_id: str) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if str(row.get("tail_id") or "") == tail_id and str(row.get("flight_id") or "") == flight_id
    ]


def _specs_from_rows(rows: list[dict[str, Any]], max_corr_sensors: int) -> list[ParameterSpec]:
    selected_rows = sorted(
        [row for row in rows if str(row.get("parameter_name") or "")],
        key=lambda row: str(row.get("parameter_name") or ""),
    )[: int(max_corr_sensors)]

    specs: list[ParameterSpec] = []
    for row in selected_rows:
        detected_type = str(row.get("detected_type") or "")
        parameter_name = str(row.get("parameter_name") or "")
        if not parameter_name:
            continue

        sampling_rate_hz = max(float(row.get("sampling_rate_hz") or 1.0), 0.5)
        missing_rate = max(min(float(row.get("missing_rate") or 0.0), 0.95), 0.0)

        if detected_type in {"categorical", "binary"}:
            categories = ("ON", "OFF") if detected_type == "binary" else ("STATE_A", "STATE_B", "STATE_C")
            specs.append(
                ParameterSpec(
                    parameter_name=parameter_name,
                    detected_type="categorical",
                    sampling_rate_hz=sampling_rate_hz,
                    categories=categories,
                    missing_rate=missing_rate,
                )
            )
            continue

        if detected_type != "numeric":
            continue

        std = max(float(row.get("num_std") or 1.0), 1e-6)
        specs.append(
            ParameterSpec(
                parameter_name=parameter_name,
                detected_type="numeric",
                sampling_rate_hz=sampling_rate_hz,
                mean=float(row.get("num_mean") or 0.0),
                std=std,
                min_value=float(row["num_min"]) if row.get("num_min") is not None else None,
                max_value=float(row["num_max"]) if row.get("num_max") is not None else None,
                noise_std=std,
                missing_rate=missing_rate,
            )
        )
    return specs


def _build_pair_rows(pivot_df: DataFrame, sensors: list[str], label_map: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    sensor_series: dict[str, list[float | None]] = {sensor: [] for sensor in sensors}
    for row in pivot_df.select(*sensors).collect():
        values = row.asDict(recursive=True)
        for sensor in sensors:
            raw_value = values.get(sensor)
            sensor_series[sensor].append(float(raw_value) if raw_value is not None else None)

    def pearson(left_values: list[float | None], right_values: list[float | None]) -> float | None:
        paired = [
            (left, right)
            for left, right in zip(left_values, right_values)
            if left is not None and right is not None
        ]
        if len(paired) < 3:
            return None

        left_only = [left for left, _ in paired]
        right_only = [right for _, right in paired]
        left_mean = sum(left_only) / len(left_only)
        right_mean = sum(right_only) / len(right_only)
        left_centered = [left - left_mean for left in left_only]
        right_centered = [right - right_mean for right in right_only]
        left_var = sum(value * value for value in left_centered)
        right_var = sum(value * value for value in right_centered)
        if left_var <= 0.0 or right_var <= 0.0:
            return None

        covariance = sum(lv * rv for lv, rv in zip(left_centered, right_centered))
        return float(covariance / ((left_var ** 0.5) * (right_var ** 0.5)))

    pair_rows: list[dict[str, Any]] = []
    for left_index in range(len(sensors)):
        for right_index in range(left_index + 1, len(sensors)):
            left = sensors[left_index]
            right = sensors[right_index]
            corr = pearson(sensor_series[left], sensor_series[right])
            if corr is None:
                continue

            left_labels = label_map.get(left, {})
            right_labels = label_map.get(right, {})
            same_module = (
                str(left_labels.get("module_id") or "")
                and str(left_labels.get("module_id") or "") == str(right_labels.get("module_id") or "")
            )
            same_subsystem = (
                str(left_labels.get("subsystem_id") or "")
                and str(left_labels.get("subsystem_id") or "") == str(right_labels.get("subsystem_id") or "")
            )

            pair_rows.append(
                {
                    "left": left,
                    "right": right,
                    "corr": float(corr),
                    "abs_corr": float(abs(corr)),
                    "same_module": bool(same_module),
                    "same_subsystem": bool(same_subsystem),
                }
            )
    return pair_rows


def _encode_numeric_df_from_telemetry(telemetry_df: DataFrame, sensors: list[str]) -> DataFrame:
    filtered_df = telemetry_df.where(F.col("parameter_name").isin(sensors))

    distinct_pairs = [
        row.asDict(recursive=True)
        for row in filtered_df.select("parameter_name", "parameter_value").where(F.col("parameter_value").isNotNull()).distinct().collect()
    ]
    categorical_by_parameter: dict[str, list[str]] = {}
    for row in distinct_pairs:
        parameter_name = str(row.get("parameter_name") or "")
        parameter_value = str(row.get("parameter_value") or "")
        if not parameter_name or not parameter_value:
            continue
        try:
            float(parameter_value)
            continue
        except ValueError:
            pass
        categorical_by_parameter.setdefault(parameter_name, []).append(parameter_value)

    categorical_mapping_rows: list[tuple[str, str, float]] = []
    for parameter_name, values in categorical_by_parameter.items():
        for index, value in enumerate(sorted(set(values))):
            categorical_mapping_rows.append((parameter_name, value, float(index)))

    if categorical_mapping_rows:
        mapping_schema = T.StructType(
            [
                T.StructField("parameter_name", T.StringType(), nullable=False),
                T.StructField("parameter_value", T.StringType(), nullable=False),
                T.StructField("mapped_value_num", T.DoubleType(), nullable=False),
            ]
        )
        mapping_df = telemetry_df.sparkSession.createDataFrame(categorical_mapping_rows, schema=mapping_schema)
        encoded_df = filtered_df.join(mapping_df, on=["parameter_name", "parameter_value"], how="left")
        value_num_expr = F.coalesce(F.expr("try_cast(parameter_value as double)"), F.col("mapped_value_num"))
    else:
        encoded_df = filtered_df
        value_num_expr = F.expr("try_cast(parameter_value as double)")

    return (
        encoded_df.withColumn("value_num", value_num_expr)
        .where(F.col("value_num").isNotNull())
        .withColumn("ts_sec", F.date_trunc("second", F.col("timestamp")))
    )


def _pair_rows_from_numeric_df(
    numeric_df: DataFrame,
    sensors: list[str],
    label_map: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    sec_df = numeric_df.groupBy("ts_sec", "parameter_name").agg(F.avg("value_num").alias("value_num"))
    pivot_df = sec_df.groupBy("ts_sec").pivot("parameter_name", sensors).agg(F.first("value_num"))
    return _build_pair_rows(pivot_df=pivot_df, sensors=sensors, label_map=label_map)


def _flight_sensor_diagnostics_from_numeric_df(
    numeric_df: DataFrame,
    sensors: list[str],
) -> dict[str, Any]:
    stats_rows = [
        row.asDict(recursive=True)
        for row in numeric_df.groupBy("parameter_name")
        .agg(
            F.count("*").alias("value_count"),
            F.countDistinct("value_num").alias("distinct_values"),
            F.stddev_pop("value_num").alias("stddev_pop"),
        )
        .collect()
    ]
    stats_by_sensor = {
        str(row.get("parameter_name") or ""): {
            "value_count": int(row.get("value_count") or 0),
            "distinct_values": int(row.get("distinct_values") or 0),
            "stddev_pop": float(row.get("stddev_pop") or 0.0),
        }
        for row in stats_rows
        if str(row.get("parameter_name") or "")
    }

    dropped_sensors: list[str] = []
    constant_sensors: list[str] = []
    usable_sensors: list[str] = []
    for sensor in sensors:
        stats = stats_by_sensor.get(sensor)
        if not stats or int(stats["value_count"]) <= 0:
            dropped_sensors.append(sensor)
            continue

        is_constant = int(stats["distinct_values"]) <= 1 or float(stats["stddev_pop"]) <= 1e-12
        if is_constant:
            constant_sensors.append(sensor)
        else:
            usable_sensors.append(sensor)

    return {
        "dropped_sensors": sorted(dropped_sensors),
        "constant_sensors": sorted(constant_sensors),
        "usable_sensors": sorted(usable_sensors),
        "pairable": bool(len(usable_sensors) >= 2),
    }


def _pair_rows_from_telemetry_df(
    telemetry_df: DataFrame,
    sensors: list[str],
    label_map: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    numeric_df = _encode_numeric_df_from_telemetry(telemetry_df=telemetry_df, sensors=sensors)
    return _pair_rows_from_numeric_df(numeric_df=numeric_df, sensors=sensors, label_map=label_map)


def _score_pair_rows(
    preset: str,
    pair_rows: list[dict[str, Any]],
    max_corr_sensors: int,
) -> dict[str, Any]:
    if not pair_rows:
        return {
            "preset": preset,
            "pair_count": 0,
            "module_pair_count": 0,
            "subsystem_pair_count": 0,
            "auroc_module": None,
            "auroc_subsystem": None,
            "topk": 0,
            "precision_at_k_module": None,
            "precision_at_k_subsystem": None,
            "within_module_abs_corr": None,
            "between_module_abs_corr": None,
            "within_subsystem_abs_corr": None,
            "between_subsystem_abs_corr": None,
            "module_separation_ratio": None,
            "subsystem_separation_ratio": None,
        }

    module_pairs = [row for row in pair_rows if bool(row["same_module"])]
    non_module_pairs = [row for row in pair_rows if not bool(row["same_module"])]
    subsystem_pairs = [row for row in pair_rows if bool(row["same_subsystem"])]
    non_subsystem_pairs = [row for row in pair_rows if not bool(row["same_subsystem"])]

    sorted_pairs = sorted(pair_rows, key=lambda row: float(row["abs_corr"]), reverse=True)
    topk = min(len(sorted_pairs), max(10, int(max_corr_sensors * 2)))
    top_rows = sorted_pairs[:topk]

    within_module_abs = _mean([float(row["abs_corr"]) for row in module_pairs])
    between_module_abs = _mean([float(row["abs_corr"]) for row in non_module_pairs])
    within_subsystem_abs = _mean([float(row["abs_corr"]) for row in subsystem_pairs])
    between_subsystem_abs = _mean([float(row["abs_corr"]) for row in non_subsystem_pairs])
    precision_module = _mean([1.0 if bool(row["same_module"]) else 0.0 for row in top_rows]) if top_rows else None
    precision_subsystem = _mean([1.0 if bool(row["same_subsystem"]) else 0.0 for row in top_rows]) if top_rows else None

    return {
        "preset": preset,
        "pair_count": len(pair_rows),
        "module_pair_count": len(module_pairs),
        "subsystem_pair_count": len(subsystem_pairs),
        "auroc_module": _round_or_none(_binary_auroc(pair_rows, "same_module")),
        "auroc_subsystem": _round_or_none(_binary_auroc(pair_rows, "same_subsystem")),
        "topk": int(topk),
        "precision_at_k_module": _round_or_none(precision_module),
        "precision_at_k_subsystem": _round_or_none(precision_subsystem),
        "within_module_abs_corr": _round_or_none(within_module_abs),
        "between_module_abs_corr": _round_or_none(between_module_abs),
        "within_subsystem_abs_corr": _round_or_none(within_subsystem_abs),
        "between_subsystem_abs_corr": _round_or_none(between_subsystem_abs),
        "module_separation_ratio": _round_or_none(_safe_div(within_module_abs, between_module_abs)),
        "subsystem_separation_ratio": _round_or_none(_safe_div(within_subsystem_abs, between_subsystem_abs)),
    }


def _binary_auroc(pair_rows: list[dict[str, Any]], positive_key: str) -> float | None:
    positives = [float(row["abs_corr"]) for row in pair_rows if bool(row.get(positive_key))]
    negatives = [float(row["abs_corr"]) for row in pair_rows if not bool(row.get(positive_key))]
    if not positives or not negatives:
        return None

    wins = 0
    ties = 0
    for p in positives:
        for n in negatives:
            if p > n:
                wins += 1
            elif p == n:
                ties += 1

    total = len(positives) * len(negatives)
    if total <= 0:
        return None
    return float((wins + 0.5 * ties) / total)


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _safe_div(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0.0:
        return None
    return float(a / b)


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return float(round(value, 6))


def evaluate_preset(
    spark,
    base_profile_rows: list[dict[str, Any]],
    label_map: dict[str, dict[str, str]],
    preset: str,
    fleet_ids: list[tuple[str, str]],
    duration_seconds: int,
    start_ts: str,
    max_corr_sensors: int,
    seed: int,
) -> dict[str, Any]:
    rng = random.Random(seed)
    hierarchy_cfg = get_hierarchy_variance_preset(preset)

    expanded_rows = expand_parameter_profile_rows(
        base_rows=base_profile_rows,
        fleet_ids=fleet_ids,
        numeric_cfg=NumericVarianceConfig(),
        hierarchy_map_by_parameter=label_map,
        hierarchy_cfg=hierarchy_cfg,
        rng=rng,
    )

    all_pair_rows: list[dict[str, Any]] = []
    for flight_index, (tail_id, flight_id) in enumerate(fleet_ids):
        scoped_rows = _scope_rows(expanded_rows, tail_id=tail_id, flight_id=flight_id)
        specs = _specs_from_rows(scoped_rows, max_corr_sensors=max_corr_sensors)
        if len(specs) < 2:
            continue

        flight_seed = seed + (1000 * (flight_index + 1))
        telemetry_df = generate_synthetic_normal_telemetry(
            spark=spark,
            duration_seconds=int(duration_seconds),
            tail_id=tail_id,
            flight_id=flight_id,
            start_ts=start_ts,
            specs=specs,
            seed=flight_seed,
        )
        sensor_names = sorted([spec.parameter_name for spec in specs])
        all_pair_rows.extend(
            _pair_rows_from_telemetry_df(
                telemetry_df=telemetry_df,
                sensors=sensor_names,
                label_map=label_map,
            )
        )
    return _score_pair_rows(preset=preset, pair_rows=all_pair_rows, max_corr_sensors=max_corr_sensors)


def evaluate_preset_from_manifest(
    spark,
    label_map: dict[str, dict[str, str]],
    preset: str,
    manifest_path: str,
    manifest_format: str,
    telemetry_format: str,
    max_corr_sensors: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest_df = read_table(spark, manifest_path, fmt=manifest_format)
    manifest_rows = [row.asDict(recursive=True) for row in manifest_df.collect()]

    sensors = sorted(label_map.keys())[: int(max_corr_sensors)]
    all_pair_rows: list[dict[str, Any]] = []
    preflight_rows: list[dict[str, Any]] = []
    for row in manifest_rows:
        output_path = str(row.get("output_path") or "")
        if not output_path:
            continue
        row_format = str(row.get("output_format") or telemetry_format)
        telemetry_df = read_table(spark, output_path, fmt=row_format)
        numeric_df = _encode_numeric_df_from_telemetry(telemetry_df=telemetry_df, sensors=sensors)
        diagnostics = _flight_sensor_diagnostics_from_numeric_df(numeric_df=numeric_df, sensors=sensors)
        diagnostics["tail_id"] = str(row.get("tail_id") or "")
        diagnostics["flight_id"] = str(row.get("flight_id") or "")
        diagnostics["output_path"] = output_path
        preflight_rows.append(diagnostics)

        all_pair_rows.extend(_pair_rows_from_numeric_df(numeric_df=numeric_df, sensors=sensors, label_map=label_map))

    return _score_pair_rows(preset=preset, pair_rows=all_pair_rows, max_corr_sensors=max_corr_sensors), preflight_rows


def main() -> None:
    args = parse_args()
    spark = get_spark("s3ntinel.evaluate_hierarchy_recovery")

    base_profile_df = read_table(spark, args.base_parameter_profile_path, fmt=args.profile_format)
    base_profile_rows = [row.asDict(recursive=True) for row in base_profile_df.collect()]
    base_profile_rows = _dedupe_parameter_rows(base_profile_rows, max_profile_params=args.max_profile_params)
    if not base_profile_rows:
        raise ValueError("base parameter_profile is empty")

    hierarchy_df = read_table(spark, args.hierarchy_sensor_map_path, fmt=args.profile_format)
    hierarchy_rows = [row.asDict(recursive=True) for row in hierarchy_df.collect()]
    label_map: dict[str, dict[str, str]] = {}
    for row in hierarchy_rows:
        parameter_name = str(row.get("parameter_name") or "")
        if not parameter_name:
            continue
        label_map[parameter_name] = {
            "system_id": str(row.get("system_id") or ""),
            "subsystem_id": str(row.get("subsystem_id") or ""),
            "module_id": str(row.get("module_id") or ""),
            "hierarchy_profile_id": str(row.get("hierarchy_profile_id") or ""),
            "hierarchy_source": str(row.get("hierarchy_source") or ""),
        }

    if not label_map:
        raise ValueError("hierarchy sensor map is empty")

    presets = [item.strip().lower() for item in str(args.presets).split(",") if item.strip()]
    if not presets:
        presets = ["easy", "medium", "hard"]

    fleet_ids = make_fleet_ids(
        tail_count=int(args.tail_count),
        flights_per_tail=int(args.flights_per_tail),
        tail_id_prefix=str(args.tail_id_prefix),
        flight_id_prefix=str(args.flight_id_prefix),
    )
    if not fleet_ids:
        raise ValueError("fleet id set is empty; provide positive tail/flight counts")

    results: list[dict[str, Any]] = []
    telemetry_mode = bool(args.telemetry_partition_manifest_path)
    preflight_by_preset: dict[str, list[dict[str, Any]]] = {}
    for preset_index, preset in enumerate(presets):
        if telemetry_mode:
            manifest_template = str(args.telemetry_partition_manifest_path)
            manifest_path = manifest_template.format(preset=preset)
            result, preflight_rows = evaluate_preset_from_manifest(
                spark=spark,
                label_map=label_map,
                preset=preset,
                manifest_path=manifest_path,
                manifest_format=str(args.telemetry_manifest_format),
                telemetry_format=str(args.telemetry_format),
                max_corr_sensors=int(args.max_corr_sensors),
            )
            preflight_by_preset[preset] = preflight_rows
        else:
            result = evaluate_preset(
                spark=spark,
                base_profile_rows=base_profile_rows,
                label_map=label_map,
                preset=preset,
                fleet_ids=fleet_ids,
                duration_seconds=int(args.duration_seconds),
                start_ts=str(args.start_ts),
                max_corr_sensors=int(args.max_corr_sensors),
                seed=int(args.seed) + (preset_index * 10000),
            )
        results.append(result)

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_payload = {
        "mode": "existing_telemetry_manifest" if telemetry_mode else "inline_synthetic",
        "presets": presets,
        "tail_count": int(args.tail_count),
        "flights_per_tail": int(args.flights_per_tail),
        "duration_seconds": int(args.duration_seconds),
        "max_corr_sensors": int(args.max_corr_sensors),
        "results": results,
    }
    if telemetry_mode:
        output_payload["preflight_by_preset"] = preflight_by_preset
    output_path.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")

    print("Hierarchy recovery evaluation complete:")
    print(f"- output_json: {output_path}")
    print("- metrics:")
    for row in results:
        print(
            "  "
            + (
                f"preset={row['preset']} "
                f"auroc_module={row['auroc_module']} "
                f"auroc_subsystem={row['auroc_subsystem']} "
                f"p@k_module={row['precision_at_k_module']} "
                f"p@k_subsystem={row['precision_at_k_subsystem']} "
                f"module_sep={row['module_separation_ratio']} "
                f"subsystem_sep={row['subsystem_separation_ratio']}"
            )
        )
    if telemetry_mode:
        print("- preflight:")
        for preset in presets:
            rows = preflight_by_preset.get(preset, [])
            pairable = sum(1 for item in rows if bool(item.get("pairable")))
            print(f"  preset={preset} flights={len(rows)} pairable_flights={pairable}")


if __name__ == "__main__":
    main()
