"""Generate fleet-scoped profile artifacts with controlled tail/flight variance."""

from __future__ import annotations

import argparse
from dataclasses import replace
import random
from pathlib import Path

from libs.io.delta import get_spark, read_table
from libs.profiling.fleet_profile import (
    CategoricalVarianceConfig,
    HierarchyVarianceConfig,
    NumericVarianceConfig,
    expand_categorical_distribution_rows,
    expand_parameter_profile_rows,
    get_hierarchy_variance_preset,
    make_fleet_ids,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate fleet-scoped profile artifacts")
    parser.add_argument("--base-parameter-profile-path", required=True, help="Input parameter_profile table path")
    parser.add_argument(
        "--base-categorical-distribution-path",
        default=None,
        help="Optional input categorical_distribution table path",
    )
    parser.add_argument("--input-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument(
        "--hierarchy-sensor-map-path",
        default=None,
        help="Optional synthetic hierarchy sensor map path (from generate_synthetic_hierarchy_profile)",
    )
    parser.add_argument("--output-dir", default="data/profile_fleet")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "delta"])

    parser.add_argument("--tail-count", type=int, default=3)
    parser.add_argument("--flights-per-tail", type=int, default=2)
    parser.add_argument("--tail-id-prefix", default="FLEET_T")
    parser.add_argument("--flight-id-prefix", default="FL")
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--mean-tail-std-ratio", type=float, default=0.01)
    parser.add_argument("--mean-flight-std-ratio", type=float, default=0.005)
    parser.add_argument("--std-tail-std-ratio", type=float, default=0.10)
    parser.add_argument("--std-flight-std-ratio", type=float, default=0.05)
    parser.add_argument("--rate-tail-std-ratio", type=float, default=0.02)
    parser.add_argument("--rate-flight-std-ratio", type=float, default=0.01)
    parser.add_argument("--missing-tail-std", type=float, default=0.002)
    parser.add_argument("--missing-flight-std", type=float, default=0.001)

    parser.add_argument("--cat-logit-tail-std", type=float, default=0.15)
    parser.add_argument("--cat-logit-flight-std", type=float, default=0.08)
    parser.add_argument("--cat-sample-size", type=int, default=1000)

    parser.add_argument(
        "--hier-correlation-preset",
        choices=["easy", "medium", "hard"],
        default="medium",
        help="Hierarchy correlation difficulty preset. Use per-knob --hier-* flags to override specific values.",
    )
    parser.add_argument("--hier-mean-global-std-ratio", type=float, default=None)
    parser.add_argument("--hier-mean-system-std-ratio", type=float, default=None)
    parser.add_argument("--hier-mean-subsystem-std-ratio", type=float, default=None)
    parser.add_argument("--hier-mean-module-std-ratio", type=float, default=None)
    parser.add_argument("--hier-std-global-std-ratio", type=float, default=None)
    parser.add_argument("--hier-std-system-std-ratio", type=float, default=None)
    parser.add_argument("--hier-std-subsystem-std-ratio", type=float, default=None)
    parser.add_argument("--hier-std-module-std-ratio", type=float, default=None)
    parser.add_argument("--hier-rate-global-std-ratio", type=float, default=None)
    parser.add_argument("--hier-rate-system-std-ratio", type=float, default=None)
    parser.add_argument("--hier-rate-subsystem-std-ratio", type=float, default=None)
    parser.add_argument("--hier-rate-module-std-ratio", type=float, default=None)
    parser.add_argument("--hier-missing-global-std", type=float, default=None)
    parser.add_argument("--hier-missing-system-std", type=float, default=None)
    parser.add_argument("--hier-missing-subsystem-std", type=float, default=None)
    parser.add_argument("--hier-missing-module-std", type=float, default=None)
    parser.add_argument("--max-profile-params", type=int, default=1000)
    return parser.parse_args()


def _dedupe_parameter_rows(rows: list[dict[str, object]], max_profile_params: int) -> list[dict[str, object]]:
    by_parameter: dict[str, dict[str, object]] = {}
    for row in rows:
        parameter_name = str(row.get("parameter_name") or "")
        if not parameter_name:
            continue
        if parameter_name not in by_parameter:
            by_parameter[parameter_name] = row
    ordered = [by_parameter[name] for name in sorted(by_parameter.keys())]
    return ordered[: int(max_profile_params)]


def main() -> None:
    args = parse_args()
    spark = get_spark("s3ntinel.generate_fleet_profiles")
    rng = random.Random(args.seed)

    base_profile_df = read_table(spark, args.base_parameter_profile_path, fmt=args.input_format)
    base_profile_rows = [row.asDict(recursive=True) for row in base_profile_df.collect()]
    base_profile_rows = _dedupe_parameter_rows(base_profile_rows, max_profile_params=args.max_profile_params)

    if not base_profile_rows:
        raise ValueError("base parameter_profile is empty")

    base_category_rows: list[dict[str, object]] = []
    if args.base_categorical_distribution_path:
        base_category_df = read_table(spark, args.base_categorical_distribution_path, fmt=args.input_format)
        base_category_rows = [row.asDict(recursive=True) for row in base_category_df.collect()]

    hierarchy_map_by_parameter: dict[str, dict[str, str]] | None = None
    if args.hierarchy_sensor_map_path:
        hierarchy_df = read_table(spark, args.hierarchy_sensor_map_path, fmt=args.input_format)
        hierarchy_rows = [row.asDict(recursive=True) for row in hierarchy_df.collect()]
        hierarchy_map_by_parameter = {}
        for row in hierarchy_rows:
            parameter_name = str(row.get("parameter_name") or "")
            if not parameter_name:
                continue
            hierarchy_map_by_parameter[parameter_name] = {
                "system_id": str(row.get("system_id") or ""),
                "subsystem_id": str(row.get("subsystem_id") or ""),
                "module_id": str(row.get("module_id") or ""),
                "hierarchy_profile_id": str(row.get("hierarchy_profile_id") or ""),
                "hierarchy_source": str(row.get("hierarchy_source") or ""),
            }

    fleet_ids = make_fleet_ids(
        tail_count=args.tail_count,
        flights_per_tail=args.flights_per_tail,
        tail_id_prefix=args.tail_id_prefix,
        flight_id_prefix=args.flight_id_prefix,
    )

    if not fleet_ids:
        raise ValueError("fleet id set is empty; provide positive --tail-count and --flights-per-tail")

    numeric_cfg = NumericVarianceConfig(
        mean_tail_std_ratio=args.mean_tail_std_ratio,
        mean_flight_std_ratio=args.mean_flight_std_ratio,
        std_tail_std_ratio=args.std_tail_std_ratio,
        std_flight_std_ratio=args.std_flight_std_ratio,
        sampling_rate_tail_std_ratio=args.rate_tail_std_ratio,
        sampling_rate_flight_std_ratio=args.rate_flight_std_ratio,
        missing_rate_tail_std=args.missing_tail_std,
        missing_rate_flight_std=args.missing_flight_std,
    )
    cat_cfg = CategoricalVarianceConfig(
        logit_tail_std=args.cat_logit_tail_std,
        logit_flight_std=args.cat_logit_flight_std,
        sample_size=args.cat_sample_size,
    )
    hierarchy_cfg = get_hierarchy_variance_preset(args.hier_correlation_preset)
    hierarchy_overrides: dict[str, float | None] = {
        "mean_global_std_ratio": args.hier_mean_global_std_ratio,
        "mean_system_std_ratio": args.hier_mean_system_std_ratio,
        "mean_subsystem_std_ratio": args.hier_mean_subsystem_std_ratio,
        "mean_module_std_ratio": args.hier_mean_module_std_ratio,
        "std_global_std_ratio": args.hier_std_global_std_ratio,
        "std_system_std_ratio": args.hier_std_system_std_ratio,
        "std_subsystem_std_ratio": args.hier_std_subsystem_std_ratio,
        "std_module_std_ratio": args.hier_std_module_std_ratio,
        "rate_global_std_ratio": args.hier_rate_global_std_ratio,
        "rate_system_std_ratio": args.hier_rate_system_std_ratio,
        "rate_subsystem_std_ratio": args.hier_rate_subsystem_std_ratio,
        "rate_module_std_ratio": args.hier_rate_module_std_ratio,
        "missing_global_std": args.hier_missing_global_std,
        "missing_system_std": args.hier_missing_system_std,
        "missing_subsystem_std": args.hier_missing_subsystem_std,
        "missing_module_std": args.hier_missing_module_std,
    }
    for field_name, maybe_value in hierarchy_overrides.items():
        if maybe_value is not None:
            hierarchy_cfg = replace(hierarchy_cfg, **{field_name: float(maybe_value)})

    expanded_profile_rows = expand_parameter_profile_rows(
        base_rows=base_profile_rows,
        fleet_ids=fleet_ids,
        numeric_cfg=numeric_cfg,
        hierarchy_map_by_parameter=hierarchy_map_by_parameter,
        hierarchy_cfg=hierarchy_cfg,
        rng=rng,
    )

    expanded_category_rows: list[dict[str, object]] = []
    if base_category_rows:
        expanded_category_rows = expand_categorical_distribution_rows(
            base_rows=base_category_rows,
            fleet_ids=fleet_ids,
            categorical_cfg=cat_cfg,
            rng=rng,
        )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    profile_path = str(output_dir / "parameter_profile")
    category_path = str(output_dir / "categorical_distribution")
    manifest_path = str(output_dir / "fleet_manifest")

    spark.createDataFrame(expanded_profile_rows).write.format(args.output_format).mode("overwrite").save(profile_path)

    if expanded_category_rows:
        spark.createDataFrame(expanded_category_rows).write.format(args.output_format).mode("overwrite").save(category_path)

    manifest_rows = [{"tail_id": tail_id, "flight_id": flight_id} for tail_id, flight_id in fleet_ids]
    spark.createDataFrame(manifest_rows).write.format(args.output_format).mode("overwrite").save(manifest_path)

    print("Fleet profile artifacts written:")
    print(f"- parameter_profile: {profile_path}")
    if expanded_category_rows:
        print(f"- categorical_distribution: {category_path}")
    print(f"- fleet_manifest: {manifest_path}")
    print(f"- fleet_size: {len(fleet_ids)} flights ({args.tail_count} tails x {args.flights_per_tail} flights)")
    print(f"- parameters_per_flight: {len(base_profile_rows)}")
    if hierarchy_map_by_parameter is not None:
        print("- injected_hierarchy: enabled (synthetic_injected provenance columns added)")
        print(f"- hierarchy_correlation_preset: {args.hier_correlation_preset}")


if __name__ == "__main__":
    main()
