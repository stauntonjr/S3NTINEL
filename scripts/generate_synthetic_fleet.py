"""Generate synthetic telemetry for all tail/flight pairs in a fleet manifest."""

from __future__ import annotations

import argparse

from libs.io.delta import get_spark, read_table
from libs.profiling.synthetic import ParameterSpec, generate_synthetic_normal_telemetry


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic telemetry for a fleet manifest")
    parser.add_argument("--fleet-manifest-path", required=True, help="Path to fleet_manifest table")
    parser.add_argument("--profile-parameter-profile-path", required=True, help="Path to profile parameter_profile table")
    parser.add_argument(
        "--profile-categorical-distribution-path",
        default=None,
        help="Optional path to profile categorical_distribution table",
    )
    parser.add_argument("--profile-format", default="parquet", choices=["parquet", "delta"])

    parser.add_argument("--output-path", default="data/synthetic/fleet_raw_telemetry", help="Output table/path")
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append"])
    parser.add_argument(
        "--emit-manifest-partitions",
        action="store_true",
        help="Write one output dataset per tail/flight under output-path and emit a partition manifest table",
    )
    parser.add_argument(
        "--partition-manifest-path",
        default=None,
        help="Optional output path for partition manifest table (defaults to <output-path>/_partition_manifest)",
    )
    parser.add_argument("--duration-seconds", default=300, type=int)
    parser.add_argument("--start-ts", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--seed", default=42, type=int)
    parser.add_argument("--max-profile-params", default=200, type=int)
    parser.add_argument("--tail-id", default=None, help="Optional selector to run one tail_id")
    parser.add_argument("--flight-id", default=None, help="Optional selector to run one flight_id")
    parser.add_argument(
        "--strict-profile-scope",
        action="store_true",
        help="Fail if selected tail/flight has no profile rows when scoped profile columns exist",
    )
    return parser.parse_args()


def _category_map_for_scope(
    category_rows: list[dict[str, object]],
    tail_id: str,
    flight_id: str,
) -> dict[str, list[str]]:
    scoped = [
        row
        for row in category_rows
        if ("tail_id" not in row or str(row.get("tail_id") or "") == tail_id)
        and ("flight_id" not in row or str(row.get("flight_id") or "") == flight_id)
    ]
    grouped: dict[str, list[tuple[int, str]]] = {}
    for row in scoped:
        parameter_name = str(row.get("parameter_name") or "")
        parameter_value = str(row.get("parameter_value") or "")
        if not parameter_name or not parameter_value:
            continue
        rank = int(row.get("rank") or 0)
        grouped.setdefault(parameter_name, []).append((rank, parameter_value))

    out: dict[str, list[str]] = {}
    for parameter_name, ranked_values in grouped.items():
        values = [value for _, value in sorted(ranked_values, key=lambda item: item[0])]
        deduped: list[str] = []
        for value in values:
            if value not in deduped:
                deduped.append(value)
        out[parameter_name] = deduped
    return out


def _specs_from_scoped_rows(
    profile_rows: list[dict[str, object]],
    category_map: dict[str, list[str]],
    max_params: int,
) -> list[ParameterSpec]:
    selected = sorted(profile_rows, key=lambda row: str(row.get("parameter_name") or ""))[: int(max_params)]
    specs: list[ParameterSpec] = []

    for row in selected:
        parameter_name = str(row.get("parameter_name") or "")
        detected_type = str(row.get("detected_type") or "")
        if not parameter_name:
            continue

        sampling_rate_hz = max(float(row.get("sampling_rate_hz") or 1.0), 0.5)
        missing_rate = max(min(float(row.get("missing_rate") or 0.0), 0.95), 0.0)

        if detected_type == "numeric":
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
        elif detected_type in {"binary", "categorical"}:
            categories = tuple(category_map.get(parameter_name, []))
            if not categories:
                categories = ("ON", "OFF") if detected_type == "binary" else ("STATE_A", "STATE_B", "STATE_C")
            specs.append(
                ParameterSpec(
                    parameter_name=parameter_name,
                    detected_type=detected_type,
                    sampling_rate_hz=sampling_rate_hz,
                    categories=categories,
                    missing_rate=missing_rate,
                )
            )

    return specs


def _scope_profile_rows(
    all_rows: list[dict[str, object]],
    tail_id: str,
    flight_id: str,
    strict: bool,
) -> list[dict[str, object]]:
    scoped = [
        row
        for row in all_rows
        if ("tail_id" not in row or str(row.get("tail_id") or "") == tail_id)
        and ("flight_id" not in row or str(row.get("flight_id") or "") == flight_id)
    ]
    if scoped:
        return scoped

    has_scope_columns = any("tail_id" in row or "flight_id" in row for row in all_rows)
    if strict and has_scope_columns:
        raise ValueError(f"no parameter_profile rows for tail_id={tail_id}, flight_id={flight_id}")

    if has_scope_columns:
        return []
    return all_rows


def main() -> None:
    args = parse_args()
    spark = get_spark("s3ntinel.generate_synthetic_fleet")

    manifest_df = read_table(spark, args.fleet_manifest_path, fmt=args.profile_format).select("tail_id", "flight_id").distinct()
    if args.tail_id:
        manifest_df = manifest_df.where(manifest_df.tail_id == args.tail_id)
    if args.flight_id:
        manifest_df = manifest_df.where(manifest_df.flight_id == args.flight_id)

    manifest_rows = [
        (str(row["tail_id"]), str(row["flight_id"]))
        for row in manifest_df.collect()
        if row["tail_id"] is not None and row["flight_id"] is not None
    ]
    manifest_rows = sorted(set(manifest_rows))
    if not manifest_rows:
        raise ValueError("no fleet manifest rows selected")

    profile_df = read_table(spark, args.profile_parameter_profile_path, fmt=args.profile_format)
    profile_rows = [row.asDict(recursive=True) for row in profile_df.collect()]
    if not profile_rows:
        raise ValueError("profile parameter_profile is empty")

    category_rows: list[dict[str, object]] = []
    if args.profile_categorical_distribution_path:
        category_df = read_table(spark, args.profile_categorical_distribution_path, fmt=args.profile_format)
        category_rows = [row.asDict(recursive=True) for row in category_df.collect()]

    combined_df = None
    partition_rows: list[dict[str, object]] = []
    generated_flights = 0
    skipped_flights = 0

    for index, (tail_id, flight_id) in enumerate(manifest_rows):
        scoped_profile_rows = _scope_profile_rows(
            all_rows=profile_rows,
            tail_id=tail_id,
            flight_id=flight_id,
            strict=bool(args.strict_profile_scope),
        )
        if not scoped_profile_rows:
            skipped_flights += 1
            continue

        category_map = _category_map_for_scope(category_rows, tail_id=tail_id, flight_id=flight_id)
        specs = _specs_from_scoped_rows(
            profile_rows=scoped_profile_rows,
            category_map=category_map,
            max_params=args.max_profile_params,
        )
        if not specs:
            skipped_flights += 1
            continue

        flight_seed = int(args.seed) + index
        flight_df = generate_synthetic_normal_telemetry(
            spark=spark,
            duration_seconds=int(args.duration_seconds),
            tail_id=tail_id,
            flight_id=flight_id,
            start_ts=args.start_ts,
            specs=specs,
            seed=flight_seed,
        )

        generated_flights += 1
        if args.emit_manifest_partitions:
            partition_path = f"{args.output_path}/tail_id={tail_id}/flight_id={flight_id}"
            flight_df.write.format(args.output_format).mode(args.write_mode).save(partition_path)
            partition_rows.append(
                {
                    "tail_id": tail_id,
                    "flight_id": flight_id,
                    "output_path": partition_path,
                    "output_format": args.output_format,
                    "seed": int(flight_seed),
                }
            )
        else:
            combined_df = flight_df if combined_df is None else combined_df.unionByName(flight_df)

    if generated_flights <= 0:
        raise ValueError("no synthetic telemetry generated (all selected flights skipped)")

    if args.emit_manifest_partitions:
        manifest_path = args.partition_manifest_path or f"{args.output_path}/_partition_manifest"
        spark.createDataFrame(partition_rows).write.format(args.output_format).mode("overwrite").save(manifest_path)

        print("Synthetic fleet telemetry generated (partitioned mode):")
        print(f"- output_root: {args.output_path}")
        print(f"- partition_manifest_path: {manifest_path}")
        print(f"- output_format: {args.output_format}")
        print(f"- generated_flights: {generated_flights}")
        print(f"- skipped_flights: {skipped_flights}")
        return

    if combined_df is None:
        raise ValueError("no synthetic telemetry generated dataframe assembled")

    combined_df.write.format(args.output_format).mode(args.write_mode).save(args.output_path)

    print("Synthetic fleet telemetry generated:")
    print(f"- output_path: {args.output_path}")
    print(f"- output_format: {args.output_format}")
    print(f"- generated_flights: {generated_flights}")
    print(f"- skipped_flights: {skipped_flights}")
    print(f"- rows: {combined_df.count()}")


if __name__ == "__main__":
    main()
