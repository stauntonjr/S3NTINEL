"""Shared profile-driven stream specification loaders."""

from __future__ import annotations

from typing import Any

from libs.events.categorical import CategoricalDetectorConfig
from libs.io.delta import get_spark, read_parquet, read_table
from libs.profiling.synthetic import ParameterSpec


def specs_from_profile_payload(payload: dict[str, Any]) -> tuple[list[ParameterSpec], CategoricalDetectorConfig]:
    sensors = payload.get("sensors")
    if not isinstance(sensors, list) or not sensors:
        raise ValueError("profile JSON must include a non-empty 'sensors' array")

    specs: list[ParameterSpec] = []
    for item in sensors:
        if not isinstance(item, dict):
            continue
        name = str(item.get("parameter_name") or "").strip()
        detected_type = str(item.get("detected_type") or "").strip().lower()
        if not name or detected_type not in {"numeric", "categorical", "binary"}:
            continue

        rate_hz = float(item.get("sampling_rate_hz", 1.0) or 1.0)
        categories_raw = item.get("categories", ["ON", "OFF"])
        if isinstance(categories_raw, (list, tuple)):
            categories = tuple(str(x) for x in categories_raw if str(x))
        else:
            categories = ("ON", "OFF")

        specs.append(
            ParameterSpec(
                parameter_name=name,
                detected_type=detected_type,
                sampling_rate_hz=max(rate_hz, 0.5),
                mean=float(item.get("mean", 0.0) or 0.0) if detected_type == "numeric" else None,
                std=max(float(item.get("std", 1.0) or 1.0), 1e-6) if detected_type == "numeric" else None,
                min_value=float(item["min_value"]) if item.get("min_value") is not None else None,
                max_value=float(item["max_value"]) if item.get("max_value") is not None else None,
                categories=categories if categories else ("ON", "OFF"),
                missing_rate=max(min(float(item.get("missing_rate", 0.0) or 0.0), 0.95), 0.0),
                drift_per_sec=float(item.get("drift_per_sec", 0.0) or 0.0),
                noise_std=float(item["noise_std"]) if item.get("noise_std") is not None else None,
                oscillation_amplitude=float(item.get("oscillation_amplitude", 0.0) or 0.0),
                oscillation_hz=float(item.get("oscillation_hz", 0.0) or 0.0),
                switch_interval_s=float(item["switch_interval_s"]) if item.get("switch_interval_s") is not None else None,
                switch_magnitude=float(item.get("switch_magnitude", 0.0) or 0.0),
                missing_burst_every_s=(
                    float(item["missing_burst_every_s"]) if item.get("missing_burst_every_s") is not None else None
                ),
                missing_burst_len_s=float(item.get("missing_burst_len_s", 0.0) or 0.0),
            )
        )

    if not specs:
        raise ValueError("profile JSON did not produce any valid sensor specs")

    cat_cfg_raw = payload.get("categorical_detector", {})
    if not isinstance(cat_cfg_raw, dict):
        cat_cfg_raw = {}

    illegal_raw = cat_cfg_raw.get("illegal_transitions", [])
    illegal_pairs: set[tuple[str, str]] = set()
    if isinstance(illegal_raw, list):
        for pair in illegal_raw:
            if isinstance(pair, (list, tuple)) and len(pair) == 2:
                illegal_pairs.add((str(pair[0]), str(pair[1])))

    categorical_config = CategoricalDetectorConfig(
        min_dwell_seconds=max(float(cat_cfg_raw.get("min_dwell_seconds", 1.5) or 0.0), 0.0),
        max_dwell_seconds=max(float(cat_cfg_raw.get("max_dwell_seconds", 30.0) or 0.0), 0.0),
        emit_state_exit=bool(cat_cfg_raw.get("emit_state_exit", True)),
        emit_dwell_bucket=bool(cat_cfg_raw.get("emit_dwell_bucket", True)),
        illegal_transitions=frozenset(illegal_pairs),
    )
    return specs, categorical_config


def specs_from_profile_tables(
    parameter_profile_path: str,
    categorical_distribution_path: str | None,
    profile_format: str,
    max_profile_params: int,
    profile_tail_id: str | None = None,
    profile_flight_id: str | None = None,
) -> tuple[list[ParameterSpec], CategoricalDetectorConfig]:
    profile_rows: list[dict[str, Any]] = []
    category_rows: list[dict[str, Any]] = []

    if profile_format == "parquet":
        try:
            import pandas as pd
        except ModuleNotFoundError:
            try:
                spark = get_spark("s3ntinel.stream_profile_loader")
                profile_rows = [row.asDict(recursive=True) for row in read_parquet(spark, parameter_profile_path).collect()]
                if categorical_distribution_path:
                    category_rows = [
                        row.asDict(recursive=True)
                        for row in read_parquet(spark, categorical_distribution_path).collect()
                    ]
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "profile parquet loading requires either pandas (preferred for no-Spark mode) or pyspark"
                ) from exc
        else:
            profile_rows = pd.read_parquet(parameter_profile_path).to_dict(orient="records")
            if categorical_distribution_path:
                category_rows = pd.read_parquet(categorical_distribution_path).to_dict(orient="records")
    else:
        try:
            spark = get_spark("s3ntinel.stream_profile_loader")
            profile_rows = [
                row.asDict(recursive=True)
                for row in read_table(spark, parameter_profile_path, fmt=profile_format).collect()
            ]
            if categorical_distribution_path:
                category_rows = [
                    row.asDict(recursive=True)
                    for row in read_table(spark, categorical_distribution_path, fmt=profile_format).collect()
                ]
        except ModuleNotFoundError as exc:
            raise RuntimeError("profile delta loading requires pyspark in the active Python environment") from exc

    selected_tail = str(profile_tail_id).strip() if profile_tail_id else None
    selected_flight = str(profile_flight_id).strip() if profile_flight_id else None

    if selected_tail or selected_flight:
        def _matches_scope(item: dict[str, Any]) -> bool:
            item_tail = str(item.get("tail_id") or "")
            item_flight = str(item.get("flight_id") or "")
            if selected_tail and item_tail != selected_tail:
                return False
            if selected_flight and item_flight != selected_flight:
                return False
            return True

        filtered_profile_rows = [row for row in profile_rows if _matches_scope(row)]
        filtered_category_rows = [row for row in category_rows if _matches_scope(row)]

        if filtered_profile_rows:
            profile_rows = filtered_profile_rows
        elif any("tail_id" in row or "flight_id" in row for row in profile_rows):
            scope = f"tail_id={selected_tail or '*'}, flight_id={selected_flight or '*'}"
            raise ValueError(f"no parameter_profile rows found for {scope}")

        if filtered_category_rows:
            category_rows = filtered_category_rows
        elif any("tail_id" in row or "flight_id" in row for row in category_rows):
            category_rows = []

    cat_values_by_param: dict[str, list[str]] = {}
    category_rows = sorted(
        category_rows,
        key=lambda item: (
            str(item.get("parameter_name") or ""),
            int(item.get("rank") or 0),
        ),
    )
    for row in category_rows:
        parameter_name = str(row.get("parameter_name") or "")
        parameter_value = str(row.get("parameter_value") or "")
        if not parameter_name or not parameter_value:
            continue
        values = cat_values_by_param.setdefault(parameter_name, [])
        if parameter_value not in values:
            values.append(parameter_value)

    specs: list[ParameterSpec] = []
    profile_rows = sorted(profile_rows, key=lambda item: str(item.get("parameter_name") or ""))[: int(max_profile_params)]
    for row in profile_rows:
        detected_type = str(row.get("detected_type") or "")
        parameter_name = str(row.get("parameter_name") or "")
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
            categories = tuple(cat_values_by_param.get(parameter_name, []))
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

    if not specs:
        raise ValueError("profile tables did not produce any valid numeric/categorical/binary specs")

    categorical_config = CategoricalDetectorConfig(min_dwell_seconds=1.5, max_dwell_seconds=30.0)
    return specs, categorical_config
