"""Expand baseline profile artifacts into fleet-scoped profile tables."""

from __future__ import annotations

from dataclasses import dataclass
import math
import random
from typing import Any


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, float(value)))


def make_fleet_ids(
    tail_count: int,
    flights_per_tail: int,
    tail_id_prefix: str,
    flight_id_prefix: str,
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for tail_index in range(max(int(tail_count), 0)):
        tail_id = f"{tail_id_prefix}{tail_index + 1:03d}"
        for flight_index in range(max(int(flights_per_tail), 0)):
            flight_id = f"{flight_id_prefix}{flight_index + 1:03d}"
            out.append((tail_id, flight_id))
    return out


@dataclass(frozen=True)
class NumericVarianceConfig:
    mean_tail_std_ratio: float = 0.01
    mean_flight_std_ratio: float = 0.005
    std_tail_std_ratio: float = 0.10
    std_flight_std_ratio: float = 0.05
    sampling_rate_tail_std_ratio: float = 0.02
    sampling_rate_flight_std_ratio: float = 0.01
    missing_rate_tail_std: float = 0.002
    missing_rate_flight_std: float = 0.001


@dataclass(frozen=True)
class CategoricalVarianceConfig:
    logit_tail_std: float = 0.15
    logit_flight_std: float = 0.08
    sample_size: int = 1000


@dataclass(frozen=True)
class HierarchyVarianceConfig:
    mean_global_std_ratio: float = 0.005
    mean_system_std_ratio: float = 0.02
    mean_subsystem_std_ratio: float = 0.01
    mean_module_std_ratio: float = 0.005
    std_global_std_ratio: float = 0.01
    std_system_std_ratio: float = 0.08
    std_subsystem_std_ratio: float = 0.04
    std_module_std_ratio: float = 0.02
    rate_global_std_ratio: float = 0.005
    rate_system_std_ratio: float = 0.02
    rate_subsystem_std_ratio: float = 0.01
    rate_module_std_ratio: float = 0.005
    missing_global_std: float = 0.0005
    missing_system_std: float = 0.002
    missing_subsystem_std: float = 0.001
    missing_module_std: float = 0.0005


def get_hierarchy_variance_preset(preset: str) -> HierarchyVarianceConfig:
    normalized = str(preset or "medium").strip().lower()
    if normalized == "easy":
        return HierarchyVarianceConfig(
            mean_global_std_ratio=0.015,
            mean_system_std_ratio=0.05,
            mean_subsystem_std_ratio=0.03,
            mean_module_std_ratio=0.02,
            std_global_std_ratio=0.03,
            std_system_std_ratio=0.12,
            std_subsystem_std_ratio=0.07,
            std_module_std_ratio=0.04,
            rate_global_std_ratio=0.012,
            rate_system_std_ratio=0.04,
            rate_subsystem_std_ratio=0.025,
            rate_module_std_ratio=0.015,
            missing_global_std=0.002,
            missing_system_std=0.01,
            missing_subsystem_std=0.006,
            missing_module_std=0.003,
        )
    if normalized == "hard":
        return HierarchyVarianceConfig(
            mean_global_std_ratio=0.003,
            mean_system_std_ratio=0.015,
            mean_subsystem_std_ratio=0.008,
            mean_module_std_ratio=0.004,
            std_global_std_ratio=0.006,
            std_system_std_ratio=0.04,
            std_subsystem_std_ratio=0.02,
            std_module_std_ratio=0.01,
            rate_global_std_ratio=0.003,
            rate_system_std_ratio=0.015,
            rate_subsystem_std_ratio=0.008,
            rate_module_std_ratio=0.004,
            missing_global_std=0.0003,
            missing_system_std=0.0015,
            missing_subsystem_std=0.0008,
            missing_module_std=0.0004,
        )
    return HierarchyVarianceConfig(
        mean_global_std_ratio=0.008,
        mean_system_std_ratio=0.03,
        mean_subsystem_std_ratio=0.015,
        mean_module_std_ratio=0.01,
        std_global_std_ratio=0.015,
        std_system_std_ratio=0.06,
        std_subsystem_std_ratio=0.03,
        std_module_std_ratio=0.015,
        rate_global_std_ratio=0.008,
        rate_system_std_ratio=0.03,
        rate_subsystem_std_ratio=0.015,
        rate_module_std_ratio=0.01,
        missing_global_std=0.001,
        missing_system_std=0.004,
        missing_subsystem_std=0.002,
        missing_module_std=0.001,
    )


def expand_parameter_profile_rows(
    base_rows: list[dict[str, Any]],
    fleet_ids: list[tuple[str, str]],
    numeric_cfg: NumericVarianceConfig,
    hierarchy_map_by_parameter: dict[str, dict[str, str]] | None,
    hierarchy_cfg: HierarchyVarianceConfig,
    rng: random.Random,
) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    tail_offsets: dict[tuple[str, str], dict[str, float]] = {}
    global_offsets = {
        "mean": rng.gauss(0.0, max(float(hierarchy_cfg.mean_global_std_ratio), 0.0)),
        "std": rng.gauss(0.0, max(float(hierarchy_cfg.std_global_std_ratio), 0.0)),
        "rate": rng.gauss(0.0, max(float(hierarchy_cfg.rate_global_std_ratio), 0.0)),
        "missing": rng.gauss(0.0, max(float(hierarchy_cfg.missing_global_std), 0.0)),
    }
    system_offsets: dict[str, dict[str, float]] = {}
    subsystem_offsets: dict[str, dict[str, float]] = {}
    module_offsets: dict[str, dict[str, float]] = {}

    for tail_id, flight_id in fleet_ids:
        for base in base_rows:
            parameter_name = str(base.get("parameter_name") or "")
            if not parameter_name:
                continue

            dtype = str(base.get("detected_type") or "")
            output_row = dict(base)
            output_row["tail_id"] = tail_id
            output_row["flight_id"] = flight_id

            tail_key = (tail_id, parameter_name)
            if tail_key not in tail_offsets:
                tail_offsets[tail_key] = {
                    "mean": rng.gauss(0.0, max(float(numeric_cfg.mean_tail_std_ratio), 0.0)),
                    "std": rng.gauss(0.0, max(float(numeric_cfg.std_tail_std_ratio), 0.0)),
                    "rate": rng.gauss(0.0, max(float(numeric_cfg.sampling_rate_tail_std_ratio), 0.0)),
                    "missing": rng.gauss(0.0, max(float(numeric_cfg.missing_rate_tail_std), 0.0)),
                }

            flight_offsets = {
                "mean": rng.gauss(0.0, max(float(numeric_cfg.mean_flight_std_ratio), 0.0)),
                "std": rng.gauss(0.0, max(float(numeric_cfg.std_flight_std_ratio), 0.0)),
                "rate": rng.gauss(0.0, max(float(numeric_cfg.sampling_rate_flight_std_ratio), 0.0)),
                "missing": rng.gauss(0.0, max(float(numeric_cfg.missing_rate_flight_std), 0.0)),
            }

            hierarchy_info = (
                hierarchy_map_by_parameter.get(parameter_name, {})
                if hierarchy_map_by_parameter is not None
                else {}
            )
            system_id = str(hierarchy_info.get("system_id") or "")
            subsystem_id = str(hierarchy_info.get("subsystem_id") or "")
            module_id = str(hierarchy_info.get("module_id") or "")

            if system_id and system_id not in system_offsets:
                system_offsets[system_id] = {
                    "mean": rng.gauss(0.0, max(float(hierarchy_cfg.mean_system_std_ratio), 0.0)),
                    "std": rng.gauss(0.0, max(float(hierarchy_cfg.std_system_std_ratio), 0.0)),
                    "rate": rng.gauss(0.0, max(float(hierarchy_cfg.rate_system_std_ratio), 0.0)),
                    "missing": rng.gauss(0.0, max(float(hierarchy_cfg.missing_system_std), 0.0)),
                }
            if subsystem_id and subsystem_id not in subsystem_offsets:
                subsystem_offsets[subsystem_id] = {
                    "mean": rng.gauss(0.0, max(float(hierarchy_cfg.mean_subsystem_std_ratio), 0.0)),
                    "std": rng.gauss(0.0, max(float(hierarchy_cfg.std_subsystem_std_ratio), 0.0)),
                    "rate": rng.gauss(0.0, max(float(hierarchy_cfg.rate_subsystem_std_ratio), 0.0)),
                    "missing": rng.gauss(0.0, max(float(hierarchy_cfg.missing_subsystem_std), 0.0)),
                }
            if module_id and module_id not in module_offsets:
                module_offsets[module_id] = {
                    "mean": rng.gauss(0.0, max(float(hierarchy_cfg.mean_module_std_ratio), 0.0)),
                    "std": rng.gauss(0.0, max(float(hierarchy_cfg.std_module_std_ratio), 0.0)),
                    "rate": rng.gauss(0.0, max(float(hierarchy_cfg.rate_module_std_ratio), 0.0)),
                    "missing": rng.gauss(0.0, max(float(hierarchy_cfg.missing_module_std), 0.0)),
                }

            system_offset = system_offsets.get(system_id, {"mean": 0.0, "std": 0.0, "rate": 0.0, "missing": 0.0})
            subsystem_offset = subsystem_offsets.get(subsystem_id, {"mean": 0.0, "std": 0.0, "rate": 0.0, "missing": 0.0})
            module_offset = module_offsets.get(module_id, {"mean": 0.0, "std": 0.0, "rate": 0.0, "missing": 0.0})

            if dtype == "numeric":
                base_mean = float(base.get("num_mean") or 0.0)
                base_std = max(float(base.get("num_std") or 0.0), 1e-6)
                base_min = base.get("num_min")
                base_max = base.get("num_max")
                base_rate = max(float(base.get("sampling_rate_hz") or 1.0), 0.1)
                base_missing = clamp(float(base.get("missing_rate") or 0.0), 0.0, 0.95)

                mean_ratio = (
                    global_offsets["mean"]
                    +
                    tail_offsets[tail_key]["mean"]
                    + flight_offsets["mean"]
                    + system_offset["mean"]
                    + subsystem_offset["mean"]
                    + module_offset["mean"]
                )
                std_ratio = (
                    global_offsets["std"]
                    +
                    tail_offsets[tail_key]["std"]
                    + flight_offsets["std"]
                    + system_offset["std"]
                    + subsystem_offset["std"]
                    + module_offset["std"]
                )
                rate_ratio = (
                    global_offsets["rate"]
                    +
                    tail_offsets[tail_key]["rate"]
                    + flight_offsets["rate"]
                    + system_offset["rate"]
                    + subsystem_offset["rate"]
                    + module_offset["rate"]
                )
                missing_delta = (
                    global_offsets["missing"]
                    +
                    tail_offsets[tail_key]["missing"]
                    + flight_offsets["missing"]
                    + system_offset["missing"]
                    + subsystem_offset["missing"]
                    + module_offset["missing"]
                )

                new_mean = base_mean * (1.0 + mean_ratio)
                new_std = max(base_std * (1.0 + std_ratio), 1e-6)
                new_rate = max(base_rate * (1.0 + rate_ratio), 0.5)
                new_missing = clamp(base_missing + missing_delta, 0.0, 0.95)

                output_row["num_mean"] = float(new_mean)
                output_row["num_std"] = float(new_std)
                output_row["sampling_rate_hz"] = float(new_rate)
                output_row["missing_rate"] = float(new_missing)

                if base_min is not None:
                    output_row["num_min"] = float(base_min) * (1.0 + mean_ratio)
                if base_max is not None:
                    output_row["num_max"] = float(base_max) * (1.0 + mean_ratio)

                q01 = base.get("num_q01")
                q50 = base.get("num_q50")
                q99 = base.get("num_q99")
                if q01 is not None:
                    output_row["num_q01"] = float(q01) * (1.0 + mean_ratio)
                if q50 is not None:
                    output_row["num_q50"] = float(q50) * (1.0 + mean_ratio)
                if q99 is not None:
                    output_row["num_q99"] = float(q99) * (1.0 + mean_ratio)
                output_row["numeric_rate"] = 1.0
            else:
                base_rate = max(float(base.get("sampling_rate_hz") or 1.0), 0.1)
                base_missing = clamp(float(base.get("missing_rate") or 0.0), 0.0, 0.95)
                rate_ratio = (
                    global_offsets["rate"]
                    +
                    tail_offsets[tail_key]["rate"]
                    + flight_offsets["rate"]
                    + system_offset["rate"]
                    + subsystem_offset["rate"]
                    + module_offset["rate"]
                )
                missing_delta = (
                    global_offsets["missing"]
                    +
                    tail_offsets[tail_key]["missing"]
                    + flight_offsets["missing"]
                    + system_offset["missing"]
                    + subsystem_offset["missing"]
                    + module_offset["missing"]
                )
                output_row["sampling_rate_hz"] = float(max(base_rate * (1.0 + rate_ratio), 0.5))
                output_row["missing_rate"] = float(clamp(base_missing + missing_delta, 0.0, 0.95))

            if hierarchy_info:
                output_row["injected_system_id"] = system_id
                output_row["injected_subsystem_id"] = subsystem_id
                output_row["injected_module_id"] = module_id
                output_row["injected_hierarchy_profile_id"] = str(hierarchy_info.get("hierarchy_profile_id") or "")
                output_row["injected_hierarchy_source"] = str(hierarchy_info.get("hierarchy_source") or "")

            expanded.append(output_row)

    return expanded


def expand_categorical_distribution_rows(
    base_rows: list[dict[str, Any]],
    fleet_ids: list[tuple[str, str]],
    categorical_cfg: CategoricalVarianceConfig,
    rng: random.Random,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in base_rows:
        parameter_name = str(row.get("parameter_name") or "")
        value = str(row.get("parameter_value") or "")
        if not parameter_name or not value:
            continue
        grouped.setdefault(parameter_name, []).append(row)

    out: list[dict[str, Any]] = []
    for tail_id, flight_id in fleet_ids:
        for parameter_name, rows in grouped.items():
            base_counts = [max(float(item.get("value_count") or 0.0), 0.0) for item in rows]
            total = sum(base_counts)
            if total <= 0:
                continue

            base_probs = [count / total for count in base_counts]
            tail_noise = [
                rng.gauss(0.0, max(float(categorical_cfg.logit_tail_std), 0.0))
                for _ in rows
            ]
            flight_noise = [
                rng.gauss(0.0, max(float(categorical_cfg.logit_flight_std), 0.0))
                for _ in rows
            ]

            logits = [
                math.log(max(prob, 1e-12)) + tail_noise[index] + flight_noise[index]
                for index, prob in enumerate(base_probs)
            ]
            max_logit = max(logits)
            exp_scores = [math.exp(item - max_logit) for item in logits]
            score_total = max(sum(exp_scores), 1e-12)
            probs = [item / score_total for item in exp_scores]

            sample_size = max(int(categorical_cfg.sample_size), 1)
            raw_counts = [int(round(prob * sample_size)) for prob in probs]
            residual = sample_size - sum(raw_counts)
            if residual != 0:
                order = sorted(range(len(probs)), key=lambda idx: probs[idx], reverse=True)
                step = 1 if residual > 0 else -1
                for idx in order[: abs(residual)]:
                    raw_counts[idx] = max(raw_counts[idx] + step, 0)

            rank_order = sorted(range(len(rows)), key=lambda idx: (raw_counts[idx], probs[idx]), reverse=True)
            rank_by_index = {idx: rank + 1 for rank, idx in enumerate(rank_order)}

            for index, row in enumerate(rows):
                out.append(
                    {
                        "tail_id": tail_id,
                        "flight_id": flight_id,
                        "parameter_name": str(row.get("parameter_name") or ""),
                        "parameter_value": str(row.get("parameter_value") or ""),
                        "value_count": int(raw_counts[index]),
                        "rank": int(rank_by_index[index]),
                    }
                )

    return out
