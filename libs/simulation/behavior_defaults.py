from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from libs.common import SensorDataType, normalize_sensor_datatype
from libs.simulation.legacy import default_behavior_family_for_datatype


_BEHAVIOR_DEFAULTS = {
    SensorDataType.NUMERIC.value: {
        "trend_per_sec": 0.0,
        "osc_amp": 0.5,
        "osc_period_sec": 120.0,
        "noise_sigma": 0.1,
        "corr_scale": 0.4,
        "min_val": None,
        "max_val": None,
    },
    SensorDataType.BINARY.value: {
        "base_on_prob": 0.5,
        "latent_gain": 0.9,
        "persistence": 0.985,
    },
    SensorDataType.CATEGORICAL.value: {
        "states": ["STATE_A", "STATE_B", "STATE_C"],
        "latent_gain": 0.6,
        "persistence": 0.97,
    },
    SensorDataType.HIGH_CARDINALITY.value: {
        "base_prob": 0.01,
        "codes": [f"PFAULT_{i:03d}" for i in range(1, 40)],
    },
}


@dataclass(frozen=True, slots=True)
class ParameterBehaviorContext:
    parameter_name: str
    dtype: str
    corr_group: str
    behavior_family_label: str | None
    behavior_profile: dict[str, object]
    behavior_profile_confidence: float | None
    resolved_behavior_family: str | None
    scaling_profile: dict[str, float]
    incoming_coupling_count: int
    outgoing_coupling_count: int
    incoming_relation_types: list[str]
    outgoing_relation_types: list[str]
    upstream_module_ids: list[str]
    downstream_module_ids: list[str]


def _build_behavior_profile_lookup(
    parameter_behavior_profile_df: pd.DataFrame | None,
    *,
    min_confidence: float = 0.55,
) -> dict[str, dict[str, object]]:
    if parameter_behavior_profile_df is None or parameter_behavior_profile_df.empty:
        return {}

    expected_cols = {"parameter_name", "behavior_family_profiled", "behavior_profile_confidence"}
    if not expected_cols.issubset(parameter_behavior_profile_df.columns):
        return {}

    ranked = (
        parameter_behavior_profile_df.loc[
            :, [col for col in parameter_behavior_profile_df.columns if col in expected_cols]
        ]
        .copy()
        .assign(
            parameter_name=lambda df: df["parameter_name"].astype(str),
            behavior_family_profiled=lambda df: df["behavior_family_profiled"].astype(str),
            behavior_profile_confidence=lambda df: pd.to_numeric(
                df["behavior_profile_confidence"], errors="coerce"
            ).fillna(0.0),
        )
        .sort_values(["parameter_name", "behavior_profile_confidence"], ascending=[True, False])
        .drop_duplicates(subset=["parameter_name"], keep="first")
    )

    lookup: dict[str, dict[str, object]] = {}
    for row in ranked.to_dict("records"):
        confidence = float(row["behavior_profile_confidence"])
        if confidence < min_confidence:
            continue
        family = row.get("behavior_family_profiled")
        if family is None or pd.isna(family):
            continue
        family_text = str(family).strip()
        if not family_text or family_text.lower() == "nan":
            continue
        lookup[str(row["parameter_name"])] = {
            "behavior_family_profiled": family_text,
            "behavior_profile_confidence": confidence,
        }
    return lookup


def _build_scaling_profile_lookup(
    continuous_scaling_profile_df: pd.DataFrame | None,
) -> dict[str, dict[str, float]]:
    if continuous_scaling_profile_df is None or continuous_scaling_profile_df.empty:
        return {}

    expected_cols = {"parameter_name", "scaling_center_median", "scaling_iqr"}
    if not expected_cols.issubset(continuous_scaling_profile_df.columns):
        return {}

    ranked = (
        continuous_scaling_profile_df.loc[
            :,
            [
                col
                for col in continuous_scaling_profile_df.columns
                if col in expected_cols or col == "support_count"
            ],
        ]
        .copy()
        .assign(
            parameter_name=lambda df: df["parameter_name"].astype(str),
            scaling_center_median=lambda df: pd.to_numeric(df["scaling_center_median"], errors="coerce"),
            scaling_iqr=lambda df: pd.to_numeric(df["scaling_iqr"], errors="coerce"),
        )
        .drop_duplicates(subset=["parameter_name"], keep="first")
    )

    lookup: dict[str, dict[str, float]] = {}
    for row in ranked.to_dict("records"):
        center = row.get("scaling_center_median")
        iqr = row.get("scaling_iqr")
        if center is None or pd.isna(center):
            continue
        lookup[str(row["parameter_name"])] = {
            "scaling_center_median": float(center),
            "scaling_iqr": float(iqr) if iqr is not None and not pd.isna(iqr) else 0.0,
        }
    return lookup


def _as_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item is not None and str(item).strip()]
    if pd.isna(value):
        return []
    return [str(value)]


def _clean_optional_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _resolve_behavior_family(
    *,
    parameter_name: str,
    dtype: str,
    row: dict[str, object],
    behavior_profile_lookup: dict[str, dict[str, object]],
) -> tuple[str | None, dict[str, object], float | None]:
    behavior_family_label = _clean_optional_text(row.get("behavior_family_label"))
    behavior_profile = behavior_profile_lookup.get(parameter_name, {})
    resolved_behavior_family = _clean_optional_text(
        behavior_profile.get("behavior_family_profiled")
        or behavior_family_label
        or default_behavior_family_for_datatype(dtype)
    )
    behavior_profile_confidence = (
        float(behavior_profile["behavior_profile_confidence"])
        if "behavior_profile_confidence" in behavior_profile
        else None
    )
    return resolved_behavior_family, behavior_profile, behavior_profile_confidence


def _build_parameter_context(
    row: dict[str, object],
    *,
    behavior_profile_lookup: dict[str, dict[str, object]],
    scaling_profile_lookup: dict[str, dict[str, float]],
) -> ParameterBehaviorContext:
    parameter_name = str(row.get("parameter_name", row["sensor"]))
    dtype = normalize_sensor_datatype(row.get("parameter_datatype", SensorDataType.UNKNOWN.value))
    system_id = str(row["system_id"])
    subsystem_id = str(row["subsystem_id"])
    corr_group = f"{system_id}::{subsystem_id}"
    scaling_profile = scaling_profile_lookup.get(parameter_name, {})
    resolved_behavior_family, behavior_profile, behavior_profile_confidence = _resolve_behavior_family(
        parameter_name=parameter_name,
        dtype=dtype,
        row=row,
        behavior_profile_lookup=behavior_profile_lookup,
    )
    return ParameterBehaviorContext(
        parameter_name=parameter_name,
        dtype=dtype,
        corr_group=corr_group,
        behavior_family_label=_clean_optional_text(row.get("behavior_family_label")),
        behavior_profile=behavior_profile,
        behavior_profile_confidence=behavior_profile_confidence,
        resolved_behavior_family=resolved_behavior_family,
        scaling_profile=scaling_profile,
        incoming_coupling_count=int(row.get("incoming_coupling_count", 0) or 0),
        outgoing_coupling_count=int(row.get("outgoing_coupling_count", 0) or 0),
        incoming_relation_types=_as_string_list(row.get("incoming_relation_types")),
        outgoing_relation_types=_as_string_list(row.get("outgoing_relation_types")),
        upstream_module_ids=_as_string_list(row.get("upstream_module_ids")),
        downstream_module_ids=_as_string_list(row.get("downstream_module_ids")),
    )


def _build_coupling_metadata(context: ParameterBehaviorContext) -> dict[str, object]:
    scaling_profile = context.scaling_profile
    return {
        "parameter_name": context.parameter_name,
        "behavior_family_label": context.behavior_family_label,
        "behavior_family_profiled": _clean_optional_text(
            context.behavior_profile.get("behavior_family_profiled")
        ),
        "behavior_profile_confidence": context.behavior_profile_confidence,
        "resolved_behavior_family": context.resolved_behavior_family,
        "scaling_center_median": (
            float(scaling_profile["scaling_center_median"])
            if "scaling_center_median" in scaling_profile
            else None
        ),
        "scaling_iqr": float(scaling_profile.get("scaling_iqr", 0.0)) if scaling_profile else None,
        "incoming_coupling_count": context.incoming_coupling_count,
        "outgoing_coupling_count": context.outgoing_coupling_count,
        "incoming_relation_types": context.incoming_relation_types,
        "outgoing_relation_types": context.outgoing_relation_types,
        "upstream_module_ids": context.upstream_module_ids,
        "downstream_module_ids": context.downstream_module_ids,
    }


def _build_numeric_parameter_behavior(context: ParameterBehaviorContext) -> dict[str, object]:
    coupling_metadata = _build_coupling_metadata(context)
    parameter_name = context.parameter_name
    scaling_profile = context.scaling_profile

    baseline = {
        "elev_pos_l": 0.0,
        "elev_pos_r": 0.0,
        "ail_l_pos": 0.0,
        "ail_r_pos": 0.0,
        "rudder_pos": 0.0,
        "ap_pitch_cmd": 0.0,
        "fd_pitch_bar": 0.0,
        "fd_roll_bar": 0.0,
        "alpha_margin": 0.45,
        "gen_l_freq": 400.0,
        "gen_r_freq": 400.0,
        "gen_l_voltage": 115.0,
        "gen_r_voltage": 115.0,
        "apu_gen_load": 0.0,
        "ac_bus_a_load": 48.0,
        "ac_bus_b_load": 52.0,
        "dc_bus_v": 28.0,
        "dc_bus_i": 220.0,
        "bat_temp": 29.0,
        "pack_l_temp_out": 12.0,
        "pack_r_temp_out": 12.5,
        "pack_l_flow": 1.6,
        "pack_r_flow": 1.5,
        "outflow_cmd": 35.0,
        "outflow_pos": 34.0,
        "cabin_alt": 800.0,
        "cabin_rate": 0.0,
    }.get(parameter_name, 1.0)
    clip = {
        "alpha_margin": (0.05, 1.5),
        "ac_bus_a_load": (0.0, 140.0),
        "ac_bus_b_load": (0.0, 140.0),
        "outflow_cmd": (0.0, 100.0),
        "outflow_pos": (0.0, 100.0),
        "cabin_alt": (0.0, 12000.0),
        "pack_l_flow": (0.0, 4.0),
        "pack_r_flow": (0.0, 4.0),
    }.get(parameter_name, (None, None))
    if "scaling_center_median" in scaling_profile:
        baseline = float(scaling_profile["scaling_center_median"])
    scaling_iqr = float(max(scaling_profile.get("scaling_iqr", 0.0), 0.0))

    numeric_corr_scale = float(_BEHAVIOR_DEFAULTS[SensorDataType.NUMERIC.value]["corr_scale"])
    numeric_corr_scale *= 1.0 + 0.12 * min(context.incoming_coupling_count, 3) + 0.08 * min(
        context.outgoing_coupling_count, 3
    )
    if "drive" in context.incoming_relation_types:
        numeric_corr_scale *= 1.15
    if "drive" in context.outgoing_relation_types:
        numeric_corr_scale *= 1.05

    numeric_noise_sigma = (
        0.15
        if "cabin" in parameter_name
        else _BEHAVIOR_DEFAULTS[SensorDataType.NUMERIC.value]["noise_sigma"]
    )
    numeric_noise_sigma *= max(0.7, 1.0 - 0.06 * min(context.incoming_coupling_count, 3))
    numeric_osc_amp = (
        0.25
        if "bus" in parameter_name
        else _BEHAVIOR_DEFAULTS[SensorDataType.NUMERIC.value]["osc_amp"]
    )
    numeric_osc_period_sec = (
        90.0
        if "pack" in parameter_name
        else _BEHAVIOR_DEFAULTS[SensorDataType.NUMERIC.value]["osc_period_sec"]
    )
    numeric_trend_per_sec = float(_BEHAVIOR_DEFAULTS[SensorDataType.NUMERIC.value]["trend_per_sec"])

    if scaling_iqr > 0.0:
        numeric_osc_amp = float(max(min(0.5 * scaling_iqr, 5.0), 0.02))
        numeric_noise_sigma = float(max(min(0.08 * scaling_iqr, 1.5), 0.005))

    if context.resolved_behavior_family == "regulated":
        numeric_osc_amp *= 0.35
        numeric_noise_sigma *= 0.8
        numeric_corr_scale *= 0.95
    elif context.resolved_behavior_family == "inertial":
        numeric_osc_amp *= 0.2
        numeric_noise_sigma *= 0.85
        numeric_corr_scale *= 1.2
    elif context.resolved_behavior_family == "accumulative":
        numeric_osc_amp *= 0.05
        numeric_noise_sigma *= 0.6
        numeric_corr_scale *= 1.05
        numeric_trend_per_sec = float(max(abs(numeric_trend_per_sec), 0.002))

    return {
        "datatype": SensorDataType.NUMERIC.value,
        "baseline": float(baseline),
        "corr_group": context.corr_group,
        "trend_per_sec": float(numeric_trend_per_sec),
        "osc_amp": float(numeric_osc_amp),
        "osc_period_sec": float(numeric_osc_period_sec),
        "noise_sigma": float(numeric_noise_sigma),
        "corr_scale": float(numeric_corr_scale),
        "min_val": clip[0],
        "max_val": clip[1],
        **coupling_metadata,
    }


def _build_binary_parameter_behavior(context: ParameterBehaviorContext) -> dict[str, object]:
    coupling_metadata = _build_coupling_metadata(context)
    binary_latent_gain = float(_BEHAVIOR_DEFAULTS[SensorDataType.BINARY.value]["latent_gain"])
    binary_latent_gain *= 1.0 + 0.10 * min(
        context.incoming_coupling_count + context.outgoing_coupling_count, 3
    )
    binary_persistence = min(
        0.997,
        float(_BEHAVIOR_DEFAULTS[SensorDataType.BINARY.value]["persistence"])
        + 0.002 * context.incoming_coupling_count
        + 0.004 * context.outgoing_coupling_count,
    )
    if context.resolved_behavior_family == "discrete_state":
        binary_latent_gain *= 1.05
        binary_persistence = min(binary_persistence + 0.002, 0.998)
    return {
        "datatype": SensorDataType.BINARY.value,
        "corr_group": context.corr_group,
        "base_on_prob": 0.15 if "apu" in context.parameter_name else 0.7,
        "latent_gain": float(binary_latent_gain),
        "persistence": float(binary_persistence),
        "states": ["0", "1"],
        **coupling_metadata,
    }


def _build_categorical_parameter_behavior(context: ParameterBehaviorContext) -> dict[str, object]:
    coupling_metadata = _build_coupling_metadata(context)
    states = {
        "yaw_damper_mode": ["OFF", "STBY", "ON"],
        "bat_contact_state": ["OPEN", "TRANSIENT", "CLOSED"],
        "press_mode": ["AUTO", "ALTN", "MANUAL"],
    }.get(context.parameter_name, _BEHAVIOR_DEFAULTS[SensorDataType.CATEGORICAL.value]["states"])
    categorical_latent_gain = float(_BEHAVIOR_DEFAULTS[SensorDataType.CATEGORICAL.value]["latent_gain"])
    categorical_latent_gain *= 1.0 + 0.08 * min(
        context.incoming_coupling_count + context.outgoing_coupling_count, 3
    )
    categorical_persistence = min(
        0.995,
        float(_BEHAVIOR_DEFAULTS[SensorDataType.CATEGORICAL.value]["persistence"])
        + 0.003 * context.incoming_coupling_count
        + 0.002 * context.outgoing_coupling_count,
    )
    if context.resolved_behavior_family == "discrete_state":
        categorical_latent_gain *= 1.05
        categorical_persistence = min(categorical_persistence + 0.003, 0.996)
    return {
        "datatype": SensorDataType.CATEGORICAL.value,
        "corr_group": context.corr_group,
        "states": states,
        "base_probs": [0.65, 0.25, 0.10][: len(states)] if len(states) == 3 else None,
        "latent_gain": float(categorical_latent_gain),
        "persistence": float(categorical_persistence),
        **coupling_metadata,
    }


def _build_high_cardinality_parameter_behavior(context: ParameterBehaviorContext) -> dict[str, object]:
    return {
        "datatype": SensorDataType.HIGH_CARDINALITY.value,
        "corr_group": context.corr_group,
        "base_prob": _BEHAVIOR_DEFAULTS[SensorDataType.HIGH_CARDINALITY.value]["base_prob"],
        "codes": _BEHAVIOR_DEFAULTS[SensorDataType.HIGH_CARDINALITY.value]["codes"],
        **_build_coupling_metadata(context),
    }


def build_default_parameter_behavior(
    hierarchy_df: pd.DataFrame,
    *,
    parameter_behavior_profile_df: pd.DataFrame | None = None,
    continuous_scaling_profile_df: pd.DataFrame | None = None,
) -> dict[str, dict]:
    behavior_profile_lookup = _build_behavior_profile_lookup(parameter_behavior_profile_df)
    scaling_profile_lookup = _build_scaling_profile_lookup(continuous_scaling_profile_df)

    behavior: dict[str, dict] = {}
    for row in hierarchy_df.to_dict("records"):
        context = _build_parameter_context(
            row,
            behavior_profile_lookup=behavior_profile_lookup,
            scaling_profile_lookup=scaling_profile_lookup,
        )
        if context.dtype == SensorDataType.NUMERIC.value:
            behavior[context.parameter_name] = _build_numeric_parameter_behavior(context)
        elif context.dtype == SensorDataType.BINARY.value:
            behavior[context.parameter_name] = _build_binary_parameter_behavior(context)
        elif context.dtype == SensorDataType.CATEGORICAL.value:
            behavior[context.parameter_name] = _build_categorical_parameter_behavior(context)
        else:
            behavior[context.parameter_name] = _build_high_cardinality_parameter_behavior(context)
    return behavior
