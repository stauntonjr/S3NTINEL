from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from libs.simulation.dataset_bridge import (
    build_subsystem_slice_hierarchy_df as _bridge_build_subsystem_slice_hierarchy_df,
)
from libs.simulation.dataset_bridge import flatten_assembly_spec as _bridge_flatten_assembly_spec
from libs.simulation.fleet_dataset import (
    attach_event_labels_to_telemetry_df as _fleet_attach_event_labels_to_telemetry_df,
)
from libs.simulation.fleet_dataset import (
    attach_event_labels_to_telemetry_rows as _fleet_attach_event_labels_to_telemetry_rows,
)
from libs.simulation.fleet_dataset import (
    detect_parameter_event_labels_df as _fleet_detect_parameter_event_labels_df,
)
from libs.simulation.fleet_dataset import (
    normalize_corr_group_key as _fleet_normalize_corr_group_key,
)
from libs.simulation.fleet_dataset import (
    resolve_delay_map_for_groups as _fleet_resolve_delay_map_for_groups,
)
from libs.simulation.fleet_dataset import (
    simulate_fleet_dataset as _fleet_simulate_fleet_dataset,
)
from libs.simulation.fleet_dataset import (
    simulate_fleet_dataset_from_assembly as _fleet_simulate_fleet_dataset_from_assembly,
)
from libs.simulation.fleet_dataset import (
    simulate_fleet_dataset_from_subsystem_slice as _fleet_simulate_fleet_dataset_from_subsystem_slice,
)
from libs.simulation.fleet_dataset import (
    simulate_fleet_dataset_spark as _fleet_simulate_fleet_dataset_spark,
)
from libs.simulation.fleet_dataset import (
    simulate_fleet_dataset_spark_from_assembly as _fleet_simulate_fleet_dataset_spark_from_assembly,
)
from libs.simulation.fleet_dataset import (
    simulate_fleet_dataset_spark_from_subsystem_slice as _fleet_simulate_fleet_dataset_spark_from_subsystem_slice,
)
from libs.simulation.fleet_dataset import value_col as _fleet_value_col
from libs.simulation.setup_builders import (
    build_default_parameter_behavior as _builders_build_default_parameter_behavior,
)
from libs.simulation.setup_builders import build_fleet_manifest as _builders_build_fleet_manifest
from libs.simulation.setup_builders import build_mermaid_hierarchy as _builders_build_mermaid_hierarchy
from libs.simulation.setup_builders import build_tail_profiles as _builders_build_tail_profiles
from libs.simulation.setup_builders import default_phase_definitions as _builders_default_phase_definitions
from libs.simulation.setup_builders import flatten_hierarchy_spec as _builders_flatten_hierarchy_spec
from libs.simulation.specs import HierarchyAssemblySpec


def _normalize_corr_group_key(value: object) -> str:
    return _fleet_normalize_corr_group_key(value)


def _resolve_delay_map_for_groups(delay_map_raw: dict, corr_groups: list[str]) -> tuple[dict[str, float], list[str]]:
    return _fleet_resolve_delay_map_for_groups(delay_map_raw, corr_groups)


def flatten_hierarchy_spec(hierarchy_spec: dict) -> pd.DataFrame:
    return _builders_flatten_hierarchy_spec(hierarchy_spec)


def flatten_assembly_spec(assembly_spec: HierarchyAssemblySpec) -> pd.DataFrame:
    return _bridge_flatten_assembly_spec(assembly_spec)


def build_subsystem_slice_hierarchy_df(slice_name: str) -> pd.DataFrame:
    return _bridge_build_subsystem_slice_hierarchy_df(slice_name)


def build_mermaid_hierarchy(hierarchy_df: pd.DataFrame, max_sensors: int = 200) -> str:
    return _builders_build_mermaid_hierarchy(hierarchy_df, max_sensors=max_sensors)


def build_default_parameter_behavior(
    hierarchy_df: pd.DataFrame,
    *,
    parameter_behavior_profile_df: pd.DataFrame | None = None,
    continuous_scaling_profile_df: pd.DataFrame | None = None,
) -> dict[str, dict]:
    return _builders_build_default_parameter_behavior(
        hierarchy_df,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        continuous_scaling_profile_df=continuous_scaling_profile_df,
    )


def default_phase_definitions() -> list[dict]:
    return _builders_default_phase_definitions()


def build_tail_profiles(systems: list[str], m_tails: int, rng: np.random.Generator) -> list[dict]:
    return _builders_build_tail_profiles(systems, m_tails=m_tails, rng=rng)


def build_fleet_manifest(
    tail_profiles: list[dict],
    n_flights_per_tail: int,
    rng: np.random.Generator,
    start_ts: datetime | None = None,
) -> pd.DataFrame:
    return _builders_build_fleet_manifest(
        tail_profiles,
        n_flights_per_tail=n_flights_per_tail,
        rng=rng,
        start_ts=start_ts,
    )


def _value_col(df: pd.DataFrame) -> str | None:
    return _fleet_value_col(df)


def _detect_parameter_event_labels_df(telemetry_df: pd.DataFrame, *, value_col: str | None = None) -> pd.DataFrame:
    return _fleet_detect_parameter_event_labels_df(telemetry_df, value_col_name=value_col)


def _attach_event_labels_to_telemetry_df(telemetry_df: pd.DataFrame, *, label_value_col: str = "parameter_value_clean") -> pd.DataFrame:
    return _fleet_attach_event_labels_to_telemetry_df(telemetry_df, label_value_col=label_value_col)


def _attach_event_labels_to_telemetry_rows(telemetry_rows: list[dict], *, label_value_col: str = "parameter_value_clean") -> list[dict]:
    return _fleet_attach_event_labels_to_telemetry_rows(telemetry_rows, label_value_col=label_value_col)


def simulate_fleet_dataset(
    *,
    hierarchy_df: pd.DataFrame,
    parameter_behavior: dict[str, dict],
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _fleet_simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=parameter_behavior,
        phase_definitions=phase_definitions,
        flight_setup=flight_setup,
        tail_profiles=tail_profiles,
        fleet_manifest_df=fleet_manifest_df,
    )


def simulate_fleet_dataset_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
    parameter_behavior: dict[str, dict] | None = None,
    parameter_behavior_profile_df: pd.DataFrame | None = None,
    continuous_scaling_profile_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _fleet_simulate_fleet_dataset_from_assembly(
        assembly_spec=assembly_spec,
        phase_definitions=phase_definitions,
        flight_setup=flight_setup,
        tail_profiles=tail_profiles,
        fleet_manifest_df=fleet_manifest_df,
        parameter_behavior=parameter_behavior,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        continuous_scaling_profile_df=continuous_scaling_profile_df,
    )


def simulate_fleet_dataset_from_subsystem_slice(
    *,
    slice_name: str,
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
    parameter_behavior: dict[str, dict] | None = None,
    parameter_behavior_profile_df: pd.DataFrame | None = None,
    continuous_scaling_profile_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _fleet_simulate_fleet_dataset_from_subsystem_slice(
        slice_name=slice_name,
        phase_definitions=phase_definitions,
        flight_setup=flight_setup,
        tail_profiles=tail_profiles,
        fleet_manifest_df=fleet_manifest_df,
        parameter_behavior=parameter_behavior,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        continuous_scaling_profile_df=continuous_scaling_profile_df,
    )


def simulate_fleet_dataset_spark(
    *,
    spark: "SparkSession",
    hierarchy_df: pd.DataFrame,
    parameter_behavior: dict[str, dict],
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
) -> tuple["DataFrame", "DataFrame"]:
    return _fleet_simulate_fleet_dataset_spark(
        spark=spark,
        hierarchy_df=hierarchy_df,
        parameter_behavior=parameter_behavior,
        phase_definitions=phase_definitions,
        flight_setup=flight_setup,
        tail_profiles=tail_profiles,
        fleet_manifest_df=fleet_manifest_df,
    )


def simulate_fleet_dataset_spark_from_assembly(
    *,
    spark: "SparkSession",
    assembly_spec: HierarchyAssemblySpec,
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
    parameter_behavior: dict[str, dict] | None = None,
    parameter_behavior_profile_df: pd.DataFrame | None = None,
    continuous_scaling_profile_df: pd.DataFrame | None = None,
) -> tuple["DataFrame", "DataFrame"]:
    return _fleet_simulate_fleet_dataset_spark_from_assembly(
        spark=spark,
        assembly_spec=assembly_spec,
        phase_definitions=phase_definitions,
        flight_setup=flight_setup,
        tail_profiles=tail_profiles,
        fleet_manifest_df=fleet_manifest_df,
        parameter_behavior=parameter_behavior,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        continuous_scaling_profile_df=continuous_scaling_profile_df,
    )


def simulate_fleet_dataset_spark_from_subsystem_slice(
    *,
    spark: "SparkSession",
    slice_name: str,
    phase_definitions: list[dict],
    flight_setup: dict,
    tail_profiles: list[dict],
    fleet_manifest_df: pd.DataFrame,
    parameter_behavior: dict[str, dict] | None = None,
    parameter_behavior_profile_df: pd.DataFrame | None = None,
    continuous_scaling_profile_df: pd.DataFrame | None = None,
) -> tuple["DataFrame", "DataFrame"]:
    return _fleet_simulate_fleet_dataset_spark_from_subsystem_slice(
        spark=spark,
        slice_name=slice_name,
        phase_definitions=phase_definitions,
        flight_setup=flight_setup,
        tail_profiles=tail_profiles,
        fleet_manifest_df=fleet_manifest_df,
        parameter_behavior=parameter_behavior,
        parameter_behavior_profile_df=parameter_behavior_profile_df,
        continuous_scaling_profile_df=continuous_scaling_profile_df,
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
