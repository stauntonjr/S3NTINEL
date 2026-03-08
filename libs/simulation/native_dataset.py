"""Native assembly dataset generation on top of the V2.1 simulation seam.

This module is a local bridge from native V2.1 assembly execution into the
canonical V2 structural builders. It is intended for authored subsystem slices,
native examples, and proof-of-path validation. It is not the scalable Spark
ingestion path for large fleet simulation workloads.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from libs.backbone import build_backbone_artifacts_from_window_x_table
from libs.behavior import BehaviorSample, BehaviorStepInput, BehaviorRegistry, build_default_behavior_registry
from libs.events import build_events_table
from libs.graph import build_graph_artifacts_from_window_x_table
from libs.io.pandas_spark import pandas_records_for_spark
from libs.io.transforms import normalize_raw_telemetry
from libs.phase import build_phase_baselines_spark_table, build_phase_windows_spark_table, fit_phase_window_x_config_from_spark
from libs.scoring import build_window_scores_raw_table
from libs.simulation.assembly_runtime import AssemblyRuntime
from libs.simulation.orchestrator import step_assembly_once
from libs.simulation.specs import HierarchyAssemblySpec
from libs.windows import build_window_x_spark_table, build_windows_table


@dataclass(frozen=True)
class NativeRawTables:
    raw_df: pd.DataFrame
    phase_df: pd.DataFrame


@dataclass(frozen=True)
class NativeBackboneArtifacts:
    backbone_df: pd.DataFrame
    backbone_sensor_energy_df: pd.DataFrame


@dataclass(frozen=True)
class NativeStructuralTables:
    raw_df: pd.DataFrame
    phase_df: pd.DataFrame
    events_sdf: "DataFrame"
    windows_sdf: "DataFrame"
    window_x_sdf: "DataFrame"


@dataclass(frozen=True)
class NativePhaseTables:
    phase_windows_df: pd.DataFrame
    phase_baselines_df: pd.DataFrame


@dataclass(frozen=True)
class NativeGraphArtifacts:
    precision_graph_df: pd.DataFrame
    event_graph_df: pd.DataFrame
    lag_graph_df: pd.DataFrame
    transition_graph_df: pd.DataFrame
    fused_graph_df: pd.DataFrame
    hierarchy_sensor_map_df: pd.DataFrame


@dataclass(frozen=True)
class NativeWindowScoreArtifacts:
    phase_windows_df: pd.DataFrame
    phase_baselines_df: pd.DataFrame
    window_scores_raw_df: pd.DataFrame


def build_native_dataset_context(
    assembly_spec: HierarchyAssemblySpec,
    *,
    behavior_registry: BehaviorRegistry | None = None,
) -> AssemblyRuntime:
    resolved_behavior_registry = behavior_registry or build_default_behavior_registry()
    return AssemblyRuntime.from_spec(assembly_spec, behavior_registry=resolved_behavior_registry)


def native_samples_to_rows(
    *,
    step_index: int,
    timestamp_utc: datetime,
    samples_by_module_id: dict[str, list[BehaviorSample]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for module_id, samples in samples_by_module_id.items():
        for sample in samples:
            metadata = dict(sample.metadata)
            rows.append(
                {
                    "step_index": step_index,
                    "timestamp_utc": timestamp_utc,
                    "system_id": metadata.get("system_id"),
                    "subsystem_id": metadata.get("subsystem_id"),
                    "module_id": module_id,
                    "parameter_name": sample.parameter_name,
                    "behavior_family_label": metadata.get("behavior_family_label"),
                    "parameter_value_clean": sample.parameter_value_clean,
                    "parameter_value": sample.parameter_value,
                    "target_source": metadata.get("target_source"),
                }
            )
    return rows


def simulate_native_dataset(
    *,
    context: AssemblyRuntime,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    start_ts = start_timestamp_utc or datetime(2025, 1, 1, tzinfo=timezone.utc)
    telemetry_rows: list[dict[str, object]] = []
    phase_rows: list[dict[str, object]] = []
    initial_state_by_module = build_initial_state_by_module() if build_initial_state_by_module is not None else {}

    for step_index in range(int(max(n_steps, 0))):
        timestamp_utc = start_ts + timedelta(seconds=float(step_index) * float(dt_seconds))
        current_phase_label = phase_label_for_step(step_index) if phase_label_for_step is not None else None
        violation_context_by_module = (
            violation_context_by_module_for_step(step_index)
            if violation_context_by_module_for_step is not None
            else {}
        )
        samples_by_module_id = step_assembly_once(
            context.build_tick_request(
                step_inputs_by_module=build_step_inputs_by_module(step_index, dt_seconds),
                initial_state_by_module=(initial_state_by_module if step_index == 0 else {}),
                violation_context_by_module=violation_context_by_module,
                apply_violations=apply_violations,
                timestamp_utc=timestamp_utc,
                current_phase_label=current_phase_label,
            )
        )
        telemetry_rows.extend(
            native_samples_to_rows(
                step_index=step_index,
                timestamp_utc=timestamp_utc,
                samples_by_module_id=samples_by_module_id,
            )
        )
        phase_rows.append(
            {
                "step_index": step_index,
                "timestamp_utc": timestamp_utc,
                "phase_label": current_phase_label,
            }
        )

    return (
        pd.DataFrame.from_records(telemetry_rows),
        pd.DataFrame.from_records(phase_rows),
    )


def native_telemetry_to_raw_telemetry_df(
    telemetry_df: pd.DataFrame,
    *,
    tail_id: str,
    flight_id: str,
) -> pd.DataFrame:
    required_columns = {"timestamp_utc", "parameter_name", "parameter_value"}
    missing_columns = required_columns.difference(telemetry_df.columns)
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"native telemetry is missing required columns: {missing_list}")

    raw_df = telemetry_df.loc[:, ["timestamp_utc", "parameter_name", "parameter_value"]].copy()
    raw_df.insert(0, "flight_id", str(flight_id))
    raw_df.insert(0, "tail_id", str(tail_id))
    raw_df["timestamp_utc"] = pd.to_datetime(raw_df["timestamp_utc"], utc=True)
    raw_df["parameter_value"] = raw_df["parameter_value"].astype(str)
    raw_df["date_utc"] = raw_df["timestamp_utc"].dt.date
    return raw_df.loc[:, ["tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value", "date_utc"]]


def native_phase_labels_to_table_df(
    phase_df: pd.DataFrame,
    *,
    tail_id: str,
    flight_id: str,
) -> pd.DataFrame:
    required_columns = {"timestamp_utc", "phase_label"}
    missing_columns = required_columns.difference(phase_df.columns)
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"native phase labels are missing required columns: {missing_list}")

    resolved_df = phase_df.loc[:, ["step_index", "timestamp_utc", "phase_label"]].copy()
    resolved_df.insert(0, "flight_id", str(flight_id))
    resolved_df.insert(0, "tail_id", str(tail_id))
    resolved_df["timestamp_utc"] = pd.to_datetime(resolved_df["timestamp_utc"], utc=True)
    resolved_df["date_utc"] = resolved_df["timestamp_utc"].dt.date
    return resolved_df.loc[:, ["tail_id", "flight_id", "step_index", "timestamp_utc", "phase_label", "date_utc"]]


def native_raw_telemetry_to_events_sdf(
    raw_df: pd.DataFrame,
    *,
    spark: "SparkSession",
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
) -> "DataFrame":
    raw_sdf = spark.createDataFrame(pandas_records_for_spark(raw_df))
    normalized_sdf = normalize_raw_telemetry(raw_sdf)
    return build_events_table(
        normalized_sdf,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )


def native_events_to_windows_sdf(
    events_sdf: "DataFrame",
    *,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> "DataFrame":
    return build_windows_table(
        events_sdf,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def native_raw_telemetry_to_windows_sdf(
    raw_df: pd.DataFrame,
    *,
    spark: "SparkSession",
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> "DataFrame":
    events_sdf = native_raw_telemetry_to_events_sdf(
        raw_df,
        spark=spark,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )
    return native_events_to_windows_sdf(
        events_sdf,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def native_raw_telemetry_to_window_x_sdf(
    raw_df: pd.DataFrame,
    *,
    spark: "SparkSession",
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> "DataFrame":
    raw_sdf = spark.createDataFrame(pandas_records_for_spark(raw_df))
    normalized_raw_sdf = normalize_raw_telemetry(raw_sdf)
    events_sdf = build_events_table(
        normalized_raw_sdf,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )
    windows_sdf = native_events_to_windows_sdf(
        events_sdf,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )
    return build_window_x_spark_table(normalized_raw_sdf, events_sdf, windows_sdf)


def _native_dataset_to_raw_tables(
    *,
    simulator: Callable[..., tuple[pd.DataFrame, pd.DataFrame]],
    tail_id: str,
    flight_id: str,
    **simulate_kwargs: Any,
) -> NativeRawTables:
    telemetry_df, phase_df = simulator(**simulate_kwargs)
    return NativeRawTables(
        raw_df=native_telemetry_to_raw_telemetry_df(telemetry_df, tail_id=tail_id, flight_id=flight_id),
        phase_df=native_phase_labels_to_table_df(phase_df, tail_id=tail_id, flight_id=flight_id),
    )


def _native_raw_tables_to_event_table(
    *,
    raw_df: pd.DataFrame,
    phase_df: pd.DataFrame,
    spark: "SparkSession",
    delta_threshold: float,
    slope_source: str,
    ema_alpha: float,
) -> tuple["DataFrame", pd.DataFrame]:
    return (
        native_raw_telemetry_to_events_sdf(
            raw_df,
            spark=spark,
            delta_threshold=delta_threshold,
            slope_source=slope_source,
            ema_alpha=ema_alpha,
        ),
        phase_df,
    )


def _native_raw_tables_to_window_table(
    *,
    raw_df: pd.DataFrame,
    phase_df: pd.DataFrame,
    spark: "SparkSession",
    delta_threshold: float,
    slope_source: str,
    ema_alpha: float,
    max_ms: int,
    event_threshold: int,
    min_ms: int,
    inactivity_timeout_ms: int,
    strategy: str,
) -> tuple["DataFrame", pd.DataFrame]:
    return (
        native_raw_telemetry_to_windows_sdf(
            raw_df,
            spark=spark,
            delta_threshold=delta_threshold,
            slope_source=slope_source,
            ema_alpha=ema_alpha,
            max_ms=max_ms,
            event_threshold=event_threshold,
            min_ms=min_ms,
            inactivity_timeout_ms=inactivity_timeout_ms,
            strategy=strategy,
        ),
        phase_df,
    )


def _native_raw_tables_to_window_x_table(
    *,
    raw_df: pd.DataFrame,
    phase_df: pd.DataFrame,
    spark: "SparkSession",
    delta_threshold: float,
    slope_source: str,
    ema_alpha: float,
    max_ms: int,
    event_threshold: int,
    min_ms: int,
    inactivity_timeout_ms: int,
    strategy: str,
) -> tuple["DataFrame", pd.DataFrame]:
    return (
        native_raw_telemetry_to_window_x_sdf(
            raw_df,
            spark=spark,
            delta_threshold=delta_threshold,
            slope_source=slope_source,
            ema_alpha=ema_alpha,
            max_ms=max_ms,
            event_threshold=event_threshold,
            min_ms=min_ms,
            inactivity_timeout_ms=inactivity_timeout_ms,
            strategy=strategy,
        ),
        phase_df,
    )


def _native_raw_tables_to_structural_tables(
    *,
    raw_df: pd.DataFrame,
    phase_df: pd.DataFrame,
    spark: "SparkSession",
    delta_threshold: float,
    slope_source: str,
    ema_alpha: float,
    max_ms: int,
    event_threshold: int,
    min_ms: int,
    inactivity_timeout_ms: int,
    strategy: str,
) -> NativeStructuralTables:
    raw_sdf = spark.createDataFrame(pandas_records_for_spark(raw_df))
    normalized_raw_sdf = normalize_raw_telemetry(raw_sdf)
    events_sdf = build_events_table(
        normalized_raw_sdf,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )
    windows_sdf = native_events_to_windows_sdf(
        events_sdf,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )
    window_x_sdf = build_window_x_spark_table(normalized_raw_sdf, events_sdf, windows_sdf)
    return NativeStructuralTables(
        raw_df=raw_df,
        phase_df=phase_df,
        events_sdf=events_sdf,
        windows_sdf=windows_sdf,
        window_x_sdf=window_x_sdf,
    )


def _simulate_native_structural_tables_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> NativeStructuralTables:
    raw_tables = _simulate_native_raw_tables_from_assembly(
        assembly_spec=assembly_spec,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )
    return _native_raw_tables_to_structural_tables(
        raw_df=raw_tables.raw_df,
        phase_df=raw_tables.phase_df,
        spark=spark,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def _simulate_native_structural_tables_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> NativeStructuralTables:
    raw_tables = _simulate_native_raw_tables_from_subsystem_slice(
        slice_name=slice_name,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )
    return _native_raw_tables_to_structural_tables(
        raw_df=raw_tables.raw_df,
        phase_df=raw_tables.phase_df,
        spark=spark,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def _native_window_x_to_backbone_artifacts(
    *,
    window_x_sdf: "DataFrame",
    backbone_sensor_count: int,
    backbone_ridge_lambda: float,
) -> NativeBackboneArtifacts:
    window_x_df = window_x_sdf.toPandas()
    backbone_df, energy_df = build_backbone_artifacts_from_window_x_table(
        window_x_df,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
    )
    return NativeBackboneArtifacts(
        backbone_df=backbone_df,
        backbone_sensor_energy_df=energy_df,
    )


def _native_window_x_to_phase_artifacts(
    *,
    window_x_sdf: "DataFrame",
    phase_count: int,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
) -> NativePhaseTables:
    phase_config = fit_phase_window_x_config_from_spark(
        window_x_sdf,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
    )
    phase_windows_sdf = build_phase_windows_spark_table(
        window_x_sdf,
        phase_config=phase_config,
        phase_count=phase_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )
    phase_baselines_sdf = build_phase_baselines_spark_table(
        phase_windows_sdf,
        phase_config=phase_config,
    )
    return NativePhaseTables(
        phase_windows_df=phase_windows_sdf.toPandas(),
        phase_baselines_df=phase_baselines_sdf.toPandas(),
    )


def _native_structural_tables_to_graph_artifacts(
    *,
    window_x_sdf: "DataFrame",
    events_sdf: "DataFrame",
    windows_sdf: "DataFrame",
    backbone_df: pd.DataFrame,
    precision_ridge_lambda: float,
    min_abs_partial_corr: float,
    min_event_count: int,
    min_event_npmi: float,
    event_top_k_per_parameter_name: int,
    lag_tau_max_seconds: float,
    min_lag_count: int,
    max_mean_lag_seconds: float | None,
    lag_top_k_outgoing: int,
    min_transition_count: int,
    alpha: float,
    beta: float,
    gamma: float,
    min_fused_edge_weight: float,
    hierarchy_top_k_per_parameter_name: int,
    hierarchy_subsystem_min_edge_weight: float | None,
    hierarchy_system_min_edge_weight: float | None,
) -> NativeGraphArtifacts:
    precision_df, event_df, lag_df, transition_df, fused_df, hierarchy_df = build_graph_artifacts_from_window_x_table(
        window_x_sdf.toPandas(),
        events_sdf.toPandas(),
        windows_sdf.toPandas(),
        backbone_df,
        precision_ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
        min_event_count=min_event_count,
        min_event_npmi=min_event_npmi,
        event_top_k_per_parameter_name=event_top_k_per_parameter_name,
        lag_tau_max_seconds=lag_tau_max_seconds,
        min_lag_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        lag_top_k_outgoing=lag_top_k_outgoing,
        min_transition_count=min_transition_count,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return NativeGraphArtifacts(
        precision_graph_df=precision_df,
        event_graph_df=event_df,
        lag_graph_df=lag_df,
        transition_graph_df=transition_df,
        fused_graph_df=fused_df,
        hierarchy_sensor_map_df=hierarchy_df,
    )


def _native_structural_tables_to_phase_artifacts(
    *,
    window_x_sdf: "DataFrame",
    phase_count: int,
    backbone_sensor_count: int,
    backbone_ridge_lambda: float,
    phase_detect_sensor_count: int,
    phase_detect_event_type_count: int,
    phase_detect_categorical_state_count: int,
    phase_detect_window_cooccurrence_count: int,
    phase_stable_drift_quantile: float,
    phase_smoothing_radius: int,
    phase_transition_penalty: float,
    phase_min_dwell_windows: int,
) -> dict[str, pd.DataFrame]:
    phase_tables = _native_window_x_to_phase_artifacts(
        window_x_sdf=window_x_sdf,
        phase_count=phase_count,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )
    return {
        "phase_windows": phase_tables.phase_windows_df,
        "phase_baselines": phase_tables.phase_baselines_df,
    }


def _native_phase_and_graph_artifacts_to_scores(
    *,
    phase_windows_df: pd.DataFrame,
    phase_baselines_df: pd.DataFrame,
    hierarchy_sensor_map_df: pd.DataFrame,
) -> NativeWindowScoreArtifacts:
    return NativeWindowScoreArtifacts(
        phase_windows_df=phase_windows_df,
        phase_baselines_df=phase_baselines_df,
        window_scores_raw_df=build_window_scores_raw_table(
            phase_windows_df,
            phase_baselines_df,
            hierarchy_sensor_map_df,
        ),
    )


def simulate_native_dataset_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return simulate_native_dataset(
        context=build_native_dataset_context(assembly_spec, behavior_registry=behavior_registry),
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
    )


def _simulate_native_raw_tables_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
) -> NativeRawTables:
    return _native_dataset_to_raw_tables(
        simulator=simulate_native_dataset_from_assembly,
        tail_id=tail_id,
        flight_id=flight_id,
        assembly_spec=assembly_spec,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )


def simulate_native_raw_telemetry_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw_tables = _simulate_native_raw_tables_from_assembly(
        assembly_spec=assembly_spec,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )
    return raw_tables.raw_df, raw_tables.phase_df


def _build_native_subsystem_slice(slice_name: str) -> HierarchyAssemblySpec:
    from libs.simulation.subsystem_slices import build_native_subsystem_slice

    return build_native_subsystem_slice(slice_name)


def simulate_native_dataset_from_subsystem_slice(
    *,
    slice_name: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return simulate_native_dataset_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )


def _simulate_native_raw_tables_from_subsystem_slice(
    *,
    slice_name: str,
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
) -> NativeRawTables:
    return _simulate_native_raw_tables_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )


def simulate_native_raw_telemetry_from_subsystem_slice(
    *,
    slice_name: str,
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return simulate_native_raw_telemetry_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )


def simulate_native_event_table_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
) -> tuple["DataFrame", pd.DataFrame]:
    raw_tables = _simulate_native_raw_tables_from_assembly(
        assembly_spec=assembly_spec,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )
    return _native_raw_tables_to_event_table(
        raw_df=raw_tables.raw_df,
        phase_df=raw_tables.phase_df,
        spark=spark,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )


def simulate_native_event_table_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
) -> tuple["DataFrame", pd.DataFrame]:
    return simulate_native_event_table_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )


def simulate_native_window_table_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> tuple["DataFrame", pd.DataFrame]:
    raw_tables = _simulate_native_raw_tables_from_assembly(
        assembly_spec=assembly_spec,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )
    return _native_raw_tables_to_window_table(
        raw_df=raw_tables.raw_df,
        phase_df=raw_tables.phase_df,
        spark=spark,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def simulate_native_window_table_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> tuple["DataFrame", pd.DataFrame]:
    return simulate_native_window_table_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def simulate_native_window_x_table_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> tuple["DataFrame", pd.DataFrame]:
    raw_tables = _simulate_native_raw_tables_from_assembly(
        assembly_spec=assembly_spec,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
    )
    return _native_raw_tables_to_window_x_table(
        raw_df=raw_tables.raw_df,
        phase_df=raw_tables.phase_df,
        spark=spark,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def simulate_native_window_x_table_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
) -> tuple["DataFrame", pd.DataFrame]:
    return simulate_native_window_x_table_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )


def _backbone_artifacts_to_public_dict(
    artifacts: NativeBackboneArtifacts,
) -> dict[str, pd.DataFrame]:
    return {
        "backbone": artifacts.backbone_df,
        "backbone_sensor_energy": artifacts.backbone_sensor_energy_df,
    }


def _graph_artifacts_to_public_dict(
    *,
    backbone_artifacts: NativeBackboneArtifacts,
    graph_artifacts: NativeGraphArtifacts,
) -> dict[str, pd.DataFrame]:
    return {
        "backbone": backbone_artifacts.backbone_df,
        "backbone_sensor_energy": backbone_artifacts.backbone_sensor_energy_df,
        "precision_graph": graph_artifacts.precision_graph_df,
        "event_graph": graph_artifacts.event_graph_df,
        "lag_graph": graph_artifacts.lag_graph_df,
        "transition_graph": graph_artifacts.transition_graph_df,
        "fused_graph": graph_artifacts.fused_graph_df,
        "hierarchy_sensor_map": graph_artifacts.hierarchy_sensor_map_df,
    }


def _window_score_artifacts_to_public_dict(
    artifacts: NativeWindowScoreArtifacts,
) -> dict[str, pd.DataFrame]:
    return {
        "phase_windows": artifacts.phase_windows_df,
        "phase_baselines": artifacts.phase_baselines_df,
        "window_scores_raw": artifacts.window_scores_raw_df,
    }


def simulate_native_backbone_artifacts_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    structural_tables = _simulate_native_structural_tables_from_assembly(
        assembly_spec=assembly_spec,
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )
    backbone_artifacts = _native_window_x_to_backbone_artifacts(
            window_x_sdf=structural_tables.window_x_sdf,
            backbone_sensor_count=backbone_sensor_count,
            backbone_ridge_lambda=backbone_ridge_lambda,
        )
    return (
        _backbone_artifacts_to_public_dict(backbone_artifacts),
        structural_tables.phase_df,
    )


def simulate_native_backbone_artifacts_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    return simulate_native_backbone_artifacts_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
    )


def simulate_native_phase_artifacts_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    phase_count: int = 3,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    structural_tables = _simulate_native_structural_tables_from_assembly(
        assembly_spec=assembly_spec,
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )
    return (
        _native_structural_tables_to_phase_artifacts(
            window_x_sdf=structural_tables.window_x_sdf,
            phase_count=phase_count,
            backbone_sensor_count=backbone_sensor_count,
            backbone_ridge_lambda=backbone_ridge_lambda,
            phase_detect_sensor_count=phase_detect_sensor_count,
            phase_detect_event_type_count=phase_detect_event_type_count,
            phase_detect_categorical_state_count=phase_detect_categorical_state_count,
            phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
            phase_stable_drift_quantile=phase_stable_drift_quantile,
            phase_smoothing_radius=phase_smoothing_radius,
            phase_transition_penalty=phase_transition_penalty,
            phase_min_dwell_windows=phase_min_dwell_windows,
        ),
        structural_tables.phase_df,
    )


def simulate_native_graph_artifacts_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_parameter_name: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    structural_tables = _simulate_native_structural_tables_from_assembly(
        assembly_spec=assembly_spec,
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )
    backbone_artifacts = _native_window_x_to_backbone_artifacts(
        window_x_sdf=structural_tables.window_x_sdf,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
    )
    graph_artifacts = _native_structural_tables_to_graph_artifacts(
        window_x_sdf=structural_tables.window_x_sdf,
        events_sdf=structural_tables.events_sdf,
        windows_sdf=structural_tables.windows_sdf,
        backbone_df=backbone_artifacts.backbone_df,
        precision_ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
        min_event_count=min_event_count,
        min_event_npmi=min_event_npmi,
        event_top_k_per_parameter_name=event_top_k_per_parameter_name,
        lag_tau_max_seconds=lag_tau_max_seconds,
        min_lag_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        lag_top_k_outgoing=lag_top_k_outgoing,
        min_transition_count=min_transition_count,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    return (
        _graph_artifacts_to_public_dict(
            backbone_artifacts=backbone_artifacts,
            graph_artifacts=graph_artifacts,
        ),
        structural_tables.phase_df,
    )


def simulate_native_graph_artifacts_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_parameter_name: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    return simulate_native_graph_artifacts_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        precision_ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
        min_event_count=min_event_count,
        min_event_npmi=min_event_npmi,
        event_top_k_per_parameter_name=event_top_k_per_parameter_name,
        lag_tau_max_seconds=lag_tau_max_seconds,
        min_lag_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        lag_top_k_outgoing=lag_top_k_outgoing,
        min_transition_count=min_transition_count,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )


def simulate_native_phase_artifacts_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    phase_count: int = 3,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    return simulate_native_phase_artifacts_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
        phase_count=phase_count,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )


def simulate_native_window_scores_raw_from_assembly(
    *,
    assembly_spec: HierarchyAssemblySpec,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    phase_count: int = 3,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_parameter_name: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    structural_tables = _simulate_native_structural_tables_from_assembly(
        assembly_spec=assembly_spec,
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
    )
    backbone_artifacts = _native_window_x_to_backbone_artifacts(
        window_x_sdf=structural_tables.window_x_sdf,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
    )
    graph_artifacts = _native_structural_tables_to_graph_artifacts(
        window_x_sdf=structural_tables.window_x_sdf,
        events_sdf=structural_tables.events_sdf,
        windows_sdf=structural_tables.windows_sdf,
        backbone_df=backbone_artifacts.backbone_df,
        precision_ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
        min_event_count=min_event_count,
        min_event_npmi=min_event_npmi,
        event_top_k_per_parameter_name=event_top_k_per_parameter_name,
        lag_tau_max_seconds=lag_tau_max_seconds,
        min_lag_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        lag_top_k_outgoing=lag_top_k_outgoing,
        min_transition_count=min_transition_count,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )
    phase_artifacts = _native_structural_tables_to_phase_artifacts(
        window_x_sdf=structural_tables.window_x_sdf,
        phase_count=phase_count,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )
    score_artifacts = _native_phase_and_graph_artifacts_to_scores(
        phase_windows_df=phase_artifacts["phase_windows"],
        phase_baselines_df=phase_artifacts["phase_baselines"],
        hierarchy_sensor_map_df=graph_artifacts.hierarchy_sensor_map_df,
    )
    return (
        _window_score_artifacts_to_public_dict(score_artifacts),
        structural_tables.phase_df,
    )


def simulate_native_window_scores_raw_from_subsystem_slice(
    *,
    slice_name: str,
    spark: "SparkSession",
    tail_id: str,
    flight_id: str,
    n_steps: int,
    dt_seconds: float,
    start_timestamp_utc: datetime | None,
    build_step_inputs_by_module: Callable[[int, float], dict[str, dict[str, BehaviorStepInput]]],
    build_initial_state_by_module: Callable[[], dict[str, dict[str, object]]] | None = None,
    phase_label_for_step: Callable[[int], str | None] | None = None,
    violation_context_by_module_for_step: Callable[[int], dict[str, dict[str, dict[str, Any]]]] | None = None,
    apply_violations: bool = False,
    behavior_registry: BehaviorRegistry | None = None,
    delta_threshold: float = 0.0,
    slope_source: str = "ema",
    ema_alpha: float = 0.2,
    max_ms: int = 10000,
    event_threshold: int = 20,
    min_ms: int = 50,
    inactivity_timeout_ms: int = 0,
    strategy: str = "bucketed",
    phase_count: int = 3,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
    precision_ridge_lambda: float = 1.0,
    min_abs_partial_corr: float = 0.05,
    min_event_count: int = 1,
    min_event_npmi: float = 0.0,
    event_top_k_per_parameter_name: int = 8,
    lag_tau_max_seconds: float = 30.0,
    min_lag_count: int = 1,
    max_mean_lag_seconds: float | None = None,
    lag_top_k_outgoing: int = 8,
    min_transition_count: int = 1,
    alpha: float = 1.0,
    beta: float = 1.0,
    gamma: float = 1.0,
    min_fused_edge_weight: float = 0.05,
    hierarchy_top_k_per_parameter_name: int = 3,
    hierarchy_subsystem_min_edge_weight: float | None = None,
    hierarchy_system_min_edge_weight: float | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    return simulate_native_window_scores_raw_from_assembly(
        assembly_spec=_build_native_subsystem_slice(slice_name),
        spark=spark,
        tail_id=tail_id,
        flight_id=flight_id,
        n_steps=n_steps,
        dt_seconds=dt_seconds,
        start_timestamp_utc=start_timestamp_utc,
        build_step_inputs_by_module=build_step_inputs_by_module,
        build_initial_state_by_module=build_initial_state_by_module,
        phase_label_for_step=phase_label_for_step,
        violation_context_by_module_for_step=violation_context_by_module_for_step,
        apply_violations=apply_violations,
        behavior_registry=behavior_registry,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
        max_ms=max_ms,
        event_threshold=event_threshold,
        min_ms=min_ms,
        inactivity_timeout_ms=inactivity_timeout_ms,
        strategy=strategy,
        phase_count=phase_count,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
        precision_ridge_lambda=precision_ridge_lambda,
        min_abs_partial_corr=min_abs_partial_corr,
        min_event_count=min_event_count,
        min_event_npmi=min_event_npmi,
        event_top_k_per_parameter_name=event_top_k_per_parameter_name,
        lag_tau_max_seconds=lag_tau_max_seconds,
        min_lag_count=min_lag_count,
        max_mean_lag_seconds=max_mean_lag_seconds,
        lag_top_k_outgoing=lag_top_k_outgoing,
        min_transition_count=min_transition_count,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        min_fused_edge_weight=min_fused_edge_weight,
        hierarchy_top_k_per_parameter_name=hierarchy_top_k_per_parameter_name,
        hierarchy_subsystem_min_edge_weight=hierarchy_subsystem_min_edge_weight,
        hierarchy_system_min_edge_weight=hierarchy_system_min_edge_weight,
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
