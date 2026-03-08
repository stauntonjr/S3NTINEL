"""Run simulation + event detection + adaptive windows + streaming metrics in one command."""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Iterator
from datetime import datetime
import heapq
import json
import os
from pathlib import Path
from typing import Any

os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

import numpy as np
import pandas as pd

from libs.backbone import (
    aggregate_backbone_gh,
    compute_backbone_gh_by_flight,
    reconstruct_window_vector,
    reconstruction_error,
    select_backbone_sensors_by_energy,
    solve_backbone_weights,
)
from libs.common import SensorDataType, normalize_sensor_datatype
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.cur import aggregate_sensor_energy_over_corpus, compute_window_sensor_energy
from libs.events.categorical import CategoricalSample, detect_categorical_events_stream
from libs.events.cooccur import (
    CooccurrencePairCountConfig,
    stream_cooccurrence_pair_counts,
    stream_immediate_precedence_pair_counts,
)
from libs.events.extrema import ContinuousSample, detect_continuous_events_stream
from libs.phase import (
    compute_phase_behavior_diagnostics,
    detect_phases_from_windows,
    evaluate_detected_phases,
)
from libs.graph import build_graph_artifact_tables, build_graph_component_tables_from_window_x_table
from libs.profiling import stream_profiler_validation
from libs.scoring import build_phase_score_baselines, score_window_s_rows
from libs.simulation import (
    HierarchyAssemblySpec,
    InterModuleCouplingSpec,
    ModuleSpec,
    ParameterSpec,
    PortSpec,
    build_default_parameter_behavior,
    build_fleet_manifest,
    build_tail_profiles,
    build_hierarchy_assembly_spec,
    default_phase_definitions,
    flatten_assembly_spec,
    simulate_fleet_dataset,
)
from libs.windows import (
    build_continuous_robust_scaler,
    build_window_s_rows,
    build_window_x_row,
    sample_windows_for_coverage,
    top_window_cooccurrence_sensor_pairs,
    top_categorical_state_pairs,
    top_phase_event_types,
)
from libs.windows.stream import StreamWindowConfig, build_adaptive_windows_stream

_BINARY_TOKENS = {
    "0",
    "1",
    "true",
    "false",
    "on",
    "off",
    "yes",
    "no",
    "open",
    "closed",
    "enabled",
    "disabled",
}


def _parameter_name_from_row(row: Any) -> str:
    return str(getattr(row, "parameter_name", getattr(row, "sensor", "")))


def _parameter_name_from_mapping(row: dict[str, Any]) -> str:
    return str(row.get("parameter_name", row.get("sensor", "")))


def _telemetry_parameter_key(df: pd.DataFrame) -> str:
    return "parameter_name" if "parameter_name" in df.columns else "sensor"


def _normalized_mutual_information(labels_true: list[str], labels_pred: list[str]) -> float | None:
    if len(labels_true) != len(labels_pred) or not labels_true:
        return None
    n = len(labels_true)
    true_counts: Counter[str] = Counter(str(item) for item in labels_true)
    pred_counts: Counter[str] = Counter(str(item) for item in labels_pred)
    joint_counts: Counter[tuple[str, str]] = Counter(zip((str(item) for item in labels_true), (str(item) for item in labels_pred), strict=False))

    mi = 0.0
    for (true_label, pred_label), joint_count in joint_counts.items():
        p_xy = float(joint_count) / float(n)
        p_x = float(true_counts[true_label]) / float(n)
        p_y = float(pred_counts[pred_label]) / float(n)
        if p_xy <= 0.0 or p_x <= 0.0 or p_y <= 0.0:
            continue
        mi += p_xy * np.log(p_xy / (p_x * p_y))

    h_true = 0.0
    for count in true_counts.values():
        p = float(count) / float(n)
        if p > 0.0:
            h_true -= p * np.log(p)

    h_pred = 0.0
    for count in pred_counts.values():
        p = float(count) / float(n)
        if p > 0.0:
            h_pred -= p * np.log(p)

    if h_true <= 0.0 or h_pred <= 0.0:
        return 1.0 if labels_true == labels_pred else 0.0
    return float(mi / np.sqrt(h_true * h_pred))


def _dataframe_records_json_safe(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


def _hierarchy_recovery_metrics(
    *,
    hierarchy_label_df: pd.DataFrame,
    hierarchy_pred_df: pd.DataFrame,
) -> dict[str, Any]:
    if hierarchy_label_df.empty or hierarchy_pred_df.empty:
        return {"parameter_count_compared": 0, "levels": {}}

    label_df = hierarchy_label_df.copy()
    pred = hierarchy_pred_df.copy()
    label_key = "parameter_name" if "parameter_name" in label_df.columns else "sensor"
    pred_key = "parameter_name" if "parameter_name" in pred.columns else "sensor"
    label_df["parameter_name"] = label_df[label_key].astype(str)
    pred["parameter_name"] = pred[pred_key].astype(str)
    merged = label_df.merge(
        pred[["parameter_name", "system_id", "subsystem_id", "module_id"]],
        on="parameter_name",
        how="inner",
        suffixes=("_label", "_detected"),
    )
    levels: dict[str, Any] = {}
    for level in ["system_id", "subsystem_id", "module_id"]:
        true_col = f"{level}_label"
        pred_col = f"{level}_detected"
        labels_true = [str(item) for item in merged[true_col].fillna("").tolist()]
        labels_pred = [str(item) for item in merged[pred_col].fillna("").tolist()]
        exact = 0.0
        if labels_true:
            exact = float(sum(1 for left, right in zip(labels_true, labels_pred, strict=False) if left == right)) / float(len(labels_true))
        levels[level] = {
            "nmi": _normalized_mutual_information(labels_true, labels_pred),
            "exact_match_ratio": exact,
            "label_cluster_count": int(len(set(labels_true) - {""})),
            "detected_cluster_count": int(len(set(labels_pred) - {""})),
        }
    return {
        "parameter_count_compared": int(len(merged)),
        "levels": levels,
    }


def _graph_violation_scores(
    *,
    windows: list[dict[str, Any]],
    fused_graph_df: pd.DataFrame,
) -> list[dict[str, Any]]:
    edge_weights: dict[tuple[str, str], float] = {}
    for row in fused_graph_df.to_dict(orient="records"):
        left = str(row.get("parameter_name_u", ""))
        right = str(row.get("parameter_name_v", ""))
        if not left or not right:
            continue
        edge_weights[(left, right)] = float(row.get("fused_weight", 0.0) or 0.0)
        edge_weights[(right, left)] = float(row.get("fused_weight", 0.0) or 0.0)

    out: list[dict[str, Any]] = []
    for window in windows:
        event_parameter_names = window.get("event_parameter_names")
        if not isinstance(event_parameter_names, list):
            event_parameter_names = window.get("event_sensors", [])
        parameter_names = sorted(
            set(str(item) for item in event_parameter_names if str(item))
        )
        pair_total = 0
        supported_pairs = 0
        supported_weight_sum = 0.0
        for idx, left in enumerate(parameter_names):
            for right in parameter_names[idx + 1 :]:
                pair_total += 1
                weight = float(edge_weights.get((left, right), 0.0) or 0.0)
                if weight > 0.0:
                    supported_pairs += 1
                    supported_weight_sum += weight
        unsupported_pairs = max(pair_total - supported_pairs, 0)
        graph_violation_score = 0.0 if pair_total <= 0 else float(unsupported_pairs) / float(pair_total)
        out.append(
            {
                "tail_id": str(window.get("tail_id", "")),
                "flight_id": str(window.get("flight_id", "")),
                "win_id": int(window.get("win_id", 0)),
                "graph_violation_score": float(graph_violation_score),
                "active_event_parameter_count": int(len(parameter_names)),
                "pair_total": int(pair_total),
                "supported_pairs": int(supported_pairs),
                "supported_weight_sum": float(supported_weight_sum),
            }
        )
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run end-to-end simulation + detection + windowing")
    parser.add_argument("--tail-count", type=int, default=3)
    parser.add_argument("--flights-per-tail", type=int, default=3)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--phase-count", type=int, default=2, help="Use first N default phases")
    parser.add_argument("--tolerance-seconds", type=float, default=0.0, help="Deprecated in streaming mode; exact timestamp/type matching is used")
    parser.add_argument(
        "--output-json",
        default="reports/eda/sim_detection_eval_report.json",
        help="Path to JSON report",
    )
    parser.add_argument(
        "--validator-snapshots-jsonl",
        default=None,
        help="Optional JSONL path for final validator snapshot",
    )
    parser.add_argument(
        "--profiler-validator-snapshots-jsonl",
        default=None,
        help="Optional JSONL path for profiler validator snapshots",
    )
    parser.add_argument("--window-max-ms", type=int, default=10000)
    parser.add_argument("--window-min-ms", type=int, default=50)
    parser.add_argument("--window-event-threshold", type=int, default=20)
    parser.add_argument("--window-inactivity-timeout-ms", type=int, default=0)
    parser.add_argument("--window-sample-size-per-flight", type=int, default=32)
    parser.add_argument("--window-sample-bins", type=int, default=4)
    parser.add_argument("--backbone-sensor-count", type=int, default=8)
    parser.add_argument("--backbone-ridge-lambda", type=float, default=0.1)
    parser.add_argument("--graph-min-abs-partial-corr", type=float, default=0.05)
    parser.add_argument("--graph-min-event-npmi", type=float, default=0.15)
    parser.add_argument("--graph-event-top-k-per-sensor", type=int, default=6)
    parser.add_argument("--graph-lag-top-k-outgoing", type=int, default=4)
    parser.add_argument("--graph-alpha", type=float, default=1.0)
    parser.add_argument("--graph-beta", type=float, default=1.0)
    parser.add_argument("--graph-gamma", type=float, default=1.0)
    parser.add_argument("--graph-min-fused-edge-weight", type=float, default=0.05)
    parser.add_argument("--graph-hierarchy-top-k-per-sensor", type=int, default=3)
    parser.add_argument("--graph-hierarchy-subsystem-min-edge-weight", type=float, default=0.7)
    parser.add_argument("--graph-hierarchy-system-min-edge-weight", type=float, default=0.5)
    parser.add_argument("--phase-detect-sensor-count", type=int, default=8)
    parser.add_argument("--phase-detect-event-type-count", type=int, default=6)
    parser.add_argument("--phase-detect-categorical-state-count", type=int, default=8)
    parser.add_argument("--phase-detect-window-cooccurrence-count", type=int, default=8)
    parser.add_argument("--phase-stable-drift-quantile", type=float, default=0.35)
    parser.add_argument("--phase-smoothing-radius", type=int, default=2)
    parser.add_argument("--phase-transition-penalty", type=float, default=1.5)
    parser.add_argument("--phase-min-dwell-windows", type=int, default=8)
    parser.add_argument("--phase-diagnostics-top-k", type=int, default=12)
    parser.add_argument("--cooccurrence-n", type=int, default=3, help="Buffer multiplier n for lag windows")
    parser.add_argument("--cooccurrence-top-k", type=int, default=20, help="Top cooccurrence pairs per buffer in report")
    parser.add_argument("--skip-cooccurrence", action="store_true", help="Skip cooccurrence graph/count generation for faster runs")
    parser.add_argument(
        "--cooccurrence-counts-jsonl",
        default=None,
        help="Optional JSONL path for incremental cooccurrence pair-count updates",
    )
    parser.add_argument(
        "--windows-jsonl",
        default=None,
        help="Optional JSONL path for emitted adaptive windows",
    )
    parser.add_argument(
        "--graph-cache-json",
        default=None,
        help="Optional JSON path for reusable graph component cache",
    )
    return parser.parse_args()


def _module_spec(
    *,
    system_id: str,
    subsystem_id: str,
    module_id: str,
    sensors: list[tuple[str, str, str]],
    input_ports: tuple[PortSpec, ...] = (),
    output_ports: tuple[PortSpec, ...] = (),
    parameter_input_ports: dict[str, tuple[str, ...]] | None = None,
    parameter_output_ports: dict[str, str] | None = None,
) -> ModuleSpec:
    parameter_input_ports = dict(parameter_input_ports or {})
    parameter_output_ports = dict(parameter_output_ports or {})
    return ModuleSpec(
        module_id=module_id,
        subsystem_id=subsystem_id,
        system_id=system_id,
        parameters=tuple(
            ParameterSpec(
                parameter_name=parameter_name,
                system_id=system_id,
                subsystem_id=subsystem_id,
                module_id=module_id,
                parameter_datatype_label=datatype,
                unit=unit,
                input_port_names=parameter_input_ports.get(parameter_name, ()),
                output_port_name=parameter_output_ports.get(parameter_name),
            )
            for parameter_name, datatype, unit in sensors
        ),
        input_ports=input_ports,
        output_ports=output_ports,
    )


def _sample_hierarchy_assembly_spec() -> HierarchyAssemblySpec:
    module_defs: list[tuple[str, str, str, list[tuple[str, str, str]]]] = [
        ("SYS_FLIGHT_CONTROLS", "SUB_PRIMARY_CONTROL", "MOD_ELAC", [
            ("fc_elac_elevator_cmd_deg", "numeric", "deg"),
            ("fc_elac_aileron_cmd_deg", "numeric", "deg"),
            ("fc_elac_mode", "categorical", "state"),
            ("fc_elac_active", "binary", "flag"),
        ]),
        ("SYS_FLIGHT_CONTROLS", "SUB_PRIMARY_CONTROL", "MOD_SEC", [
            ("fc_sec_spoiler_cmd_deg", "numeric", "deg"),
            ("fc_sec_rudder_cmd_deg", "numeric", "deg"),
            ("fc_sec_mode", "categorical", "state"),
            ("fc_sec_active", "binary", "flag"),
        ]),
        ("SYS_FLIGHT_CONTROLS", "SUB_TRIM_STABILITY", "MOD_TRIM", [
            ("fc_trim_pitch_pos_deg", "numeric", "deg"),
            ("fc_trim_roll_pos_deg", "numeric", "deg"),
            ("fc_trim_mode", "categorical", "state"),
            ("fc_trim_manual_engaged", "binary", "flag"),
        ]),
        ("SYS_FLIGHT_CONTROLS", "SUB_TRIM_STABILITY", "MOD_YAW_DAMPER", [
            ("fc_yd_yaw_rate_deg_s", "numeric", "deg/s"),
            ("fc_yd_rudder_demand_deg", "numeric", "deg"),
            ("fc_yd_mode", "categorical", "state"),
            ("fc_yd_engaged", "binary", "flag"),
        ]),
        ("SYS_PROPULSION", "SUB_ENGINE_1", "MOD_E1_CORE", [
            ("eng1_n1_pct", "numeric", "pct"),
            ("eng1_n2_pct", "numeric", "pct"),
            ("eng1_thrust_mode", "categorical", "state"),
            ("eng1_fadec_active", "binary", "flag"),
        ]),
        ("SYS_PROPULSION", "SUB_ENGINE_1", "MOD_E1_THERMAL", [
            ("eng1_egt_c", "numeric", "c"),
            ("eng1_fuel_flow_kgph", "numeric", "kg/h"),
            ("eng1_start_mode", "categorical", "state"),
            ("eng1_igniter_on", "binary", "flag"),
        ]),
        ("SYS_PROPULSION", "SUB_ENGINE_2", "MOD_E2_CORE", [
            ("eng2_n1_pct", "numeric", "pct"),
            ("eng2_n2_pct", "numeric", "pct"),
            ("eng2_thrust_mode", "categorical", "state"),
            ("eng2_fadec_active", "binary", "flag"),
        ]),
        ("SYS_PROPULSION", "SUB_ENGINE_2", "MOD_E2_THERMAL", [
            ("eng2_egt_c", "numeric", "c"),
            ("eng2_fuel_flow_kgph", "numeric", "kg/h"),
            ("eng2_start_mode", "categorical", "state"),
            ("eng2_igniter_on", "binary", "flag"),
        ]),
        ("SYS_ELECTRICAL_POWER", "SUB_AC_POWER", "MOD_GEN_BUS", [
            ("elec_ac_bus_v", "numeric", "v"),
            ("elec_ac_bus_hz", "numeric", "hz"),
            ("elec_ac_tie_mode", "categorical", "state"),
            ("elec_gen_contact_closed", "binary", "flag"),
        ]),
        ("SYS_ELECTRICAL_POWER", "SUB_AC_POWER", "MOD_APU_GEN", [
            ("elec_apu_gen_kw", "numeric", "kw"),
            ("elec_apu_gen_load_pct", "numeric", "pct"),
            ("elec_apu_mode", "categorical", "state"),
            ("elec_apu_gen_on", "binary", "flag"),
        ]),
        ("SYS_ELECTRICAL_POWER", "SUB_DC_POWER", "MOD_BATTERY", [
            ("elec_dc_bus_v", "numeric", "v"),
            ("elec_battery_soc_pct", "numeric", "pct"),
            ("elec_battery_mode", "categorical", "state"),
            ("elec_battery_charging", "binary", "flag"),
        ]),
        ("SYS_ELECTRICAL_POWER", "SUB_DC_POWER", "MOD_INVERTER", [
            ("elec_inv_out_v", "numeric", "v"),
            ("elec_inv_temp_c", "numeric", "c"),
            ("elec_inv_mode", "categorical", "state"),
            ("elec_inv_fault", "binary", "flag"),
        ]),
        ("SYS_ENVIRONMENTAL_CONTROL", "SUB_AIR_CONDITIONING", "MOD_PACK_LEFT", [
            ("ecs_pack_l_flow_kg_s", "numeric", "kg/s"),
            ("ecs_pack_l_out_temp_c", "numeric", "c"),
            ("ecs_pack_l_mode", "categorical", "state"),
            ("ecs_pack_l_valve_open", "binary", "flag"),
        ]),
        ("SYS_ENVIRONMENTAL_CONTROL", "SUB_AIR_CONDITIONING", "MOD_PACK_RIGHT", [
            ("ecs_pack_r_flow_kg_s", "numeric", "kg/s"),
            ("ecs_pack_r_out_temp_c", "numeric", "c"),
            ("ecs_pack_r_mode", "categorical", "state"),
            ("ecs_pack_r_valve_open", "binary", "flag"),
        ]),
        ("SYS_ENVIRONMENTAL_CONTROL", "SUB_PRESSURIZATION", "MOD_CABIN_PRESSURE", [
            ("ecs_cabin_alt_ft", "numeric", "ft"),
            ("ecs_delta_p_psi", "numeric", "psi"),
            ("ecs_press_mode", "categorical", "state"),
            ("ecs_outflow_valve_open", "binary", "flag"),
        ]),
        ("SYS_ENVIRONMENTAL_CONTROL", "SUB_PRESSURIZATION", "MOD_CABIN_TEMP", [
            ("ecs_cabin_temp_c", "numeric", "c"),
            ("ecs_cockpit_temp_c", "numeric", "c"),
            ("ecs_temp_ctl_mode", "categorical", "state"),
            ("ecs_trim_air_open", "binary", "flag"),
        ]),
        ("SYS_NAVIGATION_GUIDANCE", "SUB_AIR_DATA", "MOD_ADC_1", [
            ("nav_adc1_ias_kt", "numeric", "kt"),
            ("nav_adc1_alt_ft", "numeric", "ft"),
            ("nav_adc1_source_mode", "categorical", "state"),
            ("nav_adc1_valid", "binary", "flag"),
        ]),
        ("SYS_NAVIGATION_GUIDANCE", "SUB_AIR_DATA", "MOD_ADC_2", [
            ("nav_adc2_ias_kt", "numeric", "kt"),
            ("nav_adc2_alt_ft", "numeric", "ft"),
            ("nav_adc2_source_mode", "categorical", "state"),
            ("nav_adc2_valid", "binary", "flag"),
        ]),
        ("SYS_NAVIGATION_GUIDANCE", "SUB_INERTIAL_REFERENCE", "MOD_IRS_1", [
            ("nav_irs1_pitch_deg", "numeric", "deg"),
            ("nav_irs1_roll_deg", "numeric", "deg"),
            ("nav_irs1_align_mode", "categorical", "state"),
            ("nav_irs1_valid", "binary", "flag"),
        ]),
        ("SYS_NAVIGATION_GUIDANCE", "SUB_INERTIAL_REFERENCE", "MOD_IRS_2", [
            ("nav_irs2_pitch_deg", "numeric", "deg"),
            ("nav_irs2_roll_deg", "numeric", "deg"),
            ("nav_irs2_align_mode", "categorical", "state"),
            ("nav_irs2_valid", "binary", "flag"),
        ]),
    ]
    module_specs: list[ModuleSpec] = []
    for system_id, subsystem_id, module_id, sensors in module_defs:
        kwargs: dict[str, Any] = {}
        if module_id == "MOD_APU_GEN":
            kwargs = {
                "output_ports": (
                    PortSpec("apu_power_out", "output", "numeric", unit="kw"),
                ),
                "parameter_output_ports": {
                    "elec_apu_gen_kw": "apu_power_out",
                },
            }
        elif module_id == "MOD_GEN_BUS":
            kwargs = {
                "input_ports": (
                    PortSpec("apu_power_in", "input", "numeric", unit="kw"),
                ),
                "output_ports": (
                    PortSpec("ac_bus_voltage_out", "output", "numeric", unit="v"),
                ),
                "parameter_input_ports": {
                    "elec_ac_bus_v": ("apu_power_in",),
                },
                "parameter_output_ports": {
                    "elec_ac_bus_v": "ac_bus_voltage_out",
                },
            }
        elif module_id == "MOD_INVERTER":
            kwargs = {
                "input_ports": (
                    PortSpec("ac_bus_voltage_in", "input", "numeric", unit="v"),
                ),
                "parameter_input_ports": {
                    "elec_inv_out_v": ("ac_bus_voltage_in",),
                },
            }
        elif module_id == "MOD_PACK_LEFT":
            kwargs = {
                "output_ports": (
                    PortSpec("pack_left_flow_out", "output", "numeric", unit="kg/s"),
                ),
                "parameter_output_ports": {
                    "ecs_pack_l_flow_kg_s": "pack_left_flow_out",
                },
            }
        elif module_id == "MOD_PACK_RIGHT":
            kwargs = {
                "output_ports": (
                    PortSpec("pack_right_flow_out", "output", "numeric", unit="kg/s"),
                ),
                "parameter_output_ports": {
                    "ecs_pack_r_flow_kg_s": "pack_right_flow_out",
                },
            }
        elif module_id == "MOD_CABIN_PRESSURE":
            kwargs = {
                "input_ports": (
                    PortSpec("aircraft_altitude_in", "input", "numeric", unit="ft"),
                    PortSpec("pack_left_flow_in", "input", "numeric", unit="kg/s"),
                    PortSpec("pack_right_flow_in", "input", "numeric", unit="kg/s"),
                ),
                "parameter_input_ports": {
                    "ecs_cabin_alt_ft": ("aircraft_altitude_in", "pack_left_flow_in", "pack_right_flow_in"),
                    "ecs_delta_p_psi": ("aircraft_altitude_in",),
                },
            }
        elif module_id == "MOD_ADC_1":
            kwargs = {
                "output_ports": (
                    PortSpec("aircraft_altitude_out", "output", "numeric", unit="ft"),
                ),
                "parameter_output_ports": {
                    "nav_adc1_alt_ft": "aircraft_altitude_out",
                },
            }
        module_specs.append(
            _module_spec(
                system_id=system_id,
                subsystem_id=subsystem_id,
                module_id=module_id,
                sensors=sensors,
                **kwargs,
            )
        )

    inter_module_couplings = (
        InterModuleCouplingSpec(
            source_module_id="MOD_APU_GEN",
            source_port_name="apu_power_out",
            target_module_id="MOD_GEN_BUS",
            target_port_name="apu_power_in",
            relation_type="drive",
            gain=1.0,
        ),
        InterModuleCouplingSpec(
            source_module_id="MOD_GEN_BUS",
            source_port_name="ac_bus_voltage_out",
            target_module_id="MOD_INVERTER",
            target_port_name="ac_bus_voltage_in",
            relation_type="drive",
            gain=1.0,
        ),
        InterModuleCouplingSpec(
            source_module_id="MOD_ADC_1",
            source_port_name="aircraft_altitude_out",
            target_module_id="MOD_CABIN_PRESSURE",
            target_port_name="aircraft_altitude_in",
            relation_type="drive",
            gain=1.0,
        ),
        InterModuleCouplingSpec(
            source_module_id="MOD_PACK_LEFT",
            source_port_name="pack_left_flow_out",
            target_module_id="MOD_CABIN_PRESSURE",
            target_port_name="pack_left_flow_in",
            relation_type="drive",
            gain=1.0,
        ),
        InterModuleCouplingSpec(
            source_module_id="MOD_PACK_RIGHT",
            source_port_name="pack_right_flow_out",
            target_module_id="MOD_CABIN_PRESSURE",
            target_port_name="pack_right_flow_in",
            relation_type="drive",
            gain=1.0,
        ),
    )
    return build_hierarchy_assembly_spec(
        module_specs=tuple(module_specs),
        inter_module_couplings=inter_module_couplings,
        metadata={"source": "run_sim_detection_eval"},
    )


def _phase_by_key(telemetry_df: pd.DataFrame) -> dict[tuple[str, str, str, pd.Timestamp], str]:
    phase_map: dict[tuple[str, str, str, pd.Timestamp], str] = {}
    for row in telemetry_df.itertuples(index=False):
        key = (
            str(getattr(row, "tail_id")),
            str(getattr(row, "flight_id")),
            _parameter_name_from_row(row),
            pd.to_datetime(getattr(row, "timestamp_utc"), utc=True),
        )
        phase_map[key] = str(getattr(row, "phase_name"))
    return phase_map


def _phase_label_by_tail_flight_ts(phase_labels_df: pd.DataFrame) -> dict[tuple[str, str, pd.Timestamp], str]:
    phase_map: dict[tuple[str, str, pd.Timestamp], str] = {}
    for row in phase_labels_df.itertuples(index=False):
        key = (
            str(getattr(row, "tail_id")),
            str(getattr(row, "flight_id")),
            pd.to_datetime(getattr(row, "timestamp_utc"), utc=True),
        )
        phase_map[key] = str(getattr(row, "phase_name"))
    return phase_map


def _to_ts(value: object) -> datetime:
    return pd.to_datetime(value, utc=True).to_pydatetime()


def _event_key(
    *,
    tail_id: str,
    flight_id: str,
    parameter_name: str,
    ts: pd.Timestamp,
    event_type: str,
) -> tuple[str, str, str, pd.Timestamp, str]:
    return (str(tail_id), str(flight_id), str(parameter_name), pd.to_datetime(ts, utc=True), str(event_type))


def _row_datatype_for_detection(row: Any) -> str:
    # Canonical precedence: label first, then profiled fallback.
    for field in ("parameter_datatype_label", "parameter_datatype_profiled", "parameter_datatype"):
        value = getattr(row, field, None)
        text = str(value).strip().lower() if value is not None else ""
        if text and text not in {"none", "null", "nan"}:
            return normalize_sensor_datatype(value)
    return SensorDataType.UNKNOWN.value


def _iter_detected_events_stream(telemetry_df: pd.DataFrame) -> Iterator[dict[str, Any]]:
    parameter_key = _telemetry_parameter_key(telemetry_df)
    ordered = telemetry_df.sort_values(["tail_id", "flight_id", parameter_key, "timestamp_utc"], kind="mergesort")

    def _iter_continuous_samples() -> Iterator[ContinuousSample]:
        for row in ordered.itertuples(index=False):
            dtype = _row_datatype_for_detection(row)
            if dtype != SensorDataType.NUMERIC.value:
                continue
            value = getattr(row, "parameter_value_clean", None)
            if value is None or pd.isna(value):
                raw = getattr(row, "parameter_value", None)
                value = None if raw is None or pd.isna(raw) else float(raw)
            else:
                value = float(value)
            yield ContinuousSample(
                tail_id=str(getattr(row, "tail_id")),
                flight_id=str(getattr(row, "flight_id")),
                sensor=_parameter_name_from_row(row),
                ts=_to_ts(getattr(row, "timestamp_utc")),
                value=value,
            )

    def _iter_categorical_samples() -> Iterator[CategoricalSample]:
        for row in ordered.itertuples(index=False):
            dtype = _row_datatype_for_detection(row)
            if dtype not in {SensorDataType.BINARY.value, SensorDataType.CATEGORICAL.value, SensorDataType.HIGH_CARDINALITY.value}:
                continue
            state_raw = getattr(row, "parameter_value", None)
            state = None if state_raw is None or pd.isna(state_raw) else str(state_raw)
            yield CategoricalSample(
                tail_id=str(getattr(row, "tail_id")),
                flight_id=str(getattr(row, "flight_id")),
                sensor=_parameter_name_from_row(row),
                ts=_to_ts(getattr(row, "timestamp_utc")),
                state=state,
            )

    continuous_events = detect_continuous_events_stream(_iter_continuous_samples())
    categorical_events = detect_categorical_events_stream(_iter_categorical_samples())

    merged = heapq.merge(
        continuous_events,
        categorical_events,
        key=lambda event: (
            str(event.get("tail_id", "")),
            str(event.get("flight_id", "")),
            pd.to_datetime(event.get("ts"), utc=True),
            _parameter_name_from_mapping(event),
            str(event.get("event_type_detected", "")),
        ),
    )

    for event in merged:
        out = dict(event)
        out["parameter_name"] = _parameter_name_from_mapping(out)
        out["anomaly_type_detected"] = None
        out["anomaly_score_detected"] = None
        yield out


def _iter_profiled_datatype_rows(telemetry_df: pd.DataFrame) -> Iterator[dict[str, Any]]:
    parameter_key = _telemetry_parameter_key(telemetry_df)
    ordered = telemetry_df.sort_values(["tail_id", "flight_id", parameter_key, "timestamp_utc"], kind="mergesort")

    # O(1)-state online datatype profile per parameter stream.
    state: dict[tuple[str, str, str], dict[str, Any]] = {}

    for row in ordered.itertuples(index=False):
        tail_id = str(getattr(row, "tail_id"))
        flight_id = str(getattr(row, "flight_id"))
        parameter_name = _parameter_name_from_row(row)
        ts = pd.to_datetime(getattr(row, "timestamp_utc"), utc=True)
        key = (tail_id, flight_id, parameter_name)

        row_state = state.setdefault(
            key,
            {
                "observed_count": 0,
                "numeric_count": 0,
                "first_value": None,
                "second_value": None,
                "over_two": False,
                "looks_binary_token": True,
                "prev_ts": None,
                "ema_interval_ms": None,
            },
        )

        raw_value = getattr(row, "parameter_value", None)
        value_text = "" if raw_value is None or pd.isna(raw_value) else str(raw_value).strip()
        value_present = bool(value_text) and value_text.lower() not in {"none", "null", "nan"}

        if value_present:
            row_state["observed_count"] += 1
            try:
                float(str(getattr(row, "parameter_value_clean", value_text)).strip())
                row_state["numeric_count"] += 1
            except Exception:
                pass
            if value_text.lower() not in _BINARY_TOKENS:
                row_state["looks_binary_token"] = False

            first_value = row_state["first_value"]
            second_value = row_state["second_value"]
            if first_value is None:
                row_state["first_value"] = value_text
            elif value_text != first_value and second_value is None:
                row_state["second_value"] = value_text
            elif value_text != first_value and value_text != second_value:
                row_state["over_two"] = True

        prev_ts = row_state.get("prev_ts")
        if isinstance(prev_ts, pd.Timestamp):
            interval_ms = float((ts - prev_ts).total_seconds() * 1000.0)
            if interval_ms > 0.0:
                ema_interval_ms = row_state.get("ema_interval_ms")
                if ema_interval_ms is None:
                    row_state["ema_interval_ms"] = interval_ms
                else:
                    # O(1) streaming estimate of sampling interval.
                    row_state["ema_interval_ms"] = (0.2 * interval_ms) + (0.8 * float(ema_interval_ms))
        row_state["prev_ts"] = ts

        ema_interval_ms = row_state.get("ema_interval_ms")
        if ema_interval_ms is None or float(ema_interval_ms) <= 0.0:
            sampling_rate_profiled_hz = None
        else:
            sampling_rate_profiled_hz = 1000.0 / float(ema_interval_ms)

        observed_count = int(row_state["observed_count"])
        if observed_count <= 0:
            profiled_type = SensorDataType.UNKNOWN.value
        else:
            numeric_rate = float(row_state["numeric_count"]) / float(max(observed_count, 1))
            distinct_count = 0
            if row_state["first_value"] is not None:
                distinct_count = 1
            if row_state["second_value"] is not None:
                distinct_count = 2
            if row_state["over_two"]:
                distinct_count = 3

            if distinct_count <= 1:
                if bool(row_state["looks_binary_token"]):
                    profiled_type = SensorDataType.BINARY.value
                elif numeric_rate >= 0.8:
                    profiled_type = SensorDataType.CONSTANT.value
                else:
                    profiled_type = SensorDataType.CATEGORICAL.value
            elif numeric_rate >= 0.8 and distinct_count > 2:
                profiled_type = SensorDataType.NUMERIC.value
            elif distinct_count == 2:
                profiled_type = (
                    SensorDataType.BINARY.value
                    if bool(row_state["looks_binary_token"])
                    else SensorDataType.CATEGORICAL.value
                )
            elif distinct_count > 2:
                profiled_type = SensorDataType.CATEGORICAL.value
            else:
                profiled_type = SensorDataType.UNKNOWN.value

        yield {
            "tail_id": tail_id,
            "flight_id": flight_id,
            "parameter_name": parameter_name,
            "timestamp_utc": ts,
            "parameter_datatype_profiled": profiled_type,
            "sampling_rate_profiled_hz": sampling_rate_profiled_hz,
        }


def _totals(label: int, detected: int, tp: int) -> dict[str, float | int]:
    fp = max(int(detected) - int(tp), 0)
    fn = max(int(label) - int(tp), 0)
    precision = (float(tp) / float(tp + fp)) if (tp + fp) > 0 else 0.0
    recall = (float(tp) / float(tp + fn)) if (tp + fn) > 0 else 0.0
    return {
        "label": int(label),
        "detected": int(detected),
        "tp": int(tp),
        "fp": int(fp),
        "fn": int(fn),
        "precision": precision,
        "recall": recall,
    }


def _cooccurrence_buffer_sizes_ms(
    telemetry_df: pd.DataFrame,
    *,
    max_window_ms: int,
    n: int,
) -> list[int]:
    parameter_key = _telemetry_parameter_key(telemetry_df)
    sorted_df = telemetry_df.sort_values(["tail_id", "flight_id", parameter_key, "timestamp_utc"], kind="mergesort")
    interval_state: dict[tuple[str, str, str], dict[str, Any]] = {}
    min_interval_ms_global: float | None = None
    for row in sorted_df.itertuples(index=False):
        key = (
            str(getattr(row, "tail_id", "")),
            str(getattr(row, "flight_id", "")),
            _parameter_name_from_row(row),
        )
        ts = pd.to_datetime(getattr(row, "timestamp_utc"), utc=True)
        state = interval_state.setdefault(key, {"prev_ts": None, "sum_ms": 0.0, "count": 0})
        prev_ts = state.get("prev_ts")
        if isinstance(prev_ts, pd.Timestamp):
            delta_ms = float((ts - prev_ts).total_seconds() * 1000.0)
            if delta_ms > 0.0:
                state["sum_ms"] = float(state["sum_ms"]) + delta_ms
                state["count"] = int(state["count"]) + 1
                if min_interval_ms_global is None or delta_ms < min_interval_ms_global:
                    min_interval_ms_global = delta_ms
        state["prev_ts"] = ts

    n_value = max(int(n), 1)
    buffer_sizes: set[int] = set()
    for state in interval_state.values():
        count = int(state.get("count", 0))
        sum_ms = float(state.get("sum_ms", 0.0))
        if count <= 0 or sum_ms <= 0.0:
            continue
        avg_interval_ms = sum_ms / float(count)
        buffer_sizes.add(max(int(round(avg_interval_ms * float(n_value))), 1))
    # Exact cooccurrence lag: 1 / max_sampling_rate == minimum positive observed interval.
    if min_interval_ms_global is not None and min_interval_ms_global > 0.0:
        buffer_sizes.add(max(int(round(min_interval_ms_global)), 1))
    buffer_sizes.add(max(int(max_window_ms) * n_value, 1))
    return sorted(buffer_sizes)


def _rate_metrics_from_rows(
    telemetry_df: pd.DataFrame,
    profiler_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    # Build label sampling-rate baseline from observed simulator inter-arrival intervals.
    parameter_key = _telemetry_parameter_key(telemetry_df)
    sorted_df = telemetry_df.sort_values(["tail_id", "flight_id", parameter_key, "timestamp_utc"], kind="mergesort")
    interval_state: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in sorted_df.itertuples(index=False):
        key = (
            str(getattr(row, "tail_id", "")),
            str(getattr(row, "flight_id", "")),
            _parameter_name_from_row(row),
        )
        ts = pd.to_datetime(getattr(row, "timestamp_utc"), utc=True)
        state = interval_state.setdefault(key, {"prev_ts": None, "sum_ms": 0.0, "count": 0})
        prev_ts = state.get("prev_ts")
        if isinstance(prev_ts, pd.Timestamp):
            delta_ms = float((ts - prev_ts).total_seconds() * 1000.0)
            if delta_ms > 0.0:
                state["sum_ms"] = float(state["sum_ms"]) + delta_ms
                state["count"] = int(state["count"]) + 1
        state["prev_ts"] = ts

    label_rate_by_key: dict[tuple[str, str, str], float] = {}
    for key, state in interval_state.items():
        count = int(state.get("count", 0))
        sum_ms = float(state.get("sum_ms", 0.0))
        if count <= 0 or sum_ms <= 0.0:
            continue
        avg_ms = sum_ms / float(count)
        if avg_ms > 0.0:
            label_rate_by_key[key] = 1000.0 / avg_ms

    profiled_by_key: dict[tuple[str, str, str, pd.Timestamp], float | None] = {}
    for item in profiler_rows:
        key = (
            str(item.get("tail_id", "")),
            str(item.get("flight_id", "")),
            _parameter_name_from_mapping(item),
            pd.to_datetime(item.get("timestamp_utc"), utc=True),
        )
        value = item.get("sampling_rate_profiled_hz")
        profiled_by_key[key] = None if value is None else float(value)

    parameter_stats: dict[str, dict[str, float]] = {}
    total_count = 0
    profiled_count = 0
    abs_err_sum = 0.0
    sq_err_sum = 0.0
    ape_sum = 0.0
    within_5 = 0
    within_10 = 0
    within_20 = 0

    for row in telemetry_df.itertuples(index=False):
        parameter_name = _parameter_name_from_row(row)
        stream_key = (
            str(getattr(row, "tail_id", "")),
            str(getattr(row, "flight_id", "")),
            parameter_name,
        )
        label_rate_hz = label_rate_by_key.get(stream_key)
        if label_rate_hz is None:
            continue
        if label_rate_hz <= 0.0:
            continue
        total_count += 1
        key = (
            str(getattr(row, "tail_id", "")),
            str(getattr(row, "flight_id", "")),
            parameter_name,
            pd.to_datetime(getattr(row, "timestamp_utc"), utc=True),
        )
        profiled_rate_hz = profiled_by_key.get(key)
        if profiled_rate_hz is None or profiled_rate_hz <= 0.0:
            continue
        profiled_count += 1
        abs_err = abs(profiled_rate_hz - label_rate_hz)
        rel_err = abs_err / max(label_rate_hz, 1e-9)
        abs_err_sum += abs_err
        sq_err_sum += abs_err * abs_err
        ape_sum += rel_err
        if rel_err <= 0.05:
            within_5 += 1
        if rel_err <= 0.10:
            within_10 += 1
        if rel_err <= 0.20:
            within_20 += 1

        stat = parameter_stats.setdefault(
            parameter_name,
            {
                "count": 0.0,
                "label_rate_hz": label_rate_hz,
                "abs_err_sum": 0.0,
                "ape_sum": 0.0,
            },
        )
        stat["count"] += 1.0
        stat["abs_err_sum"] += abs_err
        stat["ape_sum"] += rel_err

    by_parameter_name: list[dict[str, Any]] = []
    for parameter_name in sorted(parameter_stats.keys()):
        item = parameter_stats[parameter_name]
        count = max(int(item["count"]), 1)
        by_parameter_name.append(
            {
                "parameter_name": parameter_name,
                "label_rate_hz": float(item["label_rate_hz"]),
                "mae_hz": float(item["abs_err_sum"]) / float(count),
                "mape_pct": (float(item["ape_sum"]) / float(count)) * 100.0,
                "count": count,
            }
        )

    if profiled_count <= 0:
        return {
            "row_count_with_label_rate": int(total_count),
            "row_count_with_profiled_rate": 0,
            "coverage": 0.0,
            "mae_hz": None,
            "rmse_hz": None,
            "mape_pct": None,
            "within_5pct": None,
            "within_10pct": None,
            "within_20pct": None,
            "by_parameter_name": by_parameter_name,
        }

    return {
        "row_count_with_label_rate": int(total_count),
        "row_count_with_profiled_rate": int(profiled_count),
        "coverage": float(profiled_count) / float(max(total_count, 1)),
        "mae_hz": abs_err_sum / float(profiled_count),
        "rmse_hz": (sq_err_sum / float(profiled_count)) ** 0.5,
        "mape_pct": (ape_sum / float(profiled_count)) * 100.0,
        "within_5pct": float(within_5) / float(profiled_count),
        "within_10pct": float(within_10) / float(profiled_count),
        "within_20pct": float(within_20) / float(profiled_count),
        "by_parameter_name": by_parameter_name,
    }


def _build_continuous_robust_scaler(telemetry_df: pd.DataFrame) -> dict[str, dict[str, float]]:
    values = telemetry_df.copy()
    parameter_key = "parameter_name" if "parameter_name" in values.columns else "sensor"
    values["parameter_name"] = values[parameter_key].astype(str)
    values["value_num"] = pd.to_numeric(values.get("parameter_value_clean"), errors="coerce")
    values = values.dropna(subset=["parameter_name", "value_num"])
    if values.empty:
        return {}

    scaler: dict[str, dict[str, float]] = {}
    grouped = values.groupby("parameter_name")["value_num"]
    for parameter_name, series in grouped:
        median = float(series.median())
        q25 = float(series.quantile(0.25))
        q75 = float(series.quantile(0.75))
        iqr = max(q75 - q25, 1e-6)
        scaler[str(parameter_name)] = {
            "median": median,
            "iqr": iqr,
        }
    return scaler


def _window_continuous_vectors(
    window_events: list[dict[str, Any]],
    scaler_by_parameter_name: dict[str, dict[str, float]],
) -> tuple[dict[str, float], dict[str, float]]:
    raw_by_parameter_name: dict[str, float] = {}
    for event in window_events:
        parameter_name = _parameter_name_from_mapping(event)
        if not parameter_name:
            continue
        payload = event.get("payload")
        if not isinstance(payload, dict):
            continue
        value = payload.get("value")
        if value is None:
            continue
        try:
            value_num = float(value)
        except Exception:
            continue
        raw_by_parameter_name[parameter_name] = value_num

    scaled_by_parameter_name: dict[str, float] = {}
    for parameter_name, value in raw_by_parameter_name.items():
        scaler = scaler_by_parameter_name.get(parameter_name)
        if scaler is None:
            continue
        median = float(scaler.get("median", 0.0))
        iqr = max(float(scaler.get("iqr", 1.0)), 1e-6)
        scaled_by_parameter_name[parameter_name] = (float(value) - median) / iqr
    return raw_by_parameter_name, scaled_by_parameter_name


def _window_categorical_state_t_end(window: dict[str, Any]) -> dict[str, str]:
    snapshot = window.get("zoh_snapshot")
    if not isinstance(snapshot, dict):
        return {}
    out: dict[str, str] = {}
    for parameter_name, value in snapshot.items():
        parameter_name_text = str(parameter_name)
        if not parameter_name_text:
            continue
        value_text = "" if value is None else str(value).strip()
        if not value_text:
            continue
        try:
            float(value_text)
            continue
        except Exception:
            pass
        out[parameter_name_text] = value_text
    return out


def _window_vector_drift_magnitude(previous_scaled: dict[str, float], current_scaled: dict[str, float]) -> float:
    parameter_name_union = set(previous_scaled.keys()) | set(current_scaled.keys())
    if not parameter_name_union:
        return 0.0
    drift_sq = 0.0
    for parameter_name in parameter_name_union:
        prev = float(previous_scaled.get(parameter_name, 0.0))
        curr = float(current_scaled.get(parameter_name, 0.0))
        delta = curr - prev
        drift_sq += delta * delta
    return drift_sq ** 0.5


def _top_phase_parameter_names(parameter_energy_rows: list[dict[str, Any]], *, k: int) -> list[str]:
    limit = max(int(k), 1)
    return [
        str(item.get("parameter_name", ""))
        for item in parameter_energy_rows[:limit]
        if str(item.get("parameter_name", ""))
    ]


def _top_phase_event_types(window_vectors: list[dict[str, Any]], *, k: int) -> list[str]:
    counts: Counter[str] = Counter()
    for item in window_vectors:
        event_type_counts = item.get("event_type_counts")
        if not isinstance(event_type_counts, dict):
            continue
        for event_type, count in event_type_counts.items():
            counts[str(event_type)] += int(count)
    limit = max(int(k), 0)
    if limit <= 0:
        return []
    continuous = [(event_type, count) for event_type, count in counts.most_common() if event_type in CONTINUOUS_EVENT_TYPES]
    categorical = [(event_type, count) for event_type, count in counts.most_common() if event_type in CATEGORICAL_EVENT_TYPES]
    continuous_k = max(limit // 2, 1) if continuous else 0
    categorical_k = max(limit - continuous_k, 0) if categorical else 0
    selected = [event_type for event_type, _ in continuous[:continuous_k]]
    selected.extend(event_type for event_type, _ in categorical[:categorical_k] if event_type not in selected)
    if len(selected) < limit:
        for event_type, _ in counts.most_common():
            if event_type in selected:
                continue
            selected.append(event_type)
            if len(selected) >= limit:
                break
    return selected


def _top_categorical_state_pairs(window_vectors: list[dict[str, Any]], *, k: int) -> list[tuple[str, str]]:
    counts: Counter[tuple[str, str]] = Counter()
    for item in window_vectors:
        categorical_state_t_end = item.get("categorical_state_t_end")
        if not isinstance(categorical_state_t_end, dict):
            continue
        for parameter_name, state in categorical_state_t_end.items():
            counts[(str(parameter_name), str(state))] += 1
    return [pair for pair, _ in counts.most_common(max(int(k), 0))]


def main() -> None:
    args = parse_args()
    if float(args.tolerance_seconds) != 0.0:
        print("warning: streaming mode uses exact timestamp/type matching; --tolerance-seconds is ignored")

    rng = np.random.default_rng(int(args.seed))
    assembly_spec = _sample_hierarchy_assembly_spec()
    hierarchy_df = flatten_assembly_spec(assembly_spec)
    behavior = build_default_parameter_behavior(hierarchy_df)
    phases = default_phase_definitions()[: max(int(args.phase_count), 1)]
    flight_setup = {
        "phase_sequence": [item["phase_name"] for item in phases],
        "flight_noise_scale_mean": 1.0,
        "flight_noise_scale_std": 0.01,
        "causal_delay": {
            "mode": "random_pair",
            "default_lag_sec": 0.0,
            "random_pair_delay_sec": {"min": 1.0, "max": 4.0},
            "jitter_sec_std": 0.25,
            "jitter_cap_steps": 2,
            "startup_fill": "hold_first",
            "seed_offset": 97,
        },
        "anomaly_plan": {
            "base_event_rate_per_min": 0.3,
            "burst_phases": [item["phase_name"] for item in phases[:1]],
            "burst_multiplier": 4.0,
            "primary_targets": ["ecs_cabin_alt_ft", "eng1_n1_pct", "nav_adc1_ias_kt", "elec_ac_bus_v"],
        },
    }

    system_ids = sorted({str(module_spec.system_id) for module_spec in assembly_spec.module_specs})
    tails = build_tail_profiles(system_ids, m_tails=max(int(args.tail_count), 1), rng=rng)
    fleet = build_fleet_manifest(tails, n_flights_per_tail=max(int(args.flights_per_tail), 1), rng=rng)
    telemetry_df, phase_labels_df = simulate_fleet_dataset(
        hierarchy_df=hierarchy_df,
        parameter_behavior=behavior,
        phase_definitions=phases,
        flight_setup=flight_setup,
        tail_profiles=tails,
        fleet_manifest_df=fleet,
    )
    if "parameter_datatype_label" not in telemetry_df.columns and "parameter_datatype" in telemetry_df.columns:
        telemetry_df = telemetry_df.copy()
        telemetry_df["parameter_datatype_label"] = telemetry_df["parameter_datatype"]

    phase_map = _phase_by_key(telemetry_df)
    phase_label_by_tail_flight_ts = _phase_label_by_tail_flight_ts(phase_labels_df)
    robust_scaler_by_parameter_name = build_continuous_robust_scaler(telemetry_df)
    phase_behavior_diagnostics = compute_phase_behavior_diagnostics(
        telemetry_df,
        top_k=max(int(args.phase_diagnostics_top_k), 0),
    )

    telemetry_parameter_key = _telemetry_parameter_key(telemetry_df)
    profiler_simulator_rows = (
        telemetry_df[
            ["tail_id", "flight_id", telemetry_parameter_key, "timestamp_utc", "parameter_datatype_label"]
        ]
        .rename(columns={telemetry_parameter_key: "parameter_name"})
        .to_dict(orient="records")
    )
    profiler_rows = list(_iter_profiled_datatype_rows(telemetry_df))
    sampling_rate_metrics = _rate_metrics_from_rows(
        telemetry_df=telemetry_df,
        profiler_rows=profiler_rows,
    )
    profiler_snapshots = list(
        stream_profiler_validation(
            simulator_rows=profiler_simulator_rows,
            profiler_rows=profiler_rows,
            emit_orphan_fp=True,
        )
    )
    profiler_validator_final = profiler_snapshots[-1] if profiler_snapshots else {"tp": 0, "fp": 0, "fn": 0, "tn": 0}

    if args.profiler_validator_snapshots_jsonl:
        profiler_jsonl = Path(str(args.profiler_validator_snapshots_jsonl))
        profiler_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with profiler_jsonl.open("w", encoding="utf-8") as handle:
            for item in profiler_snapshots:
                handle.write(json.dumps(item, default=str) + "\n")

    # Label indexes/counters (streaming-friendly counters, no event list materialization).
    label_key_counts: Counter[tuple[str, str, str, pd.Timestamp, str]] = Counter()
    detected_row_counts: Counter[tuple[str, str, str, pd.Timestamp]] = Counter()

    label_by_phase: Counter[str] = Counter()
    label_by_parameter: Counter[str] = Counter()
    label_by_event_type: Counter[str] = Counter()
    label_by_triplet: Counter[tuple[str, str, str]] = Counter()
    label_by_tail_flight: Counter[tuple[str, str]] = Counter()

    for row in telemetry_df.itertuples(index=False):
        event_type_label = str(getattr(row, "event_type_label", "")).strip()
        if not event_type_label or event_type_label == "none":
            continue
        tail_id = str(getattr(row, "tail_id"))
        flight_id = str(getattr(row, "flight_id"))
        parameter_name = _parameter_name_from_row(row)
        ts = pd.to_datetime(getattr(row, "timestamp_utc"), utc=True)
        phase_name = str(getattr(row, "phase_name"))

        label_key_counts[
            _event_key(
                tail_id=tail_id,
                flight_id=flight_id,
                parameter_name=parameter_name,
                ts=ts,
                event_type=event_type_label,
            )
        ] += 1
        label_by_tail_flight[(tail_id, flight_id)] += 1
        label_by_phase[phase_name] += 1
        label_by_parameter[parameter_name] += 1
        label_by_event_type[event_type_label] += 1
        label_by_triplet[(phase_name, parameter_name, event_type_label)] += 1

    # Window + detected accumulation in one pass over streaming windows.
    matched_key_counts: Counter[tuple[str, str, str, pd.Timestamp, str]] = Counter()

    detected_by_phase: Counter[str] = Counter()
    detected_by_parameter: Counter[str] = Counter()
    detected_by_event_type: Counter[str] = Counter()
    detected_by_triplet: Counter[tuple[str, str, str]] = Counter()
    detected_by_tail_flight: Counter[tuple[str, str]] = Counter()

    tp_by_phase: Counter[str] = Counter()
    tp_by_parameter: Counter[str] = Counter()
    tp_by_event_type: Counter[str] = Counter()
    tp_by_triplet: Counter[tuple[str, str, str]] = Counter()
    tp_by_tail_flight: Counter[tuple[str, str]] = Counter()

    total_detected = 0
    tp_total = 0

    close_reason_counts: Counter[str] = Counter()
    window_event_type_counts: Counter[str] = Counter()
    total_window_event_count = 0
    total_window_duration_ms = 0
    max_window_event_count = 0
    max_window_duration_ms = 0
    window_count = 0
    window_count_by_tail_flight: Counter[tuple[str, str]] = Counter()
    window_duration_hist_counts: Counter[str] = Counter()
    detected_events_materialized: list[dict[str, Any]] = []
    window_vectors: list[dict[str, Any]] = []
    previous_scaled_vector_by_flight: dict[tuple[str, str], dict[str, float]] = {}

    windows_writer = None
    if args.windows_jsonl:
        windows_path = Path(str(args.windows_jsonl))
        windows_path.parent.mkdir(parents=True, exist_ok=True)
        windows_writer = windows_path.open("w", encoding="utf-8")

    detected_events_stream = _iter_detected_events_stream(telemetry_df)
    windows_stream = build_adaptive_windows_stream(
        detected_events_stream,
        StreamWindowConfig(
            max_ms=max(int(args.window_max_ms), 1),
            min_ms=max(int(args.window_min_ms), 0),
            event_threshold=max(int(args.window_event_threshold), 1),
            inactivity_timeout_ms=max(int(args.window_inactivity_timeout_ms), 0),
            include_window_events=True,
        ),
    )

    for window in windows_stream:
        window_count += 1
        window_tail_id = str(window.get("tail_id", ""))
        window_flight_id = str(window.get("flight_id", ""))
        window_count_by_tail_flight[(window_tail_id, window_flight_id)] += 1
        event_count = int(window.get("event_count", 0))
        duration_ms = int(window.get("duration_ms", 0))
        total_window_event_count += event_count
        total_window_duration_ms += duration_ms
        max_window_event_count = max(max_window_event_count, event_count)
        max_window_duration_ms = max(max_window_duration_ms, duration_ms)
        if duration_ms < 1000:
            window_duration_hist_counts["lt_1s"] += 1
        elif duration_ms < 3000:
            window_duration_hist_counts["1s_to_3s"] += 1
        elif duration_ms < 10000:
            window_duration_hist_counts["3s_to_10s"] += 1
        elif duration_ms < 30000:
            window_duration_hist_counts["10s_to_30s"] += 1
        else:
            window_duration_hist_counts["ge_30s"] += 1

        close_reason = str(window.get("close_reason", "")).strip()
        if close_reason:
            close_reason_counts[close_reason] += 1

        event_type_counts = window.get("event_type_counts")
        if isinstance(event_type_counts, dict):
            for event_type, count in event_type_counts.items():
                window_event_type_counts[str(event_type)] += int(count)

        window_events = window.get("window_events")
        if not isinstance(window_events, list):
            if windows_writer is not None:
                windows_writer.write(json.dumps(window, default=str) + "\n")
            continue

        window_x = build_window_x_row(
            window=window,
            window_events=window_events,
            scaler_by_parameter_name=robust_scaler_by_parameter_name,
            previous_scaled_by_flight=previous_scaled_vector_by_flight,
            phase_label=phase_label_by_tail_flight_ts.get(
                (
                    str(window.get("tail_id", "")),
                    str(window.get("flight_id", "")),
                    pd.to_datetime(window.get("t_end"), utc=True),
                )
            ),
        )
        window["continuous_vector_t_end"] = dict(window_x["continuous_vector_t_end"])
        window["continuous_vector_t_end_scaled"] = dict(window_x["continuous_vector_t_end_scaled"])
        window["categorical_state_t_end"] = dict(window_x["categorical_state_t_end"])
        window["drift_magnitude_profiled"] = float(window_x["drift_magnitude_profiled"])
        window_x["event_parameter_names"] = sorted(
            {
                _parameter_name_from_mapping(event)
                for event in window_events
                if _parameter_name_from_mapping(event)
            }
        )
        window_vectors.append(window_x)
        if windows_writer is not None:
            windows_writer.write(json.dumps(window, default=str) + "\n")

        for event in window_events:
            detected_events_materialized.append(dict(event))
            total_detected += 1
            tail_id = str(event.get("tail_id", ""))
            flight_id = str(event.get("flight_id", ""))
            parameter_name = _parameter_name_from_mapping(event)
            ts = pd.to_datetime(event.get("ts"), utc=True)
            detected_type = str(event.get("event_type_detected", ""))

            phase_name = phase_map.get((tail_id, flight_id, parameter_name, ts), "unknown")
            triplet = (phase_name, parameter_name, detected_type)

            detected_by_phase[phase_name] += 1
            detected_by_parameter[parameter_name] += 1
            detected_by_event_type[detected_type] += 1
            detected_by_triplet[triplet] += 1
            detected_by_tail_flight[(tail_id, flight_id)] += 1

            row_key = (tail_id, flight_id, parameter_name, ts)
            detected_row_counts[row_key] += 1

            key = _event_key(
                tail_id=tail_id,
                flight_id=flight_id,
                parameter_name=parameter_name,
                ts=ts,
                event_type=detected_type,
            )
            if matched_key_counts[key] < label_key_counts.get(key, 0):
                matched_key_counts[key] += 1
                tp_total += 1
                tp_by_phase[phase_name] += 1
                tp_by_parameter[parameter_name] += 1
                tp_by_event_type[detected_type] += 1
                tp_by_triplet[triplet] += 1
                tp_by_tail_flight[(tail_id, flight_id)] += 1

    cooccurrence_updates_by_tail_flight: Counter[tuple[str, str]] = Counter()
    immediate_precedence_updates_by_tail_flight: Counter[tuple[str, str]] = Counter()
    if bool(args.skip_cooccurrence):
        cooccurrence_buffer_sizes_ms = []
        cooccurrence_pair_counts_by_buffer = {}
        immediate_precedence_pair_counts = Counter()
        cooccurrence_update_count = 0
        immediate_precedence_update_count = 0
    else:
        cooccurrence_buffer_sizes_ms = _cooccurrence_buffer_sizes_ms(
            telemetry_df=telemetry_df,
            max_window_ms=max(int(args.window_max_ms), 1),
            n=max(int(args.cooccurrence_n), 1),
        )
        cooccurrence_pair_counts_by_buffer: dict[int, Counter[tuple[str, str, str, str]]] = {
            int(buffer_ms): Counter() for buffer_ms in cooccurrence_buffer_sizes_ms
        }
        immediate_precedence_pair_counts: Counter[tuple[str, str, str, str]] = Counter()
        cooccurrence_update_count = 0
        immediate_precedence_update_count = 0

        cooccurrence_jsonl_writer = None
        if args.cooccurrence_counts_jsonl:
            cooccurrence_jsonl_path = Path(str(args.cooccurrence_counts_jsonl))
            cooccurrence_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            cooccurrence_jsonl_writer = cooccurrence_jsonl_path.open("w", encoding="utf-8")

        detected_events_for_coocc = sorted(
            detected_events_materialized,
            key=lambda event: (
                str(event.get("tail_id", "")),
                str(event.get("flight_id", "")),
                pd.to_datetime(event.get("ts"), utc=True),
                _parameter_name_from_mapping(event),
                str(event.get("event_type_detected", "")),
            ),
        )
        for buffer_ms in cooccurrence_buffer_sizes_ms:
            updates = stream_cooccurrence_pair_counts(
                detected_events_for_coocc,
                config=CooccurrencePairCountConfig(buffer_ms=int(buffer_ms), include_self_pairs=False),
            )
            pair_counts = cooccurrence_pair_counts_by_buffer[int(buffer_ms)]
            for update in updates:
                tail_id = str(update.get("tail_id", ""))
                flight_id = str(update.get("flight_id", ""))
                parameter_name_first = str(update.get("parameter_name_first", ""))
                event_type_first = str(update.get("event_type_first", ""))
                parameter_name_second = str(update.get("parameter_name_second", ""))
                event_type_second = str(update.get("event_type_second", ""))
                increment = int(update.get("increment", 0))
                if not parameter_name_first or not event_type_first or not parameter_name_second or not event_type_second or increment <= 0:
                    continue
                pair_counts[(parameter_name_first, event_type_first, parameter_name_second, event_type_second)] += increment
                cooccurrence_update_count += 1
                cooccurrence_updates_by_tail_flight[(tail_id, flight_id)] += 1
                if cooccurrence_jsonl_writer is not None:
                    cooccurrence_jsonl_writer.write(json.dumps(update, default=str) + "\n")

        immediate_updates = stream_immediate_precedence_pair_counts(detected_events_for_coocc)
        for update in immediate_updates:
            tail_id = str(update.get("tail_id", ""))
            flight_id = str(update.get("flight_id", ""))
            parameter_name_first = str(update.get("parameter_name_first", ""))
            event_type_first = str(update.get("event_type_first", ""))
            parameter_name_second = str(update.get("parameter_name_second", ""))
            event_type_second = str(update.get("event_type_second", ""))
            increment = int(update.get("increment", 0))
            if not parameter_name_first or not event_type_first or not parameter_name_second or not event_type_second or increment <= 0:
                continue
            immediate_precedence_pair_counts[(parameter_name_first, event_type_first, parameter_name_second, event_type_second)] += increment
            immediate_precedence_update_count += 1
            immediate_precedence_updates_by_tail_flight[(tail_id, flight_id)] += increment
            if cooccurrence_jsonl_writer is not None:
                payload = dict(update)
                payload["edge_type"] = "transition"
                cooccurrence_jsonl_writer.write(json.dumps(payload, default=str) + "\n")

        if cooccurrence_jsonl_writer is not None:
            cooccurrence_jsonl_writer.close()

    adjacency_nodes: set[tuple[str, str]] = set()
    adjacency_edges: list[dict[str, Any]] = []
    for buffer_ms, pair_counts in sorted(cooccurrence_pair_counts_by_buffer.items(), key=lambda item: item[0]):
        for (parameter_name_first, event_type_first, parameter_name_second, event_type_second), count in pair_counts.items():
            if int(count) <= 0:
                continue
            node_first = (parameter_name_first, event_type_first)
            node_second = (parameter_name_second, event_type_second)
            adjacency_nodes.add(node_first)
            adjacency_nodes.add(node_second)
            adjacency_edges.append(
                {
                    "buffer_ms": int(buffer_ms),
                    "edge_type": f"lag_{int(buffer_ms)}ms",
                    "source_parameter_name": parameter_name_first,
                    "source_event_type": event_type_first,
                    "target_parameter_name": parameter_name_second,
                    "target_event_type": event_type_second,
                    "weight": int(count),
                }
            )
    for (parameter_name_first, event_type_first, parameter_name_second, event_type_second), count in immediate_precedence_pair_counts.items():
        if int(count) <= 0:
            continue
        node_first = (parameter_name_first, event_type_first)
        node_second = (parameter_name_second, event_type_second)
        adjacency_nodes.add(node_first)
        adjacency_nodes.add(node_second)
        adjacency_edges.append(
            {
                "buffer_ms": None,
                "edge_type": "transition",
                "source_parameter_name": parameter_name_first,
                "source_event_type": event_type_first,
                "target_parameter_name": parameter_name_second,
                "target_event_type": event_type_second,
                "weight": int(count),
            }
        )

    if windows_writer is not None:
        windows_writer.close()

    total_label = sum(label_key_counts.values())
    fn_total = max(total_label - tp_total, 0)
    fp_total = max(total_detected - tp_total, 0)

    tn_total = 0
    for row in telemetry_df.itertuples(index=False):
        event_type_label = str(getattr(row, "event_type_label", "")).strip()
        if event_type_label and event_type_label != "none":
            continue
        row_key = (
            str(getattr(row, "tail_id")),
            str(getattr(row, "flight_id")),
            _parameter_name_from_row(row),
            pd.to_datetime(getattr(row, "timestamp_utc"), utc=True),
        )
        if detected_row_counts.get(row_key, 0) <= 0:
            tn_total += 1

    def _slice(counter_label: Counter[Any], counter_detected: Counter[Any], counter_tp: Counter[Any], key_name: str) -> list[dict[str, Any]]:
        keys = sorted(set(counter_label.keys()) | set(counter_detected.keys()))
        out: list[dict[str, Any]] = []
        for key in keys:
            totals = _totals(counter_label.get(key, 0), counter_detected.get(key, 0), counter_tp.get(key, 0))
            out.append({key_name: key, **totals})
        return out

    triplet_keys = sorted(set(label_by_triplet.keys()) | set(detected_by_triplet.keys()))
    by_triplet: list[dict[str, Any]] = []
    for phase_name, parameter_name, event_type in triplet_keys:
        totals = _totals(
            label_by_triplet.get((phase_name, parameter_name, event_type), 0),
            detected_by_triplet.get((phase_name, parameter_name, event_type), 0),
            tp_by_triplet.get((phase_name, parameter_name, event_type), 0),
        )
        by_triplet.append(
            {
                "phase_name": phase_name,
                "parameter_name": parameter_name,
                "event_type_detected": event_type,
                **totals,
            }
        )

    metrics = {
        "overall": {
            "totals": _totals(total_label, total_detected, tp_total),
            "tolerance_seconds": 0.0,
            "tolerance_by_type_seconds": {},
        },
        "by_phase": _slice(label_by_phase, detected_by_phase, tp_by_phase, "phase_name"),
        "by_parameter_name": _slice(label_by_parameter, detected_by_parameter, tp_by_parameter, "parameter_name"),
        "by_event_type_detected": _slice(label_by_event_type, detected_by_event_type, tp_by_event_type, "event_type_detected"),
        "by_phase_parameter_name_event_type": by_triplet,
    }

    windows_summary = {
        "window_count": int(window_count),
        "close_reason_counts": dict(sorted(close_reason_counts.items())),
        "event_type_counts": dict(sorted(window_event_type_counts.items())),
        "total_event_count": int(total_window_event_count),
        "avg_event_count": (float(total_window_event_count) / float(window_count)) if window_count > 0 else 0.0,
        "max_event_count": int(max_window_event_count),
        "total_duration_ms": int(total_window_duration_ms),
        "avg_duration_ms": (float(total_window_duration_ms) / float(window_count)) if window_count > 0 else 0.0,
        "max_duration_ms": int(max_window_duration_ms),
        "duration_histogram": {
            "bins_ms": [
                {"name": "lt_1s", "min_inclusive": 0, "max_exclusive": 1000},
                {"name": "1s_to_3s", "min_inclusive": 1000, "max_exclusive": 3000},
                {"name": "3s_to_10s", "min_inclusive": 3000, "max_exclusive": 10000},
                {"name": "10s_to_30s", "min_inclusive": 10000, "max_exclusive": 30000},
                {"name": "ge_30s", "min_inclusive": 30000, "max_exclusive": None},
            ],
            "counts": {
                "lt_1s": int(window_duration_hist_counts.get("lt_1s", 0)),
                "1s_to_3s": int(window_duration_hist_counts.get("1s_to_3s", 0)),
                "3s_to_10s": int(window_duration_hist_counts.get("3s_to_10s", 0)),
                "10s_to_30s": int(window_duration_hist_counts.get("10s_to_30s", 0)),
                "ge_30s": int(window_duration_hist_counts.get("ge_30s", 0)),
            },
        },
    }

    sampled_windows = sample_windows_for_coverage(
        window_vectors,
        sample_size_per_flight=max(int(args.window_sample_size_per_flight), 0),
        bins_per_axis=max(int(args.window_sample_bins), 1),
    )
    sampled_window_ids = [
        {
            "tail_id": str(item.get("tail_id", "")),
            "flight_id": str(item.get("flight_id", "")),
            "win_id": int(item.get("win_id", 0)),
        }
        for item in sampled_windows
    ]
    sampled_windows_by_tail_flight: Counter[tuple[str, str]] = Counter()
    for item in sampled_window_ids:
        sampled_windows_by_tail_flight[(str(item.get("tail_id", "")), str(item.get("flight_id", "")))] += 1
    sampled_window_sensor_energy = compute_window_sensor_energy(
        sampled_windows,
        vector_field="continuous_vector_t_end_scaled",
    )
    full_window_sensor_energy = compute_window_sensor_energy(
        window_vectors,
        vector_field="continuous_vector_t_end_scaled",
    )
    sampled_windows_by_tail_flight_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in sampled_windows:
        key = (str(item.get("tail_id", "")), str(item.get("flight_id", "")))
        sampled_windows_by_tail_flight_rows.setdefault(key, []).append(item)
    sampled_window_sensor_energy_by_tail_flight: list[dict[str, Any]] = []
    for (tail_id, flight_id) in sorted(sampled_windows_by_tail_flight_rows.keys(), key=lambda item: (item[0], item[1])):
        energies = compute_window_sensor_energy(
            sampled_windows_by_tail_flight_rows[(tail_id, flight_id)],
            vector_field="continuous_vector_t_end_scaled",
        )
        for energy_item in energies:
            sampled_window_sensor_energy_by_tail_flight.append(
                {
                    "tail_id": tail_id,
                    "flight_id": flight_id,
                    "parameter_name": str(energy_item.get("parameter_name", "")),
                    "energy": float(energy_item.get("energy", 0.0)),
                    "support_count": int(energy_item.get("support_count", 0)),
                }
            )
    sampled_window_sensor_energy_corpus = aggregate_sensor_energy_over_corpus(
        sampled_window_sensor_energy_by_tail_flight,
    )

    backbone_selected_sensors = select_backbone_sensors_by_energy(
        full_window_sensor_energy,
        k=max(int(args.backbone_sensor_count), 1),
    )
    backbone_gh_by_flight, backbone_all_sensors = compute_backbone_gh_by_flight(
        window_vectors,
        selected_sensors=backbone_selected_sensors,
    )
    backbone_g, backbone_h, backbone_window_count = aggregate_backbone_gh(backbone_gh_by_flight)
    backbone_weights_b = solve_backbone_weights(
        backbone_g,
        backbone_h,
        ridge_lambda=float(args.backbone_ridge_lambda),
    )
    backbone_window_errors: list[dict[str, Any]] = []
    for item in window_vectors:
        x_true = dict(item.get("continuous_vector_t_end_scaled", {}))
        x_hat = reconstruct_window_vector(
            x_true,
            selected_sensors=backbone_selected_sensors,
            all_sensors=backbone_all_sensors,
            weights_b=backbone_weights_b,
        )
        error, residuals = reconstruction_error(
            x_true,
            x_hat,
            sensor_order=backbone_all_sensors,
        )
        item["backbone_reconstruction_error"] = float(error)
        item["backbone_x_c"] = [
            float(x_true.get(parameter_name, 0.0) or 0.0)
            for parameter_name in backbone_selected_sensors
        ]
        top_residuals = sorted(
            residuals.items(),
            key=lambda kv: (-abs(float(kv[1])), kv[0]),
        )[:5]
        backbone_window_errors.append(
            {
                "tail_id": str(item.get("tail_id", "")),
                "flight_id": str(item.get("flight_id", "")),
                "win_id": int(item.get("win_id", 0)),
                "reconstruction_error": float(error),
                "top_residuals": [
                    {"parameter_name": str(parameter_name), "residual": float(value)}
                    for parameter_name, value in top_residuals
                ],
            }
        )

    phase_selected_sensors = _top_phase_parameter_names(
        full_window_sensor_energy,
        k=max(int(args.phase_detect_sensor_count), 1),
    )
    phase_selected_event_types = top_phase_event_types(
        window_vectors,
        k=max(int(args.phase_detect_event_type_count), 0),
    )
    phase_selected_categorical_state_pairs = top_categorical_state_pairs(
        window_vectors,
        k=max(int(args.phase_detect_categorical_state_count), 0),
    )
    phase_selected_window_cooccurrence_pairs = top_window_cooccurrence_sensor_pairs(
        window_vectors,
        k=max(int(args.phase_detect_window_cooccurrence_count), 0),
    )
    structured_windows_no_coocc, phase_feature_names_no_coocc = build_window_s_rows(
        window_vectors,
        selected_sensors_c=phase_selected_sensors,
        selected_event_types=phase_selected_event_types,
        selected_categorical_state_pairs=phase_selected_categorical_state_pairs,
    )
    structured_windows, phase_feature_names = build_window_s_rows(
        window_vectors,
        selected_sensors_c=phase_selected_sensors,
        selected_event_types=phase_selected_event_types,
        selected_categorical_state_pairs=phase_selected_categorical_state_pairs,
        selected_cooccurrence_sensor_pairs=phase_selected_window_cooccurrence_pairs,
    )
    phase_assignments_no_coocc, _ = detect_phases_from_windows(
        structured_windows_no_coocc,
        phase_count=max(int(args.phase_count), 1),
        stable_drift_quantile=float(args.phase_stable_drift_quantile),
        smoothing_radius=max(int(args.phase_smoothing_radius), 0),
        transition_penalty=float(args.phase_transition_penalty),
        min_dwell_windows=max(int(args.phase_min_dwell_windows), 1),
        ordered_phase_progression=True,
    )
    phase_evaluation_no_coocc = evaluate_detected_phases(phase_assignments_no_coocc)
    phase_assignments, phase_baselines = detect_phases_from_windows(
        structured_windows,
        phase_count=max(int(args.phase_count), 1),
        stable_drift_quantile=float(args.phase_stable_drift_quantile),
        smoothing_radius=max(int(args.phase_smoothing_radius), 0),
        transition_penalty=float(args.phase_transition_penalty),
        min_dwell_windows=max(int(args.phase_min_dwell_windows), 1),
        ordered_phase_progression=True,
    )
    phase_evaluation = evaluate_detected_phases(phase_assignments)
    phase_score_baselines = build_phase_score_baselines(
        structured_windows,
        phase_assignments,
    )
    v2_scores = score_window_s_rows(
        structured_windows,
        phase_assignments,
        phase_score_baselines,
    )
    backbone_artifact_df = pd.DataFrame(
        [
            {
                "backbone_version": 2,
                "selected_sensors_c": list(backbone_selected_sensors),
                "all_sensors": list(backbone_all_sensors),
                "weights_b": [[float(value) for value in row] for row in backbone_weights_b] if backbone_weights_b.size > 0 else [],
                "lambda_ridge": float(args.backbone_ridge_lambda),
                "training_window_count": int(backbone_window_count),
            }
        ]
    )
    window_x_df = pd.DataFrame(window_vectors)
    windows_graph_df = pd.DataFrame(
        [
            {
                "tail_id": str(item.get("tail_id", "")),
                "flight_id": str(item.get("flight_id", "")),
                "win_id": int(item.get("win_id", 0)),
                "t_start": item.get("t_start"),
                "t_end": item.get("t_end"),
                "duration_ms": int(item.get("duration_ms", 0) or 0),
                "event_count": int(item.get("event_count", 0) or 0),
                "date_utc": pd.to_datetime(item.get("t_start"), utc=True).date() if item.get("t_start") is not None else None,
            }
            for item in window_vectors
        ]
    )
    detected_events_graph_df = pd.DataFrame(detected_events_materialized)
    causal_delay_cfg = flight_setup.get("causal_delay", {}) or {}
    random_pair_delay_cfg = causal_delay_cfg.get("random_pair_delay_sec", {}) or {}
    if isinstance(random_pair_delay_cfg, dict):
        random_pair_delay_max = float(random_pair_delay_cfg.get("max", random_pair_delay_cfg.get("min", 0.0)) or 0.0)
    else:
        random_pair_delay_max = float(random_pair_delay_cfg or 0.0)
    causal_lag_cap_seconds = max(
        2.0,
        float(causal_delay_cfg.get("default_lag_sec", 0.0) or 0.0)
        + random_pair_delay_max
        + (2.0 * float(causal_delay_cfg.get("jitter_sec_std", 0.0) or 0.0))
        + 1.0,
    )
    graph_precision_df, graph_event_df, graph_lag_df, graph_transition_df, graph_fused_df, graph_hierarchy_df = (
        build_graph_artifact_tables(
            telemetry_df,
            detected_events_graph_df,
            windows_graph_df,
            backbone_artifact_df,
            precision_ridge_lambda=float(args.backbone_ridge_lambda),
            min_abs_partial_corr=float(args.graph_min_abs_partial_corr),
            min_event_count=1,
            min_event_npmi=float(args.graph_min_event_npmi),
            event_top_k_per_parameter_name=max(int(args.graph_event_top_k_per_sensor), 1),
            lag_tau_max_seconds=min(
                float(max(int(args.window_max_ms), 1)) / 1000.0 * float(max(int(args.cooccurrence_n), 1)),
                causal_lag_cap_seconds,
            ),
            min_lag_count=1,
            max_mean_lag_seconds=causal_lag_cap_seconds,
            lag_top_k_outgoing=max(int(args.graph_lag_top_k_outgoing), 1),
            min_transition_count=1,
            alpha=float(args.graph_alpha),
            beta=float(args.graph_beta),
            gamma=float(args.graph_gamma),
            min_fused_edge_weight=float(args.graph_min_fused_edge_weight),
            hierarchy_top_k_per_parameter_name=max(int(args.graph_hierarchy_top_k_per_sensor), 1),
            hierarchy_subsystem_min_edge_weight=float(args.graph_hierarchy_subsystem_min_edge_weight),
            hierarchy_system_min_edge_weight=float(args.graph_hierarchy_system_min_edge_weight),
        )
    )
    if args.graph_cache_json:
        cache_precision_df, cache_event_df, cache_lag_df, cache_transition_df = (
            build_graph_component_tables_from_window_x_table(
                window_x_df,
                detected_events_graph_df,
                windows_graph_df,
                backbone_artifact_df,
                precision_ridge_lambda=float(args.backbone_ridge_lambda),
                min_abs_partial_corr=float(args.graph_min_abs_partial_corr),
                min_event_count=1,
                min_event_npmi=float(args.graph_min_event_npmi),
                lag_tau_max_seconds=min(
                    float(max(int(args.window_max_ms), 1)) / 1000.0 * float(max(int(args.cooccurrence_n), 1)),
                    causal_lag_cap_seconds,
                ),
                min_lag_count=1,
                max_mean_lag_seconds=causal_lag_cap_seconds,
                min_transition_count=1,
            )
        )
        hierarchy_label_cache_df = hierarchy_df.copy()
        if "parameter_name" not in hierarchy_label_cache_df.columns and "sensor" in hierarchy_label_cache_df.columns:
            hierarchy_label_cache_df["parameter_name"] = hierarchy_label_cache_df["sensor"].astype(str)
        graph_cache_path = Path(str(args.graph_cache_json))
        graph_cache_path.parent.mkdir(parents=True, exist_ok=True)
        graph_cache_payload = {
            "cache_version": 1,
            "phase_accuracy": float(phase_evaluation.get("overall_accuracy")) if phase_evaluation.get("overall_accuracy") is not None else None,
            "backbone": _dataframe_records_json_safe(backbone_artifact_df),
            "hierarchy_labels": _dataframe_records_json_safe(hierarchy_label_cache_df),
            "precision_graph": _dataframe_records_json_safe(cache_precision_df),
            "event_graph": _dataframe_records_json_safe(cache_event_df),
            "lag_graph": _dataframe_records_json_safe(cache_lag_df),
            "transition_graph": _dataframe_records_json_safe(cache_transition_df),
            "cache_config": {
                "graph_min_abs_partial_corr": float(args.graph_min_abs_partial_corr),
                "graph_min_event_npmi": float(args.graph_min_event_npmi),
                "lag_tau_max_seconds": min(
                    float(max(int(args.window_max_ms), 1)) / 1000.0 * float(max(int(args.cooccurrence_n), 1)),
                    causal_lag_cap_seconds,
                ),
                "graph_max_mean_lag_seconds": float(causal_lag_cap_seconds),
                "backbone_ridge_lambda": float(args.backbone_ridge_lambda),
            },
        }
        graph_cache_path.write_text(json.dumps(graph_cache_payload, indent=2))
    hierarchy_recovery = _hierarchy_recovery_metrics(
        hierarchy_label_df=hierarchy_df,
        hierarchy_pred_df=graph_hierarchy_df,
    )
    graph_violation_rows = _graph_violation_scores(
        windows=window_vectors,
        fused_graph_df=graph_fused_df,
    )
    graph_violation_by_key = {
        (str(item.get("tail_id", "")), str(item.get("flight_id", "")), int(item.get("win_id", 0))): item
        for item in graph_violation_rows
    }
    for item in v2_scores:
        key = (str(item.get("tail_id", "")), str(item.get("flight_id", "")), int(item.get("win_id", 0)))
        violation = graph_violation_by_key.get(key)
        item["graph_violation_score"] = float(violation.get("graph_violation_score", 0.0)) if violation is not None else 0.0

    hierarchy_with_corr_group = hierarchy_df.copy()
    hierarchy_parameter_key = "parameter_name" if "parameter_name" in hierarchy_with_corr_group.columns else "sensor"
    hierarchy_with_corr_group["parameter_name"] = hierarchy_with_corr_group[hierarchy_parameter_key].astype(str)
    hierarchy_with_corr_group["corr_group"] = hierarchy_with_corr_group["parameter_name"].map(
        lambda parameter_name: str(behavior.get(str(parameter_name), {}).get("corr_group", ""))
    )
    corr_group_by_parameter_name = {
        str(row["parameter_name"]): str(row["corr_group"])
        for row in hierarchy_with_corr_group.to_dict(orient="records")
    }
    within_group_lag_rows = [
        row
        for row in graph_lag_df.to_dict(orient="records")
        if str(corr_group_by_parameter_name.get(str(row.get("parameter_name_u", "")), "")) != ""
        and str(corr_group_by_parameter_name.get(str(row.get("parameter_name_u", "")), "")) == str(corr_group_by_parameter_name.get(str(row.get("parameter_name_v", "")), ""))
    ]
    between_group_lag_rows = [
        row
        for row in graph_lag_df.to_dict(orient="records")
        if str(corr_group_by_parameter_name.get(str(row.get("parameter_name_u", "")), "")) != str(corr_group_by_parameter_name.get(str(row.get("parameter_name_v", "")), ""))
    ]
    causal_lag_diagnostics = {
        "causal_delay_config": flight_setup.get("causal_delay", {}),
        "enabled": bool(flight_setup.get("causal_delay")),
        "within_corr_group_edge_count": int(len(within_group_lag_rows)),
        "between_corr_group_edge_count": int(len(between_group_lag_rows)),
        "within_corr_group_mean_lag_seconds": (
            float(np.mean([float(item.get("mean_lag_seconds", 0.0) or 0.0) for item in within_group_lag_rows]))
            if within_group_lag_rows
            else None
        ),
        "between_corr_group_mean_lag_seconds": (
            float(np.mean([float(item.get("mean_lag_seconds", 0.0) or 0.0) for item in between_group_lag_rows]))
            if between_group_lag_rows
            else None
        ),
        "top_within_corr_group_lag_edges": sorted(
            [
                {
                    "parameter_name_u": str(item.get("parameter_name_u", "")),
                    "parameter_name_v": str(item.get("parameter_name_v", "")),
                    "lag_count": int(item.get("lag_count", 0) or 0),
                    "mean_lag_seconds": float(item.get("mean_lag_seconds", 0.0) or 0.0),
                    "corr_group": str(corr_group_by_parameter_name.get(str(item.get("parameter_name_u", "")), "")),
                }
                for item in within_group_lag_rows
            ],
            key=lambda item: (-item["lag_count"], -item["mean_lag_seconds"], item["parameter_name_u"], item["parameter_name_v"]),
        )[:10],
    }
    phase_window_count_by_tail_flight: Counter[tuple[str, str]] = Counter()
    phase_stable_count_by_tail_flight: Counter[tuple[str, str]] = Counter()
    phase_ids_by_tail_flight: dict[tuple[str, str], set[int]] = {}
    phase_eval_by_tail_flight: dict[tuple[str, str], dict[str, Any]] = {}
    for item in phase_evaluation.get("by_tail_flight", []):
        key = (str(item.get("tail_id", "")), str(item.get("flight_id", "")))
        phase_eval_by_tail_flight[key] = dict(item)
    for item in phase_assignments:
        key = (
            str(item.get("tail_id", "")),
            str(item.get("flight_id", "")),
        )
        phase_window_count_by_tail_flight[key] += 1
        if str(item.get("phase_state_detected", "")) == "stable":
            phase_stable_count_by_tail_flight[key] += 1
        phase_ids_by_tail_flight.setdefault(key, set()).add(int(item.get("phase_id_detected", 0)))

    validator_final = {
        "tp": int(tp_total),
        "fp": int(fp_total),
        "fn": int(fn_total),
        "tn": int(tn_total),
    }

    if args.validator_snapshots_jsonl:
        jsonl_path = Path(str(args.validator_snapshots_jsonl))
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        with jsonl_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(validator_final, default=str) + "\n")

    telemetry_rows_by_tail_flight: Counter[tuple[str, str]] = Counter()
    tn_by_tail_flight: Counter[tuple[str, str]] = Counter()
    for row in telemetry_df.itertuples(index=False):
        tail_id = str(getattr(row, "tail_id"))
        flight_id = str(getattr(row, "flight_id"))
        key_tail_flight = (tail_id, flight_id)
        telemetry_rows_by_tail_flight[key_tail_flight] += 1

        event_type_label = str(getattr(row, "event_type_label", "")).strip()
        if event_type_label and event_type_label != "none":
            continue
        row_key = (
            tail_id,
            flight_id,
            _parameter_name_from_row(row),
            pd.to_datetime(getattr(row, "timestamp_utc"), utc=True),
        )
        if detected_row_counts.get(row_key, 0) <= 0:
            tn_by_tail_flight[key_tail_flight] += 1

    tail_flight_keys = sorted(
        set(telemetry_rows_by_tail_flight.keys())
        | set(label_by_tail_flight.keys())
        | set(detected_by_tail_flight.keys())
        | set(window_count_by_tail_flight.keys())
        | set(sampled_windows_by_tail_flight.keys())
        | set(cooccurrence_updates_by_tail_flight.keys())
        | set(immediate_precedence_updates_by_tail_flight.keys())
    )
    by_tail_flight: list[dict[str, Any]] = []
    for tail_id, flight_id in tail_flight_keys:
        label_count = int(label_by_tail_flight.get((tail_id, flight_id), 0))
        detected_count = int(detected_by_tail_flight.get((tail_id, flight_id), 0))
        tp_count = int(tp_by_tail_flight.get((tail_id, flight_id), 0))
        totals = _totals(label_count, detected_count, tp_count)
        by_tail_flight.append(
            {
                "tail_id": tail_id,
                "flight_id": flight_id,
                "row_counts": {
                    "telemetry_rows": int(telemetry_rows_by_tail_flight.get((tail_id, flight_id), 0)),
                    "label_events": label_count,
                    "detected_events": detected_count,
                    "windows": int(window_count_by_tail_flight.get((tail_id, flight_id), 0)),
                    "sampled_windows": int(sampled_windows_by_tail_flight.get((tail_id, flight_id), 0)),
                    "cooccurrence_pair_updates": int(cooccurrence_updates_by_tail_flight.get((tail_id, flight_id), 0)),
                    "immediate_precedence_pair_updates": int(immediate_precedence_updates_by_tail_flight.get((tail_id, flight_id), 0)),
                },
                "event_metrics": {
                    **totals,
                    "tn": int(tn_by_tail_flight.get((tail_id, flight_id), 0)),
                },
                "phase_metrics": {
                    "window_count": int(phase_window_count_by_tail_flight.get((tail_id, flight_id), 0)),
                    "stable_window_count": int(phase_stable_count_by_tail_flight.get((tail_id, flight_id), 0)),
                    "detected_phase_ids": sorted(phase_ids_by_tail_flight.get((tail_id, flight_id), set())),
                    "accuracy_vs_phase_label": phase_eval_by_tail_flight.get((tail_id, flight_id), {}).get("accuracy"),
                },
            }
        )

    report = {
        "config": {
            "tail_count": int(args.tail_count),
            "flights_per_tail": int(args.flights_per_tail),
            "seed": int(args.seed),
            "phase_count": int(args.phase_count),
            "tolerance_seconds": 0.0,
            "window_max_ms": int(args.window_max_ms),
            "window_min_ms": int(args.window_min_ms),
            "window_event_threshold": int(args.window_event_threshold),
            "window_inactivity_timeout_ms": int(args.window_inactivity_timeout_ms),
            "window_sample_size_per_flight": int(args.window_sample_size_per_flight),
            "window_sample_bins": int(args.window_sample_bins),
            "backbone_sensor_count": int(args.backbone_sensor_count),
            "backbone_ridge_lambda": float(args.backbone_ridge_lambda),
            "graph_min_abs_partial_corr": float(args.graph_min_abs_partial_corr),
            "graph_min_event_npmi": float(args.graph_min_event_npmi),
            "graph_event_top_k_per_sensor": int(args.graph_event_top_k_per_sensor),
            "graph_lag_top_k_outgoing": int(args.graph_lag_top_k_outgoing),
            "graph_alpha": float(args.graph_alpha),
            "graph_beta": float(args.graph_beta),
            "graph_gamma": float(args.graph_gamma),
            "graph_min_fused_edge_weight": float(args.graph_min_fused_edge_weight),
            "graph_hierarchy_top_k_per_sensor": int(args.graph_hierarchy_top_k_per_sensor),
            "graph_hierarchy_subsystem_min_edge_weight": float(args.graph_hierarchy_subsystem_min_edge_weight),
            "graph_hierarchy_system_min_edge_weight": float(args.graph_hierarchy_system_min_edge_weight),
            "phase_detect_sensor_count": int(args.phase_detect_sensor_count),
            "phase_detect_event_type_count": int(args.phase_detect_event_type_count),
            "phase_detect_categorical_state_count": int(args.phase_detect_categorical_state_count),
            "phase_detect_window_cooccurrence_count": int(args.phase_detect_window_cooccurrence_count),
            "phase_stable_drift_quantile": float(args.phase_stable_drift_quantile),
            "phase_smoothing_radius": int(args.phase_smoothing_radius),
            "phase_transition_penalty": float(args.phase_transition_penalty),
            "phase_min_dwell_windows": int(args.phase_min_dwell_windows),
            "phase_diagnostics_top_k": int(args.phase_diagnostics_top_k),
            "cooccurrence_n": int(args.cooccurrence_n),
            "cooccurrence_top_k": int(args.cooccurrence_top_k),
        },
        "row_counts": {
            "telemetry_rows": int(len(telemetry_df)),
            "phase_label_rows": int(len(phase_labels_df)),
            "label_events": int(total_label),
            "detected_events": int(total_detected),
            "cooccurrence_pair_updates": int(cooccurrence_update_count),
            "immediate_precedence_pair_updates": int(immediate_precedence_update_count),
            "windows": int(window_count),
            "validator_snapshots": 1 if args.validator_snapshots_jsonl else 0,
            "profiler_validator_snapshots": int(len(profiler_snapshots)),
        },
        "validator_final": validator_final,
        "profiler_validator_final": {
            "tp": int(profiler_validator_final.get("tp", 0)),
            "fp": int(profiler_validator_final.get("fp", 0)),
            "fn": int(profiler_validator_final.get("fn", 0)),
            "tn": int(profiler_validator_final.get("tn", 0)),
        },
        "sampling_rate_metrics": sampling_rate_metrics,
        "cooccurrence": {
            "buffer_sizes_ms": [int(item) for item in cooccurrence_buffer_sizes_ms],
            "adjacency_graph": {
                "nodes": [
                    {
                        "parameter_name": parameter_name,
                        "event_type": event_type,
                    }
                    for parameter_name, event_type in sorted(adjacency_nodes, key=lambda item: (item[0], item[1]))
                ],
                "edges": adjacency_edges,
            },
            "edges_by_buffer": [
                {
                    "buffer_ms": int(buffer_ms),
                    "unique_pairs": int(len(pair_counts)),
                    "total_count": int(sum(pair_counts.values())),
                }
                for buffer_ms, pair_counts in sorted(cooccurrence_pair_counts_by_buffer.items(), key=lambda item: item[0])
            ],
            "immediate_precedence": {
                "edge_type": "transition",
                "unique_pairs": int(len(immediate_precedence_pair_counts)),
                "total_count": int(sum(immediate_precedence_pair_counts.values())),
                "top_pairs": [
                    {
                        "parameter_name_first": str(parameter_name_first),
                        "event_type_first": str(event_type_first),
                        "parameter_name_second": str(parameter_name_second),
                        "event_type_second": str(event_type_second),
                        "count": int(count),
                    }
                    for (parameter_name_first, event_type_first, parameter_name_second, event_type_second), count in immediate_precedence_pair_counts.most_common(max(int(args.cooccurrence_top_k), 1))
                ],
            },
            "top_pairs_by_buffer": [
                {
                    "buffer_ms": int(buffer_ms),
                    "parameter_name_first": str(parameter_name_first),
                    "event_type_first": str(event_type_first),
                    "parameter_name_second": str(parameter_name_second),
                    "event_type_second": str(event_type_second),
                    "count": int(count),
                }
                for buffer_ms, pair_counts in sorted(cooccurrence_pair_counts_by_buffer.items(), key=lambda item: item[0])
                for (parameter_name_first, event_type_first, parameter_name_second, event_type_second), count in pair_counts.most_common(max(int(args.cooccurrence_top_k), 1))
            ],
        },
        "windows": windows_summary,
        "window_vectors": window_vectors,
        "window_sampling": {
            "selected_window_ids": sampled_window_ids,
            "sensor_energy": sampled_window_sensor_energy,
            "sensor_energy_full_windows": full_window_sensor_energy,
            "sensor_energy_by_tail_flight": sampled_window_sensor_energy_by_tail_flight,
            "sensor_energy_corpus": sampled_window_sensor_energy_corpus,
        },
        "backbone": {
            "selected_sensors_c": backbone_selected_sensors,
            "all_sensors": backbone_all_sensors,
            "training_window_count": int(backbone_window_count),
            "ridge_lambda": float(args.backbone_ridge_lambda),
            "weights_shape": [int(backbone_weights_b.shape[0]), int(backbone_weights_b.shape[1])] if backbone_weights_b.size > 0 else [0, 0],
            "g_shape": [int(backbone_g.shape[0]), int(backbone_g.shape[1])] if backbone_g.size > 0 else [0, 0],
            "h_shape": [int(backbone_h.shape[0]), int(backbone_h.shape[1])] if backbone_h.size > 0 else [0, 0],
            "per_flight_stats": [
                {
                    "tail_id": str(item.get("tail_id", "")),
                    "flight_id": str(item.get("flight_id", "")),
                    "window_count": int(item.get("window_count", 0)),
                }
                for item in backbone_gh_by_flight
            ],
            "window_reconstruction_errors": backbone_window_errors,
        },
        "graph_v2": {
            "precision_graph": {
                "edge_count": int(len(graph_precision_df)),
                "top_edges": graph_precision_df.sort_values(["precision_weight", "parameter_name_u", "parameter_name_v"], ascending=[False, True, True]).head(20).to_dict(orient="records"),
            },
            "event_graph": {
                "edge_count": int(len(graph_event_df)),
                "top_edges": graph_event_df.sort_values(["event_weight", "parameter_name_u", "parameter_name_v"], ascending=[False, True, True]).head(20).to_dict(orient="records"),
            },
            "lag_graph": {
                "edge_count": int(len(graph_lag_df)),
                "top_edges": graph_lag_df.sort_values(["lag_weight", "parameter_name_u", "parameter_name_v"], ascending=[False, True, True]).head(20).to_dict(orient="records"),
            },
            "transition_graph": {
                "edge_count": int(len(graph_transition_df)),
                "top_edges": graph_transition_df.sort_values(["precedence_weight", "parameter_name_u", "parameter_name_v"], ascending=[False, True, True]).head(20).to_dict(orient="records"),
            },
            "fused_graph": {
                "edge_count": int(len(graph_fused_df)),
                "top_edges": graph_fused_df.sort_values(["fused_weight", "parameter_name_u", "parameter_name_v"], ascending=[False, True, True]).head(20).to_dict(orient="records"),
            },
            "hierarchy_sensor_map": graph_hierarchy_df.to_dict(orient="records"),
            "hierarchy_recovery": hierarchy_recovery,
            "causal_lag_diagnostics": causal_lag_diagnostics,
        },
        "phase_detection": {
            "phase_behavior_diagnostics": phase_behavior_diagnostics,
            "selected_sensors": phase_selected_sensors,
            "selected_event_types": phase_selected_event_types,
            "selected_categorical_state_pairs": [
                {"parameter_name": parameter_name, "state": state}
                for parameter_name, state in phase_selected_categorical_state_pairs
            ],
            "selected_window_cooccurrence_pairs": [
                {"parameter_name_left": left, "parameter_name_right": right}
                for left, right in phase_selected_window_cooccurrence_pairs
            ],
            "structure_feature_names": phase_feature_names,
            "phase_baselines_by_tail": phase_baselines,
            "window_assignments": phase_assignments,
            "evaluation": phase_evaluation,
            "cooccurrence_ablation": {
                "without_window_cooccurrence": phase_evaluation_no_coocc,
                "with_window_cooccurrence": phase_evaluation,
            },
        },
        "scoring_v2": {
            "phase_score_baselines": phase_score_baselines,
            "window_scores": v2_scores,
            "graph_violation_channel": {
                "recommended": True,
                "blended_into_global_score": False,
                "window_scores": graph_violation_rows,
            },
        },
        "by_tail_flight": by_tail_flight,
        "metrics": metrics,
    }

    output_path = Path(str(args.output_json))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    totals = metrics["overall"]["totals"]
    print("Simulation detection runner")
    print(f"- telemetry_rows: {report['row_counts']['telemetry_rows']}")
    print(f"- label_events: {report['row_counts']['label_events']}")
    print(f"- detected_events: {report['row_counts']['detected_events']}")
    print(f"- cooccurrence_pair_updates: {report['row_counts']['cooccurrence_pair_updates']}")
    print(f"- immediate_precedence_pair_updates: {report['row_counts']['immediate_precedence_pair_updates']}")
    print(f"- windows: {report['row_counts']['windows']}")
    print(f"- sampled_windows: {len(report.get('window_sampling', {}).get('selected_window_ids', []))}")
    print(f"- tp/fp/fn/tn: {report['validator_final']['tp']}/{report['validator_final']['fp']}/{report['validator_final']['fn']}/{report['validator_final']['tn']}")
    print(
        "- profiler tp/fp/fn/tn: "
        f"{report['profiler_validator_final']['tp']}/"
        f"{report['profiler_validator_final']['fp']}/"
        f"{report['profiler_validator_final']['fn']}/"
        f"{report['profiler_validator_final']['tn']}"
    )
    rate_metrics = report.get("sampling_rate_metrics", {})
    mape_pct = rate_metrics.get("mape_pct")
    mae_hz = rate_metrics.get("mae_hz")
    coverage = rate_metrics.get("coverage")
    mape_text = "n/a" if mape_pct is None else f"{float(mape_pct):.3f}%"
    mae_text = "n/a" if mae_hz is None else f"{float(mae_hz):.6f} Hz"
    coverage_text = "n/a" if coverage is None else f"{float(coverage):.4f}"
    print(
        "- sampling_rate mape/mae/coverage: "
        f"{mape_text} / "
        f"{mae_text} / "
        f"{coverage_text}"
    )
    print(f"- precision: {totals['precision']:.4f}")
    print(f"- recall: {totals['recall']:.4f}")
    print(f"- report: {output_path}")


if __name__ == "__main__":
    main()
