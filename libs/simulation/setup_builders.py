from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from libs.common import SensorDataType, normalize_sensor_datatype
from libs.simulation.behavior_defaults import build_default_parameter_behavior


def flatten_hierarchy_spec(hierarchy_spec: dict) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    systems = hierarchy_spec.get("systems", {})
    for system_id, system_obj in systems.items():
        for subsystem_id, subsystem_obj in system_obj.get("subsystems", {}).items():
            for module_id, module_sensors in subsystem_obj.get("modules", {}).items():
                for sensor_obj in module_sensors:
                    parameter_name = str(sensor_obj.get("parameter_name") or sensor_obj.get("sensor", ""))
                    rows.append(
                        {
                            "system_id": str(system_id),
                            "subsystem_id": str(subsystem_id),
                            "module_id": str(module_id),
                            "sensor": parameter_name,
                            "parameter_name": parameter_name,
                            "parameter_datatype": normalize_sensor_datatype(sensor_obj.get("datatype", SensorDataType.UNKNOWN.value)),
                            "unit": str(sensor_obj.get("unit", "")),
                        }
                    )
    hierarchy_df = pd.DataFrame(rows)
    if hierarchy_df.empty:
        return pd.DataFrame(
            columns=[
                "system_id",
                "subsystem_id",
                "module_id",
                "sensor",
                "parameter_name",
                "parameter_datatype",
                "unit",
            ]
        )
    return hierarchy_df.sort_values(["system_id", "subsystem_id", "module_id", "parameter_name"]).reset_index(drop=True)


def build_mermaid_hierarchy(hierarchy_df: pd.DataFrame, max_sensors: int = 200) -> str:
    if hierarchy_df.empty:
        return "flowchart TD\n  GLOBAL[\"GLOBAL\"]"

    limited = hierarchy_df.head(max(int(max_sensors), 1)).copy()

    def safe_node_id(prefix: str, value: str) -> str:
        cleaned = "".join(ch if ch.isalnum() else "_" for ch in str(value))
        return f"{prefix}_{cleaned}"

    node_lines = set(["  GLOBAL[\"GLOBAL\"]"])
    edge_lines: set[str] = set()
    for _, row in limited.iterrows():
        system_id = str(row["system_id"])
        subsystem_id = str(row["subsystem_id"])
        module_id = str(row["module_id"])
        parameter_name = str(row.get("parameter_name", row["sensor"]))
        dtype = str(row.get("parameter_datatype", "unknown"))

        sys_node = safe_node_id("SYS", system_id)
        sub_node = safe_node_id("SUB", f"{system_id}_{subsystem_id}")
        mod_node = safe_node_id("MOD", f"{system_id}_{subsystem_id}_{module_id}")
        parameter_node = safe_node_id("SEN", parameter_name)

        node_lines.add(f"  {sys_node}[\"{system_id}\"]")
        node_lines.add(f"  {sub_node}[\"{subsystem_id}\"]")
        node_lines.add(f"  {mod_node}[\"{module_id}\"]")
        node_lines.add(f"  {parameter_node}[\"{parameter_name} ({dtype})\"]")

        edge_lines.add(f"  GLOBAL --> {sys_node}")
        edge_lines.add(f"  {sys_node} --> {sub_node}")
        edge_lines.add(f"  {sub_node} --> {mod_node}")
        edge_lines.add(f"  {mod_node} --> {parameter_node}")

    lines = ["flowchart TD"] + sorted(node_lines) + sorted(edge_lines)
    return "\n".join(lines)

def default_phase_definitions() -> list[dict]:
    return [
        {
            "phase_id": 0,
            "phase_name": "gate_turnaround",
            "duration_sec": {"mean": 180, "jitter": 30},
            "noise_scale": 0.7,
            "corr_scale": 0.5,
            "modifiers": {
                "elec_apu_gen_on": {"binary_on_prob": 0.9},
                "elec_ac_bus_v": {"add": -6.0},
                "elec_ac_bus_hz": {"add": -3.0},
                "ecs_press_mode": {"state_weights": [0.15, 0.25, 0.60]},
                "eng1_thrust_mode": {"state_weights": [0.75, 0.20, 0.05]},
                "eng2_thrust_mode": {"state_weights": [0.75, 0.20, 0.05]},
            },
            "transition": {"next": "taxi_out", "transition_sec": 20, "sharpness": 0.8},
        },
        {
            "phase_id": 1,
            "phase_name": "taxi_out",
            "duration_sec": {"mean": 240, "jitter": 40},
            "noise_scale": 0.9,
            "corr_scale": 0.7,
            "modifiers": {
                "fc_elac_active": {"binary_on_prob": 0.6},
                "fc_sec_active": {"binary_on_prob": 0.7},
                "ecs_pack_l_flow_kg_s": {"add": 0.35},
                "ecs_pack_r_flow_kg_s": {"add": 0.35},
                "ecs_cabin_alt_ft": {"trend_add": 1.0},
                "nav_adc1_ias_kt": {"add": 25.0},
                "nav_adc2_ias_kt": {"add": 25.0},
            },
            "transition": {"next": "takeoff_climb", "transition_sec": 12, "sharpness": 1.1},
        },
        {
            "phase_id": 2,
            "phase_name": "takeoff_climb",
            "duration_sec": {"mean": 320, "jitter": 50},
            "noise_scale": 1.3,
            "corr_scale": 1.1,
            "modifiers": {
                "eng1_n1_pct": {"add": 28.0, "trend_add": 0.03},
                "eng2_n1_pct": {"add": 28.0, "trend_add": 0.03},
                "eng1_n2_pct": {"add": 22.0, "trend_add": 0.02},
                "eng2_n2_pct": {"add": 22.0, "trend_add": 0.02},
                "eng1_fuel_flow_kgph": {"add": 950.0},
                "eng2_fuel_flow_kgph": {"add": 950.0},
                "ecs_cabin_alt_ft": {"trend_add": 12.0, "add": 500.0},
                "ecs_delta_p_psi": {"add": 2.5},
                "nav_adc1_ias_kt": {"add": 130.0},
                "nav_adc2_ias_kt": {"add": 130.0},
                "nav_irs1_pitch_deg": {"add": 6.0},
                "nav_irs2_pitch_deg": {"add": 6.0},
                "elec_ac_bus_v": {"add": 4.0},
            },
            "transition": {"next": "cruise", "transition_sec": 25, "sharpness": 0.7},
        },
        {
            "phase_id": 3,
            "phase_name": "cruise",
            "duration_sec": {"mean": 620, "jitter": 120},
            "noise_scale": 0.6,
            "corr_scale": 0.85,
            "modifiers": {
                "fc_yd_engaged": {"binary_on_prob": 0.9},
                "ecs_press_mode": {"state_weights": [0.88, 0.10, 0.02]},
                "eng1_thrust_mode": {"state_weights": [0.10, 0.82, 0.08]},
                "eng2_thrust_mode": {"state_weights": [0.10, 0.82, 0.08]},
                "ecs_cabin_alt_ft": {"add": 6800.0},
                "ecs_delta_p_psi": {"add": 6.8},
                "nav_adc1_ias_kt": {"add": 245.0},
                "nav_adc2_ias_kt": {"add": 245.0},
                "nav_adc1_alt_ft": {"add": 33000.0},
                "nav_adc2_alt_ft": {"add": 33000.0},
                "eng1_n1_pct": {"add": 14.0},
                "eng2_n1_pct": {"add": 14.0},
            },
            "transition": {"next": "descent_approach", "transition_sec": 25, "sharpness": 0.8},
        },
        {
            "phase_id": 4,
            "phase_name": "descent_approach",
            "duration_sec": {"mean": 300, "jitter": 60},
            "noise_scale": 1.0,
            "corr_scale": 0.75,
            "modifiers": {
                "nav_irs1_pitch_deg": {"add": -3.0},
                "nav_irs2_pitch_deg": {"add": -3.0},
                "ecs_cabin_alt_ft": {"trend_add": -8.0},
                "ecs_delta_p_psi": {"add": -1.6},
                "ecs_outflow_valve_open": {"binary_on_prob": 0.75},
                "nav_adc1_ias_kt": {"add": -70.0},
                "nav_adc2_ias_kt": {"add": -70.0},
                "nav_adc1_alt_ft": {"add": 12000.0},
                "nav_adc2_alt_ft": {"add": 12000.0},
                "eng1_n1_pct": {"add": -5.0},
                "eng2_n1_pct": {"add": -5.0},
            },
            "transition": {"next": "landing_rollout", "transition_sec": 18, "sharpness": 0.9},
        },
        {
            "phase_id": 5,
            "phase_name": "landing_rollout",
            "duration_sec": {"mean": 160, "jitter": 30},
            "noise_scale": 1.1,
            "corr_scale": 0.65,
            "modifiers": {
                "fc_elac_active": {"binary_on_prob": 0.8},
                "fc_sec_active": {"binary_on_prob": 0.85},
                "ecs_pack_l_flow_kg_s": {"add": -0.25},
                "ecs_pack_r_flow_kg_s": {"add": -0.25},
                "nav_adc1_ias_kt": {"add": -140.0},
                "nav_adc2_ias_kt": {"add": -140.0},
                "eng1_n1_pct": {"add": -18.0},
                "eng2_n1_pct": {"add": -18.0},
            },
            "transition": {"next": "post_flight", "transition_sec": 10, "sharpness": 0.7},
        },
    ]


def build_tail_profiles(systems: list[str], m_tails: int, rng: np.random.Generator) -> list[dict]:
    tails: list[dict] = []
    for index in range(max(int(m_tails), 1)):
        tail_id = f"T{index + 1:03d}"
        tails.append(
            {
                "tail_id": tail_id,
                "global_bias": float(rng.normal(0.0, 0.25)),
                "global_noise_scale": float(np.clip(rng.normal(1.0, 0.08), 0.75, 1.35)),
                "system_bias": {system_id: float(rng.normal(0.0, 0.18)) for system_id in systems},
                "system_corr_scale": {system_id: float(np.clip(rng.normal(1.0, 0.1), 0.7, 1.4)) for system_id in systems},
                "aging_drift_per_hour": float(rng.normal(0.02, 0.01)),
            }
        )
    return tails


def build_fleet_manifest(
    tail_profiles: list[dict],
    n_flights_per_tail: int,
    rng: np.random.Generator,
    start_ts: datetime | None = None,
) -> pd.DataFrame:
    base_ts = start_ts or datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows: list[dict] = []
    for tail in tail_profiles:
        for flight_index in range(1, max(int(n_flights_per_tail), 1) + 1):
            rows.append(
                {
                    "tail_id": tail["tail_id"],
                    "flight_id": f"F{flight_index:03d}",
                    "flight_seed": int(rng.integers(1_000_000, 9_999_999)),
                    "departure_ts": base_ts + timedelta(hours=2 * (flight_index - 1)),
                }
            )
    return pd.DataFrame(rows)
