from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from libs.common import SensorDataType, normalize_sensor_datatype


def flatten_hierarchy_spec(hierarchy_spec: dict) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    systems = hierarchy_spec.get("systems", {})
    for system_id, system_obj in systems.items():
        for subsystem_id, subsystem_obj in system_obj.get("subsystems", {}).items():
            for module_id, module_sensors in subsystem_obj.get("modules", {}).items():
                for sensor_obj in module_sensors:
                    rows.append(
                        {
                            "system_id": str(system_id),
                            "subsystem_id": str(subsystem_id),
                            "module_id": str(module_id),
                            "sensor": str(sensor_obj.get("sensor", "")),
                            "parameter_datatype": normalize_sensor_datatype(sensor_obj.get("datatype", SensorDataType.UNKNOWN.value)),
                            "unit": str(sensor_obj.get("unit", "")),
                        }
                    )
    hierarchy_df = pd.DataFrame(rows)
    if hierarchy_df.empty:
        return pd.DataFrame(columns=["system_id", "subsystem_id", "module_id", "sensor", "parameter_datatype", "unit"])
    return hierarchy_df.sort_values(["system_id", "subsystem_id", "module_id", "sensor"]).reset_index(drop=True)


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
        sensor = str(row["sensor"])
        dtype = str(row.get("parameter_datatype", "unknown"))

        sys_node = safe_node_id("SYS", system_id)
        sub_node = safe_node_id("SUB", f"{system_id}_{subsystem_id}")
        mod_node = safe_node_id("MOD", f"{system_id}_{subsystem_id}_{module_id}")
        sen_node = safe_node_id("SEN", sensor)

        node_lines.add(f"  {sys_node}[\"{system_id}\"]")
        node_lines.add(f"  {sub_node}[\"{subsystem_id}\"]")
        node_lines.add(f"  {mod_node}[\"{module_id}\"]")
        node_lines.add(f"  {sen_node}[\"{sensor} ({dtype})\"]")

        edge_lines.add(f"  GLOBAL --> {sys_node}")
        edge_lines.add(f"  {sys_node} --> {sub_node}")
        edge_lines.add(f"  {sub_node} --> {mod_node}")
        edge_lines.add(f"  {mod_node} --> {sen_node}")

    lines = ["flowchart TD"] + sorted(node_lines) + sorted(edge_lines)
    return "\n".join(lines)


def build_default_sensor_behavior(hierarchy_df: pd.DataFrame) -> dict[str, dict]:
    behavior_defaults = {
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

    behavior: dict[str, dict] = {}
    for row in hierarchy_df.to_dict("records"):
        sensor = str(row["sensor"])
        dtype = normalize_sensor_datatype(row.get("parameter_datatype", SensorDataType.UNKNOWN.value))
        system_id = str(row["system_id"])
        subsystem_id = str(row["subsystem_id"])
        corr_group = f"{system_id}::{subsystem_id}"

        if dtype == SensorDataType.NUMERIC.value:
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
            }.get(sensor, 1.0)
            clip = {
                "alpha_margin": (0.05, 1.5),
                "ac_bus_a_load": (0.0, 140.0),
                "ac_bus_b_load": (0.0, 140.0),
                "outflow_cmd": (0.0, 100.0),
                "outflow_pos": (0.0, 100.0),
                "cabin_alt": (0.0, 12000.0),
                "pack_l_flow": (0.0, 4.0),
                "pack_r_flow": (0.0, 4.0),
            }.get(sensor, (None, None))
            behavior[sensor] = {
                "datatype": SensorDataType.NUMERIC.value,
                "baseline": float(baseline),
                "corr_group": corr_group,
                "trend_per_sec": behavior_defaults[SensorDataType.NUMERIC.value]["trend_per_sec"],
                "osc_amp": 0.25 if "bus" in sensor else behavior_defaults[SensorDataType.NUMERIC.value]["osc_amp"],
                "osc_period_sec": 90.0 if "pack" in sensor else behavior_defaults[SensorDataType.NUMERIC.value]["osc_period_sec"],
                "noise_sigma": 0.15 if "cabin" in sensor else behavior_defaults[SensorDataType.NUMERIC.value]["noise_sigma"],
                "corr_scale": behavior_defaults[SensorDataType.NUMERIC.value]["corr_scale"],
                "min_val": clip[0],
                "max_val": clip[1],
            }
        elif dtype == SensorDataType.BINARY.value:
            behavior[sensor] = {
                "datatype": SensorDataType.BINARY.value,
                "corr_group": corr_group,
                "base_on_prob": 0.15 if "apu" in sensor else 0.7,
                "latent_gain": behavior_defaults[SensorDataType.BINARY.value]["latent_gain"],
                "persistence": behavior_defaults[SensorDataType.BINARY.value]["persistence"],
                "states": ["0", "1"],
            }
        elif dtype == SensorDataType.CATEGORICAL.value:
            states = {
                "yaw_damper_mode": ["OFF", "STBY", "ON"],
                "bat_contact_state": ["OPEN", "TRANSIENT", "CLOSED"],
                "press_mode": ["AUTO", "ALTN", "MANUAL"],
            }.get(sensor, behavior_defaults[SensorDataType.CATEGORICAL.value]["states"])
            behavior[sensor] = {
                "datatype": SensorDataType.CATEGORICAL.value,
                "corr_group": corr_group,
                "states": states,
                "base_probs": [0.65, 0.25, 0.10][: len(states)] if len(states) == 3 else None,
                "latent_gain": behavior_defaults[SensorDataType.CATEGORICAL.value]["latent_gain"],
                "persistence": behavior_defaults[SensorDataType.CATEGORICAL.value]["persistence"],
            }
        else:
            behavior[sensor] = {
                "datatype": SensorDataType.HIGH_CARDINALITY.value,
                "corr_group": corr_group,
                "base_prob": behavior_defaults[SensorDataType.HIGH_CARDINALITY.value]["base_prob"],
                "codes": behavior_defaults[SensorDataType.HIGH_CARDINALITY.value]["codes"],
            }
    return behavior


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
