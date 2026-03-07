from __future__ import annotations

import math
from datetime import timedelta
from typing import Iterator

import numpy as np
import pandas as pd

from libs.common import SensorDataType, normalize_sensor_datatype
from libs.simulation.delay_engine import _resolve_delay_map_for_groups
from libs.simulation.phase_engine import blend_modifiers, build_timeline, phase_state_for_t
from libs.simulation.sensor_generation import generate_sensor_observation
from libs.simulation.anomaly_generation import generate_anomalies_for_t
from libs.common.event_types import EventType, TruthAnomalyType


def iter_single_flight_row_events(
    *,
    hierarchy_df: pd.DataFrame,
    sensor_behavior: dict[str, dict],
    flight_setup: dict,
    phase_map: dict[str, dict],
    tail_profile: dict,
    flight_row: pd.Series,
    sensors_in_order: list[str],
    sensors_by_group: dict[str, list[str]],
    reported_unknown_delay_keys: set[str],
    warning_prefix: str,
    timestamp_mode: str,
) -> Iterator[tuple[str, dict]]:
    rng_local = np.random.default_rng(int(flight_row["flight_seed"]))

    timeline = build_timeline(
        phase_sequence=flight_setup["phase_sequence"],
        phase_map=phase_map,
        rng_local=rng_local,
    )
    total_sec = timeline[-1]["t_end"]
    corr_groups = sorted({str(sensor_behavior[sensor]["corr_group"]) for sensor in hierarchy_df["sensor"].tolist()})

    flight_noise_scale = float(np.clip(rng_local.normal(flight_setup["flight_noise_scale_mean"], flight_setup["flight_noise_scale_std"]), 0.75, 1.4))
    latent_ar1_phi = float(np.clip(flight_setup.get("latent_ar1_phi", 0.92), 0.0, 0.999))
    cross_group_latent_mix = float(np.clip(flight_setup.get("cross_group_latent_mix", 0.25), 0.0, 1.0))
    sample_period_sec = float(max(flight_setup.get("sample_period_sec", 1.0), 1e-6))
    delay_cfg = flight_setup.get("causal_delay", {}) or {}
    delay_mode = str(delay_cfg.get("mode", "random_pair")).strip().lower()
    if delay_mode not in {"random_pair", "fixed_group"}:
        delay_mode = "random_pair"

    delay_map_sec_raw = delay_cfg.get("per_corr_group_sec", flight_setup.get("causal_delay_sec_by_corr_group", {})) or {}
    delay_map_sec, unknown_delay_keys = _resolve_delay_map_for_groups(delay_map_sec_raw, corr_groups)
    unknown_delay_keys_set = set(unknown_delay_keys)
    new_unknown = sorted(unknown_delay_keys_set - reported_unknown_delay_keys)
    if new_unknown:
        print(f"{warning_prefix} causal_delay ignored unknown corr_group keys: {new_unknown}")
        reported_unknown_delay_keys.update(new_unknown)

    startup_fill = str(delay_cfg.get("startup_fill", "hold_first"))
    default_lag_sec = max(float(delay_cfg.get("default_lag_sec", 0.0)), 0.0)
    random_delay_cfg = delay_cfg.get("random_pair_delay_sec", {}) or {}
    if isinstance(random_delay_cfg, (int, float)):
        random_delay_min_sec = 0.0
        random_delay_max_sec = max(float(random_delay_cfg), 0.0)
    else:
        random_delay_min_sec = max(float(random_delay_cfg.get("min", 0.0)), 0.0)
        random_delay_max_sec = max(float(random_delay_cfg.get("max", random_delay_min_sec)), 0.0)
    if random_delay_max_sec < random_delay_min_sec:
        random_delay_min_sec, random_delay_max_sec = random_delay_max_sec, random_delay_min_sec

    jitter_sec_std = max(float(delay_cfg.get("jitter_sec_std", 0.0)), 0.0)
    jitter_cap_steps = max(int(delay_cfg.get("jitter_cap_steps", 3)), 0)
    jitter_steps_std = jitter_sec_std / sample_period_sec
    seed_offset = int(delay_cfg.get("seed_offset", 0))

    delay_steps_by_group = {
        str(group): max(int(round(float(delay_map_sec.get(str(group), delay_map_sec.get(group, default_lag_sec))) / sample_period_sec)), 0)
        for group in corr_groups
    }

    delay_steps_by_sensor: dict[str, int] = {}
    if delay_mode == "random_pair":
        pair_rng = np.random.default_rng(int(flight_row["flight_seed"]) + seed_offset + 97)
        for corr_group_name, sensor_names in sensors_by_group.items():
            group_base_sec = float(delay_map_sec.get(corr_group_name, default_lag_sec))
            for sensor_name in sensor_names:
                if random_delay_max_sec > random_delay_min_sec:
                    extra_delay_sec = float(pair_rng.uniform(random_delay_min_sec, random_delay_max_sec))
                else:
                    extra_delay_sec = float(random_delay_min_sec)
                total_delay_sec = max(group_base_sec + extra_delay_sec, 0.0)
                delay_steps_by_sensor[sensor_name] = max(int(round(total_delay_sec / sample_period_sec)), 0)
    else:
        for sensor_name in sensors_in_order:
            corr_group_name = str(sensor_behavior[sensor_name]["corr_group"])
            delay_steps_by_sensor[sensor_name] = int(delay_steps_by_group.get(corr_group_name, 0))

    max_delay_steps = max(delay_steps_by_sensor.values(), default=0) + jitter_cap_steps
    innovation_scale = math.sqrt(max(1.0 - latent_ar1_phi**2, 1e-8))

    latent_state_by_group = {group: float(rng_local.normal(0.0, 1.0)) for group in corr_groups}
    latent_history_by_group = {group: [] for group in corr_groups}
    global_latent_state = float(rng_local.normal(0.0, 1.0))
    binary_state_cache: dict[str, str] = {}
    categorical_state_cache: dict[str, str] = {}

    for t in range(total_sec):
        primary_segment, secondary_segment, blend_alpha = phase_state_for_t(timeline, t)
        phase_name = primary_segment["phase_name"]
        phase_id = primary_segment["phase_id"]
        ts = flight_row["departure_ts"] + timedelta(seconds=int(t))
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        if timestamp_mode == "iso":
            ts_out = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
        else:
            ts_out = ts

        global_latent_state = latent_ar1_phi * global_latent_state + innovation_scale * float(rng_local.normal(0.0, 1.0))
        latent_by_group: dict[str, float] = {}
        for group in corr_groups:
            latent_state_by_group[group] = (
                latent_ar1_phi * latent_state_by_group[group] + innovation_scale * float(rng_local.normal(0.0, 1.0))
            )
            mixed_latent = (1.0 - cross_group_latent_mix) * latent_state_by_group[group] + cross_group_latent_mix * global_latent_state
            history = latent_history_by_group[group]
            history.append(float(mixed_latent))
            if max_delay_steps > 0 and len(history) > (max_delay_steps + 1):
                del history[0]
            latent_by_group[group] = float(mixed_latent)

        phase_corr_scale = (1.0 - blend_alpha) * float(primary_segment["corr_scale"]) + blend_alpha * float(secondary_segment["corr_scale"])
        phase_noise_scale = (1.0 - blend_alpha) * float(primary_segment["noise_scale"]) + blend_alpha * float(secondary_segment["noise_scale"])

        # Prepare any anomalies affecting this timestep; returns mapping sensor->modifier/event
        anomalies_map = generate_anomalies_for_t(
            flight_setup=flight_setup,
            rng_local=rng_local,
            phase_name=phase_name,
            t=int(t),
            sensors_in_order=sensors_in_order,
            sensor_behavior=sensor_behavior,
        )
        # If anomalies were generated for this timestep, emit an anomaly record into the stream.
        if anomalies_map:
            affected = sorted(list(anomalies_map.keys()))
            # pick anomaly type and aggregate score
            anomaly_type = None
            scores = []
            per_sensor = {}
            for s, info in anomalies_map.items():
                per_sensor[s] = {
                    "modifier": info.get("modifier"),
                    "event_type_label": info.get("event_type_label"),
                    "anomaly_score_label": float(info.get("anomaly_score_label", 0.0)),
                }
                if anomaly_type is None:
                    anomaly_type = info.get("anomaly_type_label")
                scores.append(float(info.get("anomaly_score_label", 0.0)))

            anomaly_record = {
                "tail_id": str(tail_profile["tail_id"]),
                "flight_id": str(flight_row["flight_id"]),
                "timestamp_utc": ts_out,
                "sensors": affected,
                "anomaly_type_label": str(anomaly_type) if anomaly_type is not None else None,
                "anomaly_score_label": float(max(scores)) if scores else 0.0,
                "payload": per_sensor,
                "date_utc": ts.date().isoformat(),
            }
            yield ("anomaly", anomaly_record)

        for row in hierarchy_df.itertuples(index=False):
            sensor = str(row.sensor)
            system_id = str(row.system_id)
            subsystem_id = str(row.subsystem_id)
            module_id = str(row.module_id)
            datatype = normalize_sensor_datatype(getattr(row, "parameter_datatype", SensorDataType.UNKNOWN.value))
            spec = sensor_behavior[sensor]
            corr_group_name = str(spec["corr_group"])
            delay_steps = int(delay_steps_by_sensor.get(sensor, 0))
            if jitter_steps_std > 0:
                jitter_steps = int(round(float(rng_local.normal(0.0, jitter_steps_std))))
                jitter_steps = int(np.clip(jitter_steps, -jitter_cap_steps, jitter_cap_steps))
                delay_steps = max(delay_steps + jitter_steps, 0)

            group_history = latent_history_by_group[corr_group_name]
            latest_latent = latent_by_group[corr_group_name]
            if delay_steps <= 0:
                latent = float(latest_latent)
            elif len(group_history) > delay_steps:
                latent = float(group_history[-(delay_steps + 1)])
            elif startup_fill == "hold_current":
                latent = float(latest_latent)
            else:
                latent = float(group_history[0]) if group_history else float(latest_latent)

            modifier = blend_modifiers(
                primary_segment["modifiers"].get(sensor, {}),
                secondary_segment["modifiers"].get(sensor, {}),
                blend_alpha,
            )

            # apply per-sensor anomaly modifiers when present (anomaly generator may have prepared them)
            sensor_anom = anomalies_map.get(sensor, {})
            mod_for_sensor = dict(modifier)
            if isinstance(sensor_anom.get("modifier"), dict):
                mod_for_sensor.update(sensor_anom.get("modifier", {}))

            obs = generate_sensor_observation(
                datatype=datatype,
                sensor=sensor,
                spec=spec,
                modifier=mod_for_sensor,
                latent=float(latent),
                phase_name=phase_name,
                t=int(t),
                system_id=system_id,
                tail_profile=tail_profile,
                phase_corr_scale=float(phase_corr_scale),
                phase_noise_scale=float(phase_noise_scale),
                flight_noise_scale=float(flight_noise_scale),
                rng_local=rng_local,
                binary_state_cache=binary_state_cache,
                categorical_state_cache=categorical_state_cache,
            )

            # Attach per-sensor event label if anomaly generator marked this sensor.
            event_label_type = None
            if sensor in anomalies_map:
                event_label_type = anomalies_map[sensor].get("event_type_label")

            yield (
                "telemetry",
                {
                    "tail_id": str(tail_profile["tail_id"]),
                    "flight_id": str(flight_row["flight_id"]),
                    "timestamp_utc": ts_out,
                    "system_id": system_id,
                    "subsystem_id": subsystem_id,
                    "module_id": module_id,
                    "sensor": sensor,
                    "parameter_name": sensor,
                    "parameter_datatype": datatype,
                    "parameter_value": str(obs.get("parameter_value")),
                    "parameter_value_clean": str(obs.get("parameter_value_clean")) if obs.get("parameter_value_clean") is not None else None,
                    "phase_id_detected": int(phase_id),
                    "phase_name": phase_name,
                    "anomaly_type_label": (
                        str(sensor_anom.get("anomaly_type_label")) if sensor_anom.get("anomaly_type_label") is not None else TruthAnomalyType.NONE
                    ),
                    "anomaly_score_label": float(sensor_anom.get("anomaly_score_label", 0.0)),
                    "event_type_label": str(event_label_type) if event_label_type is not None else None,
                    "date_utc": ts.date().isoformat(),
                },
            )

        phase_ts = flight_row["departure_ts"] + timedelta(seconds=int(t))
        if hasattr(phase_ts, "to_pydatetime"):
            phase_ts = phase_ts.to_pydatetime()
        if timestamp_mode == "iso":
            phase_ts_out = phase_ts.isoformat() if hasattr(phase_ts, "isoformat") else str(phase_ts)
        else:
            phase_ts_out = phase_ts

        yield (
            "phase",
            {
                "tail_id": str(tail_profile["tail_id"]),
                "flight_id": str(flight_row["flight_id"]),
                "timestamp_utc": phase_ts_out,
                "phase_id_detected": int(phase_id),
                "phase_name": phase_name,
            },
        )

def simulate_single_flight_rows(
    *,
    hierarchy_df: pd.DataFrame,
    sensor_behavior: dict[str, dict],
    flight_setup: dict,
    phase_map: dict[str, dict],
    tail_profile: dict,
    flight_row: pd.Series,
    sensors_in_order: list[str],
    sensors_by_group: dict[str, list[str]],
    reported_unknown_delay_keys: set[str],
    warning_prefix: str,
    timestamp_mode: str,
) -> tuple[list[dict], list[dict]]:
    telemetry_rows: list[dict] = []
    phase_rows: list[dict] = []

    for row_type, row in iter_single_flight_row_events(
        hierarchy_df=hierarchy_df,
        sensor_behavior=sensor_behavior,
        flight_setup=flight_setup,
        phase_map=phase_map,
        tail_profile=tail_profile,
        flight_row=flight_row,
        sensors_in_order=sensors_in_order,
        sensors_by_group=sensors_by_group,
        reported_unknown_delay_keys=reported_unknown_delay_keys,
        warning_prefix=warning_prefix,
        timestamp_mode=timestamp_mode,
    ):
        if row_type == "telemetry":
            telemetry_rows.append(row)
        else:
            phase_rows.append(row)

    return telemetry_rows, phase_rows
