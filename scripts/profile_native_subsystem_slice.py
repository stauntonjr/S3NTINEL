"""Profile a native subsystem slice through the active fitting-stage artifact builders."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from libs.behavior import BehaviorStepInput
from libs.io.delta import get_spark
from libs.profiling import (
    build_continuous_scaling_profile_table,
    build_parameter_behavior_profile_table,
    build_parameter_datatype_profile_table,
)
from libs.simulation import simulate_native_raw_telemetry_from_subsystem_slice


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile a native subsystem slice")
    parser.add_argument("--slice-name", default="power_chain", help="Named native subsystem slice")
    parser.add_argument("--tail-id", default="T_NATIVE", help="Synthetic tail identifier")
    parser.add_argument("--flight-id", default="F_NATIVE", help="Synthetic flight identifier")
    parser.add_argument("--n-steps", type=int, default=8, help="Number of native simulation ticks")
    parser.add_argument("--dt-seconds", type=float, default=1.0, help="Tick duration in seconds")
    return parser.parse_args()


def _build_step_inputs(slice_name: str, step_index: int, dt_seconds: float) -> dict[str, dict[str, BehaviorStepInput]]:
    if slice_name == "pressurization":
        return {
            "MOD_PRESS_MODE": {
                "press_mode_state": BehaviorStepInput(
                    dt_seconds=dt_seconds,
                    latent_state={},
                    context={"target_state": "AUTO" if step_index >= 1 else "GROUND"},
                )
            },
            "MOD_AIRCRAFT_ALT": {
                "aircraft_altitude_ft": BehaviorStepInput(
                    dt_seconds=dt_seconds,
                    latent_state={},
                    context={"target_value": 1000.0 * float(step_index)},
                )
            },
        }
    return {
        "MOD_SWITCH": {
            "contactor_state": BehaviorStepInput(
                dt_seconds=dt_seconds,
                latent_state={},
                context={"target_state": 1 if step_index >= 2 else 0},
            )
        }
    }


def _build_initial_state(slice_name: str) -> dict[str, dict[str, object]]:
    if slice_name == "pressurization":
        return {
            "MOD_PRESS_MODE": {"press_mode_state": "GROUND"},
            "MOD_AIRCRAFT_ALT": {"aircraft_altitude_ft": 0.0},
            "MOD_PRESS_CTRL": {"outflow_valve_pct": 0.0},
            "MOD_CABIN": {"cabin_altitude_ft": 0.0, "cabin_delta_p_psi": 0.0},
        }
    return {
        "MOD_SWITCH": {"contactor_state": 0},
        "MOD_SOURCE": {"supply_voltage": 0.0, "fuel_flow_rate": 0.0},
        "MOD_TARGET": {"motor_speed": 0.0},
        "MOD_TANK": {"fuel_used_total": 0.0},
    }


def main() -> None:
    args = parse_args()
    raw_df, phase_df = simulate_native_raw_telemetry_from_subsystem_slice(
        slice_name=str(args.slice_name),
        tail_id=str(args.tail_id),
        flight_id=str(args.flight_id),
        n_steps=int(args.n_steps),
        dt_seconds=float(args.dt_seconds),
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=lambda step_index, resolved_dt_seconds: _build_step_inputs(
            str(args.slice_name), step_index, resolved_dt_seconds
        ),
        build_initial_state_by_module=lambda: _build_initial_state(str(args.slice_name)),
        phase_label_for_step=lambda _step_index: "native_slice",
    )

    spark = get_spark("s3ntinel.profile_native_subsystem_slice")
    raw_sdf = spark.createDataFrame(raw_df)
    datatype_sdf = build_parameter_datatype_profile_table(raw_sdf)
    scaling_sdf = build_continuous_scaling_profile_table(raw_sdf, datatype_sdf)
    behavior_sdf = build_parameter_behavior_profile_table(raw_sdf, datatype_sdf)

    print(f"slice_name={args.slice_name}")
    print(f"raw_rows={raw_sdf.count()} phase_rows={len(phase_df)}")
    print(f"datatype_profile_rows={datatype_sdf.count()}")
    print(f"continuous_scaling_profile_rows={scaling_sdf.count()}")
    print(f"parameter_behavior_profile_rows={behavior_sdf.count()}")
    print("datatype_profile_preview")
    datatype_sdf.orderBy("parameter_name").show(truncate=False)
    print("behavior_profile_preview")
    behavior_sdf.orderBy("parameter_name").show(truncate=False)


if __name__ == "__main__":
    main()
