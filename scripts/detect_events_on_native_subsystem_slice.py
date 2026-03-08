"""Extract canonical events from a native subsystem slice through the active event path."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from libs.io.delta import get_spark
from libs.simulation import build_native_subsystem_slice_scenario, simulate_native_event_table_from_subsystem_slice


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract events on a native subsystem slice")
    parser.add_argument("--slice-name", default="power_chain", help="Named native subsystem slice")
    parser.add_argument("--tail-id", default="T_NATIVE", help="Synthetic tail identifier")
    parser.add_argument("--flight-id", default="F_NATIVE", help="Synthetic flight identifier")
    parser.add_argument("--n-steps", type=int, default=8, help="Number of native simulation ticks")
    parser.add_argument("--dt-seconds", type=float, default=1.0, help="Tick duration in seconds")
    parser.add_argument("--delta-threshold", type=float, default=0.0, help="Continuous event delta threshold")
    parser.add_argument("--slope-source", default="ema", choices=("ema", "raw"), help="Continuous slope source")
    parser.add_argument("--ema-alpha", type=float, default=0.2, help="EMA alpha when slope-source=ema")
    return parser.parse_args()

def main() -> None:
    args = parse_args()
    scenario = build_native_subsystem_slice_scenario(str(args.slice_name))
    spark = get_spark("s3ntinel.detect_events_on_native_subsystem_slice")
    events_sdf, phase_df = simulate_native_event_table_from_subsystem_slice(
        slice_name=str(args.slice_name),
        spark=spark,
        tail_id=str(args.tail_id),
        flight_id=str(args.flight_id),
        n_steps=int(args.n_steps),
        dt_seconds=float(args.dt_seconds),
        start_timestamp_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
        build_step_inputs_by_module=scenario.build_step_inputs_by_module,
        build_initial_state_by_module=scenario.build_initial_state_by_module,
        phase_label_for_step=scenario.phase_label_for_step,
        delta_threshold=float(args.delta_threshold),
        slope_source=str(args.slope_source),
        ema_alpha=float(args.ema_alpha),
    )

    print(f"slice_name={args.slice_name}")
    print(f"phase_rows={len(phase_df)}")
    print(f"event_rows={events_sdf.count()}")
    print("event_preview")
    events_sdf.orderBy("timestamp_utc", "parameter_name").show(truncate=False)


if __name__ == "__main__":
    main()
