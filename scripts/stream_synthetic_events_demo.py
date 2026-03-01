# File: scripts/stream_synthetic_events_demo.py
"""Run generator-based synthetic streaming + mixed event detection without Spark."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from libs.events.categorical import CategoricalDetectorConfig
from libs.events.extrema import ContinuousDetectorConfig
from libs.profiling.stream_profile import specs_from_profile_payload, specs_from_profile_tables
from libs.profiling.synthetic import ParameterSpec, iter_synthetic_telemetry_records
from libs.testing.stream_workflow import detect_mixed_stream_events, split_records_to_samples
from libs.windows.stream import StreamWindowConfig, build_adaptive_windows_stream, build_window_cooccurrence_events


def _default_demo_profile_payload() -> dict[str, Any]:
    return {
        "categorical_detector": {
            "min_dwell_seconds": 1.5,
            "max_dwell_seconds": 30.0,
            "illegal_transitions": [["OFF", "STBY"]],
        },
        "sensors": [
            {
                "parameter_name": "HYD_PRESS_1",
                "detected_type": "numeric",
                "sampling_rate_hz": 20.0,
                "mean": 3000.0,
                "std": 3.0,
                "min_value": 2880.0,
                "max_value": 3120.0,
                "noise_std": 3.0,
                "drift_per_sec": 0.0,
                "oscillation_amplitude": 8.0,
                "oscillation_hz": 0.08,
                "switch_interval_s": 45.0,
                "switch_magnitude": 25.0,
                "missing_rate": 0.002,
                "missing_burst_every_s": 120.0,
                "missing_burst_len_s": 3.0,
            },
            {
                "parameter_name": "HYD_PRESS_2",
                "detected_type": "numeric",
                "sampling_rate_hz": 20.0,
                "mean": 3030.0,
                "std": 3.0,
                "min_value": 2910.0,
                "max_value": 3150.0,
                "noise_std": 3.0,
                "drift_per_sec": 0.0,
                "oscillation_amplitude": 8.0,
                "oscillation_hz": 0.08,
                "switch_interval_s": 45.0,
                "switch_magnitude": 25.0,
                "missing_rate": 0.002,
                "missing_burst_every_s": 120.0,
                "missing_burst_len_s": 3.0,
            },
            {
                "parameter_name": "PUMP_STATE",
                "detected_type": "categorical",
                "sampling_rate_hz": 2.0,
                "categories": ["OFF", "ON", "STBY"],
                "missing_rate": 0.001,
                "switch_interval_s": 30.0,
                "missing_burst_every_s": 150.0,
                "missing_burst_len_s": 2.0,
            },
            {
                "parameter_name": "DOOR_STATE",
                "detected_type": "binary",
                "sampling_rate_hz": 1.0,
                "categories": ["CLOSED", "OPEN"],
                "missing_rate": 0.001,
            },
        ],
    }


def _write_demo_profile(path_str: str) -> Path:
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_default_demo_profile_payload(), indent=2), encoding="utf-8")
    return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generator-based synthetic streaming event demo")
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--rate-hz", type=float, default=20.0)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-ts", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--tail-id", default="SYN_T001")
    parser.add_argument("--flight-id", default="SYN_F001")
    parser.add_argument("--profile-json", default=None, help="Optional JSON profile for mixed sensor demo")
    parser.add_argument("--profile-parameter-profile-path", default=None, help="Path to profiled parameter_profile table")
    parser.add_argument(
        "--profile-categorical-distribution-path",
        default=None,
        help="Optional path to profiled categorical_distribution table",
    )
    parser.add_argument("--profile-tail-id", default=None, help="Optional tail_id selector for fleet-scoped profile tables")
    parser.add_argument("--profile-flight-id", default=None, help="Optional flight_id selector for fleet-scoped profile tables")
    parser.add_argument("--profile-format", default="parquet", choices=["parquet", "delta"])
    parser.add_argument("--max-profile-params", type=int, default=100)
    parser.add_argument("--write-demo-profile", default=None, help="Optional path to write a demo profile JSON")

    parser.add_argument("--mean", type=float, default=3000.0)
    parser.add_argument("--peer-sensor", action="store_true", help="Include a second numeric sensor stream")
    parser.add_argument("--peer-mean-offset", type=float, default=30.0)
    parser.add_argument("--noise-std", type=float, default=3.0)
    parser.add_argument("--drift-per-sec", type=float, default=0.0)
    parser.add_argument("--osc-amp", type=float, default=8.0)
    parser.add_argument("--osc-hz", type=float, default=0.08)
    parser.add_argument("--switch-interval", type=float, default=45.0)
    parser.add_argument("--switch-mag", type=float, default=25.0)
    parser.add_argument("--missing-rate", type=float, default=0.002)
    parser.add_argument("--missing-burst-every", type=float, default=120.0)
    parser.add_argument("--missing-burst-len", type=float, default=3.0)

    parser.add_argument("--ema-alpha", type=float, default=0.2)
    parser.add_argument("--residual-z", type=float, default=3.0)
    parser.add_argument("--switch-z", type=float, default=4.0)
    parser.add_argument("--switch-delta-z", type=float, default=3.0)
    parser.add_argument("--switch-min-abs-delta", type=float, default=15.0)
    parser.add_argument("--switch-refractory", type=int, default=20)
    parser.add_argument("--slope-threshold", type=float, default=0.0)
    parser.add_argument("--osc-window", type=int, default=8)
    parser.add_argument("--osc-amp-window", type=int, default=200)
    parser.add_argument("--osc-ema-alpha", type=float, default=0.12)
    parser.add_argument("--osc-sign-changes", type=int, default=4)
    parser.add_argument("--osc-min-amp", type=float, default=10.0)
    parser.add_argument("--osc-min-extrema", type=int, default=4)
    parser.add_argument("--osc-period-cv-max", type=float, default=0.9)
    parser.add_argument("--osc-min-period-samples", type=float, default=2.0)
    parser.add_argument("--osc-min-alternation-ratio", type=float, default=0.6)
    parser.add_argument("--osc-refractory", type=int, default=80)
    parser.add_argument("--drift-guard-abs-change", type=float, default=0.0)
    parser.add_argument("--drift-guard-max-gap", type=int, default=0)
    parser.add_argument("--emit-extrema-events", action="store_true")
    parser.add_argument("--emit-cooccur-events", action="store_true")
    parser.add_argument("--cooccur-min-sensors", type=int, default=2)
    parser.add_argument("--window-max-ms", type=int, default=200)
    parser.add_argument("--window-min-ms", type=int, default=50)
    parser.add_argument("--window-event-threshold", type=int, default=20)
    parser.add_argument("--window-inactivity-timeout-ms", type=int, default=0)
    parser.add_argument("--windows-jsonl", default=None, help="Optional output path for emitted windows JSONL")
    parser.add_argument("--warmup", type=int, default=8)
    parser.add_argument("--cat-min-dwell-seconds", type=float, default=1.5)
    parser.add_argument("--cat-max-dwell-seconds", type=float, default=30.0)

    parser.add_argument("--events-jsonl", default=None, help="Optional output path for detected events JSONL")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.write_demo_profile:
        written_path = _write_demo_profile(args.write_demo_profile)
        print(f"Demo profile written to: {written_path}")
        if args.profile_json is None:
            args.profile_json = str(written_path)

    categorical_config = CategoricalDetectorConfig(
        min_dwell_seconds=max(args.cat_min_dwell_seconds, 0.0),
        max_dwell_seconds=max(args.cat_max_dwell_seconds, 0.0),
    )

    if args.profile_parameter_profile_path:
        specs, categorical_config = specs_from_profile_tables(
            parameter_profile_path=args.profile_parameter_profile_path,
            categorical_distribution_path=args.profile_categorical_distribution_path,
            profile_format=args.profile_format,
            max_profile_params=args.max_profile_params,
            profile_tail_id=args.profile_tail_id,
            profile_flight_id=args.profile_flight_id,
        )
    elif args.profile_json:
        profile_payload = json.loads(Path(args.profile_json).read_text(encoding="utf-8"))
        specs, categorical_config = specs_from_profile_payload(profile_payload)
    else:
        spec = ParameterSpec(
            parameter_name="HYD_PRESS_1",
            detected_type="numeric",
            sampling_rate_hz=args.rate_hz,
            mean=args.mean,
            std=max(args.noise_std, 1e-6),
            min_value=args.mean - 120.0,
            max_value=args.mean + 120.0,
            noise_std=args.noise_std,
            drift_per_sec=args.drift_per_sec,
            oscillation_amplitude=args.osc_amp,
            oscillation_hz=args.osc_hz,
            switch_interval_s=args.switch_interval,
            switch_magnitude=args.switch_mag,
            missing_rate=args.missing_rate,
            missing_burst_every_s=args.missing_burst_every,
            missing_burst_len_s=args.missing_burst_len,
        )

        specs = [spec]
        if args.peer_sensor:
            specs.append(
                ParameterSpec(
                    parameter_name="HYD_PRESS_2",
                    detected_type="numeric",
                    sampling_rate_hz=args.rate_hz,
                    mean=args.mean + args.peer_mean_offset,
                    std=max(args.noise_std, 1e-6),
                    min_value=(args.mean + args.peer_mean_offset) - 120.0,
                    max_value=(args.mean + args.peer_mean_offset) + 120.0,
                    noise_std=args.noise_std,
                    drift_per_sec=args.drift_per_sec,
                    oscillation_amplitude=args.osc_amp,
                    oscillation_hz=args.osc_hz,
                    switch_interval_s=args.switch_interval,
                    switch_magnitude=args.switch_mag,
                    missing_rate=args.missing_rate,
                    missing_burst_every_s=args.missing_burst_every,
                    missing_burst_len_s=args.missing_burst_len,
                )
            )

    type_by_sensor = {spec.parameter_name: spec.detected_type for spec in specs}

    records = iter_synthetic_telemetry_records(
        duration_seconds=args.duration_seconds,
        tail_id=args.tail_id,
        flight_id=args.flight_id,
        start_ts=args.start_ts,
        specs=specs,
        seed=args.seed,
    )
    _, truth_counts, continuous_samples, categorical_samples = split_records_to_samples(records, type_by_sensor)

    detector_config = ContinuousDetectorConfig(
        ema_alpha=args.ema_alpha,
        residual_z_threshold=args.residual_z,
        slope_abs_threshold=args.slope_threshold,
        switch_z_threshold=args.switch_z,
        switch_delta_z_threshold=args.switch_delta_z,
        switch_min_abs_delta=args.switch_min_abs_delta,
        switch_refractory_samples=args.switch_refractory,
        oscillation_window=args.osc_window,
        oscillation_amplitude_window=args.osc_amp_window,
        oscillation_ema_alpha=args.osc_ema_alpha,
        oscillation_sign_changes=args.osc_sign_changes,
        oscillation_min_amplitude=args.osc_min_amp,
        oscillation_min_extrema=args.osc_min_extrema,
        oscillation_period_cv_max=args.osc_period_cv_max,
        oscillation_min_period_samples=args.osc_min_period_samples,
        oscillation_min_alternation_ratio=args.osc_min_alternation_ratio,
        oscillation_refractory_samples=args.osc_refractory,
        drift_guard_abs_change=max(args.drift_guard_abs_change, 0.0),
        drift_guard_max_gap_samples=max(args.drift_guard_max_gap, 0),
        emit_extrema_events=args.emit_extrema_events,
        warmup_points=args.warmup,
    )

    detected_counts: Counter[str] = Counter()
    window_counts: Counter[str] = Counter()
    window_close_reason_counts: Counter[str] = Counter()
    writer = None
    if args.events_jsonl:
        path = Path(args.events_jsonl)
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = path.open("w", encoding="utf-8")

    windows_writer = None
    if args.windows_jsonl:
        path = Path(args.windows_jsonl)
        path.parent.mkdir(parents=True, exist_ok=True)
        windows_writer = path.open("w", encoding="utf-8")

    detected_events = detect_mixed_stream_events(
        continuous_samples=continuous_samples,
        categorical_samples=categorical_samples,
        continuous_config=detector_config,
        categorical_config=categorical_config,
        cooccur_config=None,
    )

    windows = list(
        build_adaptive_windows_stream(
            detected_events,
            StreamWindowConfig(
                max_ms=max(args.window_max_ms, 1),
                min_ms=max(args.window_min_ms, 0),
                event_threshold=max(args.window_event_threshold, 1),
                inactivity_timeout_ms=max(args.window_inactivity_timeout_ms, 0),
                include_window_events=False,
            ),
        )
    )

    if args.emit_cooccur_events:
        cooccur_events = list(
            build_window_cooccurrence_events(
                windows,
                min_distinct_sensors=max(args.cooccur_min_sensors, 2),
            )
        )
        detected_events.extend(cooccur_events)

    try:
        for event in detected_events:
            event_type = str(event["event_type"])
            detected_counts[event_type] += 1
            if writer is not None:
                writer.write(json.dumps(event, default=str) + "\n")

        for window in windows:
            event_type_counts = window.get("event_type_counts")
            if isinstance(event_type_counts, dict):
                for key, value in event_type_counts.items():
                    window_counts[str(key)] += int(value)
            close_reason = window.get("close_reason")
            if close_reason is not None:
                window_close_reason_counts[str(close_reason)] += 1
            if windows_writer is not None:
                windows_writer.write(json.dumps(window, default=str) + "\n")
    finally:
        if writer is not None:
            writer.close()
        if windows_writer is not None:
            windows_writer.close()

    print("Synthetic stream summary")
    print(f"- duration_seconds: {args.duration_seconds}")
    print(f"- sensors: {len(specs)}")
    print(f"- continuous_sensors: {sum(1 for s in specs if s.detected_type == 'numeric')}")
    print(f"- categorical_sensors: {sum(1 for s in specs if s.detected_type in {'categorical', 'binary'})}")
    print(f"- windows: {len(windows)}")
    if windows:
        avg_event_count = sum(int(item.get("event_count", 0)) for item in windows) / max(len(windows), 1)
        print(f"- avg_window_event_count: {avg_event_count:.2f}")

    print("\nGround truth event counts")
    if truth_counts:
        for key in sorted(truth_counts):
            print(f"- {key}: {truth_counts[key]}")
    else:
        print("- none")

    print("\nDetected event counts")
    if detected_counts:
        for key in sorted(detected_counts):
            print(f"- {key}: {detected_counts[key]}")
    else:
        print("- none")

    print("\nWindow event composition")
    if window_counts:
        for key in sorted(window_counts):
            print(f"- {key}: {window_counts[key]}")
    else:
        print("- none")

    print("\nWindow close reasons")
    if window_close_reason_counts:
        for key in sorted(window_close_reason_counts):
            print(f"- {key}: {window_close_reason_counts[key]}")
    else:
        print("- none")

    if args.events_jsonl:
        print(f"\nDetected events written to: {args.events_jsonl}")
    if args.windows_jsonl:
        print(f"Windows written to: {args.windows_jsonl}")


if __name__ == "__main__":
    main()
