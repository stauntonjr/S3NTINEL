# File: scripts/evaluate_stream_event_detection.py
"""Evaluate streaming event detection against generator-produced truth (no Spark required)."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path

from libs.events.categorical import (
    CategoricalDetectorConfig,
)
from libs.events.cooccur import CooccurrenceDetectorConfig
from libs.events.extrema import (
    ContinuousDetectorConfig,
)
from libs.profiling.stream_profile import specs_from_profile_payload, specs_from_profile_tables
from libs.profiling.synthetic import ParameterSpec, iter_synthetic_telemetry_records
from libs.testing.stream_eval import evaluate_event_detection
from libs.testing.stream_workflow import detect_mixed_stream_events, split_records_to_samples


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate generator-based stream detectors")
    parser.add_argument("--duration-seconds", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-ts", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--profile-json", default=None, help="Optional JSON profile for mixed sensor evaluation")
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
    parser.add_argument("--tolerance-seconds", type=float, default=0.5)
    parser.add_argument("--oscillation-tolerance-seconds", type=float, default=2.0)
    parser.add_argument("--cooccur-window-seconds", type=float, default=0.5)
    parser.add_argument("--cooccur-min-sensors", type=int, default=2)
    parser.add_argument("--cooccur-refractory-seconds", type=float, default=0.5)
    parser.add_argument(
        "--event-types",
        default="transition,dropped,oscillation,switch",
        help="Comma-separated event types to include in scoring",
    )
    parser.add_argument("--metrics-json", default=None, help="Optional output path for metrics JSON")
    return parser.parse_args()


def build_specs() -> list[ParameterSpec]:
    return [
        ParameterSpec(
            parameter_name="HYD_PRESS_1",
            detected_type="numeric",
            sampling_rate_hz=20.0,
            mean=3000.0,
            std=3.5,
            min_value=2920.0,
            max_value=3080.0,
            noise_std=3.0,
            oscillation_amplitude=8.0,
            oscillation_hz=0.08,
            switch_interval_s=45.0,
            switch_magnitude=25.0,
            missing_rate=0.002,
            missing_burst_every_s=120.0,
            missing_burst_len_s=3.0,
        ),
        ParameterSpec(
            parameter_name="PUMP_STATE",
            detected_type="categorical",
            sampling_rate_hz=2.0,
            categories=("OFF", "ON", "STBY"),
            switch_interval_s=30.0,
            missing_rate=0.001,
            missing_burst_every_s=150.0,
            missing_burst_len_s=2.0,
        ),
    ]


def main() -> None:
    args = parse_args()
    included_event_types = {
        item.strip()
        for item in str(args.event_types).split(",")
        if item.strip()
    }
    categorical_config = CategoricalDetectorConfig(
        min_dwell_seconds=1.5,
        illegal_transitions=frozenset({("OFF", "STBY")}),
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
        specs = build_specs()

    records = iter_synthetic_telemetry_records(
        duration_seconds=args.duration_seconds,
        tail_id="SYN_T001",
        flight_id="SYN_F001",
        start_ts=args.start_ts,
        specs=specs,
        seed=args.seed,
    )
    type_by_sensor = {spec.parameter_name: spec.detected_type for spec in specs}
    truth_events, _, continuous_samples, categorical_samples = split_records_to_samples(records, type_by_sensor)

    detected_events_all = detect_mixed_stream_events(
        continuous_samples=continuous_samples,
        categorical_samples=categorical_samples,
        continuous_config=ContinuousDetectorConfig(
            ema_alpha=0.2,
            residual_z_threshold=3.0,
            switch_z_threshold=4.0,
            switch_delta_z_threshold=3.0,
            switch_min_abs_delta=15.0,
            switch_refractory_samples=20,
            slope_abs_threshold=0.0,
            oscillation_window=8,
            oscillation_amplitude_window=200,
            oscillation_ema_alpha=0.12,
            oscillation_sign_changes=4,
            oscillation_min_amplitude=10.0,
            oscillation_min_extrema=4,
            oscillation_period_cv_max=0.9,
            oscillation_min_period_samples=2,
            oscillation_min_alternation_ratio=0.6,
            oscillation_period_ema_alpha=0.2,
            oscillation_period_band_ratio=0.8,
            oscillation_refractory_samples=80,
            emit_extrema_events=True,
            warmup_points=8,
        ),
        categorical_config=categorical_config,
        cooccur_config=CooccurrenceDetectorConfig(
            window_seconds=max(args.cooccur_window_seconds, 0.0),
            min_distinct_sensors=max(args.cooccur_min_sensors, 2),
            emit_refractory_seconds=max(args.cooccur_refractory_seconds, 0.0),
        ),
    )
    detected_counts_all: Counter[str] = Counter(str(event.get("event_type")) for event in detected_events_all)

    detected_events = detected_events_all
    truth_events = [event for event in truth_events if str(event.get("event_type")) in included_event_types]
    detected_events = [event for event in detected_events if str(event.get("event_type")) in included_event_types]

    metrics = evaluate_event_detection(
        truth_events=truth_events,
        detected_events=detected_events,
        tolerance_seconds=args.tolerance_seconds,
        tolerance_by_type_seconds={"oscillation": args.oscillation_tolerance_seconds},
    )

    print("Stream event detection metrics")
    print(f"- event_types: {sorted(included_event_types)}")
    totals = metrics["totals"]
    print(f"- truth: {totals['truth']}")
    print(f"- detected: {totals['detected']}")
    print(f"- tp: {totals['tp']}")
    print(f"- fp: {totals['fp']}")
    print(f"- fn: {totals['fn']}")
    print(f"- precision: {totals['precision']:.4f}")
    print(f"- recall: {totals['recall']:.4f}")

    print("\nAll detected event counts (graph population view)")
    for key in sorted(detected_counts_all):
        print(f"- {key}: {detected_counts_all[key]}")

    print("\nPer-type metrics")
    per_type = metrics["per_type"]
    if not per_type:
        print("- none")
    else:
        for key in sorted(per_type):
            item = per_type[key]
            print(
                f"- {key}: tp={item['tp']} fp={item['fp']} fn={item['fn']} "
                f"precision={item['precision']:.4f} recall={item['recall']:.4f}"
            )

    if args.metrics_json:
        out_path = Path(args.metrics_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(metrics, default=str, indent=2), encoding="utf-8")
        print(f"\nMetrics written to: {args.metrics_json}")


if __name__ == "__main__":
    main()
