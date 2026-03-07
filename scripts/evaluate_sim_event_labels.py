"""Evaluate detector performance against simulator event labels per sensor type and event type."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import pandas as pd

from libs.common import SensorDataType, normalize_sensor_datatype
from libs.events.categorical import CategoricalSample, detect_categorical_events_stream
from libs.events.extrema import ContinuousDetectorConfig, ContinuousSample, detect_continuous_events_stream
from libs.testing.stream_eval import evaluate_event_detection


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate simulator event labels")
    parser.add_argument("--dataset-root", default="data/evaluation/demo_experiment_v1/dataset")
    parser.add_argument("--telemetry-table", default="telemetry_long")
    parser.add_argument(
        "--output-json",
        default="reports/eda/event_detection_by_sensor_type_event_type.json",
        help="Path to write JSON metrics report",
    )
    parser.add_argument("--tolerance-seconds", type=float, default=0.5)
    parser.add_argument("--oscillation-tolerance-seconds", type=float, default=2.0)
    parser.add_argument("--max-flights", type=int, default=0, help="Optional cap for quick runs (0 means no cap)")
    parser.add_argument(
        "--emit-extrema-events",
        action="store_true",
        help="Enable extrema event emission for continuous sensors",
    )
    return parser.parse_args()


def _load_telemetry_df(dataset_root: Path, telemetry_table: str) -> pd.DataFrame:
    table_path = dataset_root / telemetry_table
    if table_path.is_file():
        return pd.read_parquet(table_path)
    parquet_parts = sorted(table_path.glob("*.parquet"))
    if parquet_parts:
        return pd.read_parquet(table_path)
    raise FileNotFoundError(f"Telemetry table not found at {table_path}")


def _build_label_events(telemetry_df: pd.DataFrame) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for row in telemetry_df.itertuples(index=False):
        # Use per-sensor simulated event label `event_type_label` when present
        sim_event = getattr(row, "event_type_label", None)
        if not sim_event:
            continue
        ts = pd.to_datetime(getattr(row, "timestamp_utc"), utc=True).to_pydatetime()
        parameter_datatype = normalize_sensor_datatype(getattr(row, "parameter_datatype", SensorDataType.UNKNOWN.value))
        events.append(
            {
                "tail_id": str(getattr(row, "tail_id")),
                "flight_id": str(getattr(row, "flight_id")),
                "sensor": str(getattr(row, "sensor")),
                "ts": ts,
                "event_type_label": str(sim_event),
                "parameter_datatype": parameter_datatype,
            }
        )
    return events


def _build_detected_events(
    telemetry_df: pd.DataFrame,
    *,
    emit_extrema_events: bool,
) -> list[dict[str, object]]:
    ordered = telemetry_df.sort_values(["tail_id", "flight_id", "sensor", "timestamp_utc"]).reset_index(drop=True)
    cfg = ContinuousDetectorConfig(emit_extrema_events=emit_extrema_events)
    events: list[dict[str, object]] = []

    for (tail_id, flight_id, sensor), group_df in ordered.groupby(["tail_id", "flight_id", "sensor"], sort=False):
        group_df = group_df.sort_values("timestamp_utc")
        dtype_series = group_df.get("parameter_datatype")
        dtype = (
            normalize_sensor_datatype(dtype_series.dropna().astype(str).iloc[0])
            if dtype_series is not None and dtype_series.notna().any()
            else SensorDataType.UNKNOWN.value
        )

        if dtype == SensorDataType.NUMERIC.value:
            samples: list[ContinuousSample] = []
            for row in group_df.itertuples(index=False):
                value = getattr(row, "parameter_value_clean", None)
                if pd.isna(value):
                    raw = getattr(row, "parameter_value", None)
                    value = None if pd.isna(raw) else float(raw)
                ts = pd.to_datetime(getattr(row, "timestamp_utc"), utc=True).to_pydatetime()
                samples.append(ContinuousSample(str(tail_id), str(flight_id), str(sensor), ts, None if value is None else float(value)))
            for event in detect_continuous_events_stream(samples, config=cfg):
                events.append({**event, "parameter_datatype": dtype})
        elif dtype in {SensorDataType.BINARY.value, SensorDataType.CATEGORICAL.value, SensorDataType.HIGH_CARDINALITY.value}:
            samples: list[CategoricalSample] = []
            for row in group_df.itertuples(index=False):
                state = getattr(row, "parameter_value", None)
                if pd.isna(state):
                    state = getattr(row, "parameter_value", None)
                ts = pd.to_datetime(getattr(row, "timestamp_utc"), utc=True).to_pydatetime()
                samples.append(CategoricalSample(str(tail_id), str(flight_id), str(sensor), ts, None if pd.isna(state) else str(state)))
            for event in detect_categorical_events_stream(samples):
                events.append({**event, "parameter_datatype": dtype})

    return events


def main() -> None:
    args = parse_args()
    dataset_root = Path(args.dataset_root)
    telemetry_df = _load_telemetry_df(dataset_root, args.telemetry_table)
    telemetry_df["timestamp_utc"] = pd.to_datetime(telemetry_df["timestamp_utc"], utc=True)

    if args.max_flights > 0:
        flight_keys = (
            telemetry_df[["tail_id", "flight_id"]]
            .drop_duplicates(subset=["tail_id", "flight_id"])
            .head(args.max_flights)
        )
        telemetry_df = telemetry_df.merge(flight_keys, on=["tail_id", "flight_id"], how="inner")

    label_events = _build_label_events(telemetry_df)
    detected_events = _build_detected_events(telemetry_df, emit_extrema_events=bool(args.emit_extrema_events))

    metrics_overall = evaluate_event_detection(
        label_events=label_events,
        detected_events=detected_events,
        tolerance_seconds=float(args.tolerance_seconds),
        tolerance_by_type_seconds={"oscillation": float(args.oscillation_tolerance_seconds)},
    )

    key_space = sorted(
        {
            (
                str(event.get("parameter_datatype", SensorDataType.UNKNOWN.value)),
                str(event.get("event_type_label") or event.get("event_type_detected") or ""),
            )
            for event in label_events + detected_events
            if str(event.get("event_type_label") or event.get("event_type_detected") or "")
        }
    )

    by_parameter_datatype_event_type: list[dict[str, object]] = []
    for parameter_datatype, event_type in key_space:
        label_slice = [
            event
            for event in label_events
            if str(event.get("parameter_datatype")) == parameter_datatype and str(event.get("event_type_label")) == event_type
        ]
        detected_slice = [
            event
            for event in detected_events
            if str(event.get("parameter_datatype")) == parameter_datatype and str(event.get("event_type_detected")) == event_type
        ]
        metrics = evaluate_event_detection(
            label_events=label_slice,
            detected_events=detected_slice,
            tolerance_seconds=float(args.tolerance_seconds),
            tolerance_by_type_seconds={"oscillation": float(args.oscillation_tolerance_seconds)},
        )
        totals = metrics["totals"]
        by_parameter_datatype_event_type.append(
            {
                "parameter_datatype": parameter_datatype,
                "event_type_detected": event_type,
                "label": int(totals["label"]),
                "detected": int(totals["detected"]),
                "tp": int(totals["tp"]),
                "fp": int(totals["fp"]),
                "fn": int(totals["fn"]),
                "precision": float(totals["precision"]),
                "recall": float(totals["recall"]),
            }
        )

    label_counts = Counter(str(event.get("event_type_label", "")) for event in label_events)
    detected_counts = Counter(str(event.get("event_type_detected", "")) for event in detected_events)

    report = {
        "dataset_root": str(dataset_root),
        "rows_evaluated": int(len(telemetry_df)),
        "flights_evaluated": int(
            telemetry_df[["tail_id", "flight_id"]].drop_duplicates(subset=["tail_id", "flight_id"]).shape[0]
        ),
        "label_columns": {
            "event": [
                "event_type_label",
            ],
            "anomaly": ["anomaly_type_label", "anomaly_score_label"],
        },
        "overall": metrics_overall,
        "counts": {
            "event_type_label": dict(sorted(label_counts.items())),
            "detected_event_type": dict(sorted(detected_counts.items())),
        },
        "by_parameter_datatype_event_type": by_parameter_datatype_event_type,
    }

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    totals = report["overall"]["totals"]
    print("Simulator event-label evaluation")
    print(f"- rows: {report['rows_evaluated']}")
    print(f"- flights: {report['flights_evaluated']}")
    print(
        f"- label: {totals['label']} detected: {totals['detected']} "
        f"tp: {totals['tp']} fp: {totals['fp']} fn: {totals['fn']}"
    )
    print(f"- precision: {totals['precision']:.4f} recall: {totals['recall']:.4f}")
    print(f"- report: {output_path}")


if __name__ == "__main__":
    main()
