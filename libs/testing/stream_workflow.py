"""Shared helpers for synthetic stream record conversion and mixed detector execution."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

from libs.events.categorical import CategoricalDetectorConfig, CategoricalSample, detect_categorical_events_stream
from libs.events.cooccur import CooccurrenceDetectorConfig, detect_cooccurrence_events_stream
from libs.events.extrema import ContinuousDetectorConfig, ContinuousSample, detect_continuous_events_stream
from libs.profiling.synthetic import SyntheticTelemetryRecord


def split_records_to_samples(
    records: Iterable[SyntheticTelemetryRecord],
    sensor_type_by_name: dict[str, str],
) -> tuple[list[dict[str, object]], Counter[str], list[ContinuousSample], list[CategoricalSample]]:
    truth_events: list[dict[str, object]] = []
    truth_counts: Counter[str] = Counter()
    continuous_samples: list[ContinuousSample] = []
    categorical_samples: list[CategoricalSample] = []

    for record in records:
        for event_name in record.truth_events:
            truth_counts[event_name] += 1
            truth_events.append(
                {
                    "tail_id": record.tail_id,
                    "flight_id": record.flight_id,
                    "sensor": record.parameter_name,
                    "ts": record.timestamp,
                    "event_type": event_name,
                }
            )

        sensor_type = str(sensor_type_by_name.get(record.parameter_name, "numeric"))
        if sensor_type == "numeric":
            numeric_value = float(record.parameter_value) if record.parameter_value is not None else None
            continuous_samples.append(
                ContinuousSample(
                    tail_id=record.tail_id,
                    flight_id=record.flight_id,
                    sensor=record.parameter_name,
                    ts=record.timestamp,
                    value=numeric_value,
                )
            )
        elif sensor_type in {"categorical", "binary"}:
            categorical_samples.append(
                CategoricalSample(
                    tail_id=record.tail_id,
                    flight_id=record.flight_id,
                    sensor=record.parameter_name,
                    ts=record.timestamp,
                    state=record.parameter_value,
                )
            )

    return truth_events, truth_counts, continuous_samples, categorical_samples


def detect_mixed_stream_events(
    continuous_samples: Iterable[ContinuousSample],
    categorical_samples: Iterable[CategoricalSample],
    continuous_config: ContinuousDetectorConfig,
    categorical_config: CategoricalDetectorConfig,
    cooccur_config: CooccurrenceDetectorConfig | None = None,
) -> list[dict[str, Any]]:
    detected_events: list[dict[str, Any]] = []
    continuous_list = list(continuous_samples)
    categorical_list = list(categorical_samples)

    if continuous_list:
        detected_events.extend(detect_continuous_events_stream(continuous_list, continuous_config))
    if categorical_list:
        detected_events.extend(detect_categorical_events_stream(categorical_list, categorical_config))

    detected_events = sorted(detected_events, key=lambda item: item["ts"])
    if cooccur_config is not None and detected_events:
        detected_events.extend(detect_cooccurrence_events_stream(detected_events, cooccur_config))

    return detected_events
