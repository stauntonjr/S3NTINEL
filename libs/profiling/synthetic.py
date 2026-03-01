# File: libs/profiling/synthetic.py
"""Generate synthetic normal telemetry for testing and baseline workflows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import heapq
import math
import random
from typing import Any, Iterator


@dataclass(frozen=True)
class ParameterSpec:
    parameter_name: str
    detected_type: str
    sampling_rate_hz: float
    mean: float | None = None
    std: float | None = None
    min_value: float | None = None
    max_value: float | None = None
    categories: tuple[str, ...] = ("ON", "OFF")
    missing_rate: float = 0.0
    drift_per_sec: float = 0.0
    noise_std: float | None = None
    oscillation_amplitude: float = 0.0
    oscillation_hz: float = 0.0
    switch_interval_s: float | None = None
    switch_magnitude: float = 0.0
    missing_burst_every_s: float | None = None
    missing_burst_len_s: float = 0.0


@dataclass(frozen=True)
class SyntheticTelemetryRecord:
    tail_id: str
    flight_id: str
    timestamp: datetime
    parameter_name: str
    parameter_value: str | None
    truth_events: tuple[str, ...] = ()


def default_parameter_specs() -> list[ParameterSpec]:
    return [
        ParameterSpec(
            "ENG_TEMP_1",
            "numeric",
            10.0,
            mean=450.0,
            std=1.5,
            min_value=430.0,
            max_value=470.0,
            noise_std=0.8,
            oscillation_amplitude=0.6,
            oscillation_hz=0.15,
        ),
        ParameterSpec(
            "HYD_PRESS_1",
            "numeric",
            20.0,
            mean=3000.0,
            std=8.0,
            min_value=2950.0,
            max_value=3050.0,
            noise_std=3.0,
            switch_interval_s=90.0,
            switch_magnitude=18.0,
        ),
        ParameterSpec("ELEC_VOLT_1", "numeric", 5.0, mean=28.0, std=0.2, min_value=26.0, max_value=30.0),
        ParameterSpec("PUMP_STATE", "categorical", 2.0, categories=("ON", "OFF"), missing_rate=0.01),
        ParameterSpec("DOOR_STATE", "binary", 1.0, categories=("CLOSED", "OPEN"), missing_rate=0.005),
    ]


def _start_epoch(start_ts: str) -> int:
    dt = datetime.fromisoformat(start_ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _start_datetime(start_ts: str) -> datetime:
    dt = datetime.fromisoformat(start_ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _numeric_value(spec: ParameterSpec, elapsed_s: float, rng: random.Random, switch_state: int) -> float:
    mean = spec.mean if spec.mean is not None else 0.0
    std = max(spec.std if spec.std is not None else 1.0, 1e-6)
    noise_std = spec.noise_std if spec.noise_std is not None else std

    value = float(mean) + (float(spec.drift_per_sec) * elapsed_s)
    value += rng.gauss(0.0, float(noise_std))

    if spec.oscillation_amplitude > 0 and spec.oscillation_hz > 0:
        value += float(spec.oscillation_amplitude) * math.sin(2.0 * math.pi * float(spec.oscillation_hz) * elapsed_s)

    if switch_state != 0 and spec.switch_magnitude != 0:
        value += float(spec.switch_magnitude) * switch_state

    if spec.min_value is not None:
        value = max(value, float(spec.min_value))
    if spec.max_value is not None:
        value = min(value, float(spec.max_value))
    return value


def _is_in_missing_burst(spec: ParameterSpec, elapsed_s: float) -> bool:
    if spec.missing_burst_every_s is None or spec.missing_burst_every_s <= 0 or spec.missing_burst_len_s <= 0:
        return False
    phase = elapsed_s % float(spec.missing_burst_every_s)
    return phase < float(spec.missing_burst_len_s)


def iter_parameter_records(
    spec: ParameterSpec,
    duration_seconds: int,
    tail_id: str,
    flight_id: str,
    start_ts: str,
    seed: int = 42,
) -> Iterator[SyntheticTelemetryRecord]:
    start_dt = _start_datetime(start_ts)
    effective_hz = max(spec.sampling_rate_hz, 0.5)
    sample_count = max(int(duration_seconds * effective_hz), 1)
    dt_s = 1.0 / effective_hz
    rng = random.Random(seed)

    switch_state = 0
    last_switch_slot: int | None = None
    category_index = 0
    prev_wave: float | None = None
    prev_wave_delta_sign = 0
    previous_emitted_value: str | None = None

    for i in range(sample_count):
        elapsed_s = i * dt_s
        timestamp = start_dt.fromtimestamp(start_dt.timestamp() + elapsed_s, tz=timezone.utc)
        events: list[str] = []

        if spec.switch_interval_s and spec.switch_interval_s > 0:
            switch_slot = int(elapsed_s // float(spec.switch_interval_s))
            if last_switch_slot is None:
                last_switch_slot = switch_slot
            elif switch_slot != last_switch_slot:
                last_switch_slot = switch_slot
                switch_state = -switch_state if switch_state != 0 else 1
                if spec.detected_type == "numeric":
                    events.append("switch")

        if spec.detected_type == "numeric":
            if spec.oscillation_amplitude > 0 and spec.oscillation_hz > 0:
                wave = math.sin(2.0 * math.pi * float(spec.oscillation_hz) * elapsed_s)
                if prev_wave is not None:
                    wave_delta = wave - prev_wave
                    wave_delta_sign = 1 if wave_delta > 0 else (-1 if wave_delta < 0 else 0)
                    if (
                        prev_wave_delta_sign != 0
                        and wave_delta_sign != 0
                        and wave_delta_sign != prev_wave_delta_sign
                    ):
                        events.append("oscillation")
                    if wave_delta_sign != 0:
                        prev_wave_delta_sign = wave_delta_sign
                prev_wave = wave

            value_num = _numeric_value(spec, elapsed_s, rng, switch_state)
            value: str | None = f"{value_num:.4f}"
        else:
            categories = spec.categories if spec.categories else ("ON", "OFF")
            if spec.switch_interval_s and spec.switch_interval_s > 0 and "switch" in events:
                category_index = (category_index + 1) % len(categories)
            elif spec.detected_type == "categorical":
                category_index = (category_index + 1) % len(categories)
            value = categories[category_index]

        missing = rng.random() < max(min(spec.missing_rate, 0.95), 0.0)
        if _is_in_missing_burst(spec, elapsed_s):
            missing = True
            events.append("missing_burst")
        if missing:
            value = None

        if spec.detected_type in {"categorical", "binary"}:
            if value is None and previous_emitted_value is not None:
                events.append("dropped")
            elif value is not None and previous_emitted_value is not None and value != previous_emitted_value:
                events.append("transition")

        previous_emitted_value = value

        yield SyntheticTelemetryRecord(
            tail_id=tail_id,
            flight_id=flight_id,
            timestamp=timestamp,
            parameter_name=spec.parameter_name,
            parameter_value=value,
            truth_events=tuple(sorted(set(events))),
        )


def iter_synthetic_telemetry_records(
    duration_seconds: int,
    tail_id: str,
    flight_id: str,
    start_ts: str,
    specs: list[ParameterSpec] | None = None,
    seed: int = 42,
) -> Iterator[SyntheticTelemetryRecord]:
    parameter_specs = specs if specs else default_parameter_specs()
    generators = [
        iter_parameter_records(
            spec=spec,
            duration_seconds=duration_seconds,
            tail_id=tail_id,
            flight_id=flight_id,
            start_ts=start_ts,
            seed=seed + idx,
        )
        for idx, spec in enumerate(parameter_specs)
    ]

    heap: list[tuple[datetime, int, SyntheticTelemetryRecord]] = []
    for idx, gen in enumerate(generators):
        try:
            first = next(gen)
            heapq.heappush(heap, (first.timestamp, idx, first))
        except StopIteration:
            continue

    while heap:
        _, idx, record = heapq.heappop(heap)
        yield record
        try:
            nxt = next(generators[idx])
            heapq.heappush(heap, (nxt.timestamp, idx, nxt))
        except StopIteration:
            continue


def iter_synthetic_telemetry_rows(
    duration_seconds: int,
    tail_id: str,
    flight_id: str,
    start_ts: str,
    specs: list[ParameterSpec] | None = None,
    seed: int = 42,
    include_truth: bool = False,
) -> Iterator[dict[str, Any]]:
    for record in iter_synthetic_telemetry_records(
        duration_seconds=duration_seconds,
        tail_id=tail_id,
        flight_id=flight_id,
        start_ts=start_ts,
        specs=specs,
        seed=seed,
    ):
        row: dict[str, Any] = {
            "tail_id": record.tail_id,
            "flight_id": record.flight_id,
            "timestamp": record.timestamp,
            "parameter_name": record.parameter_name,
            "parameter_value": record.parameter_value,
        }
        if include_truth:
            row["truth_events"] = list(record.truth_events)
        yield row


def generate_synthetic_normal_telemetry(
    spark: "SparkSession",
    duration_seconds: int,
    tail_id: str,
    flight_id: str,
    start_ts: str,
    specs: list[ParameterSpec] | None = None,
    seed: int = 42,
) -> "DataFrame":
    from pyspark.sql import types as T

    schema = T.StructType(
        [
            T.StructField("tail_id", T.StringType(), nullable=False),
            T.StructField("flight_id", T.StringType(), nullable=False),
            T.StructField("timestamp", T.TimestampType(), nullable=False),
            T.StructField("parameter_name", T.StringType(), nullable=False),
            T.StructField("parameter_value", T.StringType(), nullable=True),
        ]
    )

    rows = iter_synthetic_telemetry_rows(
        duration_seconds=duration_seconds,
        tail_id=tail_id,
        flight_id=flight_id,
        start_ts=start_ts,
        specs=specs,
        seed=seed,
        include_truth=False,
    )
    return spark.createDataFrame(rows, schema=schema)


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
