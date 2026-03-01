# File: libs/events/extrema.py
"""Continuous-channel event detectors over Spark DataFrames."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import math
from typing import Any, Iterable, Iterator

from libs.perf.annotations import hot_path


@dataclass(frozen=True)
class ContinuousSample:
    tail_id: str
    flight_id: str
    sensor: str
    ts: "datetime"
    value: float | None


@dataclass(frozen=True)
class ContinuousDetectorConfig:
    ema_alpha: float = 0.2
    residual_z_threshold: float = 3.0
    slope_abs_threshold: float = 0.0
    switch_z_threshold: float = 4.0
    switch_delta_z_threshold: float = 3.0
    switch_min_abs_delta: float = 15.0
    switch_delta_scale: float = 6.0
    switch_residual_z_min: float = 0.75
    switch_refractory_samples: int = 20
    min_sigma: float = 1e-3
    oscillation_window: int = 8
    oscillation_amplitude_window: int = 200
    oscillation_ema_alpha: float = 0.12
    oscillation_sign_changes: int = 4
    oscillation_min_amplitude: float = 10.0
    oscillation_min_extrema: int = 4
    oscillation_period_cv_max: float = 0.9
    oscillation_min_period_samples: int = 2
    oscillation_min_alternation_ratio: float = 0.6
    oscillation_period_ema_alpha: float = 0.2
    oscillation_period_band_ratio: float = 0.8
    oscillation_refractory_samples: int = 80
    drift_guard_abs_change: float = 0.0
    drift_guard_max_gap_samples: int = 0
    emit_extrema_events: bool = False
    warmup_points: int = 5


@hot_path
def detect_extrema(values: list[float]) -> list[str]:
    # HOT PATH: extrema detection executes at high sample rates; keep this O(n) and allocation-light.
    if len(values) < 3:
        return []
    events: list[str] = []
    for idx in range(1, len(values) - 1):
        prev_val = values[idx - 1]
        cur_val = values[idx]
        next_val = values[idx + 1]
        if cur_val > prev_val and cur_val >= next_val:
            events.append(f"peak:{idx}")
        elif cur_val < prev_val and cur_val <= next_val:
            events.append(f"trough:{idx}")
    return events


def detect_continuous_events_stream(
    samples: Iterable[ContinuousSample],
    config: ContinuousDetectorConfig | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield continuous-channel events from streaming samples without DataFrame materialization."""
    active = config if config else ContinuousDetectorConfig()
    sensor_state: dict[tuple[str, str, str], dict[str, Any]] = {}

    for sample in samples:
        key = (sample.tail_id, sample.flight_id, sample.sensor)
        state = sensor_state.get(key)
        if state is None:
            state = {
                "ema": None,
                "var": 0.0,
                "count": 0,
                "prev_value": None,
                "osc_ema": None,
                "signs": deque(maxlen=max(active.oscillation_window, 2)),
                "values": deque(maxlen=max(active.oscillation_amplitude_window, 2)),
                "recent_samples": deque(maxlen=3),
                "extrema": deque(maxlen=max(active.oscillation_window * 3, 6)),
                "period_ema": None,
                "delta_abs_ema": 0.0,
                "oscillation_active": False,
                "sample_index": 0,
                "last_switch_index": -10_000_000,
                "last_oscillation_index": -10_000_000,
                "last_drift_guard_index": 0,
                "drift_guard_cum_abs": 0.0,
            }
            sensor_state[key] = state

        if sample.value is None:
            continue

        value = float(sample.value)
        state["sample_index"] += 1
        sample_index = int(state["sample_index"])
        ema_prev = state["ema"]
        prev_value = state["prev_value"]
        state["count"] += 1
        values: deque[float] = state["values"]
        osc_ema_prev = state["osc_ema"]
        if osc_ema_prev is None:
            osc_value = value
        else:
            osc_value = (active.oscillation_ema_alpha * value) + ((1.0 - active.oscillation_ema_alpha) * float(osc_ema_prev))
        state["osc_ema"] = osc_value
        values.append(osc_value)
        recent_samples: deque[tuple[int, float, "datetime"]] = state["recent_samples"]
        recent_samples.append((sample_index, osc_value, sample.ts))

        if ema_prev is None:
            state["ema"] = value
            state["prev_value"] = value
            continue

        residual = value - float(ema_prev)
        ema_new = (active.ema_alpha * value) + ((1.0 - active.ema_alpha) * float(ema_prev))
        var_new = (active.ema_alpha * (residual * residual)) + ((1.0 - active.ema_alpha) * float(state["var"]))
        sigma = max(math.sqrt(max(var_new, 0.0)), float(active.min_sigma))

        delta = 0.0 if prev_value is None else value - float(prev_value)
        state["drift_guard_cum_abs"] = float(state["drift_guard_cum_abs"]) + abs(delta)
        osc_delta = 0.0 if osc_ema_prev is None else (osc_value - float(osc_ema_prev))
        delta_sign = 1 if osc_delta > 0 else (-1 if osc_delta < 0 else 0)
        signs: deque[int] = state["signs"]
        if delta_sign != 0:
            signs.append(delta_sign)

        sign_changes = 0
        if len(signs) >= 2:
            prev_sign = signs[0]
            for current_sign in list(signs)[1:]:
                if current_sign != prev_sign:
                    sign_changes += 1
                prev_sign = current_sign

        local_amplitude = 0.0
        if len(values) >= 2:
            local_amplitude = max(values) - min(values)

        extrema: deque[tuple[str, int, float, "datetime"]] = state["extrema"]
        new_extrema: tuple[str, int, float, "datetime"] | None = None
        if len(recent_samples) == 3:
            (idx_a, val_a, ts_a), (idx_b, val_b, ts_b), (idx_c, val_c, ts_c) = list(recent_samples)
            if val_b > val_a and val_b >= val_c:
                new_extrema = ("peak", idx_b, val_b, ts_b)
            elif val_b < val_a and val_b <= val_c:
                new_extrema = ("trough", idx_b, val_b, ts_b)

        if new_extrema is not None:
            if not extrema or extrema[-1][1] != new_extrema[1]:
                extrema.append(new_extrema)

        extrema_count = len(extrema)
        extrema_intervals: list[int] = []
        alternation_matches = 0
        alternation_total = 0
        if extrema_count >= 2:
            extrema_list = list(extrema)
            for i in range(1, extrema_count):
                extrema_intervals.append(max(extrema_list[i][1] - extrema_list[i - 1][1], 1))
                alternation_total += 1
                if extrema_list[i][0] != extrema_list[i - 1][0]:
                    alternation_matches += 1

        interval_mean = 0.0
        interval_cv = 1e9
        if extrema_intervals:
            interval_mean = sum(extrema_intervals) / len(extrema_intervals)
            variance = sum((item - interval_mean) ** 2 for item in extrema_intervals) / len(extrema_intervals)
            interval_std = math.sqrt(max(variance, 0.0))
            if interval_mean > 0:
                interval_cv = interval_std / interval_mean

        period_ema_prev = state["period_ema"]
        latest_interval = float(extrema_intervals[-1]) if extrema_intervals else None
        if latest_interval is not None:
            if period_ema_prev is None:
                period_ema = latest_interval
            else:
                period_ema = (
                    (active.oscillation_period_ema_alpha * latest_interval)
                    + ((1.0 - active.oscillation_period_ema_alpha) * float(period_ema_prev))
                )
            state["period_ema"] = period_ema
        else:
            period_ema = float(period_ema_prev) if period_ema_prev is not None else None

        alternation_ratio = 0.0
        if alternation_total > 0:
            alternation_ratio = alternation_matches / alternation_total

        period_band_ok = True
        if period_ema is not None and latest_interval is not None and period_ema > 0:
            band = abs(float(active.oscillation_period_band_ratio))
            lower = period_ema * max(0.0, (1.0 - band))
            upper = period_ema * (1.0 + band)
            period_band_ok = lower <= latest_interval <= upper

        oscillating_now = (
            local_amplitude >= float(active.oscillation_min_amplitude)
            and extrema_count >= int(active.oscillation_min_extrema)
            and interval_mean >= float(active.oscillation_min_period_samples)
            and alternation_ratio >= float(active.oscillation_min_alternation_ratio)
            and (
                interval_cv <= float(active.oscillation_period_cv_max)
                or period_band_ok
            )
        )

        switch_refractory_ready = (
            sample_index - int(state["last_switch_index"]) >= int(active.switch_refractory_samples)
        )

        delta_abs_ema = float(state["delta_abs_ema"])
        switch_delta_scale_threshold = max(
            float(active.switch_min_abs_delta),
            float(active.switch_delta_scale) * max(delta_abs_ema, float(active.min_sigma)),
        )

        switch_delta_threshold = max(
            float(active.switch_min_abs_delta),
            float(active.switch_delta_z_threshold) * sigma,
        )
        residual_ready = abs(residual) >= float(active.switch_residual_z_min) * sigma

        switch_step_detected = (
            switch_refractory_ready
            and abs(delta) >= switch_delta_scale_threshold
            and residual_ready
        )

        switch_sigma_detected = (
            switch_refractory_ready
            and abs(delta) >= switch_delta_threshold
        )

        switch_detected = (
            switch_step_detected
            or switch_sigma_detected
        )

        if state["count"] >= int(active.warmup_points):
            if abs(residual) >= float(active.residual_z_threshold) * sigma:
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "threshold",
                    "payload": {
                        "value": value,
                        "ema": float(ema_prev),
                        "residual": residual,
                        "sigma": sigma,
                    },
                }

            if delta > float(active.slope_abs_threshold):
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "slope_pos",
                    "payload": {"delta": delta, "value": value},
                }
            elif delta < -float(active.slope_abs_threshold):
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "slope_neg",
                    "payload": {"delta": delta, "value": value},
                }

            if switch_detected or (switch_refractory_ready and abs(residual) >= float(active.switch_z_threshold) * sigma):
                state["last_switch_index"] = sample_index
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "switch",
                    "payload": {
                        "value": value,
                        "ema": float(ema_prev),
                        "residual": residual,
                        "delta": delta,
                        "sigma": sigma,
                    },
                }

            if active.emit_extrema_events and new_extrema is not None:
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": new_extrema[3],
                    "event_type": "extrema",
                    "payload": {
                        "kind": new_extrema[0],
                        "legacy_type": "max" if new_extrema[0] == "peak" else "min",
                        "value": new_extrema[2],
                        "index": new_extrema[1],
                    },
                }

            oscillation_refractory_ready = (
                sample_index - int(state["last_oscillation_index"]) >= int(active.oscillation_refractory_samples)
            )
            if oscillating_now and new_extrema is not None and oscillation_refractory_ready:
                state["last_oscillation_index"] = sample_index
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": new_extrema[3],
                    "event_type": "oscillation",
                    "payload": {
                        "sign_changes": sign_changes,
                        "window": int(active.oscillation_window),
                        "amplitude": local_amplitude,
                        "extrema_count": extrema_count,
                        "extrema_kind": new_extrema[0],
                        "period_mean_samples": interval_mean,
                        "period_cv": interval_cv,
                        "period_ema": period_ema,
                        "period_band_ok": period_band_ok,
                        "alternation_ratio": alternation_ratio,
                    },
                }

            drift_guard_abs_change = float(active.drift_guard_abs_change)
            drift_guard_max_gap = int(active.drift_guard_max_gap_samples)
            samples_since_guard = sample_index - int(state["last_drift_guard_index"])
            drift_by_change = drift_guard_abs_change > 0 and float(state["drift_guard_cum_abs"]) >= drift_guard_abs_change
            drift_by_gap = drift_guard_max_gap > 0 and samples_since_guard >= drift_guard_max_gap
            if drift_by_change or drift_by_gap:
                reason = "abs_change" if drift_by_change else "max_gap"
                yield {
                    "tail_id": sample.tail_id,
                    "flight_id": sample.flight_id,
                    "sensor": sample.sensor,
                    "ts": sample.ts,
                    "event_type": "drift_guard",
                    "payload": {
                        "reason": reason,
                        "cum_abs_change": float(state["drift_guard_cum_abs"]),
                        "samples_since_guard": samples_since_guard,
                    },
                }
                state["last_drift_guard_index"] = sample_index
                state["drift_guard_cum_abs"] = 0.0

        state["oscillation_active"] = oscillating_now
        state["delta_abs_ema"] = (
            (active.ema_alpha * abs(delta))
            + ((1.0 - active.ema_alpha) * delta_abs_ema)
        )
        state["ema"] = ema_new
        state["var"] = var_new
        state["prev_value"] = value


@hot_path
def build_continuous_events(raw_df: "DataFrame", delta_threshold: float = 0.0) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    order_window = Window.partitionBy("tail_id", "flight_id", "sensor").orderBy("timestamp_utc")

    enriched = (
        raw_df.where(F.col("val").isNotNull())
        .withColumn("prev_val", F.lag("val").over(order_window))
        .withColumn("delta", F.col("val") - F.col("prev_val"))
    )

    event_type = (
        F.when(F.col("prev_val").isNull(), F.lit(None).cast("string"))
        .when(F.abs(F.col("delta")) >= F.lit(delta_threshold), F.lit("threshold"))
        .when(F.col("delta") > 0, F.lit("slope_pos"))
        .when(F.col("delta") < 0, F.lit("slope_neg"))
    )

    return (
        enriched.withColumn("event_type", event_type)
        .where(F.col("event_type").isNotNull())
        .select(
            "tail_id",
            "flight_id",
            F.lit(None).cast("long").alias("win_id"),
            F.col("timestamp_utc").alias("ts"),
            "sensor",
            F.lit("unknown").alias("subsystem"),
            "event_type",
            F.create_map(
                F.lit("delta"),
                F.col("delta").cast("string"),
                F.lit("value"),
                F.col("val").cast("string"),
            ).alias("payload"),
            "date_utc",
        )
    )

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from datetime import datetime
