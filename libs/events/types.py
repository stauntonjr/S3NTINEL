"""Typed event builders for canonical detector output rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.common.event_types import EventType


def _functions():
    from pyspark.sql import functions as F

    return F


def _as_column(value: "Column | object") -> "Column":
    from pyspark.sql import Column

    F = _functions()
    return value if isinstance(value, Column) else F.lit(value)


DETECTED_EVENT_STRUCT_TYPE = (
    "struct<"
    "tail_id:string,"
    "flight_id:string,"
    "win_id:bigint,"
    "timestamp_utc:timestamp,"
    "parameter_name:string,"
    "event_type_detected:string,"
    "payload:map<string,string>,"
    "date_utc:date"
    ">"
)

DETECTED_EVENT_ARRAY_TYPE = f"array<{DETECTED_EVENT_STRUCT_TYPE}>"


def empty_detected_event_array() -> "Column":
    F = _functions()

    return F.array().cast(DETECTED_EVENT_ARRAY_TYPE)


def null_detected_event() -> "Column":
    F = _functions()

    return F.lit(None).cast(DETECTED_EVENT_STRUCT_TYPE)


def append_detected_events(existing_events: "Column", *candidate_events: "Column") -> "Column":
    F = _functions()

    return F.concat(
        F.coalesce(existing_events, empty_detected_event_array()),
        F.filter(F.array(*candidate_events), lambda event: event.isNotNull()),
    )


@dataclass(frozen=True)
class Event:
    event_type_detected: str

    def type_column(self) -> "Column":
        F = _functions()

        return F.lit(self.event_type_detected)

    def payload_map(self, **fields: "Column | object") -> "Column":
        F = _functions()
        entries: list["Column"] = []
        for key, value in fields.items():
            entries.extend([F.lit(str(key)), _as_column(value).cast("string")])
        return F.create_map(*entries)

    def struct(
        self,
        *,
        tail_id: "Column",
        flight_id: "Column",
        timestamp_utc: "Column",
        parameter_name: "Column",
        payload: "Column",
        date_utc: "Column",
        win_id: "Column | None" = None,
    ) -> "Column":
        F = _functions()

        return F.struct(
            tail_id.cast("string").alias("tail_id"),
            flight_id.cast("string").alias("flight_id"),
            (win_id if win_id is not None else F.lit(None)).cast("long").alias("win_id"),
            timestamp_utc.cast("timestamp").alias("timestamp_utc"),
            parameter_name.cast("string").alias("parameter_name"),
            self.type_column().alias("event_type_detected"),
            payload.cast("map<string,string>").alias("payload"),
            date_utc.cast("date").alias("date_utc"),
        )

    def struct_from_step(
        self,
        step: "Column",
        *,
        payload: "Column",
        win_id: "Column | None" = None,
    ) -> "Column":
        return self.struct(
            tail_id=step["tail_id"],
            flight_id=step["flight_id"],
            timestamp_utc=step["timestamp_utc"],
            parameter_name=step["parameter_name"],
            payload=payload,
            date_utc=step["date_utc"],
            win_id=win_id,
        )

    def optional_struct(
        self,
        *,
        condition: "Column",
        step: "Column",
        payload: "Column",
        win_id: "Column | None" = None,
    ) -> "Column":
        F = _functions()

        return F.when(
            condition,
            self.struct_from_step(step, payload=payload, win_id=win_id),
        ).otherwise(null_detected_event())


@dataclass(frozen=True)
class ContinuousEvent(Event):
    pass


@dataclass(frozen=True)
class CategoricalEvent(Event):
    pass


@dataclass(frozen=True)
class ThresholdEvent(ContinuousEvent):
    event_type_detected: str = EventType.THRESHOLD

    def struct_from_observation(
        self,
        *,
        tail_id: "Column",
        flight_id: "Column",
        timestamp_utc: "Column",
        parameter_name: "Column",
        value: "Column",
        ema_prev: "Column",
        residual: "Column",
        delta: "Column",
        delta_raw: "Column",
        sigma: "Column",
        date_utc: "Column",
        win_id: "Column | None" = None,
    ) -> "Column":
        return self.struct(
            tail_id=tail_id,
            flight_id=flight_id,
            timestamp_utc=timestamp_utc,
            parameter_name=parameter_name,
            payload=self.payload_map(
                value=value,
                ema=ema_prev,
                residual=residual,
                delta=delta,
                delta_raw=delta_raw,
                sigma=sigma,
            ),
            date_utc=date_utc,
            win_id=win_id,
        )


@dataclass(frozen=True)
class SlopePositiveEvent(ContinuousEvent):
    event_type_detected: str = EventType.SLOPE_POS

    def struct_from_observation(
        self,
        *,
        tail_id: "Column",
        flight_id: "Column",
        timestamp_utc: "Column",
        parameter_name: "Column",
        delta: "Column",
        delta_raw: "Column",
        value: "Column",
        slope_source: "Column | object",
        date_utc: "Column",
        win_id: "Column | None" = None,
    ) -> "Column":
        return self.struct(
            tail_id=tail_id,
            flight_id=flight_id,
            timestamp_utc=timestamp_utc,
            parameter_name=parameter_name,
            payload=self.payload_map(
                delta=delta,
                delta_raw=delta_raw,
                value=value,
                slope_source=slope_source,
            ),
            date_utc=date_utc,
            win_id=win_id,
        )


@dataclass(frozen=True)
class SlopeNegativeEvent(ContinuousEvent):
    event_type_detected: str = EventType.SLOPE_NEG

    def struct_from_observation(
        self,
        *,
        tail_id: "Column",
        flight_id: "Column",
        timestamp_utc: "Column",
        parameter_name: "Column",
        delta: "Column",
        delta_raw: "Column",
        value: "Column",
        slope_source: "Column | object",
        date_utc: "Column",
        win_id: "Column | None" = None,
    ) -> "Column":
        return self.struct(
            tail_id=tail_id,
            flight_id=flight_id,
            timestamp_utc=timestamp_utc,
            parameter_name=parameter_name,
            payload=self.payload_map(
                delta=delta,
                delta_raw=delta_raw,
                value=value,
                slope_source=slope_source,
            ),
            date_utc=date_utc,
            win_id=win_id,
        )


@dataclass(frozen=True)
class SwitchEvent(ContinuousEvent):
    event_type_detected: str = EventType.SWITCH

    def optional_from_step(self, *, condition: "Column", step: "Column") -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                value=step["val"],
                ema=step["ema_prev"],
                residual=step["residual"],
                delta=step["delta_raw"],
                sigma=step["sigma"],
            ),
        )


@dataclass(frozen=True)
class ExtremaEvent(ContinuousEvent):
    event_type_detected: str = EventType.EXTREMA

    def optional_from_step(self, *, condition: "Column", step: "Column") -> "Column":
        F = _functions()

        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                kind=step["extrema_kind"],
                legacy_type=F.when(step["extrema_kind"] == F.lit("peak"), F.lit("max")).otherwise(F.lit("min")),
                value=step["osc_value"],
                index=step["sample_index"],
            ),
        )


@dataclass(frozen=True)
class OscillationEvent(ContinuousEvent):
    event_type_detected: str = EventType.OSCILLATION

    def optional_from_step(self, *, condition: "Column", step: "Column", oscillation_window: "Column | object") -> "Column":
        F = _functions()

        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                sign_changes=step["sign_changes"],
                window=oscillation_window,
                amplitude=step["local_amplitude"],
                extrema_count=step["extrema_count"],
                extrema_kind=step["extrema_kind"],
                period_mean_samples=step["period_mean_samples"],
                period_cv=step["period_cv"],
                period_ema=step["period_ema"],
                period_band_ok=F.when(step["period_band_ok"], F.lit("true")).otherwise(F.lit("false")),
                alternation_ratio=step["alternation_ratio"],
            ),
        )


@dataclass(frozen=True)
class DriftGuardEvent(ContinuousEvent):
    event_type_detected: str = EventType.DRIFT_GUARD

    def optional_from_step(
        self,
        *,
        condition: "Column",
        step: "Column",
        reason: "Column | object",
        cum_abs_change: "Column | object",
        samples_since_guard: "Column | object",
    ) -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                reason=reason,
                cum_abs_change=cum_abs_change,
                samples_since_guard=samples_since_guard,
            ),
        )


@dataclass(frozen=True)
class StateEnterEvent(CategoricalEvent):
    event_type_detected: str = EventType.STATE_ENTER

    def optional_from_step(self, *, condition: "Column", step: "Column", from_state: "Column | object", to_state: "Column | object") -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(**{"from": from_state, "to": to_state}),
        )


@dataclass(frozen=True)
class StateExitEvent(CategoricalEvent):
    event_type_detected: str = EventType.STATE_EXIT

    def optional_from_step(
        self,
        *,
        condition: "Column",
        step: "Column",
        from_state: "Column | object",
        to_state: "Column | object",
        dwell_seconds: "Column | object | None" = None,
    ) -> "Column":
        payload_fields: dict[str, "Column | object"] = {"from": from_state, "to": to_state}
        if dwell_seconds is not None:
            payload_fields["dwell_seconds"] = dwell_seconds
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(**payload_fields),
        )


@dataclass(frozen=True)
class DroppedEvent(CategoricalEvent):
    event_type_detected: str = EventType.DROPPED

    def optional_from_step(self, *, condition: "Column", step: "Column", from_state: "Column | object", to_state: "Column | object") -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(**{"from": from_state, "to": to_state}),
        )


@dataclass(frozen=True)
class DwellBucketEvent(CategoricalEvent):
    event_type_detected: str = EventType.DWELL_BUCKET

    def optional_from_step(
        self,
        *,
        condition: "Column",
        step: "Column",
        state: "Column | object",
        dwell_seconds: "Column | object",
        bucket: "Column | object",
    ) -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                state=state,
                dwell_seconds=dwell_seconds,
                bucket=bucket,
            ),
        )


@dataclass(frozen=True)
class TransitionEvent(CategoricalEvent):
    event_type_detected: str = EventType.TRANSITION

    def optional_from_step(
        self,
        *,
        condition: "Column",
        step: "Column",
        from_state: "Column | object",
        to_state: "Column | object",
        dwell_seconds: "Column | object",
    ) -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                **{"from": from_state, "to": to_state},
                dwell_seconds=dwell_seconds,
            ),
        )


@dataclass(frozen=True)
class DwellViolationEvent(CategoricalEvent):
    event_type_detected: str = EventType.DWELL_VIOLATION

    def optional_from_step(
        self,
        *,
        condition: "Column",
        step: "Column",
        from_state: "Column | object",
        to_state: "Column | object",
        dwell_seconds: "Column | object",
        min_dwell_seconds: "Column | object",
    ) -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                **{"from": from_state, "to": to_state},
                dwell_seconds=dwell_seconds,
                min_dwell_seconds=min_dwell_seconds,
            ),
        )


@dataclass(frozen=True)
class IllegalTransitionEvent(CategoricalEvent):
    event_type_detected: str = EventType.ILLEGAL_TRANSITION

    def optional_from_step(self, *, condition: "Column", step: "Column", from_state: "Column | object", to_state: "Column | object") -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(**{"from": from_state, "to": to_state}),
        )


@dataclass(frozen=True)
class CategoricalDwellGuardEvent(CategoricalEvent):
    event_type_detected: str = EventType.DWELL_GUARD

    def optional_from_step(
        self,
        *,
        condition: "Column",
        step: "Column",
        state: "Column | object",
        dwell_seconds: "Column | object",
        max_dwell_seconds: "Column | object",
    ) -> "Column":
        return self.optional_struct(
            condition=condition,
            step=step,
            payload=self.payload_map(
                state=state,
                dwell_seconds=dwell_seconds,
                max_dwell_seconds=max_dwell_seconds,
            ),
        )


if TYPE_CHECKING:
    from pyspark.sql import Column
