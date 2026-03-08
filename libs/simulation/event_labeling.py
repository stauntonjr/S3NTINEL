from __future__ import annotations

import pandas as pd

from libs.common.event_types import EventType
from libs.events.detection import detect_events_from_rows


_EVENT_LABEL_PRIORITY: dict[str, int] = {
    EventType.TRANSITION: 0,
    EventType.STATE_EXIT: 1,
    EventType.STATE_ENTER: 2,
    EventType.DROPPED: 3,
    EventType.ILLEGAL_TRANSITION: 4,
    EventType.DWELL_VIOLATION: 5,
    EventType.DWELL_GUARD: 6,
    EventType.DWELL_BUCKET: 7,
    EventType.SWITCH: 10,
    EventType.EXTREMA: 11,
    EventType.OSCILLATION: 12,
    EventType.THRESHOLD: 13,
    EventType.SLOPE_POS: 14,
    EventType.SLOPE_NEG: 15,
    EventType.DRIFT_GUARD: 16,
    EventType.COOCCUR: 17,
}

# Canonical telemetry keeps one `event_type_label` per row. When a single categorical
# timestamp yields multiple detector events (for example `transition` + `state_exit` +
# `dwell_bucket`), the canonical label is the highest-priority event under
# `_EVENT_LABEL_PRIORITY`. Evaluation and validator paths intentionally rely on this
# single-label policy today.

def _choose_primary_event_type(event_types: list[str]) -> str:
    if not event_types:
        return EventType.NONE
    unique = sorted({str(item) for item in event_types if str(item)})
    if not unique:
        return EventType.NONE
    return min(unique, key=lambda item: (_EVENT_LABEL_PRIORITY.get(item, 999), item))


def _value_col(df: pd.DataFrame) -> str | None:
    for col in ("parameter_value_clean", "parameter_value"):
        if col in df.columns:
            return str(col)
    return None


def _detect_parameter_event_labels_df(telemetry_df: pd.DataFrame, *, value_col: str | None = None) -> pd.DataFrame:
    """Build parameter-level detector event labels from telemetry samples.

    This emits per-(tail, flight, parameter_name, timestamp) event labels aligned to the
    stream detectors used by event evaluation (continuous + categorical). Co-occurrence
    events are intentionally excluded because they are flight-level aggregate events.
    """
    if telemetry_df.empty:
        return pd.DataFrame(columns=["tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_label"])

    out_df = telemetry_df.copy()
    if value_col and str(value_col) in out_df.columns:
        # Simulation labels must be derived from the selected label stream,
        # usually `parameter_value_clean`. The detector consumes `parameter_value`,
        # so mirror the chosen source into both columns for this local labeling pass.
        out_df["parameter_value"] = out_df[str(value_col)]
        out_df["parameter_value_clean"] = out_df[str(value_col)]
    event_rows: list[dict[str, object]] = []
    for event in detect_events_from_rows(out_df.to_dict(orient="records"), include_cooccur=False):
        event_type_label = str(event.get("event_type_detected", EventType.NONE))
        if not event_type_label or event_type_label == EventType.NONE:
            continue
        event_rows.append(
            {
                "tail_id": str(event.get("tail_id", "")),
                "flight_id": str(event.get("flight_id", "")),
                "parameter_name": str(event.get("parameter_name", event.get("sensor", ""))),
                "timestamp_utc": pd.to_datetime(event.get("timestamp_utc", event.get("ts")), utc=True),
                "event_type_label": event_type_label,
            }
        )

    if not event_rows:
        return pd.DataFrame(columns=["tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_label"])

    labels_df = pd.DataFrame(event_rows)
    labels_df = labels_df[labels_df["event_type_label"].astype(str).str.len() > 0].copy()
    if labels_df.empty:
        return pd.DataFrame(columns=["tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_label"])
    labels_df["timestamp_utc"] = pd.to_datetime(labels_df["timestamp_utc"], utc=True)
    return labels_df


def _attach_event_labels_to_telemetry_df(telemetry_df: pd.DataFrame, *, label_value_col: str = "parameter_value_clean") -> pd.DataFrame:
    """Attach detector event labels to telemetry rows with explicit event-label fields.

    By default, labels are derived from ``parameter_value_clean`` (pre-noise numeric signal)
    when available so injected observation noise does not create noisy slope label events.
    """
    out_df = telemetry_df.copy()
    out_df["timestamp_utc"] = pd.to_datetime(out_df["timestamp_utc"], utc=True)
    if "event_type_label" in out_df.columns:
        out_df = out_df.drop(columns=["event_type_label"])

    active_label_value_col = str(label_value_col) if str(label_value_col) in out_df.columns else _value_col(out_df)
    labels_df = _detect_parameter_event_labels_df(out_df, value_col=active_label_value_col)
    if labels_df.empty:
        out_df["event_type_label"] = EventType.NONE
        return out_df

    grouped = (
        labels_df.groupby(["tail_id", "flight_id", "parameter_name", "timestamp_utc"])["event_type_label"]
        .agg(lambda items: sorted(set(str(item) for item in items if str(item))))
        .reset_index(name="_event_type_label_list")
    )
    grouped["event_type_label"] = grouped["_event_type_label_list"].apply(
        lambda items: _choose_primary_event_type(list(items))
    )
    out_df = out_df.merge(
        grouped[["tail_id", "flight_id", "parameter_name", "timestamp_utc", "event_type_label"]],
        on=["tail_id", "flight_id", "parameter_name", "timestamp_utc"],
        how="left",
    )
    out_df["event_type_label"] = out_df["event_type_label"].fillna(EventType.NONE)
    return out_df


def _attach_event_labels_to_telemetry_rows(telemetry_rows: list[dict], *, label_value_col: str = "parameter_value_clean") -> list[dict]:
    """Attach event labels directly on row dicts without DataFrame materialization."""
    if not telemetry_rows:
        return []

    out_rows: list[dict] = [dict(row) for row in telemetry_rows]

    active_label_value_col = str(label_value_col) if any(str(label_value_col) in row for row in out_rows) else "parameter_value_clean"
    for row in out_rows:
        label_value = row.get(active_label_value_col)
        row["parameter_value"] = label_value
        row["parameter_value_clean"] = label_value

    labels_by_key: dict[tuple[str, str, str, pd.Timestamp], set[str]] = {}
    for event in detect_events_from_rows(out_rows, include_cooccur=False):
        event_type_label = str(event.get("event_type_detected", EventType.NONE))
        if not event_type_label or event_type_label == EventType.NONE:
            continue
        event_key = (
            str(event.get("tail_id", "")),
            str(event.get("flight_id", "")),
            str(event.get("parameter_name", event.get("sensor", ""))),
            pd.to_datetime(event.get("timestamp_utc", event.get("ts")), utc=True),
        )
        labels_by_key.setdefault(event_key, set()).add(event_type_label)

    for row in out_rows:
        row_key = (
            str(row.get("tail_id", "")),
            str(row.get("flight_id", "")),
            str(row.get("parameter_name", row.get("sensor", ""))),
            pd.to_datetime(row.get("timestamp_utc"), utc=True),
        )
        labels = sorted(labels_by_key.get(row_key, set()))
        row["event_type_label"] = _choose_primary_event_type(labels)

    return out_rows
