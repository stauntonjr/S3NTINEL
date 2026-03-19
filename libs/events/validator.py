"""Streaming and summary validators for detector outputs against simulator labels."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Generator, Iterable

import pandas as pd

from libs.io.contracts import DetectedEventRow, EventLabelRow, EventValidatorSnapshot, TelemetryRow

def _event_key(event: EventLabelRow | DetectedEventRow) -> tuple[str, str, str]:
    return (
        str(event.get("tail_id", "")),
        str(event.get("flight_id", "")),
        str(event.get("parameter_name", event.get("sensor", ""))),
    )


def _label_type(event: EventLabelRow) -> str:
    return str(event.get("event_type_label", ""))


def _detected_type(event: DetectedEventRow) -> str:
    return str(event.get("event_type_detected", ""))


def _event_ts(event: TelemetryRow | EventLabelRow | DetectedEventRow, *, field: str = "timestamp_utc") -> datetime:
    value = event.get(field)
    if isinstance(value, datetime):
        return value
    return pd.to_datetime(value, utc=True).to_pydatetime()


def simulator_label_events(
    simulator_rows: Iterable[TelemetryRow],
    *,
    label_field: str = "event_type_label",
) -> Generator[EventLabelRow, None, None]:
    for row in simulator_rows:
        label_value = row.get(label_field)
        label_text = str(label_value).strip() if label_value is not None else ""
        if not label_text or label_text in {"none", "null"}:
            continue
        yield {
            "tail_id": str(row.get("tail_id", "")),
            "flight_id": str(row.get("flight_id", "")),
            "parameter_name": str(row.get("parameter_name", row.get("sensor", ""))),
            "event_type_label": label_text,
            "timestamp_utc": _event_ts(row, field="timestamp_utc"),
        }


def iter_event_validation_snapshots(
    *,
    simulator_rows: Iterable[TelemetryRow],
    detected_events: Iterable[DetectedEventRow],
    tolerance_seconds: float = 0.5,
) -> Generator[EventValidatorSnapshot, None, None]:
    """Yield cumulative TP/FP/FN/TN counters while matching detected events to simulator labels.

    TN is counted per simulator telemetry row with no label event and no matched detection.
    """
    tolerance = abs(float(tolerance_seconds))
    label_events = [dict(event) for event in simulator_label_events(simulator_rows)]
    detected = [dict(item) for item in detected_events]

    label_by_bucket: dict[tuple[tuple[str, str, str], str], list[tuple[int, datetime]]] = defaultdict(list)
    det_by_bucket: dict[tuple[tuple[str, str, str], str], list[tuple[int, datetime]]] = defaultdict(list)
    for idx, event in enumerate(label_events):
        label_by_bucket[(_event_key(event), _label_type(event))].append((idx, _event_ts(event)))
    for idx, event in enumerate(detected):
        det_by_bucket[(_event_key(event), _detected_type(event))].append((idx, _event_ts(event)))

    matched_label_ids: set[int] = set()
    matched_det_ids: set[int] = set()
    for bucket in set(label_by_bucket.keys()) | set(det_by_bucket.keys()):
        label_list = sorted(label_by_bucket.get(bucket, []), key=lambda item: item[1])
        det_list = sorted(det_by_bucket.get(bucket, []), key=lambda item: item[1])
        used_label_local: set[int] = set()
        for det_id, det_ts in det_list:
            best_label_id = None
            best_delta = None
            for label_id, label_ts in label_list:
                if label_id in used_label_local:
                    continue
                delta = abs((det_ts - label_ts).total_seconds())
                if delta <= tolerance and (best_delta is None or delta < best_delta):
                    best_label_id = label_id
                    best_delta = delta
            if best_label_id is not None:
                used_label_local.add(best_label_id)
                matched_label_ids.add(best_label_id)
                matched_det_ids.add(det_id)

    label_ids_by_row: dict[tuple[str, str, str, datetime], list[int]] = defaultdict(list)
    for idx, event in enumerate(label_events):
        key = (*_event_key(event), _event_ts(event))
        label_ids_by_row[key].append(idx)

    det_unmatched_by_ts: dict[datetime, list[int]] = defaultdict(list)
    for idx, event in enumerate(detected):
        if idx in matched_det_ids:
            continue
        det_unmatched_by_ts[_event_ts(event)].append(idx)

    tp = 0
    fp = 0
    fn = 0
    tn = 0

    rows_sorted = sorted(
        [dict(row) for row in simulator_rows],
        key=lambda row: (
            str(row.get("tail_id", "")),
            str(row.get("flight_id", "")),
            str(row.get("parameter_name", row.get("sensor", ""))),
            _event_ts(row, field="timestamp_utc"),
        ),
    )

    for row in rows_sorted:
        key_triplet = (
            str(row.get("tail_id", "")),
            str(row.get("flight_id", "")),
            str(row.get("parameter_name", row.get("sensor", ""))),
        )
        row_ts = _event_ts(row, field="timestamp_utc")
        row_label_ids = label_ids_by_row.get((*key_triplet, row_ts), [])
        if row_label_ids:
            for label_id in row_label_ids:
                if label_id in matched_label_ids:
                    tp += 1
                else:
                    fn += 1
        else:
            if not det_unmatched_by_ts.get(row_ts):
                tn += 1

        if det_unmatched_by_ts.get(row_ts):
            fp += len(det_unmatched_by_ts[row_ts])
            det_unmatched_by_ts[row_ts] = []

        yield {
            "tail_id": key_triplet[0],
            "flight_id": key_triplet[1],
            "parameter_name": key_triplet[2],
            "timestamp_utc": row_ts,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        }

    # Remaining unmatched detections are false positives with no simulator timestamp.
    for det_id, event in enumerate(detected):
        if det_id in matched_det_ids:
            continue
        event_ts = _event_ts(event)
        if det_unmatched_by_ts.get(event_ts) is not None:
            if det_id not in det_unmatched_by_ts[event_ts]:
                continue
            det_unmatched_by_ts[event_ts].remove(det_id)
            if not det_unmatched_by_ts[event_ts]:
                det_unmatched_by_ts.pop(event_ts, None)
        fp += 1
        yield {
            "tail_id": str(event.get("tail_id", "")),
            "flight_id": str(event.get("flight_id", "")),
            "parameter_name": str(event.get("parameter_name", event.get("sensor", ""))),
            "timestamp_utc": event_ts,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
        }


def build_event_validation_summary(
    *,
    simulator_rows: Iterable[TelemetryRow],
    detected_events: Iterable[DetectedEventRow],
    tolerance_seconds: float = 0.5,
) -> dict[str, Any]:
    simulator_list = [dict(row) for row in simulator_rows]
    detected_list = [dict(row) for row in detected_events]
    snapshots = list(
        iter_event_validation_snapshots(
            simulator_rows=simulator_list,
            detected_events=detected_list,
            tolerance_seconds=tolerance_seconds,
        )
    )
    last = snapshots[-1] if snapshots else {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    label_events = list(simulator_label_events(simulator_list))
    tp = int(last.get("tp", 0))
    fp = int(last.get("fp", 0))
    fn = int(last.get("fn", 0))
    precision = float(tp / (tp + fp)) if (tp + fp) > 0 else None
    recall = float(tp / (tp + fn)) if (tp + fn) > 0 else None
    if precision is None or recall is None or (precision + recall) <= 0.0:
        f1 = None
    else:
        f1 = float((2.0 * precision * recall) / (precision + recall))
    label_event_count = int(len(label_events))
    detected_event_count = int(len(detected_list))
    return {
        "status": "ok",
        "label_event_count": label_event_count,
        "detected_event_count": detected_event_count,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": int(last.get("tn", 0)),
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "detected_per_label_ratio": (
            float(detected_event_count / label_event_count)
            if label_event_count > 0
            else None
        ),
        "tolerance_seconds": float(abs(tolerance_seconds)),
    }
