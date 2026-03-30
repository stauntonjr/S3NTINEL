"""Streaming and summary validators for detector outputs against simulator labels."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generator, Iterable

import pandas as pd

from libs.io.contracts import DetectedEventRow, EventLabelRow, EventValidatorSnapshot, TelemetryRow


EVENT_VALIDATION_FAMILY_TYPES = (
    "transition",
    "slope_pos",
    "slope_neg",
)


@dataclass(frozen=True)
class EventMatchResult:
    matched_label_ids: frozenset[int]
    matched_det_ids: frozenset[int]
    matched_deltas_seconds: tuple[float, ...]
    nearest_label_delta_by_id: dict[int, float]
    nearest_detection_delta_by_id: dict[int, float]


@dataclass(frozen=True)
class _SlopeRunSummary:
    family_name: str
    row_indexes: tuple[int, ...]


@dataclass(frozen=True)
class _LabeledSlopeRun:
    event_key: tuple[str, str, str]
    family_name: str
    row_indexes: tuple[int, ...]
    label_row_indexes: tuple[int, ...]
    label_timestamps: tuple[datetime, ...]
    start_ts: datetime
    end_ts: datetime


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


def _percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    series = pd.Series(values, dtype="float64")
    return float(series.quantile(float(quantile), interpolation="linear"))


def _coerce_float(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _continuous_slope_runs(rows: list[TelemetryRow]) -> tuple[_SlopeRunSummary, ...]:
    values: list[float | None] = []
    for row in rows:
        clean_value = _coerce_float(row.get("parameter_value_clean"))
        values.append(clean_value if clean_value is not None else _coerce_float(row.get("parameter_value")))
    diffs = [
        abs(current - previous)
        for previous, current in zip(values[:-1], values[1:], strict=False)
        if previous is not None and current is not None
    ]
    if not diffs:
        return ()
    abs_diff_series = pd.Series(diffs, dtype="float64")
    threshold = max(float(abs_diff_series.quantile(0.75)) * 0.5, 1e-6)
    runs: list[_SlopeRunSummary] = []
    run_sign = 0
    run_row_indexes: list[int] = []
    previous_value = values[0]

    for row_index, current_value in enumerate(values[1:], start=1):
        if previous_value is None or current_value is None:
            if run_sign != 0 and run_row_indexes:
                runs.append(
                    _SlopeRunSummary(
                        family_name="slope_pos" if run_sign > 0 else "slope_neg",
                        row_indexes=tuple(run_row_indexes),
                    )
                )
            run_sign = 0
            run_row_indexes = []
            previous_value = current_value
            continue
        delta = float(current_value - previous_value)
        previous_value = current_value
        if abs(delta) < threshold:
            if run_sign != 0 and run_row_indexes:
                runs.append(
                    _SlopeRunSummary(
                        family_name="slope_pos" if run_sign > 0 else "slope_neg",
                        row_indexes=tuple(run_row_indexes),
                    )
                )
            run_sign = 0
            run_row_indexes = []
            continue

        sign = 1 if delta > 0.0 else -1
        if sign != run_sign:
            if run_sign != 0 and run_row_indexes:
                runs.append(
                    _SlopeRunSummary(
                        family_name="slope_pos" if run_sign > 0 else "slope_neg",
                        row_indexes=tuple(run_row_indexes),
                    )
                )
            run_sign = sign
            run_row_indexes = [row_index]
            continue

        run_row_indexes.append(row_index)

    if run_sign != 0 and run_row_indexes:
        runs.append(
            _SlopeRunSummary(
                family_name="slope_pos" if run_sign > 0 else "slope_neg",
                row_indexes=tuple(run_row_indexes),
            )
        )
    return tuple(runs)


def _collect_labeled_slope_runs(
    simulator_rows: list[TelemetryRow],
    *,
    label_field: str,
) -> tuple[_LabeledSlopeRun, ...]:
    grouped_rows: dict[tuple[str, str, str], list[TelemetryRow]] = defaultdict(list)
    for row in simulator_rows:
        grouped_rows[_event_key(row)].append(dict(row))

    labeled_runs: list[_LabeledSlopeRun] = []
    for event_key, rows in grouped_rows.items():
        rows_sorted = sorted(
            rows,
            key=lambda row: (
                _event_ts(row, field="timestamp_utc"),
                int(row.get("step_index", 0) or 0),
            ),
        )
        runs = _continuous_slope_runs(rows_sorted)
        if not runs:
            continue
        label_row_indexes_by_family: dict[str, set[int]] = defaultdict(set)
        for row_index, row in enumerate(rows_sorted):
            label_value = str(row.get(label_field, "") or "").strip()
            if label_value in {"slope_pos", "slope_neg"}:
                label_row_indexes_by_family[label_value].add(row_index)

        for run in runs:
            label_indexes = tuple(
                sorted(
                    row_index
                    for row_index in run.row_indexes
                    if row_index in label_row_indexes_by_family.get(run.family_name, set())
                )
            )
            if not label_indexes:
                continue
            labeled_runs.append(
                _LabeledSlopeRun(
                    event_key=event_key,
                    family_name=run.family_name,
                    row_indexes=run.row_indexes,
                    label_row_indexes=label_indexes,
                    label_timestamps=tuple(
                        _event_ts(rows_sorted[row_index], field="timestamp_utc")
                        for row_index in label_indexes
                    ),
                    start_ts=_event_ts(rows_sorted[run.row_indexes[0]], field="timestamp_utc"),
                    end_ts=_event_ts(rows_sorted[run.row_indexes[-1]], field="timestamp_utc"),
                )
            )
    return tuple(labeled_runs)


def _build_slope_label_contract_metrics(
    simulator_rows: list[TelemetryRow],
    *,
    label_field: str,
) -> dict[str, Any]:
    labeled_runs = _collect_labeled_slope_runs(simulator_rows, label_field=label_field)
    family_accumulators: dict[str, dict[str, Any]] = {
        family_name: {
            "label_event_count": 0,
            "labeled_run_count": 0,
            "runs_with_repeated_labels_count": 0,
            "repeated_same_run_label_count": 0,
            "labels_per_labeled_run": [],
            "labels_per_repeated_run": [],
            "repeated_label_spacing_seconds": [],
        }
        for family_name in ("slope_pos", "slope_neg")
    }
    parameter_details: list[dict[str, Any]] = []
    for run in labeled_runs:
        accumulator = family_accumulators[run.family_name]
        label_count = len(run.label_row_indexes)
        accumulator["label_event_count"] += label_count
        accumulator["labeled_run_count"] += 1
        accumulator["labels_per_labeled_run"].append(float(label_count))
        repeated_count = max(label_count - 1, 0)
        accumulator["repeated_same_run_label_count"] += repeated_count
        if label_count >= 2:
            accumulator["runs_with_repeated_labels_count"] += 1
            accumulator["labels_per_repeated_run"].append(float(label_count))
        spacing_seconds: list[float] = []
        for previous_ts, current_ts in zip(run.label_timestamps[:-1], run.label_timestamps[1:], strict=False):
            spacing_seconds.append(float((current_ts - previous_ts).total_seconds()))
        accumulator["repeated_label_spacing_seconds"].extend(spacing_seconds)
        if repeated_count > 0:
            parameter_details.append(
                {
                    "tail_id": run.event_key[0],
                    "flight_id": run.event_key[1],
                    "parameter_name": run.event_key[2],
                    "event_family": run.family_name,
                    "label_event_count": int(label_count),
                    "repeated_same_run_label_count": int(repeated_count),
                    "median_repeated_label_spacing_seconds": _percentile(spacing_seconds, 0.5),
                    "max_labels_in_single_run": int(label_count),
                }
            )

    family_payloads: dict[str, dict[str, Any]] = {}
    for family_name, accumulator in family_accumulators.items():
        label_event_count = int(accumulator["label_event_count"])
        repeated_same_run_label_count = int(accumulator["repeated_same_run_label_count"])
        labeled_run_count = int(accumulator["labeled_run_count"])
        family_payloads[family_name] = {
            "label_event_count": label_event_count,
            "labeled_run_count": labeled_run_count,
            "runs_with_repeated_labels_count": int(accumulator["runs_with_repeated_labels_count"]),
            "repeated_same_run_label_count": repeated_same_run_label_count,
            "repeated_same_run_label_fraction": (
                float(repeated_same_run_label_count / label_event_count)
                if label_event_count > 0
                else None
            ),
            "median_labels_per_labeled_run": _percentile(accumulator["labels_per_labeled_run"], 0.5),
            "median_labels_per_repeated_run": _percentile(accumulator["labels_per_repeated_run"], 0.5),
            "median_repeated_label_spacing_seconds": _percentile(
                accumulator["repeated_label_spacing_seconds"],
                0.5,
            ),
            "p90_repeated_label_spacing_seconds": _percentile(
                accumulator["repeated_label_spacing_seconds"],
                0.9,
            ),
        }
    parameter_details_sorted = sorted(
        parameter_details,
        key=lambda item: (
            -int(item["repeated_same_run_label_count"]),
            -(item["median_repeated_label_spacing_seconds"] is not None),
            float(item["median_repeated_label_spacing_seconds"] or 0.0),
            str(item["event_family"]),
            str(item["parameter_name"]),
        ),
    )
    return {
        "families": family_payloads,
        "parameters_with_repeated_labels": parameter_details_sorted[:20],
    }


def _build_slope_run_capture_metrics(
    simulator_rows: list[TelemetryRow],
    detected_events: list[DetectedEventRow],
    *,
    tolerance_seconds: float,
    label_field: str,
) -> dict[str, Any]:
    tolerance = abs(float(tolerance_seconds))
    labeled_runs = _collect_labeled_slope_runs(simulator_rows, label_field=label_field)
    detections_by_bucket: dict[tuple[tuple[str, str, str], str], list[tuple[int, datetime]]] = defaultdict(list)
    for det_id, event in enumerate(detected_events):
        det_type = _detected_type(event)
        if det_type not in {"slope_pos", "slope_neg"}:
            continue
        detections_by_bucket[(_event_key(event), det_type)].append((det_id, _event_ts(event)))

    payload: dict[str, dict[str, Any]] = {}
    for family_name in ("slope_pos", "slope_neg"):
        family_runs = [run for run in labeled_runs if run.family_name == family_name]
        detected_family_count = int(sum(1 for event in detected_events if _detected_type(event) == family_name))
        matched_run_count = 0
        detections_inside_run_ids: set[int] = set()
        first_detection_offsets_seconds: list[float] = []
        run_durations_seconds: list[float] = []

        for run in family_runs:
            run_durations_seconds.append(float((run.end_ts - run.start_ts).total_seconds()))
            candidates = [
                (det_id, det_ts)
                for det_id, det_ts in detections_by_bucket.get((run.event_key, family_name), [])
                if det_ts >= (run.start_ts - pd.Timedelta(seconds=tolerance))
                and det_ts <= (run.end_ts + pd.Timedelta(seconds=tolerance))
            ]
            if not candidates:
                continue
            matched_run_count += 1
            for det_id, _ in candidates:
                detections_inside_run_ids.add(det_id)
            first_det_ts = min(det_ts for _, det_ts in candidates)
            first_detection_offsets_seconds.append(float((first_det_ts - run.start_ts).total_seconds()))

        truth_run_count = len(family_runs)
        detections_inside_truth_runs_count = len(detections_inside_run_ids)
        detections_outside_truth_runs_count = int(detected_family_count - detections_inside_truth_runs_count)
        payload[family_name] = {
            "truth_run_count": int(truth_run_count),
            "runs_with_detection_count": int(matched_run_count),
            "run_recall": float(matched_run_count / truth_run_count) if truth_run_count > 0 else None,
            "detected_event_count": detected_family_count,
            "detections_inside_truth_runs_count": int(detections_inside_truth_runs_count),
            "detections_outside_truth_runs_count": detections_outside_truth_runs_count,
            "detection_in_truth_run_fraction": (
                float(detections_inside_truth_runs_count / detected_family_count)
                if detected_family_count > 0
                else None
            ),
            "median_truth_run_duration_seconds": _percentile(run_durations_seconds, 0.5),
            "median_first_detection_offset_seconds": _percentile(first_detection_offsets_seconds, 0.5),
            "p90_first_detection_offset_seconds": _percentile(first_detection_offsets_seconds, 0.9),
            "tolerance_seconds": tolerance,
        }
    return payload


def _build_event_summary_payload(
    *,
    label_events: list[EventLabelRow],
    detected_events: list[DetectedEventRow],
    match_result: EventMatchResult,
    tolerance_seconds: float,
    tn: int | None = None,
) -> dict[str, Any]:
    tp = int(len(match_result.matched_label_ids))
    fp = int(len(detected_events) - len(match_result.matched_det_ids))
    fn = int(len(label_events) - len(match_result.matched_label_ids))
    precision = float(tp / (tp + fp)) if (tp + fp) > 0 else None
    recall = float(tp / (tp + fn)) if (tp + fn) > 0 else None
    if precision is None or recall is None or (precision + recall) <= 0.0:
        f1 = None
    else:
        f1 = float((2.0 * precision * recall) / (precision + recall))
    label_event_count = int(len(label_events))
    detected_event_count = int(len(detected_events))
    matched_deltas = list(match_result.matched_deltas_seconds)
    unmatched_label_nearest_deltas = [
        delta
        for label_id, delta in match_result.nearest_label_delta_by_id.items()
        if label_id not in match_result.matched_label_ids
    ]
    unmatched_detection_nearest_deltas = [
        delta
        for det_id, delta in match_result.nearest_detection_delta_by_id.items()
        if det_id not in match_result.matched_det_ids
    ]
    payload = {
        "label_event_count": label_event_count,
        "detected_event_count": detected_event_count,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "detected_per_label_ratio": (
            float(detected_event_count / label_event_count)
            if label_event_count > 0
            else None
        ),
        "matched_event_count": int(len(match_result.matched_deltas_seconds)),
        "median_matched_delta_seconds": _percentile(matched_deltas, 0.5),
        "p90_matched_delta_seconds": _percentile(matched_deltas, 0.9),
        "max_matched_delta_seconds": (float(max(matched_deltas)) if matched_deltas else None),
        "unmatched_label_with_nearest_detection_count": int(len(unmatched_label_nearest_deltas)),
        "median_unmatched_label_nearest_delta_seconds": _percentile(unmatched_label_nearest_deltas, 0.5),
        "near_miss_label_within_1s_count": int(sum(delta <= 1.0 for delta in unmatched_label_nearest_deltas)),
        "near_miss_label_within_2s_count": int(sum(delta <= 2.0 for delta in unmatched_label_nearest_deltas)),
        "near_miss_label_within_5s_count": int(sum(delta <= 5.0 for delta in unmatched_label_nearest_deltas)),
        "unmatched_detection_with_nearest_label_count": int(len(unmatched_detection_nearest_deltas)),
        "median_unmatched_detection_nearest_delta_seconds": _percentile(unmatched_detection_nearest_deltas, 0.5),
        "tolerance_seconds": float(abs(tolerance_seconds)),
    }
    if tn is not None:
        payload["tn"] = int(tn)
    return payload


def _match_events(
    *,
    label_events: list[EventLabelRow],
    detected_events: list[DetectedEventRow],
    tolerance_seconds: float,
) -> EventMatchResult:
    tolerance = abs(float(tolerance_seconds))
    label_by_bucket: dict[tuple[tuple[str, str, str], str], list[tuple[int, datetime]]] = defaultdict(list)
    det_by_bucket: dict[tuple[tuple[str, str, str], str], list[tuple[int, datetime]]] = defaultdict(list)
    for idx, event in enumerate(label_events):
        label_by_bucket[(_event_key(event), _label_type(event))].append((idx, _event_ts(event)))
    for idx, event in enumerate(detected_events):
        det_by_bucket[(_event_key(event), _detected_type(event))].append((idx, _event_ts(event)))

    matched_label_ids: set[int] = set()
    matched_det_ids: set[int] = set()
    matched_deltas_seconds: list[float] = []
    nearest_label_delta_by_id: dict[int, float] = {}
    nearest_detection_delta_by_id: dict[int, float] = {}

    for bucket in set(label_by_bucket.keys()) | set(det_by_bucket.keys()):
        label_list = sorted(label_by_bucket.get(bucket, []), key=lambda item: item[1])
        det_list = sorted(det_by_bucket.get(bucket, []), key=lambda item: item[1])

        for label_id, label_ts in label_list:
            nearest_delta = None
            for _, det_ts in det_list:
                delta = abs((det_ts - label_ts).total_seconds())
                if nearest_delta is None or delta < nearest_delta:
                    nearest_delta = delta
            if nearest_delta is not None:
                nearest_label_delta_by_id[label_id] = float(nearest_delta)

        for det_id, det_ts in det_list:
            nearest_delta = None
            for _, label_ts in label_list:
                delta = abs((det_ts - label_ts).total_seconds())
                if nearest_delta is None or delta < nearest_delta:
                    nearest_delta = delta
            if nearest_delta is not None:
                nearest_detection_delta_by_id[det_id] = float(nearest_delta)

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
            if best_label_id is not None and best_delta is not None:
                used_label_local.add(best_label_id)
                matched_label_ids.add(best_label_id)
                matched_det_ids.add(det_id)
                matched_deltas_seconds.append(float(best_delta))

    return EventMatchResult(
        matched_label_ids=frozenset(matched_label_ids),
        matched_det_ids=frozenset(matched_det_ids),
        matched_deltas_seconds=tuple(matched_deltas_seconds),
        nearest_label_delta_by_id=nearest_label_delta_by_id,
        nearest_detection_delta_by_id=nearest_detection_delta_by_id,
    )


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
    label_field: str = "event_type_label",
) -> Generator[EventValidatorSnapshot, None, None]:
    """Yield cumulative TP/FP/FN/TN counters while matching detected events to simulator labels.

    TN is counted per simulator telemetry row with no label event and no matched detection.
    """
    label_events = [dict(event) for event in simulator_label_events(simulator_rows, label_field=label_field)]
    detected = [dict(item) for item in detected_events]
    match_result = _match_events(
        label_events=label_events,
        detected_events=detected,
        tolerance_seconds=tolerance_seconds,
    )
    matched_label_ids = set(match_result.matched_label_ids)
    matched_det_ids = set(match_result.matched_det_ids)

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
    label_field: str = "event_type_label",
) -> dict[str, Any]:
    simulator_list = [dict(row) for row in simulator_rows]
    detected_list = [dict(row) for row in detected_events]
    label_events = list(simulator_label_events(simulator_list, label_field=label_field))
    match_result = _match_events(
        label_events=label_events,
        detected_events=detected_list,
        tolerance_seconds=tolerance_seconds,
    )
    snapshots = list(
        iter_event_validation_snapshots(
            simulator_rows=simulator_list,
            detected_events=detected_list,
            tolerance_seconds=tolerance_seconds,
            label_field=label_field,
        )
    )
    last = snapshots[-1] if snapshots else {"tp": 0, "fp": 0, "fn": 0, "tn": 0}
    family_metrics: dict[str, dict[str, Any]] = {}
    for family_name in EVENT_VALIDATION_FAMILY_TYPES:
        family_label_events = [
            dict(event)
            for event in label_events
            if _label_type(event) == family_name
        ]
        family_detected_events = [
            dict(event)
            for event in detected_list
            if _detected_type(event) == family_name
        ]
        family_match_result = _match_events(
            label_events=family_label_events,
            detected_events=family_detected_events,
            tolerance_seconds=tolerance_seconds,
        )
        family_metrics[family_name] = _build_event_summary_payload(
            label_events=family_label_events,
            detected_events=family_detected_events,
            match_result=family_match_result,
            tolerance_seconds=tolerance_seconds,
        )
    slope_label_contract_metrics = _build_slope_label_contract_metrics(
        simulator_list,
        label_field=label_field,
    )
    slope_run_capture_metrics = _build_slope_run_capture_metrics(
        simulator_list,
        detected_list,
        tolerance_seconds=tolerance_seconds,
        label_field=label_field,
    )

    payload = {
        "status": "ok",
        **_build_event_summary_payload(
            label_events=label_events,
            detected_events=detected_list,
            match_result=match_result,
            tolerance_seconds=tolerance_seconds,
            tn=int(last.get("tn", 0)),
        ),
        "event_family_metrics": family_metrics,
        "slope_label_contract_metrics": slope_label_contract_metrics,
        "slope_run_capture_metrics": slope_run_capture_metrics,
    }
    return payload
