# File: libs/testing/stream_eval.py
"""Utilities for tolerance-based stream event precision/recall evaluation."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any


def _event_key(event: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(event.get("tail_id", "")),
        str(event.get("flight_id", "")),
        str(event.get("sensor", "")),
    )


def _event_type(event: dict[str, Any]) -> str:
    return str(event.get("event_type", ""))


def _event_ts(event: dict[str, Any]) -> datetime:
    value = event.get("ts")
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise ValueError("event ts must be datetime or ISO8601 string")


def evaluate_event_detection(
    truth_events: list[dict[str, Any]],
    detected_events: list[dict[str, Any]],
    tolerance_seconds: float = 0.5,
    tolerance_by_type_seconds: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Match detected to truth events by key+type within time tolerance and compute precision/recall."""
    tolerance = abs(float(tolerance_seconds))
    tolerance_by_type = {
        key: abs(float(value))
        for key, value in (tolerance_by_type_seconds or {}).items()
    }
    truth_by_bucket: dict[tuple[tuple[str, str, str], str], list[tuple[int, datetime]]] = defaultdict(list)
    det_by_bucket: dict[tuple[tuple[str, str, str], str], list[tuple[int, datetime]]] = defaultdict(list)

    for idx, event in enumerate(truth_events):
        bucket = (_event_key(event), _event_type(event))
        truth_by_bucket[bucket].append((idx, _event_ts(event)))

    for idx, event in enumerate(detected_events):
        bucket = (_event_key(event), _event_type(event))
        det_by_bucket[bucket].append((idx, _event_ts(event)))

    matched_truth_ids: set[int] = set()
    matched_det_ids: set[int] = set()
    per_type = defaultdict(lambda: {"tp": 0, "fp": 0, "fn": 0})

    all_buckets = set(truth_by_bucket.keys()) | set(det_by_bucket.keys())
    for bucket in all_buckets:
        event_type = bucket[1]
        truth_list = sorted(truth_by_bucket.get(bucket, []), key=lambda item: item[1])
        det_list = sorted(det_by_bucket.get(bucket, []), key=lambda item: item[1])

        used_truth_local: set[int] = set()
        event_tolerance = tolerance_by_type.get(event_type, tolerance)
        for det_id, det_ts in det_list:
            best_idx = None
            best_delta = None
            for local_idx, (truth_id, truth_ts) in enumerate(truth_list):
                if truth_id in used_truth_local:
                    continue
                delta = abs((det_ts - truth_ts).total_seconds())
                if delta <= event_tolerance and (best_delta is None or delta < best_delta):
                    best_idx = local_idx
                    best_delta = delta
            if best_idx is not None:
                truth_id = truth_list[best_idx][0]
                used_truth_local.add(truth_id)
                matched_truth_ids.add(truth_id)
                matched_det_ids.add(det_id)
                per_type[event_type]["tp"] += 1
            else:
                per_type[event_type]["fp"] += 1

        for truth_id, _ in truth_list:
            if truth_id not in used_truth_local:
                per_type[event_type]["fn"] += 1

    tp = len(matched_det_ids)
    fp = len(detected_events) - tp
    fn = len(truth_events) - len(matched_truth_ids)

    precision = (tp / (tp + fp)) if (tp + fp) > 0 else 0.0
    recall = (tp / (tp + fn)) if (tp + fn) > 0 else 0.0

    per_type_metrics: dict[str, Any] = {}
    for event_type, counts in sorted(per_type.items()):
        tp_t = int(counts["tp"])
        fp_t = int(counts["fp"])
        fn_t = int(counts["fn"])
        precision_t = (tp_t / (tp_t + fp_t)) if (tp_t + fp_t) > 0 else 0.0
        recall_t = (tp_t / (tp_t + fn_t)) if (tp_t + fn_t) > 0 else 0.0
        per_type_metrics[event_type] = {
            "tp": tp_t,
            "fp": fp_t,
            "fn": fn_t,
            "precision": precision_t,
            "recall": recall_t,
        }

    return {
        "totals": {
            "truth": len(truth_events),
            "detected": len(detected_events),
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": precision,
            "recall": recall,
        },
        "per_type": per_type_metrics,
        "tolerance_seconds": tolerance,
        "tolerance_by_type_seconds": tolerance_by_type,
    }
