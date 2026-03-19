"""Phase-detection validation helpers over persisted tables."""

from __future__ import annotations

from collections import Counter
from itertools import permutations
from typing import Any

import pandas as pd


def _normalize_timestamp_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _majority_label(labels: pd.Series) -> str | None:
    values = [str(item) for item in labels.fillna("").astype(str).tolist() if str(item)]
    if not values:
        return None
    ranked = Counter(values).most_common()
    return str(sorted(ranked, key=lambda item: (-item[1], item[0]))[0][0])


def build_phase_validation_assignments(
    *,
    phase_windows_df: pd.DataFrame,
    phase_labels_df: pd.DataFrame,
    windows_df: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    if phase_windows_df is None or phase_labels_df is None or phase_windows_df.empty or phase_labels_df.empty:
        return []

    windows = phase_windows_df.copy()
    if windows_df is not None and not windows_df.empty:
        window_times = windows_df[["tail_id", "flight_id", "win_id", "t_start", "t_end"]].copy()
        windows = windows.drop(columns=["t_start", "t_end"], errors="ignore").merge(
            window_times,
            on=["tail_id", "flight_id", "win_id"],
            how="left",
        )
    labels = phase_labels_df.copy()
    windows["t_start"] = _normalize_timestamp_series(windows["t_start"])
    windows["t_end"] = _normalize_timestamp_series(windows["t_end"])
    labels["timestamp_utc"] = _normalize_timestamp_series(labels["timestamp_utc"])

    assignments: list[dict[str, Any]] = []
    for (tail_id, flight_id), flight_windows in windows.groupby(["tail_id", "flight_id"], dropna=False):
        flight_labels = labels[
            (labels["tail_id"].astype(str) == str(tail_id))
            & (labels["flight_id"].astype(str) == str(flight_id))
        ].copy()
        if flight_labels.empty:
            continue
        for row in flight_windows.to_dict(orient="records"):
            t_start = row.get("t_start")
            t_end = row.get("t_end")
            if pd.isna(t_start) or pd.isna(t_end):
                continue
            overlapping = flight_labels[
                (flight_labels["timestamp_utc"] >= t_start)
                & (flight_labels["timestamp_utc"] <= t_end)
            ]
            phase_label = _majority_label(overlapping.get("phase_label", pd.Series(dtype="object")))
            assignments.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0) or 0),
                    "phase_id_detected": int(row.get("phase_id_detected", 0) or 0),
                    "phase_state_detected": str(row.get("phase_state_detected", "")),
                    "phase_confidence_detected": float(row.get("phase_confidence_detected", 0.0) or 0.0),
                    "distance_to_centroid_detected": (
                        None
                        if pd.isna(row.get("distance_to_centroid_detected"))
                        else float(row.get("distance_to_centroid_detected", 0.0) or 0.0)
                    ),
                    "phase_label": phase_label,
                }
            )
    return assignments


def validate_detected_phases_from_tables(
    *,
    phase_windows_df: pd.DataFrame,
    phase_labels_df: pd.DataFrame,
    windows_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    assignments = build_phase_validation_assignments(
        phase_windows_df=phase_windows_df,
        phase_labels_df=phase_labels_df,
        windows_df=windows_df,
    )
    if not assignments:
        return {
            "status": "skipped",
            "reason": "no overlapping phase windows and phase labels",
            "assignment_count": 0,
        }
    summary = evaluate_detected_phases(assignments)
    summary["status"] = "ok"
    summary["assignment_count"] = len(assignments)
    return summary


def evaluate_detected_phases(assignments: list[dict[str, Any]]) -> dict[str, Any]:
    """Evaluate detected phases against simulator labels using best one-to-one tail-local mapping."""
    valid = [dict(item) for item in assignments if str(item.get("phase_label", "")).strip()]
    if not valid:
        return {"overall_accuracy": None, "by_tail": [], "by_phase_label": []}

    by_tail: dict[str, list[dict[str, Any]]] = {}
    for item in valid:
        by_tail.setdefault(str(item.get("tail_id", "")), []).append(item)

    total_correct = 0
    total_count = 0
    by_tail_rows: list[dict[str, Any]] = []
    by_tail_flight_rows: list[dict[str, Any]] = []
    label_counts_global: Counter[str] = Counter()
    pred_counts_global: Counter[str] = Counter()
    tp_counts_global: Counter[str] = Counter()
    confusion_matrix_counts: Counter[tuple[str, str]] = Counter()

    for tail_id in sorted(by_tail.keys()):
        items = by_tail[tail_id]
        detected_ids = sorted({int(item["phase_id_detected"]) for item in items})
        phase_labels = sorted({str(item["phase_label"]) for item in items})
        confusion: dict[tuple[int, str], int] = Counter(
            (int(item["phase_id_detected"]), str(item["phase_label"])) for item in items
        )

        best_score = -1
        best_mapping: dict[int, str] = {}
        for label_perm in permutations(phase_labels, min(len(detected_ids), len(phase_labels))):
            score = 0
            mapping: dict[int, str] = {}
            for detected_id, phase_label in zip(detected_ids, label_perm, strict=False):
                mapping[int(detected_id)] = str(phase_label)
                score += int(confusion.get((int(detected_id), str(phase_label)), 0))
            if score > best_score:
                best_score = score
                best_mapping = mapping

        correct = 0
        for item in items:
            phase_label = str(item["phase_label"])
            predicted_label = str(best_mapping.get(int(item["phase_id_detected"]), "unmapped"))
            label_counts_global[phase_label] += 1
            pred_counts_global[predicted_label] += 1
            confusion_matrix_counts[(phase_label, predicted_label)] += 1
            if predicted_label == phase_label:
                correct += 1
                tp_counts_global[phase_label] += 1

        total_correct += correct
        total_count += len(items)
        by_tail_rows.append(
            {
                "tail_id": tail_id,
                "window_count": int(len(items)),
                "correct": int(correct),
                "accuracy": float(correct) / float(max(len(items), 1)),
                "phase_mapping": [
                    {
                        "phase_id_detected": int(phase_id_detected),
                        "phase_label": phase_label,
                    }
                    for phase_id_detected, phase_label in sorted(best_mapping.items(), key=lambda item: item[0])
                ],
            }
        )

        flight_mapping = {
            int(phase_id_detected): phase_label
            for phase_id_detected, phase_label in sorted(best_mapping.items(), key=lambda item: item[0])
        }
        by_flight: dict[str, list[dict[str, Any]]] = {}
        for item in items:
            by_flight.setdefault(str(item.get("flight_id", "")), []).append(item)
        for flight_id in sorted(by_flight.keys()):
            flight_items = by_flight[flight_id]
            flight_correct = 0
            for item in flight_items:
                if str(flight_mapping.get(int(item["phase_id_detected"]), "unmapped")) == str(item["phase_label"]):
                    flight_correct += 1
            by_tail_flight_rows.append(
                {
                    "tail_id": tail_id,
                    "flight_id": flight_id,
                    "window_count": int(len(flight_items)),
                    "correct": int(flight_correct),
                    "accuracy": float(flight_correct) / float(max(len(flight_items), 1)),
                    "phase_mapping": [
                        {
                            "phase_id_detected": int(phase_id_detected),
                            "phase_label": phase_label,
                        }
                        for phase_id_detected, phase_label in sorted(flight_mapping.items(), key=lambda item: item[0])
                    ],
                }
            )

    by_phase_label: list[dict[str, Any]] = []
    labels = sorted(set(label_counts_global.keys()) | set(pred_counts_global.keys()))
    precision_values: list[float] = []
    recall_values: list[float] = []
    f1_values: list[float] = []
    weighted_precision_total = 0.0
    weighted_recall_total = 0.0
    weighted_f1_total = 0.0
    weighted_count_total = 0
    for label in labels:
        tp = int(tp_counts_global.get(label, 0))
        label_count = int(label_counts_global.get(label, 0))
        pred_count = int(pred_counts_global.get(label, 0))
        precision = float(tp) / float(max(pred_count, 1))
        recall = float(tp) / float(max(label_count, 1))
        if (precision + recall) <= 0.0:
            f1 = 0.0
        else:
            f1 = float((2.0 * precision * recall) / (precision + recall))
        precision_values.append(precision)
        recall_values.append(recall)
        f1_values.append(f1)
        weighted_precision_total += precision * label_count
        weighted_recall_total += recall * label_count
        weighted_f1_total += f1 * label_count
        weighted_count_total += label_count
        by_phase_label.append(
            {
                "phase_label": label,
                "label_count": label_count,
                "detected_count": pred_count,
                "tp": tp,
                "precision": precision,
                "recall": recall,
                "f1": f1,
            }
        )

    return {
        "overall_accuracy": float(total_correct) / float(max(total_count, 1)),
        "macro_precision": (float(sum(precision_values) / len(precision_values)) if precision_values else None),
        "macro_recall": (float(sum(recall_values) / len(recall_values)) if recall_values else None),
        "macro_f1": (float(sum(f1_values) / len(f1_values)) if f1_values else None),
        "weighted_precision": (
            float(weighted_precision_total / weighted_count_total)
            if weighted_count_total > 0
            else None
        ),
        "weighted_recall": (
            float(weighted_recall_total / weighted_count_total)
            if weighted_count_total > 0
            else None
        ),
        "weighted_f1": (
            float(weighted_f1_total / weighted_count_total)
            if weighted_count_total > 0
            else None
        ),
        "by_tail": by_tail_rows,
        "by_tail_flight": by_tail_flight_rows,
        "by_phase_label": by_phase_label,
        "confusion_matrix": [
            {
                "phase_label": phase_label,
                "phase_label_detected": phase_label_detected,
                "count": int(count),
            }
            for (phase_label, phase_label_detected), count in sorted(
                confusion_matrix_counts.items(),
                key=lambda item: (item[0][0], item[0][1]),
            )
        ],
    }
