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
                    "drift_magnitude": (
                        None
                        if pd.isna(row.get("drift_magnitude"))
                        else float(row.get("drift_magnitude", 0.0) or 0.0)
                    ),
                    "s_w": _coerce_vector(row.get("s_w")),
                    "phase_label": phase_label,
                }
            )
    return assignments


def _coerce_vector(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, pd.Series):
        value = value.tolist()
    elif hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        return [float(item) for item in value]
    return []


def _has_vector_values(value: Any) -> bool:
    return len(_coerce_vector(value)) > 0


def _mean_vector(vectors: list[list[float]]) -> list[float]:
    if not vectors:
        return []
    width = len(vectors[0])
    if width <= 0:
        return []
    return [
        float(sum(float(vector[index]) for vector in vectors) / float(len(vectors)))
        for index in range(width)
    ]


def _euclidean_distance(left: list[float], right: list[float]) -> float | None:
    width = min(len(left), len(right))
    if width <= 0:
        return None
    return float(sum((float(left[index]) - float(right[index])) ** 2 for index in range(width)) ** 0.5)


def build_phase_centroid_comparison_summary_from_tables(
    *,
    phase_windows_df: pd.DataFrame,
    phase_labels_df: pd.DataFrame,
    phase_baselines_df: pd.DataFrame,
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
    if phase_baselines_df is None or phase_baselines_df.empty:
        return {
            "status": "skipped",
            "reason": "phase baselines are empty",
            "assignment_count": len(assignments),
        }

    assigned_df = pd.DataFrame.from_records(assignments)
    assigned_df["s_w"] = assigned_df["s_w"].apply(_coerce_vector)
    assigned_df = assigned_df[
        assigned_df["phase_label"].fillna("").astype(str).str.strip().astype(bool)
        & assigned_df["s_w"].apply(_has_vector_values)
    ].copy()
    if assigned_df.empty:
        return {
            "status": "skipped",
            "reason": "no labeled window vectors available",
            "assignment_count": len(assignments),
        }

    truth_label_centroids: list[dict[str, Any]] = []
    for (tail_id, phase_label), group in assigned_df.groupby(["tail_id", "phase_label"], dropna=False, sort=True):
        vectors = [list(item) for item in group["s_w"].tolist()]
        truth_label_centroids.append(
            {
                "tail_id": str(tail_id),
                "phase_label": str(phase_label),
                "window_subset": "all",
                "window_count": int(len(group)),
                "drift_threshold_upper": None,
                "s_w_centroid": _mean_vector(vectors),
            }
        )
        drift_series = pd.to_numeric(group["drift_magnitude"], errors="coerce")
        if drift_series.notna().any():
            for quantile, subset_name in ((0.5, "low_drift_p50"), (0.25, "low_drift_p25")):
                threshold = float(drift_series.quantile(quantile))
                subset = group[drift_series <= threshold].copy()
                if subset.empty:
                    continue
                truth_label_centroids.append(
                    {
                        "tail_id": str(tail_id),
                        "phase_label": str(phase_label),
                        "window_subset": subset_name,
                        "window_count": int(len(subset)),
                        "drift_threshold_upper": threshold,
                        "s_w_centroid": _mean_vector([list(item) for item in subset["s_w"].tolist()]),
                    }
                )

    baselines = phase_baselines_df.copy()
    baselines["s_w_centroid"] = baselines["s_w_centroid"].apply(_coerce_vector)
    baselines = baselines[baselines["s_w_centroid"].apply(_has_vector_values)].copy()
    detected_phase_centroids = [
        {
            "tail_id": str(row.get("tail_id", "")),
            "phase_id_detected": int(row.get("phase_id_detected", 0) or 0),
            "phase_name_detected": str(row.get("phase_name_detected", "")),
            "stable_window_count": int(row.get("stable_window_count", 0) or 0),
            "s_w_centroid": _coerce_vector(row.get("s_w_centroid")),
        }
        for row in baselines.to_dict(orient="records")
    ]
    if not detected_phase_centroids:
        return {
            "status": "skipped",
            "reason": "no detected phase centroids available",
            "assignment_count": len(assignments),
            "truth_label_centroids": truth_label_centroids,
        }

    distance_matrix: list[dict[str, Any]] = []
    for detected in detected_phase_centroids:
        for truth in truth_label_centroids:
            if str(detected["tail_id"]) != str(truth["tail_id"]):
                continue
            distance_matrix.append(
                {
                    "tail_id": str(detected["tail_id"]),
                    "phase_id_detected": int(detected["phase_id_detected"]),
                    "phase_name_detected": str(detected["phase_name_detected"]),
                    "phase_label": str(truth["phase_label"]),
                    "window_subset": str(truth["window_subset"]),
                    "window_count": int(truth["window_count"]),
                    "drift_threshold_upper": truth["drift_threshold_upper"],
                    "distance": _euclidean_distance(
                        list(detected["s_w_centroid"]),
                        list(truth["s_w_centroid"]),
                    ),
                }
            )
    distance_matrix = sorted(
        distance_matrix,
        key=lambda item: (
            str(item["tail_id"]),
            int(item["phase_id_detected"]),
            float("inf") if item["distance"] is None else float(item["distance"]),
            str(item["phase_label"]),
            str(item["window_subset"]),
        ),
    )
    nearest_truth_centroid_by_detected: list[dict[str, Any]] = []
    nearest_truth_centroid_by_detected_and_subset: list[dict[str, Any]] = []
    distance_df = pd.DataFrame.from_records(distance_matrix)
    if not distance_df.empty:
        for (tail_id, phase_id_detected), group in distance_df.groupby(
            ["tail_id", "phase_id_detected"],
            dropna=False,
            sort=True,
        ):
            best = group.sort_values(["distance", "phase_label", "window_subset"], kind="stable").iloc[0].to_dict()
            nearest_truth_centroid_by_detected.append(dict(best))
        for (tail_id, phase_id_detected, window_subset), group in distance_df.groupby(
            ["tail_id", "phase_id_detected", "window_subset"],
            dropna=False,
            sort=True,
        ):
            best = group.sort_values(["distance", "phase_label"], kind="stable").iloc[0].to_dict()
            nearest_truth_centroid_by_detected_and_subset.append(dict(best))

    stable_window_label_counts = Counter(
        str(item["phase_label"])
        for item in assignments
        if str(item.get("phase_state_detected", "")) == "stable" and str(item.get("phase_label", "")).strip()
    )
    truth_label_window_counts = Counter(str(item["phase_label"]) for item in assignments if str(item.get("phase_label", "")).strip())

    return {
        "status": "ok",
        "assignment_count": len(assignments),
        "centroid_vector_column": "s_w",
        "label_assignment_contract": "majority_overlap_label",
        "detected_phase_centroids": detected_phase_centroids,
        "truth_label_centroids": truth_label_centroids,
        "distance_matrix": distance_matrix,
        "nearest_truth_centroid_by_detected": nearest_truth_centroid_by_detected,
        "nearest_truth_centroid_by_detected_and_subset": nearest_truth_centroid_by_detected_and_subset,
        "stable_window_label_counts": dict(sorted(stable_window_label_counts.items())),
        "truth_label_window_counts": dict(sorted(truth_label_window_counts.items())),
    }


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
