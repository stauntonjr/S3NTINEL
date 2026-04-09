"""Phase-detection validation helpers over persisted tables."""

from __future__ import annotations

from collections import Counter
from itertools import permutations
from typing import Any

import pandas as pd


def _clean_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_timestamp_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def _majority_label(labels: pd.Series) -> str | None:
    values = [str(item) for item in labels.fillna("").astype(str).tolist() if str(item)]
    if not values:
        return None
    ranked = Counter(values).most_common()
    return str(sorted(ranked, key=lambda item: (-item[1], item[0]))[0][0])


def _ordered_nonempty_labels(labels_df: pd.DataFrame) -> list[str]:
    if labels_df is None or labels_df.empty or "phase_label" not in labels_df.columns:
        return []
    ordered = labels_df.sort_values(["timestamp_utc", "phase_label"], kind="stable")
    return [
        _clean_text(item)
        for item in ordered["phase_label"].fillna("").astype(str).tolist()
        if _clean_text(item)
    ]


def _truth_phase_context(overlapping_labels: pd.DataFrame) -> dict[str, str | None]:
    ordered_labels = _ordered_nonempty_labels(overlapping_labels)
    distinct_labels = {label for label in ordered_labels if label}
    primary_label = _majority_label(overlapping_labels.get("phase_label", pd.Series(dtype="object")))
    if not ordered_labels:
        return {
            "truth_phase_label_primary": primary_label,
            "truth_phase_state": None,
            "truth_transition_from_label": None,
            "truth_transition_to_label": None,
        }
    first_label = ordered_labels[0]
    last_label = ordered_labels[-1]
    truth_phase_state = "transition_region" if len(distinct_labels) >= 2 else "stable"
    transition_from_label = first_label if truth_phase_state == "transition_region" and first_label != last_label else None
    transition_to_label = last_label if truth_phase_state == "transition_region" and first_label != last_label else None
    return {
        "truth_phase_label_primary": primary_label,
        "truth_phase_state": truth_phase_state,
        "truth_transition_from_label": transition_from_label,
        "truth_transition_to_label": transition_to_label,
    }


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
    for (tail_id, flight_id), flight_windows in windows.groupby(["tail_id", "flight_id"], dropna=False, observed=False):
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
            truth_phase_context = _truth_phase_context(overlapping)
            phase_label = truth_phase_context["truth_phase_label_primary"]
            assignments.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0) or 0),
                    "phase_id_detected": int(row.get("phase_id_detected", 0) or 0),
                    "phase_state_detected": str(row.get("phase_state_detected", "")),
                    "transition_from_phase_id_detected": (
                        None
                        if pd.isna(row.get("transition_from_phase_id_detected"))
                        else int(row.get("transition_from_phase_id_detected", 0) or 0)
                    ),
                    "transition_to_phase_id_detected": (
                        None
                        if pd.isna(row.get("transition_to_phase_id_detected"))
                        else int(row.get("transition_to_phase_id_detected", 0) or 0)
                    ),
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
                    "truth_phase_label_primary": truth_phase_context["truth_phase_label_primary"],
                    "truth_phase_state": truth_phase_context["truth_phase_state"],
                    "truth_transition_from_label": truth_phase_context["truth_transition_from_label"],
                    "truth_transition_to_label": truth_phase_context["truth_transition_to_label"],
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


def _feature_family_for_name(feature_name: str) -> str:
    name = str(feature_name)
    if name.startswith("parameter_name::"):
        return "level"
    if name.startswith("parameter_delta::"):
        return "delta"
    if name.startswith("event_type::"):
        return "event"
    if name.startswith("categorical_start::") or name.startswith("categorical_end::") or name.startswith("categorical_changed::"):
        return "categorical"
    if name.startswith("temporal_sensor::") or name.startswith("temporal_event::") or name.startswith("temporal_categorical::") or name.startswith("temporal_summary::"):
        return "temporal"
    if name.startswith("summary::"):
        return "summary"
    return "other"


def _feature_family_index_ranges(feature_names: list[str]) -> dict[str, list[int]]:
    ranges: dict[str, list[int]] = {}
    for index, feature_name in enumerate(feature_names):
        family = _feature_family_for_name(feature_name)
        if family not in ranges:
            ranges[family] = [index, index]
        else:
            ranges[family][1] = index
    return ranges


def _distance_by_feature_family(
    left: list[float],
    right: list[float],
    feature_names: list[str],
) -> dict[str, float | None]:
    ranges = _feature_family_index_ranges(feature_names)
    return {
        family: _euclidean_distance(left[start : end + 1], right[start : end + 1])
        for family, (start, end) in sorted(ranges.items())
    }


def _tail_phase_mapping(summary: dict[str, Any]) -> dict[str, dict[int, str]]:
    by_tail = summary.get("by_tail", [])
    mapping_by_tail: dict[str, dict[int, str]] = {}
    for row in by_tail:
        tail_id = str(row.get("tail_id", ""))
        phase_mapping = row.get("phase_mapping", [])
        mapping_by_tail[tail_id] = {
            int(item.get("phase_id_detected", 0)): str(item.get("phase_label", ""))
            for item in phase_mapping
            if str(item.get("phase_label", "")).strip()
        }
    return mapping_by_tail


def _count_rows(counter: Counter[tuple[str, str]], *, left_key: str, right_key: str) -> list[dict[str, Any]]:
    return [
        {
            left_key: left,
            right_key: right,
            "count": int(count),
        }
        for (left, right), count in sorted(counter.items(), key=lambda item: (item[0][0], item[0][1]))
    ]


def _collapse_transition_events(
    assignments: list[dict[str, Any]],
    *,
    truth: bool,
    tail_phase_mapping: dict[str, dict[int, str]],
) -> list[dict[str, Any]]:
    sorted_assignments = sorted(
        (dict(item) for item in assignments),
        key=lambda item: (
            _clean_text(item.get("tail_id")),
            _clean_text(item.get("flight_id")),
            int(item.get("win_id", 0) or 0),
        ),
    )
    events: list[dict[str, Any]] = []
    active_event: dict[str, Any] | None = None
    for item in sorted_assignments:
        tail_id = _clean_text(item.get("tail_id"))
        flight_id = _clean_text(item.get("flight_id"))
        win_id = int(item.get("win_id", 0) or 0)
        if truth:
            is_transition = _clean_text(item.get("truth_phase_state")) == "transition_region"
            from_label = _clean_text(item.get("truth_transition_from_label"))
            to_label = _clean_text(item.get("truth_transition_to_label"))
        else:
            is_transition = _clean_text(item.get("phase_state_detected")) == "transition_region"
            phase_mapping = tail_phase_mapping.get(tail_id, {})
            detected_from_id = item.get("transition_from_phase_id_detected")
            detected_to_id = item.get("transition_to_phase_id_detected")
            from_label = (
                ""
                if detected_from_id is None
                else _clean_text(phase_mapping.get(int(detected_from_id)))
            )
            to_label = (
                ""
                if detected_to_id is None
                else _clean_text(phase_mapping.get(int(detected_to_id)))
            )
        if not is_transition or not from_label or not to_label:
            active_event = None
            continue
        if (
            active_event is not None
            and active_event["tail_id"] == tail_id
            and active_event["flight_id"] == flight_id
            and active_event["transition_from_label"] == from_label
            and active_event["transition_to_label"] == to_label
            and int(active_event["win_id_end"]) + 1 == win_id
        ):
            active_event["win_id_end"] = win_id
            active_event["window_count"] = int(active_event["window_count"]) + 1
            active_event["win_id_center"] = float(active_event["win_id_start"] + active_event["win_id_end"]) / 2.0
            continue
        active_event = {
            "tail_id": tail_id,
            "flight_id": flight_id,
            "transition_from_label": from_label,
            "transition_to_label": to_label,
            "win_id_start": win_id,
            "win_id_end": win_id,
            "win_id_center": float(win_id),
            "window_count": 1,
        }
        events.append(active_event)
    return events


def _evaluate_transition_event_alignment(
    assignments: list[dict[str, Any]],
    *,
    tail_phase_mapping: dict[str, dict[int, str]],
) -> dict[str, Any]:
    truth_events = _collapse_transition_events(assignments, truth=True, tail_phase_mapping=tail_phase_mapping)
    detected_events = _collapse_transition_events(assignments, truth=False, tail_phase_mapping=tail_phase_mapping)
    truth_counts = Counter(
        (str(item["transition_from_label"]), str(item["transition_to_label"]))
        for item in truth_events
    )
    detected_counts = Counter(
        (str(item["transition_from_label"]), str(item["transition_to_label"]))
        for item in detected_events
    )
    detected_by_group: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for event in detected_events:
        group_key = (
            str(event["tail_id"]),
            str(event["flight_id"]),
            str(event["transition_from_label"]),
            str(event["transition_to_label"]),
        )
        detected_by_group.setdefault(group_key, []).append(dict(event))
    for group_events in detected_by_group.values():
        group_events.sort(key=lambda item: float(item["win_id_center"]))

    matched_rows: list[dict[str, Any]] = []
    matched_deltas: list[float] = []
    matched_progress_deltas: list[float] = []
    flight_window_counts: dict[tuple[str, str], int] = Counter(
        (
            _clean_text(item.get("tail_id")),
            _clean_text(item.get("flight_id")),
        )
        for item in assignments
    )
    for truth_event in truth_events:
        group_key = (
            str(truth_event["tail_id"]),
            str(truth_event["flight_id"]),
            str(truth_event["transition_from_label"]),
            str(truth_event["transition_to_label"]),
        )
        candidates = detected_by_group.get(group_key, [])
        if not candidates:
            matched_rows.append(
                {
                    "tail_id": str(truth_event["tail_id"]),
                    "flight_id": str(truth_event["flight_id"]),
                    "transition_from_label": str(truth_event["transition_from_label"]),
                    "transition_to_label": str(truth_event["transition_to_label"]),
                    "truth_win_id_center": float(truth_event["win_id_center"]),
                    "detected_win_id_center": None,
                    "abs_win_id_delta": None,
                }
            )
            continue
        nearest = min(
            candidates,
            key=lambda item: (
                abs(float(item["win_id_center"]) - float(truth_event["win_id_center"])),
                float(item["win_id_center"]),
            ),
        )
        abs_delta = abs(float(nearest["win_id_center"]) - float(truth_event["win_id_center"]))
        flight_window_count = int(
            flight_window_counts.get(
                (str(truth_event["tail_id"]), str(truth_event["flight_id"])),
                0,
            )
            or 0
        )
        progress_denominator = max(flight_window_count - 1, 1)
        abs_progress_delta = float(abs_delta) / float(progress_denominator)
        matched_deltas.append(abs_delta)
        matched_progress_deltas.append(abs_progress_delta)
        matched_rows.append(
            {
                "tail_id": str(truth_event["tail_id"]),
                "flight_id": str(truth_event["flight_id"]),
                "transition_from_label": str(truth_event["transition_from_label"]),
                "transition_to_label": str(truth_event["transition_to_label"]),
                "truth_win_id_center": float(truth_event["win_id_center"]),
                "detected_win_id_center": float(nearest["win_id_center"]),
                "abs_win_id_delta": abs_delta,
                "abs_progress_delta": abs_progress_delta,
            }
        )
    return {
        "status": "ok",
        "truth_transition_event_count": len(truth_events),
        "detected_transition_event_count": len(detected_events),
        "truth_transition_event_counts_by_label_pair": _count_rows(
            truth_counts,
            left_key="transition_from_label",
            right_key="transition_to_label",
        ),
        "detected_transition_event_counts_by_label_pair": _count_rows(
            detected_counts,
            left_key="transition_from_label",
            right_key="transition_to_label",
        ),
        "matched_truth_transition_event_count": int(sum(1 for row in matched_rows if row["detected_win_id_center"] is not None)),
        "mean_abs_win_id_delta": (
            None
            if not matched_deltas
            else float(sum(matched_deltas) / float(len(matched_deltas)))
        ),
        "mean_abs_progress_delta": (
            None
            if not matched_progress_deltas
            else float(sum(matched_progress_deltas) / float(len(matched_progress_deltas)))
        ),
        "nearest_detected_event_by_truth_transition": matched_rows,
    }


def _evaluate_detected_phase_states(
    assignments: list[dict[str, Any]],
    *,
    tail_phase_mapping: dict[str, dict[int, str]],
) -> dict[str, Any]:
    valid = [dict(item) for item in assignments if _clean_text(item.get("truth_phase_state"))]
    if not valid:
        return {
            "status": "skipped",
            "reason": "no truth phase-state assignments available",
            "assignment_count": 0,
        }

    truth_state_counts: Counter[str] = Counter()
    detected_state_counts: Counter[str] = Counter()
    confusion_counts: Counter[tuple[str, str]] = Counter()
    truth_transition_counts: Counter[tuple[str, str]] = Counter()
    detected_transition_counts: Counter[tuple[str, str]] = Counter()
    tp = 0
    fp = 0
    fn = 0
    for item in valid:
        truth_state = _clean_text(item.get("truth_phase_state"))
        detected_state = _clean_text(item.get("phase_state_detected"))
        truth_state_counts[truth_state] += 1
        detected_state_counts[detected_state] += 1
        confusion_counts[(truth_state, detected_state)] += 1
        if truth_state == "transition_region" and detected_state == "transition_region":
            tp += 1
        elif truth_state != "transition_region" and detected_state == "transition_region":
            fp += 1
        elif truth_state == "transition_region" and detected_state != "transition_region":
            fn += 1

        truth_from = _clean_text(item.get("truth_transition_from_label"))
        truth_to = _clean_text(item.get("truth_transition_to_label"))
        if truth_from and truth_to:
            truth_transition_counts[(truth_from, truth_to)] += 1

        tail_id = _clean_text(item.get("tail_id"))
        phase_mapping = tail_phase_mapping.get(tail_id, {})
        detected_from_id = item.get("transition_from_phase_id_detected")
        detected_to_id = item.get("transition_to_phase_id_detected")
        if detected_from_id is None or detected_to_id is None:
            continue
        detected_from_label = _clean_text(phase_mapping.get(int(detected_from_id)))
        detected_to_label = _clean_text(phase_mapping.get(int(detected_to_id)))
        if detected_from_label and detected_to_label:
            detected_transition_counts[(detected_from_label, detected_to_label)] += 1

    precision = float(tp) / float(max(tp + fp, 1))
    recall = float(tp) / float(max(tp + fn, 1))
    f1 = 0.0 if (precision + recall) <= 0.0 else float((2.0 * precision * recall) / (precision + recall))
    return {
        "status": "ok",
        "assignment_count": len(valid),
        "truth_phase_state_counts": dict(sorted(truth_state_counts.items())),
        "detected_phase_state_counts": dict(sorted(detected_state_counts.items())),
        "phase_state_confusion": _count_rows(
            confusion_counts,
            left_key="truth_phase_state",
            right_key="phase_state_detected",
        ),
        "transition_region_precision": precision,
        "transition_region_recall": recall,
        "transition_region_f1": f1,
        "truth_transition_counts_by_label_pair": _count_rows(
            truth_transition_counts,
            left_key="transition_from_label",
            right_key="transition_to_label",
        ),
        "detected_transition_counts_by_label_pair": _count_rows(
            detected_transition_counts,
            left_key="transition_from_label",
            right_key="transition_to_label",
        ),
        "transition_event_alignment": _evaluate_transition_event_alignment(
            valid,
            tail_phase_mapping=tail_phase_mapping,
        ),
    }


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
        assigned_df["truth_phase_label_primary"].fillna("").astype(str).str.strip().astype(bool)
        & assigned_df["truth_phase_state"].fillna("").astype(str).eq("stable")
        & assigned_df["s_w"].apply(_has_vector_values)
    ].copy()
    if assigned_df.empty:
        return {
            "status": "skipped",
            "reason": "no labeled window vectors available",
            "assignment_count": len(assignments),
        }

    truth_label_centroids: list[dict[str, Any]] = []
    for (tail_id, phase_label), group in assigned_df.groupby(
        ["tail_id", "truth_phase_label_primary"],
        dropna=False,
        sort=True,
    ):
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
    feature_names = []
    if not baselines.empty and "feature_names" in baselines.columns:
        raw_feature_names = baselines.iloc[0].get("feature_names")
        if isinstance(raw_feature_names, pd.Series):
            raw_feature_names = raw_feature_names.tolist()
        elif hasattr(raw_feature_names, "tolist") and not isinstance(raw_feature_names, (str, bytes)):
            raw_feature_names = raw_feature_names.tolist()
        if isinstance(raw_feature_names, (list, tuple)):
            feature_names = [str(item) for item in raw_feature_names]
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
                    "distance_by_feature_family": (
                        _distance_by_feature_family(
                            list(detected["s_w_centroid"]),
                            list(truth["s_w_centroid"]),
                            feature_names,
                        )
                        if feature_names
                        else {}
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
        _clean_text(item.get("truth_phase_label_primary"))
        for item in assignments
        if _clean_text(item.get("truth_phase_state")) == "stable" and _clean_text(item.get("truth_phase_label_primary"))
    )
    truth_label_window_counts = Counter(
        _clean_text(item.get("truth_phase_label_primary"))
        for item in assignments
        if _clean_text(item.get("truth_phase_label_primary"))
    )
    excluded_transition_window_counts_by_phase_label = Counter(
        _clean_text(item.get("truth_phase_label_primary"))
        for item in assignments
        if _clean_text(item.get("truth_phase_state")) == "transition_region"
        and _clean_text(item.get("truth_phase_label_primary"))
    )

    return {
        "status": "ok",
        "assignment_count": len(assignments),
        "centroid_vector_column": "s_w",
        "label_assignment_contract": "majority_overlap_label",
        "feature_family_index_ranges": _feature_family_index_ranges(feature_names) if feature_names else {},
        "detected_phase_centroids": detected_phase_centroids,
        "truth_label_centroids": truth_label_centroids,
        "distance_matrix": distance_matrix,
        "nearest_truth_centroid_by_detected": nearest_truth_centroid_by_detected,
        "nearest_truth_centroid_by_detected_and_subset": nearest_truth_centroid_by_detected_and_subset,
        "stable_window_label_counts": dict(sorted(stable_window_label_counts.items())),
        "truth_label_window_counts": dict(sorted(truth_label_window_counts.items())),
        "excluded_transition_window_counts_by_phase_label": dict(sorted(excluded_transition_window_counts_by_phase_label.items())),
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
    summary["transition_state_validation"] = _evaluate_detected_phase_states(
        assignments,
        tail_phase_mapping=_tail_phase_mapping(summary),
    )
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
