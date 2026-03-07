"""V2-style phase detection over per-window structure vectors."""

from __future__ import annotations

from collections import Counter, defaultdict
from itertools import permutations
from typing import Any

import numpy as np
import pandas as pd

from libs.common import PhaseAssignmentRow, PhaseBaselineRow, PhaseWindowRow
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES


def _robust_scale_vectors(vectors: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Scale structure features by per-dimension median/MAD to reduce dominance by broad-range summaries."""
    if vectors.ndim != 2 or vectors.shape[0] == 0:
        return vectors, np.zeros((vectors.shape[1] if vectors.ndim == 2 else 0,), dtype=float), np.ones((vectors.shape[1] if vectors.ndim == 2 else 0,), dtype=float)
    medians = np.median(vectors, axis=0)
    mads = np.median(np.abs(vectors - medians[None, :]), axis=0)
    scales = np.where(mads > 1e-6, mads, 1.0)
    return (vectors - medians[None, :]) / scales[None, :], medians.astype(float), scales.astype(float)


def build_structure_vectors(
    windows: list[PhaseWindowRow],
    *,
    selected_sensors: list[str],
    selected_event_types: list[str] | None = None,
    selected_categorical_state_pairs: list[tuple[str, str]] | None = None,
) -> tuple[list[PhaseWindowRow], list[str]]:
    """Build compact per-window structure vectors from continuous and event summaries."""
    event_types = [str(item) for item in (selected_event_types or []) if str(item)]
    state_pairs = [
        (str(parameter_name), str(state))
        for parameter_name, state in (selected_categorical_state_pairs or [])
        if str(parameter_name) and str(state)
    ]
    feature_names = [f"parameter_name::{parameter_name}" for parameter_name in selected_sensors] + [
        f"event_type::{event_type}" for event_type in event_types
    ] + [
        f"categorical::{parameter_name}={state}" for parameter_name, state in state_pairs
    ] + [
        "summary::event_density_hz",
        "summary::continuous_event_fraction",
        "summary::categorical_event_fraction",
        "summary::active_sensor_fraction",
    ]

    structured: list[PhaseWindowRow] = []
    for window in windows:
        scaled = window.get("continuous_vector_t_end_scaled")
        if not isinstance(scaled, dict):
            scaled = {}
        event_counts = window.get("event_type_counts")
        if not isinstance(event_counts, dict):
            event_counts = {}
        categorical_t_end = window.get("categorical_state_t_end")
        if not isinstance(categorical_t_end, dict):
            categorical_t_end = {}
        event_total = max(int(window.get("event_count", 0) or 0), 0)
        duration_ms = max(int(window.get("duration_ms", 0) or 0), 1)
        duration_s = float(duration_ms) / 1000.0

        vector: list[float] = []
        for parameter_name in selected_sensors:
            vector.append(float(scaled.get(parameter_name, 0.0) or 0.0))
        for event_type in event_types:
            count = float(event_counts.get(event_type, 0) or 0.0)
            vector.append(count / float(max(event_total, 1)))
        for parameter_name, state in state_pairs:
            vector.append(1.0 if str(categorical_t_end.get(parameter_name, "")) == state else 0.0)
        continuous_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CONTINUOUS_EVENT_TYPES))
        categorical_count = float(sum(int(event_counts.get(item, 0) or 0) for item in CATEGORICAL_EVENT_TYPES))
        active_sensor_fraction = float(len(scaled)) / float(max(len(selected_sensors), 1))
        vector.extend(
            [
                float(event_total) / float(max(duration_s, 1e-6)),
                continuous_count / float(max(event_total, 1)),
                categorical_count / float(max(event_total, 1)),
                active_sensor_fraction,
            ]
        )

        enriched = dict(window)
        enriched["s_w"] = vector
        structured.append(enriched)
    return structured, feature_names


def detect_phases_from_windows(
    windows: list[PhaseWindowRow],
    *,
    phase_count: int,
    stable_drift_quantile: float = 0.35,
    max_iter: int = 12,
    smoothing_radius: int = 2,
    transition_penalty: float = 1.5,
    min_dwell_windows: int = 8,
    ordered_phase_progression: bool = True,
) -> tuple[list[PhaseAssignmentRow], list[PhaseBaselineRow]]:
    """Fit per-tail centroids on low-drift windows and assign detected phases."""
    target_k = max(int(phase_count), 1)
    drift_q = min(max(float(stable_drift_quantile), 0.0), 1.0)

    by_tail: dict[str, list[PhaseWindowRow]] = defaultdict(list)
    for window in windows:
        tail_id = str(window.get("tail_id", ""))
        if tail_id:
            by_tail[tail_id].append(dict(window))

    assignments: list[PhaseAssignmentRow] = []
    baselines: list[PhaseBaselineRow] = []

    for tail_id in sorted(by_tail.keys()):
        items = sorted(
            by_tail[tail_id],
            key=lambda item: (
                str(item.get("flight_id", "")),
                pd.to_datetime(item.get("t_end"), utc=True),
                int(item.get("win_id", 0)),
            ),
        )
        if not items:
            continue

        vectors = np.asarray([np.asarray(item.get("s_w", []), dtype=float) for item in items], dtype=float)
        if vectors.ndim != 2 or vectors.shape[1] == 0:
            for item in items:
                assignments.append(
                    {
                        "tail_id": tail_id,
                        "flight_id": str(item.get("flight_id", "")),
                        "win_id": int(item.get("win_id", 0)),
                        "phase_id_detected": 0,
                        "phase_state_detected": "unknown",
                        "phase_confidence_detected": 0.0,
                        "distance_to_centroid_detected": None,
                        "phase_label": item.get("phase_label"),
                    }
                )
            continue

        scaled_vectors, _, _ = _robust_scale_vectors(vectors)

        drifts = np.asarray([float(item.get("drift_magnitude_profiled", 0.0) or 0.0) for item in items], dtype=float)
        drift_threshold = float(np.quantile(drifts, drift_q)) if len(drifts) > 0 else 0.0
        stable_indices = np.where(drifts <= drift_threshold)[0]
        if stable_indices.size == 0:
            stable_indices = np.arange(len(items), dtype=int)

        stable_vectors = scaled_vectors[stable_indices]
        k_eff = min(target_k, len(stable_vectors))
        ordered_stable = list(stable_indices.tolist())
        seed_positions = np.linspace(0, len(ordered_stable) - 1, num=k_eff, dtype=int)
        centroid_indices = [ordered_stable[int(pos)] for pos in seed_positions]
        centroids = np.asarray([scaled_vectors[idx] for idx in centroid_indices], dtype=float)

        stable_assignments = np.zeros(len(stable_vectors), dtype=int)
        for _ in range(max(int(max_iter), 1)):
            distances = np.linalg.norm(stable_vectors[:, None, :] - centroids[None, :, :], axis=2)
            next_assignments = np.argmin(distances, axis=1)
            if np.array_equal(next_assignments, stable_assignments):
                break
            stable_assignments = next_assignments
            next_centroids: list[np.ndarray] = []
            for cluster_idx in range(k_eff):
                members = stable_vectors[stable_assignments == cluster_idx]
                if len(members) == 0:
                    next_centroids.append(centroids[cluster_idx])
                else:
                    next_centroids.append(np.mean(members, axis=0))
            centroids = np.asarray(next_centroids, dtype=float)

        all_distances = np.linalg.norm(scaled_vectors[:, None, :] - centroids[None, :, :], axis=2)
        all_assignments = np.argmin(all_distances, axis=1)

        cluster_order_basis: list[tuple[int, pd.Timestamp, int]] = []
        for cluster_idx in range(k_eff):
            member_positions = np.where(all_assignments == cluster_idx)[0]
            if member_positions.size == 0:
                continue
            first_item = items[int(member_positions.min())]
            cluster_order_basis.append(
                (
                    cluster_idx,
                    pd.to_datetime(first_item.get("t_end"), utc=True),
                    int(first_item.get("win_id", 0)),
                )
            )
        cluster_order = [item[0] for item in sorted(cluster_order_basis, key=lambda item: (item[1], item[2]))]
        cluster_remap = {cluster_idx: phase_id for phase_id, cluster_idx in enumerate(cluster_order)}

        distance_scales: dict[int, float] = {}
        for cluster_idx in range(k_eff):
            members = np.where(stable_assignments == cluster_idx)[0]
            if members.size == 0:
                distance_scales[cluster_idx] = 1.0
                continue
            member_distances = np.linalg.norm(stable_vectors[members] - centroids[cluster_idx], axis=1)
            distance_scales[cluster_idx] = max(float(np.quantile(member_distances, 0.9)), 1e-6)

        smoothed_assignments = np.asarray(all_assignments, dtype=int).copy()
        radius = max(int(smoothing_radius), 0)
        if radius > 0 and len(items) > 0:
            flight_positions: dict[str, list[int]] = defaultdict(list)
            for pos, item in enumerate(items):
                flight_positions[str(item.get("flight_id", ""))].append(pos)
            for positions in flight_positions.values():
                if len(positions) <= 1:
                    continue
                raw_ids = [int(all_assignments[pos]) for pos in positions]
                raw_conf = []
                for pos, cluster_idx in zip(positions, raw_ids, strict=False):
                    dist = float(all_distances[pos, cluster_idx])
                    scale = max(float(distance_scales.get(cluster_idx, 1.0)), 1e-6)
                    raw_conf.append(max(0.0, 1.0 - (dist / scale)))
                for offset, pos in enumerate(positions):
                    if raw_conf[offset] >= 0.5:
                        continue
                    lo = max(0, offset - radius)
                    hi = min(len(positions), offset + radius + 1)
                    neighborhood = raw_ids[lo:hi]
                    if not neighborhood:
                        continue
                    winner = Counter(neighborhood).most_common(1)[0][0]
                    smoothed_assignments[pos] = int(winner)

        # Sequence-aware phase assignment: penalize transitions so contiguous windows prefer coherent segments.
        flight_positions_seq: dict[str, list[int]] = defaultdict(list)
        for pos, item in enumerate(items):
            flight_positions_seq[str(item.get("flight_id", ""))].append(pos)
        for positions in flight_positions_seq.values():
            if not positions:
                continue
            costs = []
            for pos in positions:
                row_costs: list[float] = []
                for cluster_idx in range(k_eff):
                    scale = max(float(distance_scales.get(cluster_idx, 1.0)), 1e-6)
                    dist = float(all_distances[pos, cluster_idx])
                    row_costs.append(dist / scale)
                costs.append(row_costs)

            seq_len = len(positions)
            dp = np.full((seq_len, k_eff), np.inf, dtype=float)
            back = np.full((seq_len, k_eff), -1, dtype=int)
            for cluster_idx in range(k_eff):
                prior_bonus = 0.0 if cluster_idx == int(smoothed_assignments[positions[0]]) else 0.15
                dp[0, cluster_idx] = float(costs[0][cluster_idx]) + prior_bonus
            for t in range(1, seq_len):
                for cluster_idx in range(k_eff):
                    best_prev = 0
                    best_score = np.inf
                    for prev_idx in range(k_eff):
                        if bool(ordered_phase_progression) and prev_idx > cluster_idx:
                            continue
                        if bool(ordered_phase_progression) and (cluster_idx - prev_idx) > 1:
                            continue
                        switch_cost = 0.0 if prev_idx == cluster_idx else float(transition_penalty)
                        cand = float(dp[t - 1, prev_idx]) + switch_cost + float(costs[t][cluster_idx])
                        if cand < best_score:
                            best_score = cand
                            best_prev = prev_idx
                    dp[t, cluster_idx] = best_score
                    back[t, cluster_idx] = best_prev

            seq_assignments = [0] * seq_len
            seq_assignments[-1] = int(np.argmin(dp[-1, :]))
            for t in range(seq_len - 1, 0, -1):
                seq_assignments[t - 1] = int(back[t, seq_assignments[t]])

            # Minimum-dwell enforcement: collapse short runs into neighboring segments when possible.
            dwell = max(int(min_dwell_windows), 1)
            dwell = min(dwell, max(seq_len // max(2 * k_eff, 1), 1))
            changed = True
            while changed and seq_len > 1:
                changed = False
                start = 0
                while start < seq_len:
                    end = start + 1
                    while end < seq_len and seq_assignments[end] == seq_assignments[start]:
                        end += 1
                    run_len = end - start
                    if run_len < dwell:
                        left_state = seq_assignments[start - 1] if start > 0 else None
                        right_state = seq_assignments[end] if end < seq_len else None
                        replacement = None
                        if left_state is not None and right_state is not None:
                            left_cost = sum(float(costs[idx][left_state]) for idx in range(start, end))
                            right_cost = sum(float(costs[idx][right_state]) for idx in range(start, end))
                            replacement = left_state if left_cost <= right_cost else right_state
                        elif left_state is not None:
                            replacement = left_state
                        elif right_state is not None:
                            replacement = right_state
                        if replacement is not None and replacement != seq_assignments[start]:
                            for idx in range(start, end):
                                seq_assignments[idx] = int(replacement)
                            changed = True
                            break
                    start = end

            for offset, pos in enumerate(positions):
                smoothed_assignments[pos] = int(seq_assignments[offset])

        counts_by_cluster: Counter[int] = Counter()
        stable_counts_by_cluster: Counter[int] = Counter()
        label_by_cluster: dict[int, Counter[str]] = defaultdict(Counter)

        for idx, item in enumerate(items):
            cluster_idx = int(smoothed_assignments[idx])
            phase_id_detected = int(cluster_remap.get(cluster_idx, cluster_idx))
            distance = float(all_distances[idx, cluster_idx])
            scale = float(distance_scales.get(cluster_idx, 1.0))
            confidence = max(0.0, 1.0 - (distance / scale))
            is_stable = bool(drifts[idx] <= drift_threshold and distance <= scale)
            phase_state = "stable" if is_stable else "transition_region"
            phase_label = item.get("phase_label")
            if phase_label is not None:
                label_by_cluster[phase_id_detected][str(phase_label)] += 1

            counts_by_cluster[phase_id_detected] += 1
            if is_stable:
                stable_counts_by_cluster[phase_id_detected] += 1

            assignments.append(
                {
                    "tail_id": tail_id,
                    "flight_id": str(item.get("flight_id", "")),
                    "win_id": int(item.get("win_id", 0)),
                    "t_end": item.get("t_end"),
                    "phase_id_detected": phase_id_detected,
                    "phase_state_detected": phase_state,
                    "phase_confidence_detected": float(confidence),
                    "distance_to_centroid_detected": float(distance),
                    "phase_label": phase_label,
                }
            )

        for raw_cluster_idx in range(k_eff):
            phase_id_detected = int(cluster_remap.get(raw_cluster_idx, raw_cluster_idx))
            dominant_label = None
            dominant_label_count = 0
            if label_by_cluster.get(phase_id_detected):
                dominant_label, dominant_label_count = label_by_cluster[phase_id_detected].most_common(1)[0]
            baselines.append(
                {
                    "tail_id": tail_id,
                    "phase_id_detected": phase_id_detected,
                    "centroid": [float(value) for value in centroids[raw_cluster_idx].tolist()],
                    "distance_scale_p90": float(distance_scales.get(raw_cluster_idx, 1.0)),
                    "window_count": int(counts_by_cluster.get(phase_id_detected, 0)),
                    "stable_window_count": int(stable_counts_by_cluster.get(phase_id_detected, 0)),
                    "phase_label_dominant": dominant_label,
                    "phase_label_dominant_count": int(dominant_label_count),
                }
            )

    assignments.sort(key=lambda item: (str(item["tail_id"]), str(item["flight_id"]), int(item["win_id"])))
    baselines.sort(key=lambda item: (str(item["tail_id"]), int(item["phase_id_detected"])))
    return assignments, baselines


def evaluate_detected_phases(assignments: list[dict[str, Any]]) -> dict[str, Any]:
    """Evaluate detected phases against simulator labels using best one-to-one tail-local mapping."""
    valid = [dict(item) for item in assignments if str(item.get("phase_label", "")).strip()]
    if not valid:
        return {"overall_accuracy": None, "by_tail": [], "by_phase_label": []}

    by_tail: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in valid:
        by_tail[str(item.get("tail_id", ""))].append(item)

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
        by_flight: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for item in items:
            by_flight[str(item.get("flight_id", ""))].append(item)
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
    for label in labels:
        tp = int(tp_counts_global.get(label, 0))
        label_count = int(label_counts_global.get(label, 0))
        pred_count = int(pred_counts_global.get(label, 0))
        precision = float(tp) / float(max(pred_count, 1))
        recall = float(tp) / float(max(label_count, 1))
        by_phase_label.append(
            {
                "phase_label": label,
                "label_count": label_count,
                "detected_count": pred_count,
                "tp": tp,
                "precision": precision,
                "recall": recall,
            }
        )

    return {
        "overall_accuracy": float(total_correct) / float(max(total_count, 1)),
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
