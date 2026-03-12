from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import permutations
from typing import Any

import numpy as np

from libs.io.contracts import PhaseAssignmentRow, PhaseBaselineRow, PhaseWindowRow


@dataclass(frozen=True)
class PhaseDetectionPolicy:
    phase_count: int
    stable_drift_quantile: float = 0.35
    max_iter: int = 12
    smoothing_radius: int = 2
    transition_penalty: float = 1.5
    min_dwell_windows: int = 8
    ordered_phase_progression: bool = True

    def detect(self, windows: list[PhaseWindowRow]) -> tuple[list[PhaseAssignmentRow], list[PhaseBaselineRow]]:
        return PhaseStream.from_windows(windows, policy=self).detect()


@dataclass(frozen=True)
class Phase:
    phase_id_detected: int
    phase_state_detected: str
    phase_name_detected: str
    window_count: int
    stable_window_count: int
    dominant_phase_label: str | None
    s_w_centroid: list[float]
    distance_scale: float

    @classmethod
    def from_baseline_row(cls, row: PhaseBaselineRow) -> "Phase":
        return cls(
            phase_id_detected=int(row.get("phase_id_detected", 0) or 0),
            phase_state_detected="stable",
            phase_name_detected=str(row.get("phase_name_detected", f"phase_{int(row.get('phase_id_detected', 0) or 0)}")),
            window_count=int(row.get("window_count", 0) or 0),
            stable_window_count=int(row.get("stable_window_count", 0) or 0),
            dominant_phase_label=(
                str(row.get("dominant_phase_label"))
                if row.get("dominant_phase_label") is not None
                else (
                    str(row.get("phase_label_dominant"))
                    if row.get("phase_label_dominant") is not None
                    else None
                )
            ),
            s_w_centroid=[float(item) for item in row.get("s_w_centroid", [])],
            distance_scale=float(
                row.get("distance_scale", row.get("distance_scale_p90", 0.0)) or 0.0
            ),
        )


@dataclass(frozen=True)
class PhaseBuffer:
    tail_id: str
    windows: list[PhaseWindowRow]

    @property
    def ordered_windows(self) -> list[PhaseWindowRow]:
        import pandas as pd

        return sorted(
            self.windows,
            key=lambda item: (
                str(item.get("flight_id", "")),
                pd.to_datetime(item.get("t_end"), utc=True),
                int(item.get("win_id", 0)),
            ),
        )

    @property
    def assignment_key_set(self) -> set[tuple[str, str, int]]:
        return {
            (str(item.get("tail_id", "")), str(item.get("flight_id", "")), int(item.get("win_id", 0)))
            for item in self.windows
        }

    def ordered_positions_by_flight(self) -> dict[str, list[int]]:
        positions: dict[str, list[int]] = defaultdict(list)
        for pos, item in enumerate(self.ordered_windows):
            positions[str(item.get("flight_id", ""))].append(pos)
        return positions

    def structure_matrix(self) -> np.ndarray:
        return np.asarray(
            [np.asarray(item.get("s_w", []), dtype=float) for item in self.ordered_windows],
            dtype=float,
        )

    def drift_vector(self) -> np.ndarray:
        return np.asarray(
            [float(item.get("drift_magnitude_profiled", 0.0) or 0.0) for item in self.ordered_windows],
            dtype=float,
        )

    def stable_indices(self, *, drift_threshold: float) -> np.ndarray:
        ordered = self.ordered_windows
        if not ordered:
            return np.asarray([], dtype=int)
        drifts = self.drift_vector()
        indices = np.where(drifts <= float(drift_threshold))[0]
        if indices.size == 0:
            return np.arange(len(ordered), dtype=int)
        return indices

    def seed_centroid_indices(self, *, k_eff: int, stable_indices: np.ndarray) -> list[int]:
        if int(k_eff) <= 0 or stable_indices.size == 0:
            return []
        ordered_stable = list(stable_indices.tolist())
        seed_positions = np.linspace(0, len(ordered_stable) - 1, num=int(k_eff), dtype=int)
        return [ordered_stable[int(pos)] for pos in seed_positions]


@dataclass(frozen=True)
class PhaseClustering:
    @staticmethod
    def robust_scale_vectors(vectors: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        if vectors.ndim != 2 or vectors.shape[0] == 0:
            width = vectors.shape[1] if vectors.ndim == 2 else 0
            return vectors, np.zeros((width,), dtype=float), np.ones((width,), dtype=float)
        medians = np.median(vectors, axis=0)
        mads = np.median(np.abs(vectors - medians[None, :]), axis=0)
        scales = np.where(mads > 1e-6, mads, 1.0)
        return (vectors - medians[None, :]) / scales[None, :], medians.astype(float), scales.astype(float)

    def fit(
        self,
        *,
        buffer: PhaseBuffer,
        policy: PhaseDetectionPolicy,
    ) -> tuple[list[PhaseWindowRow], np.ndarray, np.ndarray, float, np.ndarray, np.ndarray, dict[int, int], dict[int, float], int]:
        items = buffer.ordered_windows
        vectors = buffer.structure_matrix()
        target_k = max(int(policy.phase_count), 1)
        drift_q = min(max(float(policy.stable_drift_quantile), 0.0), 1.0)
        scaled_vectors, _, _ = self.robust_scale_vectors(vectors)
        drifts = buffer.drift_vector()
        drift_threshold = float(np.quantile(drifts, drift_q)) if len(drifts) > 0 else 0.0
        stable_indices = buffer.stable_indices(drift_threshold=drift_threshold)
        stable_vectors = scaled_vectors[stable_indices]
        k_eff = min(target_k, len(stable_vectors))
        centroid_indices = buffer.seed_centroid_indices(k_eff=k_eff, stable_indices=stable_indices)
        centroids = np.asarray([scaled_vectors[idx] for idx in centroid_indices], dtype=float)
        centroids = self._refine_centroids(stable_vectors, centroids, policy=policy)
        all_distances = np.linalg.norm(scaled_vectors[:, None, :] - centroids[None, :, :], axis=2)
        all_assignments = np.argmin(all_distances, axis=1)
        cluster_remap = self._cluster_order_remap(items, all_assignments, k_eff)
        distance_scales = self._distance_scales(stable_vectors, centroids)
        return (
            items,
            drifts,
            centroids,
            drift_threshold,
            all_distances,
            all_assignments,
            cluster_remap,
            distance_scales,
            k_eff,
        )

    def _refine_centroids(self, stable_vectors: np.ndarray, centroids: np.ndarray, *, policy: PhaseDetectionPolicy) -> np.ndarray:
        if len(stable_vectors) == 0 or len(centroids) == 0:
            return centroids
        stable_assignments = np.zeros(len(stable_vectors), dtype=int)
        for _ in range(max(int(policy.max_iter), 1)):
            distances = np.linalg.norm(stable_vectors[:, None, :] - centroids[None, :, :], axis=2)
            next_assignments = np.argmin(distances, axis=1)
            if np.array_equal(next_assignments, stable_assignments):
                break
            stable_assignments = next_assignments
            next_centroids: list[np.ndarray] = []
            for cluster_idx in range(len(centroids)):
                members = stable_vectors[stable_assignments == cluster_idx]
                next_centroids.append(np.mean(members, axis=0) if len(members) else centroids[cluster_idx])
            centroids = np.asarray(next_centroids, dtype=float)
        return centroids

    def _cluster_order_remap(
        self,
        items: list[PhaseWindowRow],
        all_assignments: np.ndarray,
        k_eff: int,
    ) -> dict[int, int]:
        import pandas as pd

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
        return {cluster_idx: phase_id for phase_id, cluster_idx in enumerate(cluster_order)}

    def _distance_scales(self, stable_vectors: np.ndarray, centroids: np.ndarray) -> dict[int, float]:
        distances = np.linalg.norm(stable_vectors[:, None, :] - centroids[None, :, :], axis=2)
        assignments = np.argmin(distances, axis=1) if len(stable_vectors) else np.asarray([], dtype=int)
        distance_scales: dict[int, float] = {}
        for cluster_idx in range(len(centroids)):
            members = np.where(assignments == cluster_idx)[0]
            if members.size == 0:
                distance_scales[cluster_idx] = 1.0
                continue
            member_distances = np.linalg.norm(stable_vectors[members] - centroids[cluster_idx], axis=1)
            distance_scales[cluster_idx] = max(float(np.quantile(member_distances, 0.9)), 1e-6)
        return distance_scales

    def emit_rows(
        self,
        *,
        buffer: PhaseBuffer,
        items: list[PhaseWindowRow],
        drifts: np.ndarray,
        drift_threshold: float,
        centroids: np.ndarray,
        smoothed_assignments: np.ndarray,
        all_distances: np.ndarray,
        distance_scales: dict[int, float],
        cluster_remap: dict[int, int],
        k_eff: int,
    ) -> tuple[list[PhaseAssignmentRow], list[PhaseBaselineRow]]:
        assignments: list[PhaseAssignmentRow] = []
        baselines: list[PhaseBaselineRow] = []
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
                    "tail_id": buffer.tail_id,
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
                    "tail_id": buffer.tail_id,
                    "phase_id_detected": phase_id_detected,
                    "centroid": [float(value) for value in centroids[raw_cluster_idx].tolist()],
                    "distance_scale_p90": float(distance_scales.get(raw_cluster_idx, 1.0)),
                    "window_count": int(counts_by_cluster.get(phase_id_detected, 0)),
                    "stable_window_count": int(stable_counts_by_cluster.get(phase_id_detected, 0)),
                    "phase_label_dominant": dominant_label,
                    "phase_label_dominant_count": int(dominant_label_count),
                }
            )
        return assignments, baselines


@dataclass(frozen=True)
class PhaseClusterAssignment:
    policy: PhaseDetectionPolicy

    def assign(
        self,
        *,
        buffer: PhaseBuffer,
        all_assignments: np.ndarray,
        all_distances: np.ndarray,
        distance_scales: dict[int, float],
        k_eff: int,
    ) -> np.ndarray:
        smoothed_assignments = np.asarray(all_assignments, dtype=int).copy()
        radius = max(int(self.policy.smoothing_radius), 0)
        positions_by_flight = buffer.ordered_positions_by_flight()
        if radius > 0:
            for positions in positions_by_flight.values():
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
                    if neighborhood:
                        smoothed_assignments[pos] = int(Counter(neighborhood).most_common(1)[0][0])

        for positions in positions_by_flight.values():
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
            seq_assignments = self._sequence_assignments(costs, smoothed_assignments, positions, k_eff)
            seq_assignments = self._enforce_min_dwell(seq_assignments, costs, k_eff)
            for offset, pos in enumerate(positions):
                smoothed_assignments[pos] = int(seq_assignments[offset])
        return smoothed_assignments

    def _sequence_assignments(
        self,
        costs: list[list[float]],
        smoothed_assignments: np.ndarray,
        positions: list[int],
        k_eff: int,
    ) -> list[int]:
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
                    if bool(self.policy.ordered_phase_progression) and prev_idx > cluster_idx:
                        continue
                    if bool(self.policy.ordered_phase_progression) and (cluster_idx - prev_idx) > 1:
                        continue
                    switch_cost = 0.0 if prev_idx == cluster_idx else float(self.policy.transition_penalty)
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
        return seq_assignments

    def _enforce_min_dwell(self, seq_assignments: list[int], costs: list[list[float]], k_eff: int) -> list[int]:
        seq_len = len(seq_assignments)
        dwell = max(int(self.policy.min_dwell_windows), 1)
        dwell = min(dwell, max(seq_len // max(2 * k_eff, 1), 1))
        changed = True
        while changed and seq_len > 1:
            changed = False
            start = 0
            while start < seq_len:
                end = start + 1
                while end < seq_len and seq_assignments[end] == seq_assignments[start]:
                    end += 1
                if (end - start) < dwell:
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
        return seq_assignments


@dataclass(frozen=True)
class PhaseStream:
    policy: PhaseDetectionPolicy
    buffers_by_tail: dict[str, PhaseBuffer]
    clustering: PhaseClustering
    assignment: PhaseClusterAssignment

    @classmethod
    def from_windows(cls, windows: list[PhaseWindowRow], *, policy: PhaseDetectionPolicy) -> "PhaseStream":
        by_tail: dict[str, list[PhaseWindowRow]] = defaultdict(list)
        for window in windows:
            tail_id = str(window.get("tail_id", ""))
            if tail_id:
                by_tail[tail_id].append(dict(window))
        return cls(
            policy=policy,
            buffers_by_tail={tail_id: PhaseBuffer(tail_id=tail_id, windows=items) for tail_id, items in by_tail.items()},
            clustering=PhaseClustering(),
            assignment=PhaseClusterAssignment(policy=policy),
        )

    def detect(self) -> tuple[list[PhaseAssignmentRow], list[PhaseBaselineRow]]:
        assignments: list[PhaseAssignmentRow] = []
        baselines: list[PhaseBaselineRow] = []
        for tail_id in sorted(self.buffers_by_tail.keys()):
            buffer = self.buffers_by_tail[tail_id]
            items = buffer.ordered_windows
            if not items:
                continue
            vectors = buffer.structure_matrix()
            if vectors.ndim != 2 or vectors.shape[1] == 0:
                assignments.extend(
                    [
                        {
                            "tail_id": buffer.tail_id,
                            "flight_id": str(item.get("flight_id", "")),
                            "win_id": int(item.get("win_id", 0)),
                            "phase_id_detected": 0,
                            "phase_state_detected": "unknown",
                            "phase_confidence_detected": 0.0,
                            "distance_to_centroid_detected": None,
                            "phase_label": item.get("phase_label"),
                        }
                        for item in items
                    ]
                )
                continue
            (
                clustered_items,
                drifts,
                centroids,
                drift_threshold,
                all_distances,
                all_assignments,
                cluster_remap,
                distance_scales,
                k_eff,
            ) = self.clustering.fit(buffer=buffer, policy=self.policy)
            smoothed_assignments = self.assignment.assign(
                buffer=buffer,
                all_assignments=all_assignments,
                all_distances=all_distances,
                distance_scales=distance_scales,
                k_eff=k_eff,
            )
            tail_assignments, tail_baselines = self.clustering.emit_rows(
                buffer=buffer,
                items=clustered_items,
                drifts=drifts,
                drift_threshold=drift_threshold,
                centroids=centroids,
                smoothed_assignments=smoothed_assignments,
                all_distances=all_distances,
                distance_scales=distance_scales,
                cluster_remap=cluster_remap,
                k_eff=k_eff,
            )
            assignments.extend(tail_assignments)
            baselines.extend(tail_baselines)
        assignments.sort(key=lambda item: (str(item["tail_id"]), str(item["flight_id"]), int(item["win_id"])))
        baselines.sort(key=lambda item: (str(item["tail_id"]), int(item["phase_id_detected"])))
        return assignments, baselines

    def phases(self) -> list[Phase]:
        _, baselines = self.detect()
        return [Phase.from_baseline_row(row) for row in baselines]
