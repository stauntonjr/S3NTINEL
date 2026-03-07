"""V2-style scoring over window_s and backbone residuals."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np

from libs.common import PhaseAssignmentRow, PhaseBaselineRow, PhaseWindowRow, WindowScoreRow

def build_phase_score_baselines(
    window_s_rows: list[PhaseWindowRow],
    phase_assignments: list[PhaseAssignmentRow],
    *,
    min_stable_confidence: float = 0.5,
) -> list[PhaseBaselineRow]:
    assignment_by_key = {
        (
            str(item.get("tail_id", "")),
            str(item.get("flight_id", "")),
            int(item.get("win_id", 0)),
        ): dict(item)
        for item in phase_assignments
    }

    grouped: dict[tuple[str, int], dict[str, list[Any]]] = defaultdict(lambda: {"s_w": [], "reconstruction": [], "distance": []})
    for row in window_s_rows:
        key = (
            str(row.get("tail_id", "")),
            str(row.get("flight_id", "")),
            int(row.get("win_id", 0)),
        )
        assignment = assignment_by_key.get(key)
        if assignment is None:
            continue
        if str(assignment.get("phase_state_detected", "")) != "stable":
            continue
        if float(assignment.get("phase_confidence_detected", 0.0) or 0.0) < float(min_stable_confidence):
            continue
        bucket = (str(row.get("tail_id", "")), int(assignment.get("phase_id_detected", 0)))
        grouped[bucket]["s_w"].append(np.asarray(row.get("s_w", []), dtype=float))
        grouped[bucket]["reconstruction"].append(float(row.get("backbone_reconstruction_error", 0.0) or 0.0))
        grouped[bucket]["distance"].append(float(assignment.get("distance_to_centroid_detected", 0.0) or 0.0))

    baselines: list[PhaseBaselineRow] = []
    for (tail_id, phase_id_detected), values in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        if not values["s_w"]:
            continue
        s_mat = np.vstack(values["s_w"])
        s_centroid = np.mean(s_mat, axis=0)
        recon = np.asarray(values["reconstruction"], dtype=float)
        dist = np.asarray(values["distance"], dtype=float)
        baselines.append(
            {
                "tail_id": tail_id,
                "phase_id_detected": int(phase_id_detected),
                "s_w_centroid": [float(item) for item in s_centroid.tolist()],
                "reconstruction_median": float(np.median(recon)) if recon.size > 0 else 0.0,
                "reconstruction_mad": float(np.median(np.abs(recon - np.median(recon)))) if recon.size > 0 else 0.0,
                "distance_median": float(np.median(dist)) if dist.size > 0 else 0.0,
                "distance_mad": float(np.median(np.abs(dist - np.median(dist)))) if dist.size > 0 else 0.0,
                "stable_window_count": int(len(values["s_w"])),
            }
        )
    return baselines


def score_window_s_rows(
    window_s_rows: list[PhaseWindowRow],
    phase_assignments: list[PhaseAssignmentRow],
    phase_score_baselines: list[PhaseBaselineRow],
) -> list[WindowScoreRow]:
    assignment_by_key = {
        (
            str(item.get("tail_id", "")),
            str(item.get("flight_id", "")),
            int(item.get("win_id", 0)),
        ): dict(item)
        for item in phase_assignments
    }
    baseline_by_key = {
        (str(item.get("tail_id", "")), int(item.get("phase_id_detected", 0))): dict(item)
        for item in phase_score_baselines
    }

    rows: list[WindowScoreRow] = []
    for row in window_s_rows:
        key = (
            str(row.get("tail_id", "")),
            str(row.get("flight_id", "")),
            int(row.get("win_id", 0)),
        )
        assignment = assignment_by_key.get(key)
        if assignment is None:
            continue
        phase_id_detected = int(assignment.get("phase_id_detected", 0))
        baseline = baseline_by_key.get((str(row.get("tail_id", "")), phase_id_detected))

        reconstruction_error = float(row.get("backbone_reconstruction_error", 0.0) or 0.0)
        if baseline is None:
            structure_score = None
            reconstruction_score = reconstruction_error
            global_score = reconstruction_error
        else:
            centroid = np.asarray(baseline.get("s_w_centroid", []), dtype=float)
            s_w = np.asarray(row.get("s_w", []), dtype=float)
            structure_distance = float(np.linalg.norm(s_w - centroid))
            dist_med = float(baseline.get("distance_median", 0.0) or 0.0)
            dist_mad = max(float(baseline.get("distance_mad", 0.0) or 0.0), 1e-6)
            recon_med = float(baseline.get("reconstruction_median", 0.0) or 0.0)
            recon_mad = max(float(baseline.get("reconstruction_mad", 0.0) or 0.0), 1e-6)
            structure_score = max(0.0, (structure_distance - dist_med) / dist_mad)
            reconstruction_score = max(0.0, (reconstruction_error - recon_med) / recon_mad)
            global_score = (structure_score + reconstruction_score) / 2.0

        if global_score >= 6.0:
            severity = "high"
        elif global_score >= 3.0:
            severity = "medium"
        elif global_score > 1.0:
            severity = "low"
        else:
            severity = "normal"

        rows.append(
            {
                "tail_id": str(row.get("tail_id", "")),
                "flight_id": str(row.get("flight_id", "")),
                "win_id": int(row.get("win_id", 0)),
                "phase_id_detected": phase_id_detected,
                "phase_state_detected": str(assignment.get("phase_state_detected", "")),
                "phase_confidence_detected": float(assignment.get("phase_confidence_detected", 0.0) or 0.0),
                "reconstruction_score": float(reconstruction_score),
                "structure_score": None if structure_score is None else float(structure_score),
                "global_score": float(global_score),
                "severity": severity,
            }
        )
    return rows
