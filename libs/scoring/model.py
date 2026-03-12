from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from libs.io.contracts import PhaseAssignmentRow, PhaseBaselineRow, PhaseWindowRow, WindowScoreRow
from libs.scoring.window_scores import score_window_s_rows


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass
    if isinstance(value, (str, bytes, dict)):
        return []
    if hasattr(value, "tolist"):
        try:
            return list(value.tolist())
        except Exception:
            pass
    if hasattr(value, "__iter__"):
        return list(value)
    return []


def build_parameter_to_subsystem_map(hierarchy_rows: list[dict[str, Any]]) -> dict[str, str]:
    if not hierarchy_rows:
        return {}
    first_row = hierarchy_rows[0]
    parameter_col = "parameter_name" if "parameter_name" in first_row else ("sensor" if "sensor" in first_row else None)
    if parameter_col is None:
        return {}

    mapping: dict[str, str] = {}
    for row in hierarchy_rows:
        parameter_name = str(row.get(parameter_col, "")).strip()
        subsystem_id = str(row.get("subsystem_id", "")).strip()
        if parameter_name and subsystem_id:
            mapping[parameter_name] = subsystem_id
    return mapping


@dataclass(frozen=True)
class WindowScoreArtifacts:
    rows: list[WindowScoreRow]

    @classmethod
    def from_phase_rows(
        cls,
        phase_window_rows: list[dict[str, Any]],
        phase_baseline_rows: list[dict[str, Any]],
        hierarchy_rows: list[dict[str, Any]],
    ) -> "WindowScoreArtifacts":
        parameter_to_subsystem = build_parameter_to_subsystem_map(hierarchy_rows)

        window_rows: list[PhaseWindowRow] = []
        assignment_rows: list[PhaseAssignmentRow] = []
        for row in phase_window_rows:
            window_rows.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0) or 0),
                    "s_w": _as_list(row.get("s_w")),
                    "backbone_reconstruction_error": float(row.get("backbone_reconstruction_error", 0.0) or 0.0),
                    "backbone_residual_by_parameter": dict(row.get("backbone_residual_by_parameter", {}) or {}),
                }
            )
            assignment_rows.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0) or 0),
                    "phase_id_detected": int(row.get("phase_id_detected", 0) or 0),
                    "phase_state_detected": str(row.get("phase_state_detected", "")),
                    "phase_confidence_detected": float(row.get("phase_confidence_detected", 0.0) or 0.0),
                    "distance_to_centroid_detected": float(row.get("distance_to_centroid_detected", 0.0) or 0.0),
                }
            )

        baseline_rows: list[PhaseBaselineRow] = [
            {
                "tail_id": str(row.get("tail_id", "")),
                "phase_id_detected": int(row.get("phase_id_detected", 0) or 0),
                "s_w_centroid": _as_list(row.get("s_w_centroid")),
                "reconstruction_median": float(row.get("reconstruction_median", 0.0) or 0.0),
                "reconstruction_mad": float(row.get("reconstruction_mad", 0.0) or 0.0),
                "distance_median": float(row.get("distance_median", 0.0) or 0.0),
                "distance_mad": float(row.get("distance_mad", 0.0) or 0.0),
            }
            for row in phase_baseline_rows
        ]

        scored_rows = score_window_s_rows(window_rows, assignment_rows, baseline_rows)
        phase_row_by_key = {
            (str(row.get("tail_id", "")), str(row.get("flight_id", "")), int(row.get("win_id", 0) or 0)): row
            for row in phase_window_rows
        }
        output: list[WindowScoreRow] = []
        for row in scored_rows:
            key = (str(row.get("tail_id", "")), str(row.get("flight_id", "")), int(row.get("win_id", 0) or 0))
            phase_row = phase_row_by_key.get(key, {})
            structure_score = row.get("structure_score")
            reconstruction_score = float(row.get("reconstruction_score", 0.0) or 0.0)
            dominant_score_component = (
                "structure"
                if (structure_score is not None and float(structure_score) >= reconstruction_score)
                else "reconstruction"
            )
            residual_by_parameter = dict(phase_row.get("backbone_residual_by_parameter", {}) or {})
            subsystem_weight_by_id: dict[str, float] = {}
            for parameter_name, residual in residual_by_parameter.items():
                subsystem_id = parameter_to_subsystem.get(str(parameter_name))
                if subsystem_id:
                    subsystem_weight_by_id[subsystem_id] = subsystem_weight_by_id.get(subsystem_id, 0.0) + abs(float(residual or 0.0))
            subsystem_total = float(sum(subsystem_weight_by_id.values()))
            if subsystem_total > 0.0:
                subsystem_scores = {
                    str(subsystem_id): float(weight / subsystem_total)
                    for subsystem_id, weight in sorted(subsystem_weight_by_id.items(), key=lambda item: (-item[1], item[0]))
                }
                dominant_subsystem_id = next(iter(subsystem_scores.keys()), None)
            else:
                subsystem_scores = {}
                dominant_subsystem_id = None
            output.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0) or 0),
                    "phase_state_detected": str(row.get("phase_state_detected", "")),
                    "phase_id_detected": int(row.get("phase_id_detected", 0) or 0),
                    "phase_confidence_detected": float(row.get("phase_confidence_detected", 0.0) or 0.0),
                    "distance_to_centroid_detected": float(phase_row.get("distance_to_centroid_detected", 0.0) or 0.0),
                    "drift_magnitude": float(phase_row.get("drift_magnitude", 0.0) or 0.0),
                    "breadth": float(phase_row.get("breadth", 0.0) or 0.0),
                    "global_score": float(row.get("global_score", 0.0) or 0.0),
                    "p_value": 1.0,
                    "severity": str(row.get("severity", "normal")),
                    "dominant_subsystem_id": dominant_subsystem_id,
                    "dominant_score_component": dominant_score_component,
                    "subsystem_scores": subsystem_scores,
                    "score_component_scores": {
                        "structure": 0.0 if structure_score is None else float(structure_score),
                        "reconstruction": reconstruction_score,
                    },
                    "date_utc": phase_row.get("date_utc"),
                }
            )
        return cls(rows=output)

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows)
