"""Window score table builders for Spark pipeline stages.

The active Spark scoring path keeps the main `phase_windows` fact table
distributed. Only small reference artifacts are materialized on the driver:
- `phase_baselines`
- `hierarchy_sensor_map`

That bounded collect is transitional. Do not extend this pattern to large fact
tables.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from libs.common import PhaseAssignmentRow, PhaseBaselineRow, PhaseWindowRow, WindowScoreRow
from libs.scoring.window_scores import score_window_s_rows


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass
    if isinstance(value, (str, bytes, dict)):
        return []
    if hasattr(value, "__iter__"):
        return list(value)
    return []


def build_window_scores_raw_table(
    phase_windows_df: pd.DataFrame,
    phase_baselines_df: pd.DataFrame,
    hierarchy_sensor_map_df: pd.DataFrame,
) -> pd.DataFrame:
    if phase_windows_df.empty:
        return pd.DataFrame()

    hierarchy_rows = hierarchy_sensor_map_df.copy() if hierarchy_sensor_map_df is not None else pd.DataFrame()
    parameter_col = "parameter_name" if "parameter_name" in hierarchy_rows.columns else ("sensor" if "sensor" in hierarchy_rows.columns else None)
    parameter_to_subsystem: dict[str, str] = {}
    if parameter_col is not None and "subsystem_id" in hierarchy_rows.columns:
        for row in hierarchy_rows.to_dict(orient="records"):
            parameter_name = str(row.get(parameter_col, "")).strip()
            subsystem_id = str(row.get("subsystem_id", "")).strip()
            if parameter_name and subsystem_id:
                parameter_to_subsystem[parameter_name] = subsystem_id

    window_rows: list[PhaseWindowRow] = []
    assignment_rows: list[PhaseAssignmentRow] = []
    for row in phase_windows_df.to_dict(orient="records"):
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
        for row in phase_baselines_df.to_dict(orient="records")
    ]

    scored_rows = score_window_s_rows(window_rows, assignment_rows, baseline_rows)
    output: list[WindowScoreRow] = []
    by_key = {
        (str(row.get("tail_id", "")), str(row.get("flight_id", "")), int(row.get("win_id", 0) or 0)): row
        for row in phase_windows_df.to_dict(orient="records")
    }
    for row in scored_rows:
        key = (str(row.get("tail_id", "")), str(row.get("flight_id", "")), int(row.get("win_id", 0) or 0))
        phase_row = by_key.get(key, {})
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
            if not subsystem_id:
                continue
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
    return pd.DataFrame(output)


def build_window_scores_raw_spark_table(
    phase_windows_df: "DataFrame",
    phase_baselines_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
) -> "DataFrame":
    """Score phase windows in Spark using pandas batches over the main fact table only.

    The main fact table remains distributed. Only bounded reference artifacts
    are collected on the driver.
    """
    from pyspark.sql import types as T

    baseline_rows = phase_baselines_df.select(
        "tail_id",
        "phase_id_detected",
        "s_w_centroid",
        "reconstruction_median",
        "reconstruction_mad",
        "distance_median",
        "distance_mad",
    ).collect()
    phase_baselines = [
        {
            "tail_id": str(row["tail_id"]),
            "phase_id_detected": int(row["phase_id_detected"]),
            "s_w_centroid": _as_list(row["s_w_centroid"]),
            "reconstruction_median": float(row["reconstruction_median"] or 0.0),
            "reconstruction_mad": float(row["reconstruction_mad"] or 0.0),
            "distance_median": float(row["distance_median"] or 0.0),
            "distance_mad": float(row["distance_mad"] or 0.0),
        }
        for row in baseline_rows
    ]
    phase_baselines_pdf = pd.DataFrame(phase_baselines)

    parameter_col = (
        "parameter_name"
        if "parameter_name" in hierarchy_sensor_map_df.columns
        else ("sensor" if "sensor" in hierarchy_sensor_map_df.columns else None)
    )
    parameter_to_subsystem: dict[str, str] = {}
    if parameter_col is not None and "subsystem_id" in hierarchy_sensor_map_df.columns:
        for row in hierarchy_sensor_map_df.select(parameter_col, "subsystem_id").collect():
            parameter_name = str(row[parameter_col] or "").strip()
            subsystem_id = str(row["subsystem_id"] or "").strip()
            if parameter_name and subsystem_id:
                parameter_to_subsystem[parameter_name] = subsystem_id
    hierarchy_sensor_map_pdf = pd.DataFrame(
        [
            {"parameter_name": parameter_name, "subsystem_id": subsystem_id}
            for parameter_name, subsystem_id in parameter_to_subsystem.items()
        ]
    )

    schema = T.StructType(
        [
            T.StructField("tail_id", T.StringType(), False),
            T.StructField("flight_id", T.StringType(), False),
            T.StructField("win_id", T.IntegerType(), False),
            T.StructField("phase_state_detected", T.StringType(), False),
            T.StructField("phase_id_detected", T.IntegerType(), False),
            T.StructField("phase_confidence_detected", T.DoubleType(), False),
            T.StructField("distance_to_centroid_detected", T.DoubleType(), True),
            T.StructField("drift_magnitude", T.DoubleType(), False),
            T.StructField("breadth", T.DoubleType(), False),
            T.StructField("global_score", T.DoubleType(), False),
            T.StructField("p_value", T.DoubleType(), False),
            T.StructField("severity", T.StringType(), False),
            T.StructField("dominant_subsystem_id", T.StringType(), True),
            T.StructField("dominant_score_component", T.StringType(), False),
            T.StructField("subsystem_scores", T.MapType(T.StringType(), T.DoubleType(), False), False),
            T.StructField("score_component_scores", T.MapType(T.StringType(), T.DoubleType(), False), False),
            T.StructField("date_utc", T.DateType(), True),
        ]
    )

    def _score_batches(pdf_iter: Any) -> Any:
        for phase_windows_pdf in pdf_iter:
            scores_pdf = build_window_scores_raw_table(
                phase_windows_pdf,
                phase_baselines_pdf,
                hierarchy_sensor_map_pdf,
            )
            yield scores_pdf

    return phase_windows_df.mapInPandas(_score_batches, schema=schema)
