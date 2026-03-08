"""Phase artifact table builders for Spark pipeline stages.

The active Spark phase stage now keeps the large `window_x` fact table
distributed:
- Spark builds `window_x`
- Spark aggregates the bounded phase-fit configuration inputs
- Spark emits `phase_windows` and `phase_baselines` by tail

The remaining driver-side work is the small global phase configuration, not
fact-table materialization.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import numpy as np
import pandas as pd

from libs.backbone import (
    aggregate_backbone_gh,
    build_backbone_gh_spark_table,
    build_backbone_sensor_energy_spark_table,
    compute_backbone_gh_by_flight,
    reconstruct_window_vector,
    reconstruction_error,
    select_backbone_sensors_by_energy,
    solve_backbone_weights,
)
from libs.common import PhaseBaselineRow, PhaseWindowRow
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.phase import detect_phases_from_windows
from libs.scoring.window_scores import build_phase_score_baselines
from libs.windows import (
    build_window_s_rows,
    build_window_x_spark_table,
    build_window_x_table,
    top_categorical_state_pairs,
    top_phase_event_types,
    top_window_cooccurrence_sensor_pairs,
)

PHASE_WINDOWS_SCHEMA = """
tail_id string,
flight_id string,
win_id int,
t_start timestamp,
t_end timestamp,
duration_ms int,
event_count int,
phase_id_detected int,
phase_state_detected string,
phase_confidence_detected double,
distance_to_centroid_detected double,
drift_magnitude double,
breadth double,
backbone_reconstruction_error double,
backbone_residual_by_parameter map<string,double>,
x_c array<double>,
s_w array<double>,
date_utc date,
feature_names array<string>,
selected_sensors_c array<string>,
selected_event_types array<string>,
selected_categorical_state_pairs array<string>,
selected_window_cooccurrence_pairs array<string>,
backbone_all_sensors array<string>
"""

PHASE_BASELINES_SCHEMA = """
tail_id string,
phase_id_detected int,
phase_name_detected string,
s_w_centroid array<double>,
reconstruction_median double,
reconstruction_mad double,
distance_median double,
distance_mad double,
stable_window_count int,
feature_names array<string>,
selected_sensors_c array<string>,
selected_event_types array<string>,
selected_categorical_state_pairs array<string>,
selected_window_cooccurrence_pairs array<string>,
backbone_all_sensors array<string>,
backbone_weights_b array<array<double>>,
version int
"""


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


def fit_phase_window_x_config(
    window_x_df: pd.DataFrame,
    *,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
) -> dict[str, Any]:
    if window_x_df.empty:
        return {}

    window_x_rows = window_x_df.to_dict(orient="records")
    energy_rows: list[dict[str, Any]] = []
    energy_by_sensor: Counter[str] = Counter()
    support_by_sensor: Counter[str] = Counter()
    for row in window_x_rows:
        for parameter_name, value in row.get("continuous_vector_t_end_scaled", {}).items():
            energy_by_sensor[str(parameter_name)] += float(value) * float(value)
            support_by_sensor[str(parameter_name)] += 1
    for parameter_name in sorted(energy_by_sensor.keys()):
        energy_rows.append(
            {
                "parameter_name": parameter_name,
                "energy": float(energy_by_sensor[parameter_name]),
                "support_count": int(support_by_sensor[parameter_name]),
            }
        )

    selected_sensors_c = select_backbone_sensors_by_energy(energy_rows, k=max(int(backbone_sensor_count), 1))
    gh_by_flight, all_sensors = compute_backbone_gh_by_flight(window_x_rows, selected_sensors=selected_sensors_c)
    g, h, _ = aggregate_backbone_gh(gh_by_flight)
    weights_b = solve_backbone_weights(g, h, ridge_lambda=float(backbone_ridge_lambda))

    phase_selected_sensors = selected_sensors_c[: max(int(phase_detect_sensor_count), 1)]
    phase_selected_event_types = top_phase_event_types(window_x_rows, k=max(int(phase_detect_event_type_count), 0))
    phase_selected_categorical_state_pairs = top_categorical_state_pairs(
        window_x_rows,
        k=max(int(phase_detect_categorical_state_count), 0),
    )
    phase_selected_window_cooccurrence_pairs = top_window_cooccurrence_sensor_pairs(
        window_x_rows,
        k=max(int(phase_detect_window_cooccurrence_count), 0),
    )

    return {
        "selected_sensors_c": list(selected_sensors_c),
        "all_sensors": list(all_sensors),
        "weights_b": [[float(value) for value in row] for row in weights_b],
        "phase_selected_sensors": list(phase_selected_sensors),
        "phase_selected_event_types": list(phase_selected_event_types),
        "phase_selected_categorical_state_pairs": list(phase_selected_categorical_state_pairs),
        "phase_selected_window_cooccurrence_pairs": list(phase_selected_window_cooccurrence_pairs),
    }


def _select_phase_event_types_from_counts(counts: Counter[str], *, k: int) -> list[str]:
    limit = max(int(k), 0)
    if limit <= 0:
        return []
    continuous = [(event_type, count) for event_type, count in counts.most_common() if event_type in CONTINUOUS_EVENT_TYPES]
    categorical = [(event_type, count) for event_type, count in counts.most_common() if event_type in CATEGORICAL_EVENT_TYPES]
    continuous_k = max(limit // 2, 1) if continuous else 0
    categorical_k = max(limit - continuous_k, 0) if categorical else 0
    selected = [event_type for event_type, _ in continuous[:continuous_k]]
    selected.extend(event_type for event_type, _ in categorical[:categorical_k] if event_type not in selected)
    if len(selected) < limit:
        for event_type, _ in counts.most_common():
            if event_type in selected:
                continue
            selected.append(event_type)
            if len(selected) >= limit:
                break
    return selected


def _collect_phase_event_type_counts_spark(window_x_df: "DataFrame") -> Counter[str]:
    from pyspark.sql import functions as F

    counts = Counter()
    rows = (
        window_x_df.select(F.explode_outer(F.map_entries("event_type_counts")).alias("entry"))
        .select(
            F.col("entry.key").cast("string").alias("event_type"),
            F.col("entry.value").cast("long").alias("event_count"),
        )
        .where(F.col("event_type").isNotNull())
        .groupBy("event_type")
        .agg(F.sum("event_count").cast("long").alias("event_count"))
        .collect()
    )
    for row in rows:
        counts[str(row["event_type"])] += int(row["event_count"] or 0)
    return counts


def _collect_categorical_state_pair_counts_spark(window_x_df: "DataFrame") -> Counter[tuple[str, str]]:
    from pyspark.sql import functions as F

    counts: Counter[tuple[str, str]] = Counter()
    rows = (
        window_x_df.select(F.explode_outer(F.map_entries("categorical_state_t_end")).alias("entry"))
        .select(
            F.col("entry.key").cast("string").alias("parameter_name"),
            F.col("entry.value").cast("string").alias("state"),
        )
        .where(F.col("parameter_name").isNotNull() & F.col("state").isNotNull())
        .groupBy("parameter_name", "state")
        .agg(F.count(F.lit(1)).cast("long").alias("pair_count"))
        .collect()
    )
    for row in rows:
        counts[(str(row["parameter_name"]), str(row["state"]))] += int(row["pair_count"] or 0)
    return counts


def _collect_window_cooccurrence_pair_counts_spark(window_x_df: "DataFrame") -> Counter[tuple[str, str]]:
    from collections import Counter as PyCounter

    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    pair_schema = T.StructType(
        [
            T.StructField("left_parameter_name", T.StringType(), False),
            T.StructField("right_parameter_name", T.StringType(), False),
            T.StructField("pair_count", T.LongType(), False),
        ]
    )

    def _emit_pairs(pdf_iter: Any) -> Any:
        for pdf in pdf_iter:
            pair_counts: PyCounter[tuple[str, str]] = PyCounter()
            for row in pdf.to_dict(orient="records"):
                scaled = row.get("continuous_vector_t_end_scaled")
                if not isinstance(scaled, dict):
                    scaled = {}
                categorical = row.get("categorical_state_t_end")
                if not isinstance(categorical, dict):
                    categorical = {}
                parameter_names = sorted(set(str(item) for item in scaled.keys()) | set(str(item) for item in categorical.keys()))
                for index, left in enumerate(parameter_names):
                    for right in parameter_names[index + 1 :]:
                        pair_counts[(left, right)] += 1
            yield pd.DataFrame(
                [
                    {
                        "left_parameter_name": left,
                        "right_parameter_name": right,
                        "pair_count": int(pair_count),
                    }
                    for (left, right), pair_count in pair_counts.items()
                ],
                columns=["left_parameter_name", "right_parameter_name", "pair_count"],
            )

    pair_rows = (
        window_x_df.select("continuous_vector_t_end_scaled", "categorical_state_t_end")
        .mapInPandas(_emit_pairs, schema=pair_schema)
        .groupBy("left_parameter_name", "right_parameter_name")
        .agg(F.sum("pair_count").cast("long").alias("pair_count"))
        .collect()
    )
    counts: Counter[tuple[str, str]] = Counter()
    for row in pair_rows:
        counts[(str(row["left_parameter_name"]), str(row["right_parameter_name"]))] += int(row["pair_count"] or 0)
    return counts


def fit_phase_window_x_config_from_spark(
    window_x_df: "DataFrame",
    *,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    energy_rows = [
        {
            "parameter_name": str(row["parameter_name"]),
            "energy": float(row["energy"] or 0.0),
            "support_count": int(row["support_count"] or 0),
        }
        for row in build_backbone_sensor_energy_spark_table(window_x_df).collect()
    ]
    if not energy_rows:
        return {}

    selected_sensors_c = select_backbone_sensors_by_energy(energy_rows, k=max(int(backbone_sensor_count), 1))
    all_sensors = [
        str(row["parameter_name"])
        for row in (
            window_x_df.select(F.explode_outer(F.map_keys("continuous_vector_t_end_scaled")).alias("parameter_name"))
            .where(F.col("parameter_name").isNotNull())
            .distinct()
            .orderBy("parameter_name")
            .collect()
        )
    ]
    gh_rows = build_backbone_gh_spark_table(window_x_df, selected_sensors=selected_sensors_c)
    gh_records = [
        {
            "tail_id": str(row["tail_id"]),
            "flight_id": str(row["flight_id"]),
            "window_count": int(row["window_count"] or 0),
            "g_f": np.asarray(row["g_f"], dtype=float),
            "h_f": np.asarray(row["h_f"], dtype=float),
        }
        for row in gh_rows.collect()
    ]
    g, h, _ = aggregate_backbone_gh(gh_records)
    weights_b = solve_backbone_weights(g, h, ridge_lambda=float(backbone_ridge_lambda))

    event_type_counts = _collect_phase_event_type_counts_spark(window_x_df)
    state_pair_counts = _collect_categorical_state_pair_counts_spark(window_x_df)
    cooccurrence_pair_counts = (
        _collect_window_cooccurrence_pair_counts_spark(window_x_df)
        if max(int(phase_detect_window_cooccurrence_count), 0) > 0
        else Counter()
    )

    phase_selected_sensors = selected_sensors_c[: max(int(phase_detect_sensor_count), 1)]
    phase_selected_event_types = _select_phase_event_types_from_counts(
        event_type_counts,
        k=max(int(phase_detect_event_type_count), 0),
    )
    phase_selected_categorical_state_pairs = [
        pair for pair, _ in state_pair_counts.most_common(max(int(phase_detect_categorical_state_count), 0))
    ]
    phase_selected_window_cooccurrence_pairs = [
        pair for pair, _ in cooccurrence_pair_counts.most_common(max(int(phase_detect_window_cooccurrence_count), 0))
    ]

    return {
        "selected_sensors_c": list(selected_sensors_c),
        "all_sensors": list(all_sensors),
        "weights_b": [[float(value) for value in row] for row in weights_b],
        "phase_selected_sensors": list(phase_selected_sensors),
        "phase_selected_event_types": list(phase_selected_event_types),
        "phase_selected_categorical_state_pairs": list(phase_selected_categorical_state_pairs),
        "phase_selected_window_cooccurrence_pairs": list(phase_selected_window_cooccurrence_pairs),
    }


def _build_phase_windows_for_tail(
    window_x_df: pd.DataFrame,
    *,
    phase_config: dict[str, Any],
    phase_count: int,
    phase_stable_drift_quantile: float,
    phase_smoothing_radius: int,
    phase_transition_penalty: float,
    phase_min_dwell_windows: int,
) -> pd.DataFrame:
    if window_x_df.empty:
        return pd.DataFrame()

    window_x_rows = window_x_df.to_dict(orient="records")
    selected_sensors_c = [str(item) for item in phase_config.get("selected_sensors_c", [])]
    all_sensors = [str(item) for item in phase_config.get("all_sensors", [])]
    weights_b = np.asarray(phase_config.get("weights_b", []), dtype=float)
    phase_selected_sensors = [str(item) for item in phase_config.get("phase_selected_sensors", [])]
    phase_selected_event_types = [str(item) for item in phase_config.get("phase_selected_event_types", [])]
    phase_selected_categorical_state_pairs = [
        (str(parameter_name), str(state))
        for parameter_name, state in phase_config.get("phase_selected_categorical_state_pairs", [])
    ]
    phase_selected_window_cooccurrence_pairs = [
        (str(left), str(right))
        for left, right in phase_config.get("phase_selected_window_cooccurrence_pairs", [])
    ]

    for row in window_x_rows:
        x_true = dict(row.get("continuous_vector_t_end_scaled", {}))
        x_hat = reconstruct_window_vector(
            x_true,
            selected_sensors=selected_sensors_c,
            all_sensors=all_sensors,
            weights_b=weights_b,
        )
        error, residuals = reconstruction_error(x_true, x_hat, sensor_order=all_sensors)
        row["backbone_reconstruction_error"] = float(error)
        row["backbone_x_c"] = [float(x_true.get(parameter_name, 0.0) or 0.0) for parameter_name in selected_sensors_c]
        row["backbone_residual_by_parameter"] = {
            str(parameter_name): float(residual)
            for parameter_name, residual in residuals.items()
        }

    window_s_rows, feature_names = build_window_s_rows(
        window_x_rows,
        selected_sensors_c=phase_selected_sensors,
        selected_event_types=phase_selected_event_types,
        selected_categorical_state_pairs=phase_selected_categorical_state_pairs,
        selected_cooccurrence_sensor_pairs=phase_selected_window_cooccurrence_pairs,
    )
    phase_assignments, _ = detect_phases_from_windows(
        window_s_rows,
        phase_count=max(int(phase_count), 1),
        stable_drift_quantile=float(phase_stable_drift_quantile),
        smoothing_radius=max(int(phase_smoothing_radius), 0),
        transition_penalty=float(phase_transition_penalty),
        min_dwell_windows=max(int(phase_min_dwell_windows), 1),
        ordered_phase_progression=True,
    )
    assignment_by_key = {
        (str(item.get("tail_id", "")), str(item.get("flight_id", "")), int(item.get("win_id", 0))): item
        for item in phase_assignments
    }

    phase_window_rows: list[PhaseWindowRow] = []
    for row in window_s_rows:
        key = (str(row.get("tail_id", "")), str(row.get("flight_id", "")), int(row.get("win_id", 0)))
        assignment = assignment_by_key.get(key, {})
        phase_window_rows.append(
            {
                "tail_id": str(row.get("tail_id", "")),
                "flight_id": str(row.get("flight_id", "")),
                "win_id": int(row.get("win_id", 0)),
                "t_start": row.get("t_start"),
                "t_end": row.get("t_end"),
                "duration_ms": int(row.get("duration_ms", 0) or 0),
                "event_count": int(row.get("event_count", 0) or 0),
                "phase_id_detected": int(assignment.get("phase_id_detected", 0) or 0),
                "phase_state_detected": str(assignment.get("phase_state_detected", "unknown")),
                "phase_confidence_detected": float(assignment.get("phase_confidence_detected", 0.0) or 0.0),
                "distance_to_centroid_detected": float(assignment.get("distance_to_centroid_detected", 0.0) or 0.0),
                "drift_magnitude": float(row.get("drift_magnitude_profiled", 0.0) or 0.0),
                "breadth": float(row.get("s_w", [0.0])[-1]) if row.get("s_w") else 0.0,
                "backbone_reconstruction_error": float(row.get("backbone_reconstruction_error", 0.0) or 0.0),
                "backbone_residual_by_parameter": {
                    str(parameter_name): float(residual)
                    for parameter_name, residual in dict(row.get("backbone_residual_by_parameter", {})).items()
                },
                "x_c": [float(item) for item in row.get("x_c", [])],
                "s_w": [float(item) for item in row.get("s_w", [])],
                "date_utc": row.get("date_utc"),
                "feature_names": list(feature_names),
                "selected_sensors_c": list(selected_sensors_c),
                "selected_event_types": list(phase_selected_event_types),
                "selected_categorical_state_pairs": [f"{parameter_name}={state}" for parameter_name, state in phase_selected_categorical_state_pairs],
                "selected_window_cooccurrence_pairs": [f"{left}&{right}" for left, right in phase_selected_window_cooccurrence_pairs],
                "backbone_all_sensors": list(all_sensors),
            }
        )
    return pd.DataFrame(phase_window_rows)


def build_phase_windows_spark_table(
    window_x_df: "DataFrame",
    *,
    phase_config: dict[str, Any],
    phase_count: int,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
) -> "DataFrame":
    def _emit(group_pdf: pd.DataFrame) -> pd.DataFrame:
        return _build_phase_windows_for_tail(
            group_pdf,
            phase_config=phase_config,
            phase_count=phase_count,
            phase_stable_drift_quantile=phase_stable_drift_quantile,
            phase_smoothing_radius=phase_smoothing_radius,
            phase_transition_penalty=phase_transition_penalty,
            phase_min_dwell_windows=phase_min_dwell_windows,
        )

    return window_x_df.groupBy("tail_id").applyInPandas(_emit, schema=PHASE_WINDOWS_SCHEMA)


def build_phase_baselines_spark_table(
    phase_windows_df: "DataFrame",
    *,
    phase_config: dict[str, Any],
) -> "DataFrame":
    def _emit(group_pdf: pd.DataFrame) -> pd.DataFrame:
        if group_pdf.empty:
            return pd.DataFrame()
        window_rows = []
        assignment_rows = []
        for row in group_pdf.to_dict(orient="records"):
            window_rows.append(
                {
                    "tail_id": str(row.get("tail_id", "")),
                    "flight_id": str(row.get("flight_id", "")),
                    "win_id": int(row.get("win_id", 0) or 0),
                    "s_w": _as_list(row.get("s_w")),
                    "backbone_reconstruction_error": float(row.get("backbone_reconstruction_error", 0.0) or 0.0),
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
        baselines = build_phase_score_baselines(window_rows, assignment_rows)
        out: list[PhaseBaselineRow] = []
        for baseline in baselines:
            out.append(
                {
                    "tail_id": str(baseline.get("tail_id", "")),
                    "phase_id_detected": int(baseline.get("phase_id_detected", 0) or 0),
                    "phase_name_detected": f"phase_{int(baseline.get('phase_id_detected', 0) or 0)}",
                    "s_w_centroid": [float(item) for item in baseline.get("s_w_centroid", [])],
                    "reconstruction_median": float(baseline.get("reconstruction_median", 0.0) or 0.0),
                    "reconstruction_mad": float(baseline.get("reconstruction_mad", 0.0) or 0.0),
                    "distance_median": float(baseline.get("distance_median", 0.0) or 0.0),
                    "distance_mad": float(baseline.get("distance_mad", 0.0) or 0.0),
                    "stable_window_count": int(baseline.get("stable_window_count", 0) or 0),
                    "feature_names": _as_list(group_pdf.iloc[0].get("feature_names")),
                    "selected_sensors_c": _as_list(phase_config.get("selected_sensors_c")),
                    "selected_event_types": _as_list(phase_config.get("phase_selected_event_types")),
                    "selected_categorical_state_pairs": [
                        f"{parameter_name}={state}"
                        for parameter_name, state in _as_list(phase_config.get("phase_selected_categorical_state_pairs"))
                    ],
                    "selected_window_cooccurrence_pairs": [
                        f"{left}&{right}"
                        for left, right in _as_list(phase_config.get("phase_selected_window_cooccurrence_pairs"))
                    ],
                    "backbone_all_sensors": _as_list(phase_config.get("all_sensors")),
                    "backbone_weights_b": _as_list(phase_config.get("weights_b")),
                    "version": 2,
                }
            )
        return pd.DataFrame(out)

    return phase_windows_df.groupBy("tail_id").applyInPandas(_emit, schema=PHASE_BASELINES_SCHEMA)


def build_phase_artifacts_from_window_x_table(
    window_x_df: pd.DataFrame,
    *,
    phase_count: int,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if window_x_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    window_x_rows = window_x_df.to_dict(orient="records")

    energy_rows: list[dict[str, Any]] = []
    energy_by_sensor: Counter[str] = Counter()
    support_by_sensor: Counter[str] = Counter()
    for row in window_x_rows:
        for parameter_name, value in row.get("continuous_vector_t_end_scaled", {}).items():
            energy_by_sensor[str(parameter_name)] += float(value) * float(value)
            support_by_sensor[str(parameter_name)] += 1
    for parameter_name in sorted(energy_by_sensor.keys()):
        energy_rows.append(
            {
                "parameter_name": parameter_name,
                "energy": float(energy_by_sensor[parameter_name]),
                "support_count": int(support_by_sensor[parameter_name]),
            }
        )

    selected_sensors_c = select_backbone_sensors_by_energy(energy_rows, k=max(int(backbone_sensor_count), 1))
    gh_by_flight, all_sensors = compute_backbone_gh_by_flight(window_x_rows, selected_sensors=selected_sensors_c)
    g, h, _ = aggregate_backbone_gh(gh_by_flight)
    weights_b = solve_backbone_weights(g, h, ridge_lambda=float(backbone_ridge_lambda))

    for row in window_x_rows:
        x_true = dict(row.get("continuous_vector_t_end_scaled", {}))
        x_hat = reconstruct_window_vector(
            x_true,
            selected_sensors=selected_sensors_c,
            all_sensors=all_sensors,
            weights_b=weights_b,
        )
        error, residuals = reconstruction_error(x_true, x_hat, sensor_order=all_sensors)
        row["backbone_reconstruction_error"] = float(error)
        row["backbone_x_c"] = [float(x_true.get(parameter_name, 0.0) or 0.0) for parameter_name in selected_sensors_c]
        row["backbone_residual_by_parameter"] = {
            str(parameter_name): float(residual)
            for parameter_name, residual in residuals.items()
        }

    phase_selected_sensors = selected_sensors_c[: max(int(phase_detect_sensor_count), 1)]
    phase_selected_event_types = top_phase_event_types(window_x_rows, k=max(int(phase_detect_event_type_count), 0))
    phase_selected_categorical_state_pairs = top_categorical_state_pairs(
        window_x_rows,
        k=max(int(phase_detect_categorical_state_count), 0),
    )
    phase_selected_window_cooccurrence_pairs = top_window_cooccurrence_sensor_pairs(
        window_x_rows,
        k=max(int(phase_detect_window_cooccurrence_count), 0),
    )

    window_s_rows, feature_names = build_window_s_rows(
        window_x_rows,
        selected_sensors_c=phase_selected_sensors,
        selected_event_types=phase_selected_event_types,
        selected_categorical_state_pairs=phase_selected_categorical_state_pairs,
        selected_cooccurrence_sensor_pairs=phase_selected_window_cooccurrence_pairs,
    )
    phase_assignments, _ = detect_phases_from_windows(
        window_s_rows,
        phase_count=max(int(phase_count), 1),
        stable_drift_quantile=float(phase_stable_drift_quantile),
        smoothing_radius=max(int(phase_smoothing_radius), 0),
        transition_penalty=float(phase_transition_penalty),
        min_dwell_windows=max(int(phase_min_dwell_windows), 1),
        ordered_phase_progression=True,
    )
    phase_score_baselines = build_phase_score_baselines(window_s_rows, phase_assignments)

    assignment_by_key = {
        (str(item.get("tail_id", "")), str(item.get("flight_id", "")), int(item.get("win_id", 0))): item
        for item in phase_assignments
    }

    phase_window_rows: list[PhaseWindowRow] = []
    for row in window_s_rows:
        key = (str(row.get("tail_id", "")), str(row.get("flight_id", "")), int(row.get("win_id", 0)))
        assignment = assignment_by_key.get(key, {})
        summary_tail = str(row.get("tail_id", ""))
        phase_window_rows.append(
            {
                "tail_id": summary_tail,
                "flight_id": str(row.get("flight_id", "")),
                "win_id": int(row.get("win_id", 0)),
                "t_start": row.get("t_start"),
                "t_end": row.get("t_end"),
                "duration_ms": int(row.get("duration_ms", 0) or 0),
                "event_count": int(row.get("event_count", 0) or 0),
                "phase_id_detected": int(assignment.get("phase_id_detected", 0) or 0),
                "phase_state_detected": str(assignment.get("phase_state_detected", "unknown")),
                "phase_confidence_detected": float(assignment.get("phase_confidence_detected", 0.0) or 0.0),
                "distance_to_centroid_detected": float(assignment.get("distance_to_centroid_detected", 0.0) or 0.0),
                "drift_magnitude": float(row.get("drift_magnitude_profiled", 0.0) or 0.0),
                "breadth": float(row.get("s_w", [0.0])[-1]) if row.get("s_w") else 0.0,
                "backbone_reconstruction_error": float(row.get("backbone_reconstruction_error", 0.0) or 0.0),
                "backbone_residual_by_parameter": {
                    str(parameter_name): float(residual)
                    for parameter_name, residual in dict(row.get("backbone_residual_by_parameter", {})).items()
                },
                "x_c": [float(item) for item in row.get("x_c", [])],
                "s_w": [float(item) for item in row.get("s_w", [])],
                "date_utc": row.get("date_utc"),
                "feature_names": list(feature_names),
                "selected_sensors_c": list(selected_sensors_c),
                "selected_event_types": list(phase_selected_event_types),
                "selected_categorical_state_pairs": [f"{parameter_name}={state}" for parameter_name, state in phase_selected_categorical_state_pairs],
                "selected_window_cooccurrence_pairs": [f"{left}&{right}" for left, right in phase_selected_window_cooccurrence_pairs],
                "backbone_all_sensors": list(all_sensors),
            }
        )

    phase_baseline_rows: list[PhaseBaselineRow] = []
    for baseline in phase_score_baselines:
        phase_baseline_rows.append(
            {
                "tail_id": str(baseline.get("tail_id", "")),
                "phase_id_detected": int(baseline.get("phase_id_detected", 0) or 0),
                "phase_name_detected": f"phase_{int(baseline.get('phase_id_detected', 0) or 0)}",
                "s_w_centroid": [float(item) for item in baseline.get("s_w_centroid", [])],
                "reconstruction_median": float(baseline.get("reconstruction_median", 0.0) or 0.0),
                "reconstruction_mad": float(baseline.get("reconstruction_mad", 0.0) or 0.0),
                "distance_median": float(baseline.get("distance_median", 0.0) or 0.0),
                "distance_mad": float(baseline.get("distance_mad", 0.0) or 0.0),
                "stable_window_count": int(baseline.get("stable_window_count", 0) or 0),
                "feature_names": list(feature_names),
                "selected_sensors_c": list(selected_sensors_c),
                "selected_event_types": list(phase_selected_event_types),
                "selected_categorical_state_pairs": [f"{parameter_name}={state}" for parameter_name, state in phase_selected_categorical_state_pairs],
                "selected_window_cooccurrence_pairs": [f"{left}&{right}" for left, right in phase_selected_window_cooccurrence_pairs],
                "backbone_all_sensors": list(all_sensors),
                "backbone_weights_b": [[float(value) for value in row] for row in weights_b],
                "version": 2,
            }
        )

    return pd.DataFrame(phase_window_rows), pd.DataFrame(phase_baseline_rows)


def build_phase_artifact_tables(
    raw_df: pd.DataFrame,
    events_df: pd.DataFrame,
    windows_df: pd.DataFrame,
    *,
    phase_count: int,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
    phase_stable_drift_quantile: float = 0.35,
    phase_smoothing_radius: int = 2,
    phase_transition_penalty: float = 1.5,
    phase_min_dwell_windows: int = 8,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    window_x_df = build_window_x_table(raw_df, events_df, windows_df)
    return build_phase_artifacts_from_window_x_table(
        window_x_df,
        phase_count=phase_count,
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
        phase_stable_drift_quantile=phase_stable_drift_quantile,
        phase_smoothing_radius=phase_smoothing_radius,
        phase_transition_penalty=phase_transition_penalty,
        phase_min_dwell_windows=phase_min_dwell_windows,
    )
