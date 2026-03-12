"""Phase artifact table builders for Spark pipeline stages.

The active Spark phase stage now keeps the large window-features dataframe
distributed:
- Spark builds window features
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
    select_backbone_sensors_by_energy,
    solve_backbone_weights,
)
from libs.io.contracts import PhaseBaselineRow, PhaseWindowRow
from libs.io.schemas import PHASE_BASELINES_SCHEMA, PHASE_WINDOWS_SCHEMA
from libs.common.event_types import CATEGORICAL_EVENT_TYPES, CONTINUOUS_EVENT_TYPES
from libs.phase.model import PhaseFeatureConfig, PhaseFeatures
from libs.scoring.window_scores import build_phase_score_baselines
from libs.windows import (
    build_window_features_spark_dataframe,
    build_window_features_dataframe,
)


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


def fit_phase_feature_config(
    window_features_df: pd.DataFrame,
    *,
    backbone_sensor_count: int = 8,
    backbone_ridge_lambda: float = 1.0,
    phase_detect_sensor_count: int = 8,
    phase_detect_event_type_count: int = 6,
    phase_detect_categorical_state_count: int = 6,
    phase_detect_window_cooccurrence_count: int = 0,
) -> dict[str, Any]:
    if window_features_df.empty:
        return {}

    return PhaseFeatureConfig.from_window_feature_rows(
        window_features_df.to_dict(orient="records"),
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
    ).to_dict()


def _collect_phase_event_type_counts_spark(window_features_df: "DataFrame") -> Counter[str]:
    from pyspark.sql import functions as F

    counts = Counter()
    rows = (
        window_features_df.select(F.explode_outer(F.map_entries("event_type_counts")).alias("entry"))
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


def _collect_categorical_state_pair_counts_spark(window_features_df: "DataFrame") -> Counter[tuple[str, str]]:
    from pyspark.sql import functions as F

    counts: Counter[tuple[str, str]] = Counter()
    rows = (
        window_features_df.select(F.explode_outer(F.map_entries("categorical_state_t_end")).alias("entry"))
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


def _collect_window_cooccurrence_pair_counts_spark(window_features_df: "DataFrame") -> Counter[tuple[str, str]]:
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
        window_features_df.select("continuous_vector_t_end_scaled", "categorical_state_t_end")
        .mapInPandas(_emit_pairs, schema=pair_schema)
        .groupBy("left_parameter_name", "right_parameter_name")
        .agg(F.sum("pair_count").cast("long").alias("pair_count"))
        .collect()
    )
    counts: Counter[tuple[str, str]] = Counter()
    for row in pair_rows:
        counts[(str(row["left_parameter_name"]), str(row["right_parameter_name"]))] += int(row["pair_count"] or 0)
    return counts


def fit_phase_feature_config_from_spark(
    window_features_df: "DataFrame",
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
        for row in build_backbone_sensor_energy_spark_table(window_features_df).collect()
    ]
    if not energy_rows:
        return {}

    selected_sensors_c = select_backbone_sensors_by_energy(energy_rows, k=max(int(backbone_sensor_count), 1))
    all_sensors = [
        str(row["parameter_name"])
        for row in (
            window_features_df.select(F.explode_outer(F.map_keys("continuous_vector_t_end_scaled")).alias("parameter_name"))
            .where(F.col("parameter_name").isNotNull())
            .distinct()
            .orderBy("parameter_name")
            .collect()
        )
    ]
    gh_rows = build_backbone_gh_spark_table(window_features_df, selected_sensors=selected_sensors_c)
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

    event_type_counts = _collect_phase_event_type_counts_spark(window_features_df)
    state_pair_counts = _collect_categorical_state_pair_counts_spark(window_features_df)
    cooccurrence_pair_counts = (
        _collect_window_cooccurrence_pair_counts_spark(window_features_df)
        if max(int(phase_detect_window_cooccurrence_count), 0) > 0
        else Counter()
    )

    phase_selected_sensors = selected_sensors_c[: max(int(phase_detect_sensor_count), 1)]
    phase_selected_event_types = PhaseFeatureConfig.select_event_types_from_counts(
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
    window_features_df: pd.DataFrame,
    *,
    phase_config: dict[str, Any],
    phase_count: int,
    phase_stable_drift_quantile: float,
    phase_smoothing_radius: int,
    phase_transition_penalty: float,
    phase_min_dwell_windows: int,
) -> pd.DataFrame:
    if window_features_df.empty:
        return pd.DataFrame()

    artifacts = PhaseFeatures.from_window_feature_rows(
        window_features_df.to_dict(orient="records"),
        config=PhaseFeatureConfig.from_dict(phase_config),
        phase_count=max(int(phase_count), 1),
        phase_stable_drift_quantile=float(phase_stable_drift_quantile),
        phase_smoothing_radius=max(int(phase_smoothing_radius), 0),
        phase_transition_penalty=float(phase_transition_penalty),
        phase_min_dwell_windows=max(int(phase_min_dwell_windows), 1),
    )
    return artifacts.phase_windows_df()


def build_phase_windows_spark_table(
    window_features_df: "DataFrame",
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

    return window_features_df.groupBy("tail_id").applyInPandas(_emit, schema=PHASE_WINDOWS_SCHEMA)


def build_phase_baselines_spark_table(
    phase_windows_df: "DataFrame",
    *,
    phase_config: dict[str, Any],
) -> "DataFrame":
    def _emit(group_pdf: pd.DataFrame) -> pd.DataFrame:
        if group_pdf.empty:
            return pd.DataFrame()
        config = PhaseFeatureConfig.from_dict(phase_config)
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
        baselines = PhaseFeatures.build_phase_baseline_rows(
            phase_score_baselines=build_phase_score_baselines(window_rows, assignment_rows),
            feature_names=_as_list(group_pdf.iloc[0].get("feature_names")),
            config=config,
        )
        return pd.DataFrame(baselines)

    return phase_windows_df.groupBy("tail_id").applyInPandas(_emit, schema=PHASE_BASELINES_SCHEMA)


def build_phase_features_from_window_features_dataframe(
    window_features_df: pd.DataFrame,
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
    if window_features_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    config = PhaseFeatureConfig.from_window_feature_rows(
        window_features_df.to_dict(orient="records"),
        backbone_sensor_count=backbone_sensor_count,
        backbone_ridge_lambda=backbone_ridge_lambda,
        phase_detect_sensor_count=phase_detect_sensor_count,
        phase_detect_event_type_count=phase_detect_event_type_count,
        phase_detect_categorical_state_count=phase_detect_categorical_state_count,
        phase_detect_window_cooccurrence_count=phase_detect_window_cooccurrence_count,
    )
    artifacts = PhaseFeatures.from_window_feature_rows(
        window_features_df.to_dict(orient="records"),
        config=config,
        phase_count=max(int(phase_count), 1),
        phase_stable_drift_quantile=float(phase_stable_drift_quantile),
        phase_smoothing_radius=max(int(phase_smoothing_radius), 0),
        phase_transition_penalty=float(phase_transition_penalty),
        phase_min_dwell_windows=max(int(phase_min_dwell_windows), 1),
    )
    return artifacts.phase_windows_df(), artifacts.phase_baselines_df()


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
    window_features_df = build_window_features_dataframe(raw_df, events_df, windows_df)
    return build_phase_features_from_window_features_dataframe(
        window_features_df,
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
