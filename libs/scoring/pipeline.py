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

from libs.io.schemas import WINDOW_SCORES_RAW_SCHEMA
from libs.scoring.model import WindowScoreArtifacts


def build_window_scores_raw_table(
    phase_windows_df: pd.DataFrame,
    phase_baselines_df: pd.DataFrame,
    hierarchy_sensor_map_df: pd.DataFrame,
) -> pd.DataFrame:
    if phase_windows_df.empty:
        return pd.DataFrame()
    return WindowScoreArtifacts.from_phase_rows(
        phase_windows_df.to_dict(orient="records"),
        phase_baselines_df.to_dict(orient="records"),
        hierarchy_sensor_map_df.to_dict(orient="records") if hierarchy_sensor_map_df is not None else [],
    ).to_df()


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
    phase_baselines_pdf = pd.DataFrame([dict(row.asDict()) for row in baseline_rows])

    hierarchy_sensor_map_pdf = pd.DataFrame([dict(row.asDict()) for row in hierarchy_sensor_map_df.collect()])

    schema = WINDOW_SCORES_RAW_SCHEMA()

    def _score_batches(pdf_iter: Any) -> Any:
        for phase_windows_pdf in pdf_iter:
            scores_pdf = build_window_scores_raw_table(
                phase_windows_pdf,
                phase_baselines_pdf,
                hierarchy_sensor_map_pdf,
            )
            yield scores_pdf

    return phase_windows_df.mapInPandas(_score_batches, schema=schema)
