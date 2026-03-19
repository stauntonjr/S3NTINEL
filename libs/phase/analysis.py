from __future__ import annotations

from collections import Counter

import pandas as pd


def _continuous_phase_separation_score(group: pd.DataFrame) -> float:
    phase_means = (
        group.groupby("phase_name")["parameter_value_clean_num"]
        .mean()
        .dropna()
    )
    if len(phase_means) < 2:
        return 0.0
    value_range = float(phase_means.max() - phase_means.min())
    centered = group["parameter_value_clean_num"] - group.groupby("phase_name")["parameter_value_clean_num"].transform("mean")
    within_std = float(centered.std(ddof=0) or 0.0)
    if within_std <= 0.0:
        return 1.0 if value_range > 0.0 else 0.0
    return max(0.0, min(1.0, value_range / (value_range + within_std)))


def _categorical_phase_separation_score(group: pd.DataFrame) -> float:
    phase_modes: dict[str, str] = {}
    for phase_name, phase_df in group.groupby("phase_name"):
        values = [str(v) for v in phase_df["parameter_value_clean"].dropna().tolist()]
        if not values:
            continue
        phase_modes[str(phase_name)] = Counter(values).most_common(1)[0][0]
    if len(phase_modes) < 2:
        return 0.0
    unique_modes = set(phase_modes.values())
    return 1.0 - (1.0 / float(len(unique_modes)))


def analyze_phase_behavior(telemetry_df: pd.DataFrame, *, top_k: int = 10) -> dict[str, list[dict[str, float | str]]]:
    if telemetry_df.empty:
        return {"continuous_top": [], "categorical_top": []}

    working = telemetry_df.copy()
    working["parameter_name"] = working["parameter_name"].astype(str)
    working["phase_name"] = working["phase_name"].astype(str)
    working["parameter_datatype_label"] = working["parameter_datatype_label"].astype(str)
    working["parameter_value_clean_num"] = pd.to_numeric(working["parameter_value_clean"], errors="coerce")

    continuous_rows: list[dict[str, float | str]] = []
    categorical_rows: list[dict[str, float | str]] = []

    for parameter_name, parameter_df in working.groupby("parameter_name", sort=False):
        datatype_labels = {str(v) for v in parameter_df["parameter_datatype_label"].dropna().tolist()}
        if "numeric" in datatype_labels or "continuous" in datatype_labels:
            continuous_rows.append(
                {
                    "parameter_name": parameter_name,
                    "phase_separation_score": _continuous_phase_separation_score(parameter_df),
                }
            )
        else:
            categorical_rows.append(
                {
                    "parameter_name": parameter_name,
                    "phase_separation_score": _categorical_phase_separation_score(parameter_df),
                }
            )

    continuous_top = sorted(
        continuous_rows,
        key=lambda row: (-float(row["phase_separation_score"]), str(row["parameter_name"])),
    )[:top_k]
    categorical_top = sorted(
        categorical_rows,
        key=lambda row: (-float(row["phase_separation_score"]), str(row["parameter_name"])),
    )[:top_k]
    return {"continuous_top": continuous_top, "categorical_top": categorical_top}
