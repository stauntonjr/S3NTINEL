"""Shared behavior-family utility helpers."""

from __future__ import annotations

import math

import pandas as pd


def clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def numeric_series(telemetry_pdf: pd.DataFrame) -> pd.Series:
    source_column = "parameter_value" if "parameter_value" in telemetry_pdf.columns else "parameter_value_clean"
    return pd.to_numeric(telemetry_pdf.get(source_column), errors="coerce").dropna().astype(float)


def lag1_autocorrelation(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna().astype(float)
    if len(values) < 2:
        return None

    left = values.iloc[:-1].to_numpy(dtype=float)
    right = values.iloc[1:].to_numpy(dtype=float)
    left_centered = left - left.mean()
    right_centered = right - right.mean()
    left_norm = math.sqrt(float((left_centered**2).sum()))
    right_norm = math.sqrt(float((right_centered**2).sum()))
    if left_norm <= 0.0 or right_norm <= 0.0:
        return 1.0 if len(values) >= 2 and float(values.nunique()) <= 1.0 else 0.0
    return float((left_centered * right_centered).sum() / (left_norm * right_norm))
