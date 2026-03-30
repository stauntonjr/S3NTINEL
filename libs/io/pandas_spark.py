"""Helpers for converting pandas-native values into Spark-safe local records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
from math import isnan
from typing import Any

import pandas as pd


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if value is pd.NaT:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        ts = value.tz_convert("UTC") if value.tzinfo is not None else value.tz_localize("UTC")
        return ts.tz_localize(None).to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(datetime.UTC).replace(tzinfo=None)
        return value
    if isinstance(value, date):
        return value
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    if isinstance(value, float) and isnan(value):
        return None
    return value


def coerce_spark_map_like(value: Any) -> dict[str, Any] | None:
    """Normalize map-like values from Spark/Pandas into a plain dict.

    Parquet round-trips commonly materialize Spark map columns as either:
    - native dict-like objects
    - lists of ``(key, value)`` tuples
    """
    if hasattr(value, "asDict"):
        try:
            value = value.asDict(recursive=True)
        except Exception:
            pass
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items() if str(key)}
    if isinstance(value, list):
        out: dict[str, Any] = {}
        for item in value:
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                return None
            key, item_value = item
            key_str = str(key)
            if not key_str:
                continue
            out[key_str] = item_value
        return out
    return None


def spark_safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): spark_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [spark_safe_value(item) for item in value]
    return _normalize_scalar(value)


def pandas_records_for_spark(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    return [{str(key): spark_safe_value(value) for key, value in row.items()} for row in frame.to_dict(orient="records")]
