"""Canonical sensor datatype definitions and normalization helpers."""

from __future__ import annotations

from enum import Enum


class SensorDataType(str, Enum):
    NUMERIC = "numeric"
    BINARY = "binary"
    CATEGORICAL = "categorical"
    HIGH_CARDINALITY = "high_cardinality"
    CONSTANT = "constant"
    UNKNOWN = "unknown"


_ALIASES: dict[str, str] = {
    "": SensorDataType.UNKNOWN.value,
    "unknown": SensorDataType.UNKNOWN.value,
    "numeric": SensorDataType.NUMERIC.value,
    "number": SensorDataType.NUMERIC.value,
    "continuous": SensorDataType.NUMERIC.value,
    "float": SensorDataType.NUMERIC.value,
    "double": SensorDataType.NUMERIC.value,
    "int": SensorDataType.NUMERIC.value,
    "integer": SensorDataType.NUMERIC.value,
    "binary": SensorDataType.BINARY.value,
    "bool": SensorDataType.BINARY.value,
    "boolean": SensorDataType.BINARY.value,
    "categorical": SensorDataType.CATEGORICAL.value,
    "category": SensorDataType.CATEGORICAL.value,
    "cat": SensorDataType.CATEGORICAL.value,
    "discrete": SensorDataType.CATEGORICAL.value,
    "high_cardinality": SensorDataType.HIGH_CARDINALITY.value,
    "high-cardinality": SensorDataType.HIGH_CARDINALITY.value,
    "highcardinality": SensorDataType.HIGH_CARDINALITY.value,
    "high_card": SensorDataType.HIGH_CARDINALITY.value,
    "constant": SensorDataType.CONSTANT.value,
}


def normalize_sensor_datatype(value: object, default: str = SensorDataType.UNKNOWN.value) -> str:
    text = str(value or "").strip().lower()
    normalized = text.replace("-", "_").replace(" ", "_")
    default_norm = _ALIASES.get(str(default).strip().lower().replace("-", "_").replace(" ", "_"), SensorDataType.UNKNOWN.value)
    return _ALIASES.get(normalized, default_norm)


def is_numeric_datatype(value: object) -> bool:
    return normalize_sensor_datatype(value) == SensorDataType.NUMERIC.value


def is_categorical_family_datatype(value: object) -> bool:
    dtype = normalize_sensor_datatype(value)
    return dtype in {
        SensorDataType.BINARY.value,
        SensorDataType.CATEGORICAL.value,
        SensorDataType.HIGH_CARDINALITY.value,
        SensorDataType.CONSTANT.value,
    }


def spark_normalized_datatype_expr(col: "Column") -> "Column":
    from pyspark.sql import functions as F

    normalized = F.lower(F.trim(F.coalesce(col.cast("string"), F.lit(""))))
    normalized = F.regexp_replace(normalized, "[-\\s]+", "_")
    return (
        F.when(normalized.isin("numeric", "number", "continuous", "float", "double", "int", "integer"), F.lit(SensorDataType.NUMERIC.value))
        .when(normalized.isin("binary", "bool", "boolean"), F.lit(SensorDataType.BINARY.value))
        .when(normalized.isin("categorical", "category", "cat", "discrete"), F.lit(SensorDataType.CATEGORICAL.value))
        .when(normalized.isin("high_cardinality", "highcardinality", "high_card"), F.lit(SensorDataType.HIGH_CARDINALITY.value))
        .when(normalized == F.lit(SensorDataType.CONSTANT.value), F.lit(SensorDataType.CONSTANT.value))
        .otherwise(F.lit(SensorDataType.UNKNOWN.value))
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.column import Column
