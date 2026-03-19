"""Canonical parameter datatype definitions and normalization helpers."""

from __future__ import annotations

from enum import Enum


class ParameterDataType(str, Enum):
    NUMERIC = "numeric"
    BINARY = "binary"
    CATEGORICAL = "categorical"
    HIGH_CARDINALITY = "high_cardinality"
    CONSTANT = "constant"
    UNKNOWN = "unknown"


_ALIASES: dict[str, str] = {
    "": ParameterDataType.UNKNOWN.value,
    "unknown": ParameterDataType.UNKNOWN.value,
    "numeric": ParameterDataType.NUMERIC.value,
    "number": ParameterDataType.NUMERIC.value,
    "continuous": ParameterDataType.NUMERIC.value,
    "float": ParameterDataType.NUMERIC.value,
    "double": ParameterDataType.NUMERIC.value,
    "int": ParameterDataType.NUMERIC.value,
    "integer": ParameterDataType.NUMERIC.value,
    "binary": ParameterDataType.BINARY.value,
    "bool": ParameterDataType.BINARY.value,
    "boolean": ParameterDataType.BINARY.value,
    "categorical": ParameterDataType.CATEGORICAL.value,
    "category": ParameterDataType.CATEGORICAL.value,
    "cat": ParameterDataType.CATEGORICAL.value,
    "discrete": ParameterDataType.CATEGORICAL.value,
    "high_cardinality": ParameterDataType.HIGH_CARDINALITY.value,
    "high-cardinality": ParameterDataType.HIGH_CARDINALITY.value,
    "highcardinality": ParameterDataType.HIGH_CARDINALITY.value,
    "high_card": ParameterDataType.HIGH_CARDINALITY.value,
    "constant": ParameterDataType.CONSTANT.value,
}


def normalize_parameter_datatype(value: object, default: str = ParameterDataType.UNKNOWN.value) -> str:
    text = str(value or "").strip().lower()
    normalized = text.replace("-", "_").replace(" ", "_")
    default_norm = _ALIASES.get(
        str(default).strip().lower().replace("-", "_").replace(" ", "_"),
        ParameterDataType.UNKNOWN.value,
    )
    return _ALIASES.get(normalized, default_norm)


def is_numeric_parameter_datatype(value: object) -> bool:
    return normalize_parameter_datatype(value) == ParameterDataType.NUMERIC.value


def is_categorical_family_parameter_datatype(value: object) -> bool:
    dtype = normalize_parameter_datatype(value)
    return dtype in {
        ParameterDataType.BINARY.value,
        ParameterDataType.CATEGORICAL.value,
        ParameterDataType.HIGH_CARDINALITY.value,
        ParameterDataType.CONSTANT.value,
    }


def spark_normalized_parameter_datatype_expr(col: "Column") -> "Column":
    from pyspark.sql import functions as F

    normalized = F.lower(F.trim(F.coalesce(col.cast("string"), F.lit(""))))
    normalized = F.regexp_replace(normalized, "[-\\s]+", "_")
    return (
        F.when(
            normalized.isin("numeric", "number", "continuous", "float", "double", "int", "integer"),
            F.lit(ParameterDataType.NUMERIC.value),
        )
        .when(normalized.isin("binary", "bool", "boolean"), F.lit(ParameterDataType.BINARY.value))
        .when(normalized.isin("categorical", "category", "cat", "discrete"), F.lit(ParameterDataType.CATEGORICAL.value))
        .when(
            normalized.isin("high_cardinality", "highcardinality", "high_card"),
            F.lit(ParameterDataType.HIGH_CARDINALITY.value),
        )
        .when(normalized == F.lit(ParameterDataType.CONSTANT.value), F.lit(ParameterDataType.CONSTANT.value))
        .otherwise(F.lit(ParameterDataType.UNKNOWN.value))
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.column import Column
