"""Spark schema normalization helpers for typed frame and table objects."""

from __future__ import annotations

from typing import TYPE_CHECKING


def coerce_spark_schema(schema: "SparkSchemaLike") -> "StructType":
    from pyspark.sql import types as T

    if isinstance(schema, T.StructType):
        return schema
    if not isinstance(schema, str):
        raise TypeError(f"unsupported Spark schema type: {type(schema).__name__}")

    text = schema.strip()
    if not text:
        raise ValueError("Spark schema text must not be empty")
    if hasattr(T.StructType, "fromDDL"):
        return T.StructType.fromDDL(text)
    parsed = T._parse_datatype_string(text)
    if not isinstance(parsed, T.StructType):
        raise TypeError(f"schema text did not parse to StructType: {text!r}")
    return parsed


if TYPE_CHECKING:
    from pyspark.sql.types import StructType

    SparkSchemaLike = StructType | str
