"""Small Spark expression helpers for typed literals and JSON normalization."""

from __future__ import annotations


def empty_array(element_type: str) -> "Column":
    from pyspark.sql import functions as F

    return F.array().cast(f"array<{str(element_type)}>")


def empty_map(key_type: str = "string", value_type: str = "string") -> "Column":
    from pyspark.sql import functions as F

    return F.map_from_arrays(F.array(), F.array()).cast(f"map<{str(key_type)},{str(value_type)}>")


def sorted_map_json(map_col: "Column") -> "Column":
    from pyspark.sql import functions as F

    return F.to_json(F.map_from_entries(F.array_sort(F.map_entries(map_col))))


def try_cast_double(column_name: str) -> "Column":
    from pyspark.sql import functions as F

    return F.expr(f"try_cast({column_name} as double)")


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column
