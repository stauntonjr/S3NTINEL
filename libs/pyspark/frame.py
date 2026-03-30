"""Base typed in-memory PySpark DataFrame wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from libs.pyspark.schema import coerce_spark_schema


@dataclass(frozen=True)
class Frame:
    dataframe: "DataFrame"

    @classmethod
    def from_dataframe(cls, dataframe: "DataFrame") -> Self:
        return cls(dataframe=dataframe)

    @classmethod
    def spark_schema(cls) -> "SparkSchemaLike | None":
        return None

    def to_dataframe(self) -> "DataFrame":
        return self.dataframe

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(str(column_name) for column_name in self.dataframe.columns)

    def validate_schema(self) -> None:
        from pyspark.sql import types as T

        def normalized_type(data_type: "T.DataType") -> object:
            if isinstance(data_type, T.StructType):
                return ("struct", tuple((field.name, normalized_type(field.dataType)) for field in data_type.fields))
            if isinstance(data_type, T.ArrayType):
                return ("array", normalized_type(data_type.elementType))
            if isinstance(data_type, T.MapType):
                return ("map", normalized_type(data_type.keyType), normalized_type(data_type.valueType))
            return data_type.simpleString()

        schema = self.spark_schema()
        if schema is None:
            return
        expected_schema = coerce_spark_schema(schema)
        actual_schema = self.dataframe.schema
        if actual_schema == expected_schema:
            return
        actual_fields = {field.name: normalized_type(field.dataType) for field in actual_schema.fields}
        expected_fields = {field.name: normalized_type(field.dataType) for field in expected_schema.fields}
        if actual_fields == expected_fields:
            return
        missing_columns = tuple(name for name in expected_fields if name not in actual_fields)
        unexpected_columns = tuple(name for name in actual_fields if name not in expected_fields)
        mismatched_columns = tuple(
            name
            for name in expected_fields
            if name in actual_fields and actual_fields[name] != expected_fields[name]
        )
        if True:
            raise ValueError(
                f"{type(self).__name__} schema mismatch: expected {expected_schema.simpleString()}, "
                f"got {actual_schema.simpleString()}, "
                f"missing={missing_columns}, unexpected={unexpected_columns}, mismatched={mismatched_columns}"
            )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from libs.pyspark.schema import SparkSchemaLike
