"""Persisted PySpark table primitive built on repo IO helpers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Self

from libs.io import delta
from libs.pyspark.frame import Frame


@dataclass(frozen=True)
class Table(Frame):
    path: str = ""
    format: str = "delta"
    partition_by: tuple[str, ...] = ()

    @classmethod
    def spark_schema(cls) -> "SparkSchemaLike":
        raise NotImplementedError(f"{cls.__name__}.spark_schema() must be overridden")

    @property
    def is_bound(self) -> bool:
        return bool(str(self.path).strip())

    def bind(
        self,
        *,
        path: str,
        format: str | None = None,
        partition_by: "Sequence[str] | None" = None,
    ) -> Self:
        return replace(
            self,
            path=str(path),
            format=self.format if format is None else str(format),
            partition_by=self.partition_by if partition_by is None else tuple(partition_by),
        )

    def with_dataframe(self, dataframe: "DataFrame") -> Self:
        return replace(self, dataframe=dataframe)

    def _require_bound_path(self) -> None:
        if not self.is_bound:
            raise ValueError(f"{type(self).__name__} is not bound to a table path")

    @classmethod
    def read(
        cls,
        spark: "SparkSession",
        path: str,
        *,
        format: str = "delta",
        partition_by: "Sequence[str] | None" = None,
    ) -> Self:
        table = cls(
            dataframe=delta.read_table(spark, path=path, fmt=format),
            path=path,
            format=format,
            partition_by=tuple(partition_by or ()),
        )
        table.validate_schema()
        return table

    def write(self, *, mode: str = "append") -> None:
        self._require_bound_path()
        self.validate_schema()
        delta.write_table(
            self.dataframe,
            path=self.path,
            mode=mode,
            fmt=self.format,
            partition_by=self.partition_by,
        )

    def replace(self) -> None:
        self.write(mode="overwrite")

    def upsert(self, *, merge_keys: "Sequence[str]") -> None:
        self._require_bound_path()
        self.validate_schema()
        delta.upsert_table(
            self.dataframe,
            path=self.path,
            merge_keys=merge_keys,
            fmt=self.format,
            partition_by=self.partition_by,
        )


if TYPE_CHECKING:
    from collections.abc import Sequence

    from pyspark.sql import SparkSession
    from libs.pyspark.schema import SparkSchemaLike
