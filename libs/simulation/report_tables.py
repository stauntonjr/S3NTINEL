"""Reusable Spark table projections for simulation run reporting."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from libs.io.delta import read_table
from libs.reporting import ReportFrame


@dataclass(frozen=True)
class ArtifactView:
    artifact_name: str
    columns: tuple[str, ...] = ()
    order_by: tuple[str, ...] = ()

    def apply(self, df: Any | None) -> Any | None:
        if df is None:
            return None
        selected_columns = [column for column in self.columns if column in df.columns]
        if selected_columns:
            df = df.select(*selected_columns)
        if self.order_by:
            order_columns = [column for column in self.order_by if column in df.columns]
            if order_columns:
                df = df.orderBy(*order_columns)
        return df


@dataclass(frozen=True)
class RunArtifactBundle:
    tables: dict[str, Any | None]

    @classmethod
    def load(
        cls,
        *,
        spark: Any,
        paths: Any,
        table_format: str,
        views: Iterable[ArtifactView],
    ) -> "RunArtifactBundle":
        columns_by_artifact: dict[str, set[str]] = {}
        for view in views:
            columns_by_artifact.setdefault(str(view.artifact_name), set()).update(view.columns)

        loaded_tables: dict[str, Any | None] = {}
        for artifact_name, selected_columns in columns_by_artifact.items():
            path = paths.artifact_path(artifact_name)
            if not path.exists():
                loaded_tables[artifact_name] = None
                continue
            df = read_table(spark, str(path), fmt=table_format)
            existing_columns = [column for column in selected_columns if column in df.columns]
            if existing_columns:
                df = df.select(*existing_columns)
            loaded_tables[artifact_name] = df
        return cls(tables=loaded_tables)

    def table(self, artifact_name: str) -> Any | None:
        return self.tables.get(str(artifact_name))

    def records(self, view: ArtifactView) -> list[dict[str, Any]]:
        df = view.apply(self.table(view.artifact_name))
        if df is None:
            return []
        return [row.asDict(recursive=True) for row in df.collect()]

    def report_frame(self, view: ArtifactView) -> ReportFrame:
        return ReportFrame.from_records(self.records(view), columns=view.columns)

    def pandas(self, view: ArtifactView) -> pd.DataFrame:
        return self.report_frame(view).to_pandas()
