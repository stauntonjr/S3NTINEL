"""Narrow pandas frame helper for report and bundle objects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pandas as pd


DEFAULT_TIMESTAMP_COLUMNS = (
    "timestamp_utc",
    "timestamp_start",
    "timestamp_end",
    "t_start",
    "t_end",
)


@dataclass(frozen=True)
class ReportFrame:
    dataframe: pd.DataFrame

    @classmethod
    def empty(cls, *, columns: "Sequence[str]" = ()) -> "ReportFrame":
        return cls(dataframe=pd.DataFrame(columns=list(columns)))

    @classmethod
    def from_records(
        cls,
        records: "Sequence[dict[str, object]]",
        *,
        columns: "Sequence[str]" = (),
    ) -> "ReportFrame":
        if records:
            return cls(dataframe=pd.DataFrame.from_records(records))
        return cls.empty(columns=columns)

    def to_pandas(self) -> pd.DataFrame:
        return self.dataframe

    def select_available(self, columns: "Sequence[str]") -> "ReportFrame":
        selected = [column for column in columns if column in self.dataframe.columns]
        if not selected:
            return self
        return type(self)(dataframe=self.dataframe[selected])

    def normalize_timestamps(self, columns: "Sequence[str]" = DEFAULT_TIMESTAMP_COLUMNS) -> "ReportFrame":
        if self.dataframe.empty:
            return type(self)(dataframe=self.dataframe.copy())
        out = self.dataframe.copy()
        for column_name in columns:
            if column_name in out.columns:
                out[column_name] = pd.to_datetime(out[column_name], utc=True, errors="coerce")
        return type(self)(dataframe=out)


if TYPE_CHECKING:
    from collections.abc import Sequence
