"""Typed Spark tables for event artifacts."""

from __future__ import annotations

from dataclasses import dataclass

from libs.io.schemas.events import EVENTS_SCHEMA
from libs.pyspark import Table


@dataclass(frozen=True)
class EventsTable(Table):
    partition_by: tuple[str, ...] = ("tail_id",)

    @classmethod
    def spark_schema(cls):
        return EVENTS_SCHEMA()
