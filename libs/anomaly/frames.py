"""Typed Spark frames for reusable anomaly attribution context."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libs.perf.annotations import hot_path
from libs.pyspark import Frame


@dataclass(frozen=True)
class AnomalySubsystemContextFrame(Frame):
    @classmethod
    @hot_path
    def from_events_and_windows(
        cls,
        events_df: "DataFrame",
        windows_df: "DataFrame",
        hierarchy_sensor_map_df: "DataFrame",
        *,
        top_k_per_subsystem: int,
    ) -> "AnomalySubsystemContextFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        mapped_events = events_df.join(
            hierarchy_sensor_map_df.select("parameter_name", "system_id", "subsystem_id", "module_id"),
            on="parameter_name",
            how="inner",
        )
        events_in_windows = (
            mapped_events.alias("e")
            .join(
                windows_df.alias("w"),
                on=(
                    (F.col("e.tail_id") == F.col("w.tail_id"))
                    & (F.col("e.flight_id") == F.col("w.flight_id"))
                    & (F.col("e.timestamp_utc") >= F.col("w.t_start"))
                    & (F.col("e.timestamp_utc") <= F.col("w.t_end"))
                ),
                how="inner",
            )
            .select(
                F.col("w.tail_id").alias("tail_id"),
                F.col("w.flight_id").alias("flight_id"),
                F.col("w.win_id").alias("win_id"),
                F.col("w.date_utc").alias("date_utc"),
                F.col("e.subsystem_id").alias("subsystem_id"),
                F.col("e.parameter_name").alias("parameter_name"),
                F.col("e.event_type_detected").alias("event_type_detected"),
            )
        )

        sensor_counts = events_in_windows.groupBy(
            "tail_id", "flight_id", "win_id", "date_utc", "subsystem_id", "parameter_name"
        ).agg(
            F.count("*").alias("sensor_event_count"),
            F.sum(
                F.when(
                    F.col("event_type_detected").isin(
                        "transition",
                        "dropped",
                        "state_enter",
                        "state_exit",
                        "dwell_bucket",
                        "dwell_guard",
                        "dwell_violation",
                        "illegal_transition",
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("categorical_count"),
        )

        subsystem_totals = sensor_counts.groupBy(
            "tail_id", "flight_id", "win_id", "date_utc", "subsystem_id"
        ).agg(F.sum("sensor_event_count").alias("subsystem_event_total"))

        with_scores = (
            sensor_counts.alias("s")
            .join(
                subsystem_totals.alias("t"),
                on=["tail_id", "flight_id", "win_id", "date_utc", "subsystem_id"],
                how="inner",
            )
            .withColumn(
                "sensor_score",
                F.when(
                    F.col("subsystem_event_total") > F.lit(0),
                    F.col("sensor_event_count") / F.col("subsystem_event_total"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("events_score", F.col("sensor_score"))
            .withColumn(
                "categorical_score",
                F.when(
                    F.col("subsystem_event_total") > F.lit(0),
                    F.col("categorical_count") / F.col("subsystem_event_total"),
                ).otherwise(F.lit(0.0)),
            )
        )

        rank_window = Window.partitionBy(
            "tail_id", "flight_id", "win_id", "date_utc", "subsystem_id"
        ).orderBy(F.col("sensor_event_count").desc(), F.col("parameter_name").asc())
        top_sensors = (
            with_scores.withColumn("rn", F.row_number().over(rank_window))
            .where(F.col("rn") <= F.lit(max(int(top_k_per_subsystem), 1)))
            .select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                "subsystem_id",
                "rn",
                F.struct(
                    F.col("parameter_name").alias("parameter_name"),
                    F.col("sensor_score").cast("double").alias("sensor_score"),
                    F.col("events_score").cast("double").alias("event_score"),
                    F.col("categorical_score").cast("double").alias("categorical_event_score"),
                ).alias("sensor_struct"),
            )
        )

        top_sensors_by_subsystem = (
            top_sensors.groupBy("tail_id", "flight_id", "win_id", "date_utc", "subsystem_id")
            .agg(F.collect_list(F.struct(F.col("rn"), F.col("sensor_struct"))).alias("ranked_sensors"))
            .withColumn("top_sensors", F.expr("transform(array_sort(ranked_sensors), x -> x.sensor_struct)"))
            .drop("ranked_sensors")
        )

        sensor_scores = (
            top_sensors.groupBy(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.col("sensor_struct.parameter_name").alias("parameter_name"),
            )
            .agg(F.sum(F.col("sensor_struct.sensor_score")).cast("double").alias("sensor_score"))
            .groupBy("tail_id", "flight_id", "win_id", "date_utc")
            .agg(
                F.map_from_entries(F.collect_list(F.struct(F.col("parameter_name"), F.col("sensor_score")))).alias(
                    "sensor_scores"
                )
            )
        )

        top_sensors_map = top_sensors_by_subsystem.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
            F.map_from_entries(F.collect_list(F.struct(F.col("subsystem_id"), F.col("top_sensors")))).alias(
                "top_sensors_by_subsystem"
            )
        )

        return cls(
            dataframe=top_sensors_map.join(sensor_scores, on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")
        )


@dataclass(frozen=True)
class AnomalyPanelContextFrame(Frame):
    @classmethod
    @hot_path
    def from_raw_and_windows(
        cls,
        raw_df: "DataFrame",
        windows_df: "DataFrame",
        *,
        max_items: int = 5,
    ) -> "AnomalyPanelContextFrame":
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        ts_col = "timestamp_utc" if "timestamp_utc" in raw_df.columns else ("ts" if "ts" in raw_df.columns else None)
        if ts_col is None:
            return cls(
                dataframe=windows_df.select("tail_id", "flight_id", "win_id", "date_utc")
                .where(F.lit(False))
                .select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    F.lit(None)
                    .cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>")
                    .alias("panel_context"),
                )
            )

        name_col = "parameter_name" if "parameter_name" in raw_df.columns else ("sensor" if "sensor" in raw_df.columns else None)
        value_col = "parameter_value" if "parameter_value" in raw_df.columns else ("state" if "state" in raw_df.columns else None)
        if name_col is None or value_col is None:
            return cls(
                dataframe=windows_df.select("tail_id", "flight_id", "win_id", "date_utc")
                .where(F.lit(False))
                .select(
                    "tail_id",
                    "flight_id",
                    "win_id",
                    "date_utc",
                    F.lit(None)
                    .cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>")
                    .alias("panel_context"),
                )
            )

        keyword_expr = "(lcd|panel|msg|message|cas|warn|warning|fault|caution|annunc|text)"
        candidates = (
            raw_df.select(
                F.col("tail_id"),
                F.col("flight_id"),
                F.col(ts_col).cast("timestamp").alias("ts"),
                F.col(name_col).cast("string").alias("source_name"),
                F.trim(F.col(value_col).cast("string")).alias("text_value"),
                F.col("date_utc"),
            )
            .where(F.col("ts").isNotNull())
            .where(F.col("source_name").isNotNull())
            .where(F.col("text_value").isNotNull() & (F.col("text_value") != F.lit("")))
            .where(F.expr("try_cast(text_value as double) is null"))
            .withColumn(
                "keyword_hit",
                (F.lower(F.col("source_name")).rlike(keyword_expr) | F.lower(F.col("text_value")).rlike(keyword_expr)).cast(
                    "int"
                ),
            )
        )

        events_in_windows = (
            candidates.alias("r")
            .join(
                windows_df.alias("w"),
                on=(
                    (F.col("r.tail_id") == F.col("w.tail_id"))
                    & (F.col("r.flight_id") == F.col("w.flight_id"))
                    & (F.col("r.ts") >= F.col("w.t_start"))
                    & (F.col("r.ts") <= F.col("w.t_end"))
                ),
                how="inner",
            )
            .select(
                F.col("w.tail_id").alias("tail_id"),
                F.col("w.flight_id").alias("flight_id"),
                F.col("w.win_id").alias("win_id"),
                F.col("w.date_utc").alias("date_utc"),
                F.col("r.ts").alias("ts"),
                F.col("r.source_name").alias("source_name"),
                F.col("r.text_value").alias("text_value"),
                F.col("r.keyword_hit").alias("keyword_hit"),
            )
        )

        rank_window = Window.partitionBy("tail_id", "flight_id", "win_id", "date_utc").orderBy(
            F.col("keyword_hit").desc(),
            F.col("ts").desc(),
            F.col("source_name").asc(),
            F.col("text_value").asc(),
        )

        limited = (
            events_in_windows.withColumn("rn", F.row_number().over(rank_window))
            .where(F.col("rn") <= F.lit(max(int(max_items), 1)))
            .groupBy("tail_id", "flight_id", "win_id", "date_utc")
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("rn").alias("rn"),
                        F.col("text_value").alias("text_value"),
                        F.col("source_name").alias("source_name"),
                    )
                ).alias("items")
            )
            .withColumn("ordered_items", F.expr("array_sort(items)"))
            .drop("items")
        )

        return cls(
            dataframe=limited.select(
                "tail_id",
                "flight_id",
                "win_id",
                "date_utc",
                F.struct(
                    F.array_sort(F.array_distinct(F.expr("transform(ordered_items, x -> x.text_value)"))).alias("text"),
                    F.array_sort(
                        F.array_distinct(
                            F.expr(
                                "filter(transform(ordered_items, x -> regexp_extract(x.text_value, '([A-Z]{2,}[A-Z0-9_-]*)', 1)), x -> x != '')"
                            )
                        )
                    ).alias("message_codes"),
                    F.array_sort(F.array_distinct(F.expr("transform(ordered_items, x -> x.source_name)"))).alias("source"),
                ).alias("panel_context"),
            )
        )


@dataclass(frozen=True)
class AnomalyAttributionContextFrame(Frame):
    @classmethod
    @hot_path
    def from_context_frames(
        cls,
        *,
        subsystem_context: AnomalySubsystemContextFrame,
        panel_context: AnomalyPanelContextFrame,
    ) -> "AnomalyAttributionContextFrame":
        from pyspark.sql import functions as F

        null_panel_context = F.lit(None).cast(
            "struct<text:array<string>,message_codes:array<string>,source:array<string>>"
        )
        empty_sensor_scores = F.expr("cast(map() as map<string,double>)")
        subsystem_context_df = subsystem_context.to_dataframe()
        panel_context_df = panel_context.to_dataframe()
        return cls(
            dataframe=(
                subsystem_context_df.alias("s")
                .join(panel_context_df.alias("p"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="full_outer")
                .select(
                    F.coalesce(F.col("s.tail_id"), F.col("p.tail_id")).alias("tail_id"),
                    F.coalesce(F.col("s.flight_id"), F.col("p.flight_id")).alias("flight_id"),
                    F.coalesce(F.col("s.win_id"), F.col("p.win_id")).alias("win_id"),
                    F.coalesce(F.col("s.date_utc"), F.col("p.date_utc")).alias("date_utc"),
                    F.coalesce(
                        F.col("s.top_sensors_by_subsystem"),
                        F.expr(
                            "cast(map() as map<string,array<struct<parameter_name:string,sensor_score:double,event_score:double,categorical_event_score:double>>>)"
                        ),
                    ).alias("top_sensors_by_subsystem"),
                    F.coalesce(F.col("s.sensor_scores"), empty_sensor_scores).alias("sensor_scores"),
                    F.coalesce(F.col("p.panel_context"), null_panel_context).alias("panel_context"),
                )
            )
        )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
