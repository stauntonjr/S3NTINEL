"""Subsystem-level anomaly attribution helpers."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_window_subsystem_context_table(
    events_df: "DataFrame",
    windows_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
    *,
    top_k_per_subsystem: int,
) -> "DataFrame":
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

    return top_sensors_map.join(sensor_scores, on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
