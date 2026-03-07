# File: libs/anomaly/attribution.py
"""Anomaly attribution table builders."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_window_subsystem_top_sensors_df(
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
            F.when(F.col("subsystem_event_total") > F.lit(0), F.col("sensor_event_count") / F.col("subsystem_event_total")).otherwise(F.lit(0.0)),
        )
        .withColumn("events_score", F.col("sensor_score"))
        .withColumn(
            "categorical_score",
            F.when(F.col("subsystem_event_total") > F.lit(0), F.col("categorical_count") / F.col("subsystem_event_total")).otherwise(F.lit(0.0)),
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
        top_sensors.groupBy("tail_id", "flight_id", "win_id", "date_utc", F.col("sensor_struct.parameter_name").alias("parameter_name"))
        .agg(F.sum(F.col("sensor_struct.sensor_score")).cast("double").alias("sensor_score"))
        .groupBy("tail_id", "flight_id", "win_id", "date_utc")
        .agg(
            F.map_from_entries(F.collect_list(F.struct(F.col("parameter_name"), F.col("sensor_score")))).alias(
                "sensor_scores"
            )
        )
    )

    top_sensors_map = top_sensors_by_subsystem.groupBy("tail_id", "flight_id", "win_id", "date_utc").agg(
        F.map_from_entries(F.collect_list(F.struct(F.col("subsystem_id"), F.col("top_sensors")))).alias("top_sensors_by_subsystem")
    )

    return top_sensors_map.join(sensor_scores, on=["tail_id", "flight_id", "win_id", "date_utc"], how="left")


@hot_path
def build_window_panel_context_df(
    raw_df: "DataFrame",
    windows_df: "DataFrame",
    *,
    max_items: int = 5,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    ts_col = "timestamp_utc" if "timestamp_utc" in raw_df.columns else ("ts" if "ts" in raw_df.columns else None)
    if ts_col is None:
        return windows_df.select("tail_id", "flight_id", "win_id", "date_utc").where(F.lit(False)).select(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            F.lit(None).cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>").alias(
                "panel_context"
            ),
        )

    name_col = "parameter_name" if "parameter_name" in raw_df.columns else ("sensor" if "sensor" in raw_df.columns else None)
    value_col = "parameter_value" if "parameter_value" in raw_df.columns else ("state" if "state" in raw_df.columns else None)
    if name_col is None or value_col is None:
        return windows_df.select("tail_id", "flight_id", "win_id", "date_utc").where(F.lit(False)).select(
            "tail_id",
            "flight_id",
            "win_id",
            "date_utc",
            F.lit(None).cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>").alias(
                "panel_context"
            ),
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
            (F.lower(F.col("source_name")).rlike(keyword_expr) | F.lower(F.col("text_value")).rlike(keyword_expr)).cast("int"),
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

    return limited.select(
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


@hot_path
def build_anomaly_telemetry_attribution_df(
    calibrated_df: "DataFrame",
    windows_df: "DataFrame",
    raw_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    datatype_col = (
        F.col("r.parameter_datatype_label")
        if "parameter_datatype_label" in raw_df.columns
        else F.lit(None).cast("string")
    )
    telemetry_in_windows = (
        calibrated_df.alias("c")
        .where(F.col("emit_ready") == F.lit(True))
        .join(windows_df.alias("w"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="inner")
        .join(
            raw_df.alias("r"),
            on=(
                (F.col("c.tail_id") == F.col("r.tail_id"))
                & (F.col("c.flight_id") == F.col("r.flight_id"))
                & (F.col("r.timestamp_utc") >= F.col("w.t_start"))
                & (F.col("r.timestamp_utc") <= F.col("w.t_end"))
            ),
            how="inner",
        )
        .join(hierarchy_sensor_map_df.alias("h"), on=F.col("r.parameter_name") == F.col("h.parameter_name"), how="left")
        .select(
            F.col("c.tail_id").alias("tail_id"),
            F.col("c.flight_id").alias("flight_id"),
            F.col("c.win_id").alias("win_id"),
            F.col("r.timestamp_utc").alias("timestamp_utc"),
            F.col("r.parameter_name").alias("parameter_name"),
            F.col("r.parameter_value").alias("parameter_value"),
            datatype_col.alias("parameter_datatype_label"),
            F.col("h.system_id").alias("system_id"),
            F.col("h.subsystem_id").alias("subsystem_id"),
            F.col("h.module_id").alias("module_id"),
            F.col("c.global_score").cast("double").alias("window_global_score"),
            F.col("c.severity").alias("severity"),
            F.col("c.date_utc").alias("date_utc"),
        )
    )
    return telemetry_in_windows


@hot_path
def build_anomaly_event_attribution_df(
    calibrated_df: "DataFrame",
    windows_df: "DataFrame",
    events_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    anomaly_type_col = (
        F.col("e.anomaly_type_detected")
        if "anomaly_type_detected" in events_df.columns
        else F.lit(None).cast("string")
    )
    anomaly_score_col = (
        F.col("e.anomaly_score_detected")
        if "anomaly_score_detected" in events_df.columns
        else F.lit(None).cast("double")
    )
    events_in_windows = (
        calibrated_df.alias("c")
        .where(F.col("emit_ready") == F.lit(True))
        .join(windows_df.alias("w"), on=["tail_id", "flight_id", "win_id", "date_utc"], how="inner")
        .join(
            events_df.alias("e"),
            on=(
                (F.col("c.tail_id") == F.col("e.tail_id"))
                & (F.col("c.flight_id") == F.col("e.flight_id"))
                & (F.col("e.timestamp_utc") >= F.col("w.t_start"))
                & (F.col("e.timestamp_utc") <= F.col("w.t_end"))
            ),
            how="inner",
        )
        .join(hierarchy_sensor_map_df.alias("h"), on=F.col("e.parameter_name") == F.col("h.parameter_name"), how="left")
        .select(
            F.col("c.tail_id").alias("tail_id"),
            F.col("c.flight_id").alias("flight_id"),
            F.col("c.win_id").alias("win_id"),
            F.col("e.timestamp_utc").alias("timestamp_utc"),
            F.col("e.parameter_name").alias("parameter_name"),
            F.col("e.event_type_detected").alias("event_type_detected"),
            anomaly_type_col.alias("anomaly_type_detected"),
            anomaly_score_col.cast("double").alias("anomaly_score_detected"),
            F.col("h.system_id").alias("system_id"),
            F.col("h.subsystem_id").alias("subsystem_id"),
            F.col("h.module_id").alias("module_id"),
            F.col("c.global_score").cast("double").alias("window_global_score"),
            F.col("c.severity").alias("severity"),
            F.col("c.date_utc").alias("date_utc"),
        )
    )
    return events_in_windows


@hot_path
def build_anomaly_window_attribution_df(
    calibrated_df: "DataFrame",
    phase_windows_df: "DataFrame",
    windows_df: "DataFrame",
    events_df: "DataFrame",
    hierarchy_sensor_map_df: "DataFrame",
    raw_df: "DataFrame",
    top_k_per_subsystem: int = 5,
) -> "DataFrame":
    from pyspark.sql import functions as F

    null_panel_context = F.lit(None).cast(
        "struct<text:array<string>,message_codes:array<string>,source:array<string>>"
    )
    empty_sensor_scores = F.expr("cast(map() as map<string,double>)")

    base = (
        calibrated_df.alias("c")
        .join(
            phase_windows_df.alias("p"),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
        .join(
            windows_df.alias("w"),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
        .where(F.col("c.emit_ready") == F.lit(True))
    )

    sensor_scores_col = empty_sensor_scores
    top_sensors_df = build_window_subsystem_top_sensors_df(
        events_df=events_df,
        windows_df=windows_df,
        hierarchy_sensor_map_df=hierarchy_sensor_map_df,
        top_k_per_subsystem=top_k_per_subsystem,
    )
    base = base.join(
        top_sensors_df.alias("t"),
        on=["tail_id", "flight_id", "win_id", "date_utc"],
        how="left",
    )
    sensor_scores_col = F.coalesce(F.col("t.sensor_scores"), empty_sensor_scores)

    panel_context_df = build_window_panel_context_df(raw_df=raw_df, windows_df=windows_df)
    base = base.join(
        panel_context_df.alias("pc"),
        on=["tail_id", "flight_id", "win_id", "date_utc"],
        how="left",
    )
    panel_context_col = F.coalesce(F.col("pc.panel_context"), null_panel_context)

    return base.select(
        F.col("c.tail_id").alias("tail_id"),
        F.col("c.flight_id").alias("flight_id"),
        F.col("c.win_id").alias("win_id"),
        F.coalesce(F.col("w.t_end"), F.col("w.t_start"), F.current_timestamp()).alias("timestamp_utc"),
        F.col("c.phase_state_detected").alias("phase_state_detected"),
        F.col("c.phase_id_detected").cast("int").alias("phase_id_detected"),
        F.col("c.phase_confidence_detected").cast("double").alias("phase_confidence_detected"),
        F.col("c.distance_to_centroid_detected").cast("double").alias("distance_to_centroid_detected"),
        F.col("c.drift_magnitude").cast("double").alias("drift_magnitude"),
        F.col("c.breadth").cast("double").alias("breadth"),
        F.col("c.global_score").cast("double").alias("global_score"),
        F.col("c.p_value").cast("double").alias("p_value"),
        F.col("c.severity").alias("severity"),
        F.col("c.dominant_subsystem_id").alias("dominant_subsystem_id"),
        F.col("c.dominant_score_component").alias("dominant_score_component"),
        panel_context_col.alias("panel_context"),
        F.expr(
            """
            transform(
              map_entries(coalesce(c.subsystem_scores, cast(map() as map<string,double>))),
              x -> named_struct(
                'id', x.key,
                'name', x.key,
                'score', cast(x.value as double),
                'score_component_contrib', map(
                    'structure', cast(coalesce(c.score_component_scores['structure'], 0D) * x.value as double),
                    'reconstruction', cast(coalesce(c.score_component_scores['reconstruction'], 0D) * x.value as double)
                ),
                                'top_sensors', coalesce(
                                    element_at(t.top_sensors_by_subsystem, x.key),
                                    cast(array() as array<struct<parameter_name:string,sensor_score:double,event_score:double,categorical_event_score:double>>)
                                )
              )
            )
            """
        ).alias("subsystems"),
        F.struct(
            F.col("c.score_component_scores").alias("score_component_scores"),
            sensor_scores_col.alias("sensor_scores"),
        ).alias("attribution_context"),
        F.struct(
            F.lit(1).alias("backbone"),
            F.lit(1).alias("graph"),
            F.lit(1).alias("phase"),
            F.lit(1).alias("scoring"),
            F.lit(1).alias("calibration"),
        ).alias("artifact_versions"),
        F.col("c.date_utc").alias("date_utc"),
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
