# File: libs/anomaly/build.py
"""Spark-native anomaly object builders."""

from __future__ import annotations

from libs.perf.annotations import hot_path


@hot_path
def build_anomalies_df(
    calibrated_df: "DataFrame",
    phase_windows_df: "DataFrame",
    signatures_df: "DataFrame",
    windows_df: "DataFrame",
) -> "DataFrame":
    from pyspark.sql import functions as F

    base = (
        calibrated_df.alias("c")
        .join(
            phase_windows_df.alias("p"),
            on=["tail_id", "flight_id", "win_id", "date_utc"],
            how="left",
        )
        .join(
            signatures_df.alias("s"),
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

    return base.select(
        F.col("c.tail_id").alias("tail_id"),
        F.col("c.flight_id").alias("flight_id"),
        F.col("c.win_id").alias("win_id"),
        F.coalesce(F.col("w.t_end"), F.col("w.t_start"), F.current_timestamp()).alias("ts"),
        F.col("c.phase_state").alias("phase_state"),
        F.col("c.phase_id").cast("int").alias("phase_id"),
        F.col("c.phase_confidence").cast("double").alias("phase_confidence"),
        F.col("c.distance_to_centroid").cast("double").alias("distance_to_centroid"),
        F.col("c.drift_magnitude").cast("double").alias("drift_magnitude"),
        F.col("c.breadth").cast("double").alias("breadth"),
        F.col("c.global_score").cast("double").alias("global_score"),
        F.col("c.p_value").cast("double").alias("p_value"),
        F.col("c.severity").alias("severity"),
        F.col("c.dominant_subsystem").alias("dominant_subsystem"),
        F.col("c.dominant_block").alias("dominant_block"),
        F.lit(None).cast("struct<text:array<string>,message_codes:array<string>,source:array<string>>").alias(
            "panel_context"
        ),
        F.expr(
            "cast(array() as array<struct<id:string,name:string,score:double,block_contrib:map<string,double>,top_sensors:array<struct<sensor_id:string,score:double,pivot:double,cur:double,events:double,categorical:double,cooccurrence:double>>>>)"
        ).alias(
            "subsystems"
        ),
        F.struct(
            F.col("c.block_scores").alias("block_scores"),
            F.expr("cast(map() as map<string,double>)").alias("sensor_scores"),
        ).alias("raw"),
        F.struct(
            F.lit(1).alias("backbone"),
            F.coalesce(F.col("s.sig_version"), F.lit(1)).cast("int").alias("signature"),
            F.lit(1).alias("scoring"),
            F.lit(1).alias("calibration"),
        ).alias("versions"),
        F.col("c.date_utc").alias("date_utc"),
    )


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
