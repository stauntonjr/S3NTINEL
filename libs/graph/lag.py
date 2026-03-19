from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable

from libs.graph.event import EventGraph
from libs.io.schemas import LAG_GRAPH_SCHEMA, LAG_PROFILE_SCHEMA


def _spark_functions():
    from pyspark.sql import functions as F

    return F


@dataclass(frozen=True)
class LagBandSpec:
    name: str
    lower_seconds: float
    upper_seconds: float
    combine_weight: float


def resolve_lag_band_specs(
    bands: Iterable[LagBandSpec] | None,
    *,
    tau_max_seconds: float,
) -> tuple[LagBandSpec, ...]:
    normalized = tuple(
        LagBandSpec(
            name=str(item.name).strip(),
            lower_seconds=float(item.lower_seconds),
            upper_seconds=float(item.upper_seconds),
            combine_weight=float(item.combine_weight),
        )
        for item in (bands or ())
        if str(item.name).strip()
    )
    if not normalized:
        tau = max(float(tau_max_seconds), 1e-6)
        return (LagBandSpec(name="default", lower_seconds=0.0, upper_seconds=tau, combine_weight=1.0),)
    ordered = tuple(sorted(normalized, key=lambda item: (item.lower_seconds, item.upper_seconds, item.name)))
    names: set[str] = set()
    previous_upper: float | None = None
    for band in ordered:
        if band.name in names:
            raise ValueError(f"lag band names must be unique; duplicate name: {band.name}")
        if band.lower_seconds < 0.0:
            raise ValueError(f"lag band lower_seconds must be non-negative; got {band.lower_seconds} for {band.name}")
        if band.upper_seconds <= band.lower_seconds:
            raise ValueError(
                f"lag band upper_seconds must be greater than lower_seconds; got {band.lower_seconds}, {band.upper_seconds} for {band.name}"
            )
        if previous_upper is not None and band.lower_seconds < previous_upper:
            raise ValueError("lag bands must not overlap; configure ascending non-overlapping lag ranges")
        names.add(band.name)
        previous_upper = band.upper_seconds
    return ordered

class LagProfileGraph:
    @staticmethod
    def candidate_pairs_from_graphs_spark(event_df: "DataFrame", transition_df: "DataFrame") -> "DataFrame":
        F = _spark_functions()

        directed_event = event_df.select("parameter_name_u", "parameter_name_v").unionByName(
            event_df.select(
                F.col("parameter_name_v").alias("parameter_name_u"),
                F.col("parameter_name_u").alias("parameter_name_v"),
            )
        )
        return (
            directed_event.unionByName(transition_df.select("parameter_name_u", "parameter_name_v"))
            .where(
                F.col("parameter_name_u").isNotNull()
                & F.col("parameter_name_v").isNotNull()
                & (F.col("parameter_name_u") != F.col("parameter_name_v"))
            )
            .distinct()
        )

    @classmethod
    def from_events_spark(
        cls,
        events_df: "DataFrame",
        *,
        bands: Iterable[LagBandSpec] | None,
        tau_max_seconds: float,
        candidate_pairs_df: "DataFrame | None" = None,
    ) -> "DataFrame":
        F = _spark_functions()
        spark = events_df.sparkSession
        resolved_bands = resolve_lag_band_specs(bands, tau_max_seconds=tau_max_seconds)
        tau_seconds = max(float(max(band.upper_seconds for band in resolved_bands)), float(tau_max_seconds), 1e-6)
        tau_ms = max(int(round(tau_seconds * 1000.0)), 1)
        canonical_events_df = EventGraph.normalize_events_spark(events_df)
        base = canonical_events_df.select(
            "tail_id",
            "flight_id",
            F.col("event_seq_id").alias("event_id"),
            "timestamp_utc",
            "parameter_name",
            F.unix_millis("timestamp_utc").cast("long").alias("timestamp_ms"),
            F.floor(F.unix_millis("timestamp_utc") / F.lit(float(tau_ms))).cast("long").alias("time_bucket"),
        )

        if candidate_pairs_df is None:
            joined = (
                base.alias("prev")
                .join(
                    base.alias("curr"),
                    on=[
                        F.col("prev.tail_id") == F.col("curr.tail_id"),
                        F.col("prev.flight_id") == F.col("curr.flight_id"),
                        F.col("prev.time_bucket") >= F.col("curr.time_bucket") - F.lit(1),
                        F.col("prev.time_bucket") <= F.col("curr.time_bucket"),
                        F.col("prev.event_id") < F.col("curr.event_id"),
                        F.col("prev.parameter_name") != F.col("curr.parameter_name"),
                    ],
                    how="inner",
                )
                .where(F.col("prev.timestamp_ms") >= F.col("curr.timestamp_ms") - F.lit(tau_ms))
                .select(
                    F.col("curr.tail_id").alias("tail_id"),
                    F.col("curr.flight_id").alias("flight_id"),
                    F.col("curr.event_id").alias("curr_event_id"),
                    F.col("curr.timestamp_ms").alias("curr_timestamp_ms"),
                    F.col("prev.parameter_name").alias("parameter_name_u"),
                    F.col("curr.parameter_name").alias("parameter_name_v"),
                    F.col("prev.timestamp_ms").alias("prev_timestamp_ms"),
                )
            )
        else:
            candidate_pairs = (
                candidate_pairs_df.select(
                    F.col("parameter_name_u").cast("string").alias("parameter_name_u"),
                    F.col("parameter_name_v").cast("string").alias("parameter_name_v"),
                )
                .where(
                    F.col("parameter_name_u").isNotNull()
                    & F.col("parameter_name_v").isNotNull()
                    & (F.col("parameter_name_u") != F.col("parameter_name_v"))
                )
                .distinct()
            )
            joined = (
                base.alias("curr")
                .join(
                    F.broadcast(candidate_pairs).alias("cand"),
                    F.col("curr.parameter_name") == F.col("cand.parameter_name_v"),
                    how="inner",
                )
                .join(
                    base.alias("prev"),
                    on=[
                        F.col("prev.tail_id") == F.col("curr.tail_id"),
                        F.col("prev.flight_id") == F.col("curr.flight_id"),
                        F.col("prev.time_bucket") >= F.col("curr.time_bucket") - F.lit(1),
                        F.col("prev.time_bucket") <= F.col("curr.time_bucket"),
                        F.col("prev.event_id") < F.col("curr.event_id"),
                        F.col("prev.parameter_name") == F.col("cand.parameter_name_u"),
                    ],
                    how="inner",
                )
                .where(F.col("prev.timestamp_ms") >= F.col("curr.timestamp_ms") - F.lit(tau_ms))
                .select(
                    F.col("curr.tail_id").alias("tail_id"),
                    F.col("curr.flight_id").alias("flight_id"),
                    F.col("curr.event_id").alias("curr_event_id"),
                    F.col("curr.timestamp_ms").alias("curr_timestamp_ms"),
                    F.col("cand.parameter_name_u").alias("parameter_name_u"),
                    F.col("cand.parameter_name_v").alias("parameter_name_v"),
                    F.col("prev.timestamp_ms").alias("prev_timestamp_ms"),
                )
            )

        deduped = (
            joined.groupBy(
                "tail_id",
                "flight_id",
                "curr_event_id",
                "curr_timestamp_ms",
                "parameter_name_u",
                "parameter_name_v",
            )
            .agg(F.max("prev_timestamp_ms").alias("nearest_prev_timestamp_ms"))
            .withColumn(
                "lag_seconds",
                (F.col("curr_timestamp_ms") - F.col("nearest_prev_timestamp_ms")).cast("double") / F.lit(1000.0),
            )
        )

        band_rows = [
            (band.name, float(band.lower_seconds), float(band.upper_seconds), float(band.combine_weight))
            for band in resolved_bands
        ]
        band_df = spark.createDataFrame(
            band_rows,
            schema="lag_band string, lower_seconds double, upper_seconds double, combine_weight double",
        )
        banded = (
            deduped.crossJoin(F.broadcast(band_df))
            .where(
                (
                    (F.col("lag_seconds") > F.col("lower_seconds"))
                    | ((F.col("lower_seconds") <= F.lit(0.0)) & (F.col("lag_seconds") >= F.col("lower_seconds")))
                )
                & (F.col("lag_seconds") <= F.col("upper_seconds"))
            )
        )
        aggregated = banded.groupBy("parameter_name_u", "parameter_name_v", "lag_band", "upper_seconds").agg(
            F.count(F.lit(1)).cast("int").alias("lag_count"),
            F.avg("lag_seconds").alias("mean_lag_seconds"),
            F.countDistinct(F.struct("tail_id", "flight_id")).cast("int").alias("support_flight_count"),
        )
        source_totals = aggregated.groupBy("parameter_name_u").agg(F.sum("lag_count").cast("double").alias("source_total"))
        return (
            aggregated.join(source_totals, on="parameter_name_u", how="inner")
            .withColumn("conditional_probability", F.col("lag_count") / F.col("source_total"))
            .withColumn(
                "shortness",
                F.greatest(F.lit(0.0), F.lit(1.0) - (F.col("mean_lag_seconds") / F.col("upper_seconds"))),
            )
            .withColumn("lag_weight", F.col("conditional_probability") * F.col("shortness"))
            .withColumn("edge_family", F.lit("lag_profile_directed"))
            .select(
                "parameter_name_u",
                "parameter_name_v",
                "lag_band",
                "lag_count",
                "lag_weight",
                "mean_lag_seconds",
                "support_flight_count",
                "edge_family",
            )
        )

    @classmethod
    def collapse_to_lag_graph_spark(
        cls,
        lag_profile_df: "DataFrame",
        *,
        bands: Iterable[LagBandSpec] | None,
        tau_max_seconds: float,
        min_count: int,
        max_mean_lag_seconds: float | None,
        top_k_outgoing: int,
    ) -> "DataFrame":
        F = _spark_functions()
        from pyspark.sql import Window

        spark = lag_profile_df.sparkSession
        resolved_bands = resolve_lag_band_specs(bands, tau_max_seconds=tau_max_seconds)
        band_df = spark.createDataFrame(
            [(band.name, float(band.combine_weight)) for band in resolved_bands],
            schema="lag_band string, combine_weight double",
        )
        collapsed = (
            lag_profile_df.join(F.broadcast(band_df), on="lag_band", how="inner")
            .groupBy("parameter_name_u", "parameter_name_v")
            .agg(
                F.sum("lag_count").cast("int").alias("lag_count"),
                (
                    F.sum(F.col("mean_lag_seconds") * F.col("lag_count").cast("double"))
                    / F.greatest(F.sum(F.col("lag_count").cast("double")), F.lit(1.0))
                ).alias("mean_lag_seconds"),
                F.sum(F.col("lag_weight") * F.col("combine_weight")).alias("lag_weight"),
            )
        )
        collapsed = collapsed.where(F.col("lag_count") >= F.lit(max(int(min_count), 1)))
        if max_mean_lag_seconds is not None:
            collapsed = collapsed.where(F.col("mean_lag_seconds") <= F.lit(float(max_mean_lag_seconds)))
        out = collapsed.withColumn("edge_family", F.lit("lag_directed")).select(
            "parameter_name_u",
            "parameter_name_v",
            "edge_family",
            "lag_count",
            "lag_weight",
            "mean_lag_seconds",
        )
        if int(top_k_outgoing) > 0:
            rank_window = Window.partitionBy("parameter_name_u").orderBy(
                F.col("lag_weight").desc(),
                F.col("parameter_name_v").asc(),
            )
            out = (
                out.withColumn("rank", F.row_number().over(rank_window))
                .where(F.col("rank") <= F.lit(int(top_k_outgoing)))
                .drop("rank")
            )
        return out

    @staticmethod
    def empty_profile_table(spark: "SparkSession") -> "DataFrame":
        return spark.createDataFrame([], schema=LAG_PROFILE_SCHEMA())

    @staticmethod
    def empty_lag_graph_table(spark: "SparkSession") -> "DataFrame":
        return spark.createDataFrame([], schema=LAG_GRAPH_SCHEMA())


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
