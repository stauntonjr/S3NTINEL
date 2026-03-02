"""Build and fuse sensor graphs from CUR-like continuous structure and event co-occurrence."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from libs.events.categorical import build_categorical_events
from libs.events.extrema import build_continuous_events
from libs.perf.annotations import hot_path


def build_sensor_normalization_profile(
    raw_df: "DataFrame",
    *,
    min_sensor_points: int,
) -> "DataFrame":
    numeric = raw_df.where(F.col("val").isNotNull()).select("sensor", F.col("val").cast("double").alias("val"))
    quantiles = F.expr("percentile_approx(val, array(0.25, 0.50, 0.75), 1000)")
    stats = (
        numeric.groupBy("sensor")
        .agg(
            F.count("*").alias("value_count"),
            F.avg("val").alias("mean"),
            F.stddev_pop("val").alias("stddev_pop"),
            F.min("val").alias("min_value"),
            F.max("val").alias("max_value"),
            quantiles.alias("quantiles"),
        )
        .where(F.col("value_count") >= int(max(min_sensor_points, 1)))
        .withColumn("q25", F.col("quantiles").getItem(0).cast("double"))
        .withColumn("q50", F.col("quantiles").getItem(1).cast("double"))
        .withColumn("q75", F.col("quantiles").getItem(2).cast("double"))
        .withColumn("iqr", (F.col("q75") - F.col("q25")).cast("double"))
        .withColumn("scale_std", F.when(F.col("stddev_pop") > 1e-12, F.col("stddev_pop")).otherwise(F.lit(1.0)))
        .withColumn("scale_iqr", F.when(F.col("iqr") > 1e-12, F.col("iqr") / F.lit(1.349)).otherwise(F.lit(1.0)))
        .drop("quantiles")
    )
    return stats


def _normalize_numeric_values(
    numeric_df: "DataFrame",
    normalization_profile_df: "DataFrame",
    *,
    normalize_mode: str,
    normalize_clip_sigma: float,
) -> "DataFrame":
    mode = str(normalize_mode or "none").strip().lower()
    if mode == "none":
        return numeric_df

    joined = numeric_df.join(
        normalization_profile_df.select("sensor", "mean", "q50", "scale_std", "scale_iqr"),
        on="sensor",
        how="left",
    )
    if mode == "robust":
        robust_expr = (F.col("val") - F.col("q50")) / F.greatest(F.coalesce(F.col("scale_iqr"), F.lit(1.0)), F.lit(1e-6))
        centered = F.when(F.col("q50").isNull(), F.col("val")).otherwise(robust_expr)
    else:
        zscore_expr = (F.col("val") - F.col("mean")) / F.greatest(F.coalesce(F.col("scale_std"), F.lit(1.0)), F.lit(1e-6))
        centered = F.when(F.col("mean").isNull(), F.col("val")).otherwise(zscore_expr)

    clip_bound = max(float(normalize_clip_sigma), 0.0)
    clipped = F.when(centered > clip_bound, F.lit(clip_bound)).when(centered < -clip_bound, F.lit(-clip_bound)).otherwise(centered)
    return joined.withColumn("val", clipped).select("timestamp_utc", "sensor", "val")


def build_cur_proxy_sensor_graph(
    raw_df: "DataFrame",
    normalization_profile_df: "DataFrame",
    *,
    max_sensors: int,
    min_overlap: int,
    min_abs_corr: float,
    downsample_hz: float,
    normalize_mode: str,
    normalize_clip_sigma: float,
) -> "DataFrame":
    numeric = raw_df.where(F.col("val").isNotNull()).select("timestamp_utc", "sensor", F.col("val").cast("double").alias("val"))
    numeric = _normalize_numeric_values(
        numeric,
        normalization_profile_df,
        normalize_mode=normalize_mode,
        normalize_clip_sigma=normalize_clip_sigma,
    )

    sensor_counts = (
        numeric.groupBy("sensor")
        .agg(F.count("*").alias("sensor_points"))
        .orderBy(F.col("sensor_points").desc(), F.col("sensor").asc())
        .limit(max(int(max_sensors), 2))
    )
    selected_sensors_df = sensor_counts.select("sensor")

    sampled = (
        numeric.join(selected_sensors_df, on="sensor", how="inner")
        .withColumn("bucket_index", F.floor(F.col("timestamp_utc").cast("double") * F.lit(max(float(downsample_hz), 0.1))))
        .groupBy("bucket_index", "sensor")
        .agg(F.avg("val").alias("x"))
    )

    left = sampled.alias("left")
    right = sampled.alias("right")
    paired = left.join(
        right,
        on=(F.col("left.bucket_index") == F.col("right.bucket_index")) & (F.col("left.sensor") < F.col("right.sensor")),
        how="inner",
    )

    aggregated = (
        paired.groupBy(F.col("left.sensor").alias("sensor_u"), F.col("right.sensor").alias("sensor_v"))
        .agg(
            F.count("*").alias("overlap_points"),
            F.sum(F.col("left.x")).alias("sum_x"),
            F.sum(F.col("right.x")).alias("sum_y"),
            F.sum(F.col("left.x") * F.col("left.x")).alias("sum_x2"),
            F.sum(F.col("right.x") * F.col("right.x")).alias("sum_y2"),
            F.sum(F.col("left.x") * F.col("right.x")).alias("sum_xy"),
        )
    )

    numerator = F.col("overlap_points") * F.col("sum_xy") - (F.col("sum_x") * F.col("sum_y"))
    denom_left = F.col("overlap_points") * F.col("sum_x2") - (F.col("sum_x") * F.col("sum_x"))
    denom_right = F.col("overlap_points") * F.col("sum_y2") - (F.col("sum_y") * F.col("sum_y"))
    denominator = F.sqrt(F.greatest(denom_left, F.lit(0.0)) * F.greatest(denom_right, F.lit(0.0)))

    return (
        aggregated.withColumn(
            "cur_corr",
            F.when((F.col("overlap_points") > 2) & (denominator > 0), numerator / denominator).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("cur_weight", F.abs(F.col("cur_corr")))
        .where(F.col("overlap_points") >= int(max(min_overlap, 1)))
        .where(F.col("cur_weight") >= float(max(min_abs_corr, 0.0)))
        .select("sensor_u", "sensor_v", "overlap_points", "cur_corr", "cur_weight")
    )


def build_event_cooccurrence_sensor_graph(
    raw_df: "DataFrame",
    *,
    min_cooccur_count: int,
) -> "DataFrame":
    continuous_events = build_continuous_events(raw_df)
    categorical_events = build_categorical_events(raw_df)
    base_events = continuous_events.unionByName(categorical_events)

    grouped = (
        base_events.select("tail_id", "flight_id", "ts", "sensor")
        .where(F.col("sensor").isNotNull())
        .groupBy("tail_id", "flight_id", "ts")
        .agg(F.collect_set("sensor").alias("sensors"), F.countDistinct("sensor").alias("sensor_count"))
        .where(F.col("sensor_count") > 1)
    )

    left = grouped.select("tail_id", "flight_id", "ts", "sensors", F.posexplode("sensors").alias("i", "sensor_u"))
    pairs = (
        left.select("tail_id", "flight_id", "ts", "sensor_u", "i", F.posexplode("sensors").alias("j", "sensor_v"))
        .where(F.col("j") > F.col("i"))
        .select(
            F.least(F.col("sensor_u"), F.col("sensor_v")).alias("sensor_u"),
            F.greatest(F.col("sensor_u"), F.col("sensor_v")).alias("sensor_v"),
            F.col("tail_id"),
            F.col("flight_id"),
            F.col("ts"),
        )
    )

    counted = (
        pairs.groupBy("sensor_u", "sensor_v")
        .agg(
            F.count("*").alias("cooccur_count"),
            F.countDistinct(F.concat_ws("|", F.col("tail_id"), F.col("flight_id"), F.col("ts").cast("string"))).alias("cooccur_slots"),
        )
        .where(F.col("cooccur_count") >= int(max(min_cooccur_count, 1)))
    )

    max_count_df = counted.agg(F.max("cooccur_count").alias("max_count"))
    with_max = counted.crossJoin(max_count_df)
    return with_max.withColumn(
        "event_weight",
        F.when(F.col("max_count") > F.lit(0), F.col("cooccur_count") / F.col("max_count")).otherwise(F.lit(0.0)),
    ).drop("max_count")


def build_precision_sensor_graph(
    raw_df: "DataFrame",
    normalization_profile_df: "DataFrame",
    *,
    sampled_sensors_df: "DataFrame | None" = None,
    max_sensors: int,
    min_overlap: int,
    min_abs_partial_corr: float,
    downsample_hz: float,
    normalize_mode: str,
    normalize_clip_sigma: float,
    ridge_lambda: float,
) -> "DataFrame":
    numeric = raw_df.where(F.col("val").isNotNull()).select("timestamp_utc", "sensor", F.col("val").cast("double").alias("val"))
    numeric = _normalize_numeric_values(
        numeric,
        normalization_profile_df,
        normalize_mode=normalize_mode,
        normalize_clip_sigma=normalize_clip_sigma,
    )

    if sampled_sensors_df is not None:
        selected_sensors_df = sampled_sensors_df.select("sensor").distinct().limit(max(int(max_sensors), 2))
    else:
        selected_sensors_df = (
            numeric.groupBy("sensor")
            .agg(F.count("*").alias("sensor_points"))
            .orderBy(F.col("sensor_points").desc(), F.col("sensor").asc())
            .limit(max(int(max_sensors), 2))
            .select("sensor")
        )

    sampled = (
        numeric.join(selected_sensors_df, on="sensor", how="inner")
        .withColumn("bucket_index", F.floor(F.col("timestamp_utc").cast("double") * F.lit(max(float(downsample_hz), 0.1))))
        .groupBy("bucket_index", "sensor")
        .agg(F.avg("val").alias("x"))
    )

    bucket_common = sampled.groupBy("bucket_index").agg(
        F.avg("x").alias("bucket_mean_x"),
        F.stddev_pop("x").alias("bucket_std_x"),
    )

    centered = (
        sampled.join(bucket_common, on="bucket_index", how="left")
        .withColumn(
            "x_centered",
            (F.col("x") - F.coalesce(F.col("bucket_mean_x"), F.lit(0.0)))
            / F.greatest(F.coalesce(F.col("bucket_std_x"), F.lit(1.0)), F.lit(1e-6)),
        )
        .select("bucket_index", "sensor", "x_centered")
    )

    left = centered.alias("left")
    right = centered.alias("right")
    paired = left.join(
        right,
        on=(F.col("left.bucket_index") == F.col("right.bucket_index")) & (F.col("left.sensor") < F.col("right.sensor")),
        how="inner",
    )

    aggregated = (
        paired.groupBy(F.col("left.sensor").alias("sensor_u"), F.col("right.sensor").alias("sensor_v"))
        .agg(
            F.count("*").alias("overlap_points"),
            F.sum(F.col("left.x_centered")).alias("sum_x"),
            F.sum(F.col("right.x_centered")).alias("sum_y"),
            F.sum(F.col("left.x_centered") * F.col("left.x_centered")).alias("sum_x2"),
            F.sum(F.col("right.x_centered") * F.col("right.x_centered")).alias("sum_y2"),
            F.sum(F.col("left.x_centered") * F.col("right.x_centered")).alias("sum_xy"),
        )
    )

    ridge = max(float(ridge_lambda), 1e-9)
    numerator = F.col("overlap_points") * F.col("sum_xy") - (F.col("sum_x") * F.col("sum_y"))
    denom_left = (F.col("overlap_points") * F.col("sum_x2") - (F.col("sum_x") * F.col("sum_x"))) + F.lit(ridge)
    denom_right = (F.col("overlap_points") * F.col("sum_y2") - (F.col("sum_y") * F.col("sum_y"))) + F.lit(ridge)
    denominator = F.sqrt(F.greatest(denom_left, F.lit(0.0)) * F.greatest(denom_right, F.lit(0.0)))

    return (
        aggregated.withColumn(
            "partial_corr",
            F.when((F.col("overlap_points") > F.lit(2)) & (denominator > F.lit(0.0)), numerator / denominator).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("precision_weight", F.abs(F.col("partial_corr")))
        .where(F.col("overlap_points") >= int(max(min_overlap, 1)))
        .where(F.col("precision_weight") >= float(max(min_abs_partial_corr, 0.0)))
        .select("sensor_u", "sensor_v", "overlap_points", "partial_corr", "precision_weight")
    )


@hot_path
def build_sensor_subsystem_map(
    fused_graph_df: "DataFrame",
    precision_graph_df: "DataFrame",
    sampled_sensors_df: "DataFrame",
    *,
    min_edge_weight: float,
) -> "DataFrame":
    sensors_df = sampled_sensors_df.select(F.col("sensor").cast("string").alias("sensor")).where(F.col("sensor").isNotNull()).distinct()

    fused_edges = fused_graph_df.select(
        F.least(F.col("sensor_u"), F.col("sensor_v")).cast("string").alias("sensor_u"),
        F.greatest(F.col("sensor_u"), F.col("sensor_v")).cast("string").alias("sensor_v"),
        F.col("fused_weight").cast("double").alias("weight"),
    )
    precision_edges = precision_graph_df.select(
        F.least(F.col("sensor_u"), F.col("sensor_v")).cast("string").alias("sensor_u"),
        F.greatest(F.col("sensor_u"), F.col("sensor_v")).cast("string").alias("sensor_v"),
        F.col("precision_weight").cast("double").alias("weight"),
    )

    undirected_edges = (
        fused_edges.unionByName(precision_edges)
        .where(F.col("sensor_u").isNotNull() & F.col("sensor_v").isNotNull() & (F.col("sensor_u") != F.col("sensor_v")))
        .groupBy("sensor_u", "sensor_v")
        .agg(F.max("weight").alias("weight"))
        .where(F.col("weight") >= F.lit(float(min_edge_weight)))
    )

    directed_edges = undirected_edges.select(
        F.col("sensor_u").alias("src"),
        F.col("sensor_v").alias("dst"),
    ).unionByName(
        undirected_edges.select(
            F.col("sensor_v").alias("src"),
            F.col("sensor_u").alias("dst"),
        )
    )

    vertices = sensors_df.select("sensor").withColumn("label", F.col("sensor"))

    max_iterations = 20
    for _ in range(max_iterations):
        neighbor_min = (
            directed_edges.alias("e")
            .join(vertices.alias("v"), F.col("e.src") == F.col("v.sensor"), "inner")
            .groupBy(F.col("e.dst").alias("sensor"))
            .agg(F.min(F.col("v.label")).alias("min_neighbor_label"))
        )

        updated = (
            vertices.alias("v")
            .join(neighbor_min.alias("n"), on="sensor", how="left")
            .withColumn(
                "new_label",
                F.when(F.col("n.min_neighbor_label").isNull(), F.col("v.label")).otherwise(F.least(F.col("v.label"), F.col("n.min_neighbor_label"))),
            )
            .select("sensor", F.col("new_label").alias("label"))
        )

        changes = (
            vertices.alias("old")
            .join(updated.alias("new"), on="sensor", how="inner")
            .where(F.col("old.label") != F.col("new.label"))
            .limit(1)
            .count()
        )

        vertices = updated
        if changes == 0:
            break

    components = vertices.select(F.col("sensor"), F.col("label").alias("component_id"))
    component_sizes = components.groupBy("component_id").agg(F.count("*").alias("component_size"))
    ranked_components = fused_graph_df.sparkSession.createDataFrame(
        component_sizes.orderBy(F.col("component_size").desc(), F.col("component_id").asc())
        .select("component_id")
        .rdd.map(lambda row: str(row["component_id"]))
        .zipWithIndex()
        .map(lambda indexed: (indexed[0], int(indexed[1]) + 1)),
        schema="component_id string, component_rank long",
    ).withColumn("subsystem_id", F.format_string("SUBSYS_%04d", F.col("component_rank"))).select(
        "component_id", "subsystem_id"
    )

    return (
        components.join(ranked_components, on="component_id", how="left")
        .select("sensor", "subsystem_id")
    )


def fuse_sensor_graphs(
    cur_graph_df: "DataFrame",
    event_graph_df: "DataFrame",
    *,
    cur_weight_alpha: float,
) -> "DataFrame":
    alpha = float(cur_weight_alpha)
    alpha = min(max(alpha, 0.0), 1.0)
    event_alpha = 1.0 - alpha

    cur_edges = cur_graph_df.select(
        "sensor_u",
        "sensor_v",
        F.col("cur_weight").cast("double").alias("cur_weight"),
        F.col("cur_corr").cast("double").alias("cur_corr"),
        F.col("overlap_points").cast("long").alias("overlap_points"),
    )
    event_edges = event_graph_df.select(
        "sensor_u",
        "sensor_v",
        F.col("event_weight").cast("double").alias("event_weight"),
        F.col("cooccur_count").cast("long").alias("cooccur_count"),
        F.col("cooccur_slots").cast("long").alias("cooccur_slots"),
    )

    fused = cur_edges.join(event_edges, on=["sensor_u", "sensor_v"], how="full_outer")
    return (
        fused.withColumn("cur_weight", F.coalesce(F.col("cur_weight"), F.lit(0.0)))
        .withColumn("event_weight", F.coalesce(F.col("event_weight"), F.lit(0.0)))
        .withColumn("overlap_points", F.coalesce(F.col("overlap_points"), F.lit(0)).cast("long"))
        .withColumn("cooccur_count", F.coalesce(F.col("cooccur_count"), F.lit(0)).cast("long"))
        .withColumn("cooccur_slots", F.coalesce(F.col("cooccur_slots"), F.lit(0)).cast("long"))
        .withColumn("fused_weight", (F.col("cur_weight") * F.lit(alpha)) + (F.col("event_weight") * F.lit(event_alpha)))
        .withColumn(
            "edge_source",
            F.when((F.col("cur_weight") > 0) & (F.col("event_weight") > 0), F.lit("cur+event"))
            .when(F.col("cur_weight") > 0, F.lit("cur"))
            .when(F.col("event_weight") > 0, F.lit("event"))
            .otherwise(F.lit("none")),
        )
        .where(F.col("fused_weight") > 0)
        .select(
            "sensor_u",
            "sensor_v",
            "edge_source",
            "cur_weight",
            "event_weight",
            "fused_weight",
            "cur_corr",
            "overlap_points",
            "cooccur_count",
            "cooccur_slots",
        )
    )


if TYPE_CHECKING:
    from pyspark.sql import DataFrame
