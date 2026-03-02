"""Spark/JVM hierarchy discovery from weighted sensor graphs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from libs.perf.annotations import hot_path


if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def _prepare_edge_list(
    fused_graph_df: "DataFrame",
    precision_graph_df: "DataFrame",
    *,
    min_edge_weight: float,
) -> "DataFrame":
    from pyspark.sql import functions as F

    fused_edges = fused_graph_df.select(
        F.col("sensor_u").cast("string").alias("sensor_u"),
        F.col("sensor_v").cast("string").alias("sensor_v"),
        F.col("fused_weight").cast("double").alias("weight"),
    )
    precision_edges = precision_graph_df.select(
        F.col("sensor_u").cast("string").alias("sensor_u"),
        F.col("sensor_v").cast("string").alias("sensor_v"),
        F.col("precision_weight").cast("double").alias("weight"),
    )
    merged = fused_edges.unionByName(precision_edges)
    return (
        merged.where(F.col("sensor_u").isNotNull() & F.col("sensor_v").isNotNull() & (F.col("sensor_u") != F.col("sensor_v")) )
        .groupBy("sensor_u", "sensor_v")
        .agg(F.max("weight").alias("weight"))
        .where(F.col("weight") >= F.lit(float(max(min_edge_weight, 0.0))))
    )


def _cluster_with_pic(
    spark: "SparkSession",
    sensors_df: "DataFrame",
    edge_list_df: "DataFrame",
    *,
    k: int,
    level_name: str,
) -> "DataFrame":
    from pyspark.ml.clustering import PowerIterationClustering
    from pyspark.sql import functions as F

    ordered_sensors_df = sensors_df.select("sensor").distinct().orderBy(F.col("sensor").asc())
    sensor_ids_df = spark.createDataFrame(
        ordered_sensors_df.rdd.map(lambda row: str(row["sensor"]))
        .zipWithIndex()
        .map(lambda indexed: (indexed[0], int(indexed[1]))),
        schema="sensor string, vertex_id long",
    )

    if edge_list_df.limit(1).count() == 0 or k <= 1:
        return sensor_ids_df.select(
            "sensor",
            F.col("vertex_id").cast("long").alias(f"{level_name}_cluster_raw"),
        )

    left = sensor_ids_df.select(F.col("sensor").alias("sensor_u"), F.col("vertex_id").alias("src"))
    right = sensor_ids_df.select(F.col("sensor").alias("sensor_v"), F.col("vertex_id").alias("dst"))
    edges_vid = (
        edge_list_df.join(left, on="sensor_u", how="inner")
        .join(right, on="sensor_v", how="inner")
        .select(F.col("src").cast("long"), F.col("dst").cast("long"), F.col("weight").cast("double"))
    )

    if edges_vid.limit(1).count() == 0:
        return sensor_ids_df.select(
            "sensor",
            F.col("vertex_id").cast("long").alias(f"{level_name}_cluster_raw"),
        )

    pic = PowerIterationClustering(k=max(int(k), 1), maxIter=40, initMode="degree")
    assignments = pic.assignClusters(edges_vid).select(
        F.col("id").cast("long").alias("vertex_id"),
        F.col("cluster").cast("long").alias(f"{level_name}_cluster_raw"),
    )

    return (
        sensor_ids_df.join(assignments, on="vertex_id", how="left")
        .withColumn(f"{level_name}_cluster_raw", F.coalesce(F.col(f"{level_name}_cluster_raw"), F.col("vertex_id")))
        .select("sensor", F.col(f"{level_name}_cluster_raw").cast("long").alias(f"{level_name}_cluster_raw"))
    )


def _majority_parent_mapping(
    child_parent_pairs_df: "DataFrame",
    *,
    child_col: str,
    parent_col: str,
    out_parent_col: str,
) -> "DataFrame":
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    ranked = (
        child_parent_pairs_df.groupBy(child_col, parent_col)
        .agg(F.count("*").alias("cnt"))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy(child_col).orderBy(F.col("cnt").desc(), F.col(parent_col).asc())
            ),
        )
        .where(F.col("rn") == F.lit(1))
    )
    return ranked.select(F.col(child_col), F.col(parent_col).alias(out_parent_col))


@hot_path
def build_sensor_hierarchy_from_graphs(
    spark: "SparkSession",
    sampled_sensors_df: "DataFrame",
    fused_graph_df: "DataFrame",
    precision_graph_df: "DataFrame",
    *,
    min_edge_weight: float,
    k_system: int,
    k_subsystem: int,
    k_module: int,
    hierarchy_profile_id: str,
    hierarchy_source: str,
) -> tuple["DataFrame", "DataFrame", "DataFrame", dict[str, int]]:
    from pyspark.sql import functions as F

    sensors_df = sampled_sensors_df.select(F.col("sensor").cast("string").alias("sensor")).distinct()
    sensor_count = sensors_df.count()
    if sensor_count <= 0:
        empty_map = spark.createDataFrame([], schema="sensor string, system_id string, subsystem_id string, module_id string, hierarchy_profile_id string, hierarchy_source string")
        empty_nodes = spark.createDataFrame([], schema="node_id string, parent_node_id string, node_type string, node_name string, hierarchy_profile_id string, hierarchy_source string")
        empty_edges = spark.createDataFrame([], schema="parent_node_id string, child_node_id string, edge_type string, hierarchy_profile_id string, hierarchy_source string")
        return empty_map, empty_nodes, empty_edges, {
            "sensor_count": 0,
            "system_count": 0,
            "subsystem_count": 0,
            "module_count": 0,
            "edge_count": 0,
        }

    edge_list_df = _prepare_edge_list(fused_graph_df, precision_graph_df, min_edge_weight=min_edge_weight)
    edge_count = edge_list_df.count()

    k_system_eff = max(1, min(int(k_system), sensor_count))
    k_subsystem_eff = max(1, min(int(k_subsystem), sensor_count))
    k_module_eff = max(1, min(int(k_module), sensor_count))

    system_raw_df = _cluster_with_pic(spark, sensors_df, edge_list_df, k=k_system_eff, level_name="system")
    subsystem_raw_df = _cluster_with_pic(spark, sensors_df, edge_list_df, k=k_subsystem_eff, level_name="subsystem")
    module_raw_df = _cluster_with_pic(spark, sensors_df, edge_list_df, k=k_module_eff, level_name="module")

    base = (
        sensors_df
        .join(system_raw_df, on="sensor", how="left")
        .join(subsystem_raw_df, on="sensor", how="left")
        .join(module_raw_df, on="sensor", how="left")
    )

    module_to_subsystem = _majority_parent_mapping(
        base.select("module_cluster_raw", "subsystem_cluster_raw"),
        child_col="module_cluster_raw",
        parent_col="subsystem_cluster_raw",
        out_parent_col="subsystem_cluster_final",
    )

    with_subsystem = (
        base.join(module_to_subsystem, on="module_cluster_raw", how="left")
        .withColumn("subsystem_cluster_final", F.coalesce(F.col("subsystem_cluster_final"), F.col("subsystem_cluster_raw")))
    )

    subsystem_to_system = _majority_parent_mapping(
        with_subsystem.select("subsystem_cluster_final", "system_cluster_raw"),
        child_col="subsystem_cluster_final",
        parent_col="system_cluster_raw",
        out_parent_col="system_cluster_final",
    )

    nested = (
        with_subsystem.join(subsystem_to_system, on="subsystem_cluster_final", how="left")
        .withColumn("system_cluster_final", F.coalesce(F.col("system_cluster_final"), F.col("system_cluster_raw")))
    )

    ordered_systems_df = nested.select("system_cluster_final").distinct().orderBy(F.col("system_cluster_final").asc())
    system_labels = spark.createDataFrame(
        ordered_systems_df.rdd.map(lambda row: row["system_cluster_final"])
        .zipWithIndex()
        .map(lambda indexed: (indexed[0], int(indexed[1]) + 1)),
        schema="system_cluster_final long, system_rank long",
    ).withColumn("system_id", F.format_string("SYS_%04d", F.col("system_rank"))).select("system_cluster_final", "system_id")

    ordered_subsystems_df = (
        nested.select("subsystem_cluster_final", "system_cluster_final").distinct()
        .join(system_labels, on="system_cluster_final", how="left")
        .orderBy(F.col("system_id").asc(), F.col("subsystem_cluster_final").asc())
        .select("subsystem_cluster_final", "system_id")
    )
    subsystem_labels = spark.createDataFrame(
        ordered_subsystems_df.rdd.map(lambda row: (row["subsystem_cluster_final"], row["system_id"]))
        .zipWithIndex()
        .map(lambda indexed: (indexed[0][0], indexed[0][1], int(indexed[1]) + 1)),
        schema="subsystem_cluster_final long, system_id string, sub_rank long",
    ).withColumn("subsystem_id", F.format_string("SUBSYS_%04d", F.col("sub_rank"))).select(
        "subsystem_cluster_final", "subsystem_id", "system_id"
    )

    ordered_modules_df = (
        nested.select("module_cluster_raw", "subsystem_cluster_final").distinct()
        .join(subsystem_labels.select("subsystem_cluster_final", "subsystem_id"), on="subsystem_cluster_final", how="left")
        .orderBy(F.col("subsystem_id").asc(), F.col("module_cluster_raw").asc())
        .select("module_cluster_raw", "subsystem_id")
    )
    module_labels = spark.createDataFrame(
        ordered_modules_df.rdd.map(lambda row: (row["module_cluster_raw"], row["subsystem_id"]))
        .zipWithIndex()
        .map(lambda indexed: (indexed[0][0], indexed[0][1], int(indexed[1]) + 1)),
        schema="module_cluster_raw long, subsystem_id string, mod_rank long",
    ).withColumn("module_id", F.format_string("MOD_%04d", F.col("mod_rank"))).select(
        "module_cluster_raw", "module_id", "subsystem_id"
    )

    sensor_map_df = (
        nested.join(system_labels, on="system_cluster_final", how="left")
        .join(subsystem_labels.select("subsystem_cluster_final", "subsystem_id"), on="subsystem_cluster_final", how="left")
        .join(module_labels.select("module_cluster_raw", "module_id"), on="module_cluster_raw", how="left")
        .select("sensor", "system_id", "subsystem_id", "module_id")
        .withColumn("hierarchy_profile_id", F.lit(str(hierarchy_profile_id)))
        .withColumn("hierarchy_source", F.lit(str(hierarchy_source)))
    )

    global_nodes = spark.createDataFrame(
        [("GLOBAL", None, "global", "GLOBAL", str(hierarchy_profile_id), str(hierarchy_source))],
        schema="node_id string, parent_node_id string, node_type string, node_name string, hierarchy_profile_id string, hierarchy_source string",
    )

    system_nodes = (
        sensor_map_df.select("system_id").distinct()
        .where(F.col("system_id").isNotNull())
        .select(
            F.col("system_id").alias("node_id"),
            F.lit("GLOBAL").alias("parent_node_id"),
            F.lit("system").alias("node_type"),
            F.col("system_id").alias("node_name"),
            F.lit(str(hierarchy_profile_id)).alias("hierarchy_profile_id"),
            F.lit(str(hierarchy_source)).alias("hierarchy_source"),
        )
    )

    subsystem_nodes = (
        sensor_map_df.select("subsystem_id", "system_id").distinct()
        .where(F.col("subsystem_id").isNotNull())
        .select(
            F.col("subsystem_id").alias("node_id"),
            F.col("system_id").alias("parent_node_id"),
            F.lit("subsystem").alias("node_type"),
            F.col("subsystem_id").alias("node_name"),
            F.lit(str(hierarchy_profile_id)).alias("hierarchy_profile_id"),
            F.lit(str(hierarchy_source)).alias("hierarchy_source"),
        )
    )

    module_nodes = (
        sensor_map_df.select("module_id", "subsystem_id").distinct()
        .where(F.col("module_id").isNotNull())
        .select(
            F.col("module_id").alias("node_id"),
            F.col("subsystem_id").alias("parent_node_id"),
            F.lit("module").alias("node_type"),
            F.col("module_id").alias("node_name"),
            F.lit(str(hierarchy_profile_id)).alias("hierarchy_profile_id"),
            F.lit(str(hierarchy_source)).alias("hierarchy_source"),
        )
    )

    sensor_nodes = (
        sensor_map_df.select("sensor", "module_id")
        .where(F.col("sensor").isNotNull())
        .select(
            F.concat(F.lit("SENSOR::"), F.col("sensor")).alias("node_id"),
            F.col("module_id").alias("parent_node_id"),
            F.lit("sensor").alias("node_type"),
            F.col("sensor").alias("node_name"),
            F.lit(str(hierarchy_profile_id)).alias("hierarchy_profile_id"),
            F.lit(str(hierarchy_source)).alias("hierarchy_source"),
        )
    )

    hierarchy_nodes_df = global_nodes.unionByName(system_nodes).unionByName(subsystem_nodes).unionByName(module_nodes).unionByName(sensor_nodes)

    hierarchy_edges_df = (
        hierarchy_nodes_df.where(F.col("parent_node_id").isNotNull())
        .select(
            F.col("parent_node_id"),
            F.col("node_id").alias("child_node_id"),
            F.lit("contains").alias("edge_type"),
            F.col("hierarchy_profile_id"),
            F.col("hierarchy_source"),
        )
    )

    meta = {
        "sensor_count": int(sensor_count),
        "system_count": int(sensor_map_df.select("system_id").distinct().count()),
        "subsystem_count": int(sensor_map_df.select("subsystem_id").distinct().count()),
        "module_count": int(sensor_map_df.select("module_id").distinct().count()),
        "edge_count": int(edge_count),
    }
    return sensor_map_df, hierarchy_nodes_df, hierarchy_edges_df, meta
