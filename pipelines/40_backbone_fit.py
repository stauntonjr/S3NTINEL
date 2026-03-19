"""Fit backbone artifacts from adaptive windows and raw telemetry."""

import numpy as np

from libs.backbone import (
    build_backbone_g_spark_table,
    build_backbone_h_spark_table,
    build_backbone_selected_sensor_frame,
    build_backbone_sensor_energy_spark_table,
    select_backbone_sensors_by_energy_spark,
    solve_backbone_weights,
)
from libs.io.schemas import BACKBONE_SCHEMA
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_memory_usage,
    log_dict_artifact_if_active,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.windows import build_window_features_spark_table
from pipelines.common import build_context, context_artifacts, context_execution, context_settings


LOGGER = get_logger(__name__)

@track_mlflow_run(stage_name="40_backbone_fit", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="40_backbone_fit")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark.sql import functions as F
    from pyspark import StorageLevel

    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)
    raw_path = artifacts.raw_table
    events_path = artifacts.events
    windows_path = artifacts.windows
    window_features_path = artifacts.window_features
    backbone_path = artifacts.backbone
    backbone_energy_path = artifacts.backbone_sensor_energy
    table_format = execution.table_format
    write_mode = execution.fit_write_mode
    max_backbone_sensor_universe = settings.backbone.max_sensor_universe

    backbone_sensor_count = settings.backbone.sensor_count
    backbone_ridge_lambda = settings.backbone.ridge_lambda

    spark = get_spark("s3ntinel.backbone_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    raw_count = int(raw_df.count())
    events_count = int(events_df.count())
    windows_count = int(windows_df.count())

    window_features_df = build_window_features_spark_table(raw_df, events_df, windows_df).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        window_features_count = int(window_features_df.count())

        energy_sdf = build_backbone_sensor_energy_spark_table(window_features_df).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            sensor_energy_count = int(energy_sdf.count())
            selected_sensors_c = select_backbone_sensors_by_energy_spark(energy_sdf, k=max(int(backbone_sensor_count), 1))
            selected_sensor_frame_df = build_backbone_selected_sensor_frame(
                window_features_df,
                selected_sensors=selected_sensors_c,
            ).persist(StorageLevel.MEMORY_AND_DISK)
            try:
                g_row = build_backbone_g_spark_table(
                    window_features_df,
                    selected_sensors=selected_sensors_c,
                    selected_sensor_frame_df=selected_sensor_frame_df,
                ).first().asDict()
                sensor_rows = (
                    build_backbone_h_spark_table(
                        window_features_df,
                        selected_sensors=selected_sensors_c,
                        selected_sensor_frame_df=selected_sensor_frame_df,
                    )
                    .limit(max_backbone_sensor_universe + 1)
                    .collect()
                )
            finally:
                selected_sensor_frame_df.unpersist()
            if len(sensor_rows) > max_backbone_sensor_universe:
                raise RuntimeError(
                    "40_backbone_fit performs a bounded local ridge solve over the sensor universe; "
                    f"sensor count {len(sensor_rows)} exceeds S3NTINEL_MAX_BACKBONE_SENSOR_UNIVERSE={max_backbone_sensor_universe}."
                )

            total_window_count = int(g_row.get("window_count", 0) or 0)
            g = np.asarray(
                [
                    [float(g_row.get(f"g_{i}_{j}", 0.0) or 0.0) for j in range(len(selected_sensors_c))]
                    for i in range(len(selected_sensors_c))
                ],
                dtype=float,
            )
            all_sensors = [str(row["parameter_name"]) for row in sensor_rows if str(row["parameter_name"])]
            h = np.asarray(
                [
                    [float(row["h_vector_c"][idx] or 0.0) for row in sensor_rows]
                    for idx in range(len(selected_sensors_c))
                ],
                dtype=float,
            )
            weights_b = solve_backbone_weights(g, h, ridge_lambda=float(backbone_ridge_lambda))

            backbone_rows = [
                [
                    {
                        "backbone_version": 2,
                        "selected_sensors_c": list(selected_sensors_c),
                        "all_sensors": list(all_sensors),
                        "weights_b": [[float(value) for value in row] for row in weights_b.tolist()],
                        "lambda_ridge": float(backbone_ridge_lambda),
                        "training_window_count": int(total_window_count),
                    }
                ][0]
            ]

            backbone_df = (
                spark.createDataFrame(backbone_rows)
                if backbone_rows
                else spark.createDataFrame([], schema=BACKBONE_SCHEMA)
            )
            energy_df = (
                energy_sdf.withColumn("selected_backbone", F.col("parameter_name").isin(selected_sensors_c))
                .withColumn("backbone_version", F.lit(2).cast("int"))
                .select(
                    F.col("parameter_name").cast("string").alias("parameter_name"),
                    F.col("energy").cast("double").alias("energy"),
                    F.col("support_count").cast("int").alias("support_count"),
                    F.col("selected_backbone").cast("boolean").alias("selected_backbone"),
                    F.col("backbone_version").cast("int").alias("backbone_version"),
                )
            )
        finally:
            energy_sdf.unpersist()

        if str(window_features_path).strip():
            write_table(
                window_features_df,
                path=window_features_path,
                mode=write_mode,
                fmt=table_format,
                partition_by=context.config["output"]["partition_by"],
            )
        write_table(backbone_df, path=backbone_path, mode=write_mode, fmt=table_format)
        write_table(energy_df, path=backbone_energy_path, mode=write_mode, fmt=table_format)
    finally:
        window_features_df.unpersist()

    selected_sensor_count = len(backbone_rows[0]["selected_sensors_c"]) if backbone_rows else 0
    training_window_count = int(backbone_rows[0]["training_window_count"]) if backbone_rows else 0

    log_params_if_active(
        {
            "backbone_sensor_count": backbone_sensor_count,
            "backbone_ridge_lambda": backbone_ridge_lambda,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "40_backbone_fit",
            "raw_path": raw_path,
            "events_path": events_path,
            "windows_path": windows_path,
            "window_features_path": window_features_path,
            "backbone_path": backbone_path,
            "backbone_energy_path": backbone_energy_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "max_backbone_sensor_universe": max_backbone_sensor_universe,
            "raw_count_bounded": raw_count,
            "events_count_bounded": events_count,
            "windows_count_bounded": windows_count,
            "window_features_count": window_features_count,
            "backbone_sensor_count": backbone_sensor_count,
            "selected_sensor_count": selected_sensor_count,
            "training_window_count": training_window_count,
            "backbone_ridge_lambda": backbone_ridge_lambda,
        },
        "reports/stages/40_backbone_fit_summary.json",
    )
    output_artifacts = {
        "backbone": build_artifact_manifest(
            path=backbone_path,
            dataframe=backbone_df,
            row_count=len(backbone_rows),
            artifact_version="BACKBONE_V2",
        ),
        "backbone_sensor_energy": build_artifact_manifest(
            path=backbone_energy_path,
            dataframe=energy_df,
            row_count=sensor_energy_count,
            artifact_version="BACKBONE_SENSOR_ENERGY_V2",
        ),
    }
    if str(window_features_path).strip():
        output_artifacts["window_features"] = build_artifact_manifest(
            path=window_features_path,
            dataframe=window_features_df,
            row_count=window_features_count,
        )

    stage_manifest = build_stage_manifest(
        stage_name="40_backbone_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "backbone_sensor_count": backbone_sensor_count,
            "backbone_ridge_lambda": backbone_ridge_lambda,
            "max_backbone_sensor_universe": max_backbone_sensor_universe,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
            "events": build_artifact_manifest(path=events_path, dataframe=events_df, row_count=events_count),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df, row_count=windows_count),
            "window_features": build_artifact_manifest(
                path=(window_features_path or "window_features::ephemeral"),
                dataframe=window_features_df,
                row_count=window_features_count,
            ),
        },
        output_artifacts=output_artifacts,
        replayable_from=["window_features", "backbone_sensor_energy"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/40_backbone_fit_manifest.json")
    LOGGER.info(
        "pipeline=backbone_fit format=%s write_mode=%s selected_sensor_count=%s training_window_count=%s raw=%s windows=%s backbone=%s backbone_energy=%s",
        table_format,
        write_mode,
        selected_sensor_count,
        training_window_count,
        raw_path,
        windows_path,
        backbone_path,
        backbone_energy_path,
    )


if __name__ == "__main__":
    run()
