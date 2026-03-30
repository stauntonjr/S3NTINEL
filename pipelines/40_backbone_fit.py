"""Fit backbone artifacts from adaptive windows and raw telemetry."""

import numpy as np

from libs.backbone import (
    BackboneCrossTermFrame,
    BackboneGramFrame,
    BackboneSelectedSensorFrame,
    BackboneSensorEnergyTable,
    BackboneTable,
    select_backbone_sensors_by_energy_spark,
    solve_backbone_weights,
)
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
from libs.windows import WindowFeaturesTable
from pipelines.common import build_stage_runtime, require_artifact_path


LOGGER = get_logger(__name__)

@track_mlflow_run(stage_name="40_backbone_fit", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="40_backbone_fit")
@log_wall_time(logger=LOGGER)
def run() -> None:
    from pyspark.sql import functions as F
    from pyspark import StorageLevel

    runtime = build_stage_runtime("40_backbone_fit")
    context = runtime.context
    raw_path = runtime.artifacts.raw_table
    events_path = runtime.artifacts.events
    windows_path = runtime.artifacts.windows
    scaling_profile_path = runtime.artifacts.continuous_scaling_profile
    window_features_path = runtime.artifacts.window_features
    backbone_path = runtime.artifacts.backbone
    backbone_energy_path = runtime.artifacts.backbone_sensor_energy
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.fit_write_mode
    max_backbone_sensor_universe = runtime.settings.backbone.max_sensor_universe

    backbone_sensor_count = runtime.settings.backbone.sensor_count
    backbone_ridge_lambda = runtime.settings.backbone.ridge_lambda
    backbone_event_prior_alpha = runtime.settings.backbone.event_prior_alpha

    spark = get_spark("s3ntinel.backbone_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    events_df = read_table(spark, events_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    resolved_scaling_profile_path = require_artifact_path(
        scaling_profile_path,
        env_name="S3NTINEL_CONTINUOUS_SCALING_PROFILE_TABLE_PATH",
        artifact_name="continuous_scaling_profile",
    )
    scaling_profile_df = read_table(spark, str(resolved_scaling_profile_path), fmt=table_format)
    raw_count = int(raw_df.count())
    events_count = int(events_df.count())
    windows_count = int(windows_df.count())
    scaling_profile_count = int(scaling_profile_df.count())

    window_features_df = WindowFeaturesTable.from_raw_events_and_windows(
        raw_df,
        events_df,
        windows_df,
        scaling_profile_df=scaling_profile_df,
    ).to_dataframe().persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        window_features_count = int(window_features_df.count())

        energy_sdf = BackboneSensorEnergyTable.from_window_features(
            window_features_df,
            event_prior_alpha=float(backbone_event_prior_alpha),
        ).to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
        try:
            sensor_energy_count = int(energy_sdf.count())
            selected_sensors_c = select_backbone_sensors_by_energy_spark(energy_sdf, k=max(int(backbone_sensor_count), 1))
            selected_sensor_frame_df = BackboneSelectedSensorFrame.from_window_features(
                window_features_df,
                selected_sensors=selected_sensors_c,
            ).to_dataframe().persist(StorageLevel.MEMORY_AND_DISK)
            try:
                g_row = BackboneGramFrame.from_window_features(
                    window_features_df,
                    selected_sensors=selected_sensors_c,
                    selected_sensor_frame_df=selected_sensor_frame_df,
                ).to_dataframe().first().asDict()
                sensor_rows = (
                    BackboneCrossTermFrame.from_window_features(
                        window_features_df,
                        selected_sensors=selected_sensors_c,
                        selected_sensor_frame_df=selected_sensor_frame_df,
                    )
                    .to_dataframe()
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

            backbone_df = BackboneTable(
                dataframe=spark.createDataFrame(backbone_rows)
                if backbone_rows
                else spark.createDataFrame([], schema=BackboneTable.spark_schema())
            ).to_dataframe()
            energy_df = (
                energy_sdf.withColumn("selected_backbone", F.col("parameter_name").isin(selected_sensors_c))
                .withColumn("backbone_version", F.lit(2).cast("int"))
                .select(
                    F.col("parameter_name").cast("string").alias("parameter_name"),
                    F.col("energy").cast("double").alias("energy"),
                    F.col("support_count").cast("int").alias("support_count"),
                    F.col("event_prior").cast("double").alias("event_prior"),
                    F.col("selection_score").cast("double").alias("selection_score"),
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
            "backbone_event_prior_alpha": backbone_event_prior_alpha,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "40_backbone_fit",
            "raw_path": raw_path,
            "events_path": events_path,
            "windows_path": windows_path,
            "continuous_scaling_profile_path": str(resolved_scaling_profile_path),
            "window_features_path": window_features_path,
            "backbone_path": backbone_path,
            "backbone_energy_path": backbone_energy_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "max_backbone_sensor_universe": max_backbone_sensor_universe,
            "raw_count_bounded": raw_count,
            "events_count_bounded": events_count,
            "windows_count_bounded": windows_count,
            "continuous_scaling_profile_count": scaling_profile_count,
            "window_features_count": window_features_count,
            "backbone_sensor_count": backbone_sensor_count,
            "selected_sensor_count": selected_sensor_count,
            "training_window_count": training_window_count,
            "backbone_ridge_lambda": backbone_ridge_lambda,
            "backbone_event_prior_alpha": backbone_event_prior_alpha,
        },
        runtime.report_paths.summary_artifact_path,
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
            "backbone_event_prior_alpha": backbone_event_prior_alpha,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
            "events": build_artifact_manifest(path=events_path, dataframe=events_df, row_count=events_count),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df, row_count=windows_count),
            "continuous_scaling_profile": build_artifact_manifest(
                path=str(resolved_scaling_profile_path),
                dataframe=scaling_profile_df,
                row_count=scaling_profile_count,
            ),
            "window_features": build_artifact_manifest(
                path=(window_features_path or "window_features::ephemeral"),
                dataframe=window_features_df,
                row_count=window_features_count,
            ),
        },
        output_artifacts=output_artifacts,
        replayable_from=["window_features", "backbone_sensor_energy"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
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
