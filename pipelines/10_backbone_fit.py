"""Fit backbone artifacts from adaptive windows and raw telemetry."""

import os

from libs.backbone import (
    aggregate_backbone_gh,
    build_backbone_gh_spark_table,
    build_backbone_sensor_energy_spark_table,
    select_backbone_sensors_by_energy,
    solve_backbone_weights,
)
from libs.io.schemas import BACKBONE_SCHEMA, BACKBONE_SENSOR_ENERGY_SCHEMA
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dict_artifact_if_active,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from libs.windows import build_window_features_spark_dataframe
from pipelines.common import build_context


LOGGER = get_logger(__name__)


def _bounded_count(df: "DataFrame", *, limit: int) -> int:
    return int(df.limit(max(int(limit), 0) + 1).count())


@track_mlflow_run(stage_name="10_backbone_fit", logger=LOGGER)
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    raw_path = os.getenv("S3NTINEL_RAW_TABLE_PATH", "data/delta/raw_telemetry")
    windows_path = os.getenv("S3NTINEL_WINDOWS_TABLE_PATH", "data/delta/windows")
    backbone_path = os.getenv("S3NTINEL_BACKBONE_TABLE_PATH", "data/delta/backbone")
    backbone_energy_path = os.getenv("S3NTINEL_BACKBONE_SENSOR_ENERGY_TABLE_PATH", "data/delta/backbone_sensor_energy")
    table_format = os.getenv("S3NTINEL_TABLE_FORMAT", "delta")
    write_mode = os.getenv("S3NTINEL_FIT_WRITE_MODE", "overwrite")
    max_bridge_rows = int(os.getenv("S3NTINEL_MAX_BRIDGE_BACKBONE_INPUT_ROWS", "250000"))

    backbone_sensor_count = int(os.getenv("S3NTINEL_BACKBONE_SENSOR_COUNT", "8"))
    backbone_ridge_lambda = float(os.getenv("S3NTINEL_BACKBONE_RIDGE_LAMBDA", "1.0"))

    spark = get_spark("s3ntinel.backbone_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    windows_df = read_table(spark, windows_path, fmt=table_format)
    raw_count = _bounded_count(raw_df, limit=max_bridge_rows)
    windows_count = _bounded_count(windows_df, limit=max_bridge_rows)
    if raw_count > max_bridge_rows or windows_count > max_bridge_rows:
        raise RuntimeError(
            "10_backbone_fit still uses a bounded pandas bridge; input exceeds "
            f"S3NTINEL_MAX_BRIDGE_BACKBONE_INPUT_ROWS={max_bridge_rows}. "
            "Reduce input size or replace this stage with a distributed implementation."
        )

    empty_events_df = spark.createDataFrame(
        [],
        schema="tail_id string, flight_id string, parameter_name string, timestamp_utc timestamp, event_type_detected string, payload map<string,string>",
    )
    window_x_df = build_window_features_spark_dataframe(raw_df, empty_events_df, windows_df)
    window_x_count = _bounded_count(window_x_df, limit=max_bridge_rows)
    if window_x_count > max_bridge_rows:
        raise RuntimeError(
            "10_backbone_fit still performs a bounded driver-side solve after Spark aggregation; "
            f"window_x exceeds S3NTINEL_MAX_BRIDGE_BACKBONE_INPUT_ROWS={max_bridge_rows}. "
            "Replace the remaining backbone fit with a distributed implementation."
        )

    energy_sdf = build_backbone_sensor_energy_spark_table(window_x_df)
    energy_pdf = energy_sdf.orderBy(energy_sdf.energy.desc(), energy_sdf.parameter_name.asc()).toPandas()
    sensor_energy_rows = energy_pdf.to_dict(orient="records")
    selected_sensors_c = select_backbone_sensors_by_energy(sensor_energy_rows, k=max(int(backbone_sensor_count), 1))

    gh_sdf = build_backbone_gh_spark_table(window_x_df, selected_sensors=selected_sensors_c)
    gh_pdf = gh_sdf.toPandas()
    gh_rows = gh_pdf.to_dict(orient="records")
    g, h, total_window_count = aggregate_backbone_gh(gh_rows)
    all_sensors = (
        window_x_df.selectExpr("explode(map_keys(continuous_vector_t_end_scaled)) as parameter_name")
        .where("parameter_name is not null")
        .distinct()
        .orderBy("parameter_name")
        .toPandas()["parameter_name"]
        .astype(str)
        .tolist()
    )
    weights_b = solve_backbone_weights(g, h, ridge_lambda=float(backbone_ridge_lambda))

    backbone_pdf = spark.createDataFrame(
        [
            {
                "backbone_version": 2,
                "selected_sensors_c": list(selected_sensors_c),
                "all_sensors": list(all_sensors),
                "weights_b": [[float(value) for value in row] for row in weights_b.tolist()],
                "lambda_ridge": float(backbone_ridge_lambda),
                "training_window_count": int(total_window_count),
            }
        ]
    ).toPandas()

    energy_pdf = energy_pdf.copy()
    energy_pdf["selected_backbone"] = energy_pdf["parameter_name"].astype(str).isin(set(selected_sensors_c))
    energy_pdf["backbone_version"] = 2

    backbone_df = (
        spark.createDataFrame(backbone_pdf)
        if not backbone_pdf.empty
        else spark.createDataFrame([], schema=BACKBONE_SCHEMA)
    )
    energy_df = (
        spark.createDataFrame(energy_pdf)
        if not energy_pdf.empty
        else spark.createDataFrame([], schema=BACKBONE_SENSOR_ENERGY_SCHEMA)
    )

    write_table(backbone_df, path=backbone_path, mode=write_mode, fmt=table_format)
    write_table(energy_df, path=backbone_energy_path, mode=write_mode, fmt=table_format)

    backbone_rows = backbone_pdf.to_dict(orient="records")
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
            "stage": "10_backbone_fit",
            "raw_path": raw_path,
            "windows_path": windows_path,
            "backbone_path": backbone_path,
            "backbone_energy_path": backbone_energy_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "max_bridge_backbone_input_rows": max_bridge_rows,
            "raw_count_bounded": raw_count,
            "windows_count_bounded": windows_count,
            "window_x_count_bounded": window_x_count,
            "backbone_sensor_count": backbone_sensor_count,
            "selected_sensor_count": selected_sensor_count,
            "training_window_count": training_window_count,
            "backbone_ridge_lambda": backbone_ridge_lambda,
        },
        "reports/stages/10_backbone_fit_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="10_backbone_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "backbone_sensor_count": backbone_sensor_count,
            "backbone_ridge_lambda": backbone_ridge_lambda,
            "max_bridge_backbone_input_rows": max_bridge_rows,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
            "windows": build_artifact_manifest(path=windows_path, dataframe=windows_df, row_count=windows_count),
            "window_x": build_artifact_manifest(path="window_x::ephemeral", dataframe=window_x_df, row_count=window_x_count),
        },
        output_artifacts={
            "backbone": build_artifact_manifest(
                path=backbone_path,
                dataframe=backbone_df,
                row_count=len(backbone_rows),
                artifact_version="BACKBONE_V2",
            ),
            "backbone_sensor_energy": build_artifact_manifest(
                path=backbone_energy_path,
                dataframe=energy_df,
                row_count=len(sensor_energy_rows),
                artifact_version="BACKBONE_SENSOR_ENERGY_V2",
            ),
        },
        replayable_from=["window_x", "backbone_sensor_energy"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/10_backbone_fit_manifest.json")
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
