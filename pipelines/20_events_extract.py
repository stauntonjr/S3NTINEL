# File: pipelines/20_events_extract.py
"""Extract event stream from mixed-rate sensor channels."""

from libs.events import build_events_table
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
from pipelines.common import build_context, context_artifacts, context_execution, context_settings


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="20_events_extract", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="20_events_extract")
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)
    input_path = artifacts.raw_table
    datatype_profile_path = artifacts.parameter_datatype_profile
    output_path = artifacts.events
    table_format = execution.table_format
    write_mode = execution.write_mode
    delta_threshold = settings.events.delta_threshold
    slope_source = settings.events.slope_source
    ema_alpha = settings.events.ema_alpha

    spark = get_spark("s3ntinel.events_extract")
    raw_df = read_table(spark, input_path, fmt=table_format)
    datatype_profile_df = read_table(spark, datatype_profile_path, fmt=table_format)

    events_df = build_events_table(
        raw_df,
        datatype_profile_df=datatype_profile_df,
        delta_threshold=delta_threshold,
        slope_source=slope_source,
        ema_alpha=ema_alpha,
    )

    write_table(
        events_df,
        path=output_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )
    raw_count = int(raw_df.count())
    datatype_profile_count = int(datatype_profile_df.count())
    events_count = int(events_df.count())

    log_params_if_active(
        {
            "event_threshold": context.config["windowing"]["event_threshold"],
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "20_events_extract",
            "input_path": input_path,
            "output_path": output_path,
            "parameter_datatype_profile_path": datatype_profile_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "delta_threshold": delta_threshold,
            "slope_source": slope_source,
            "ema_alpha": ema_alpha,
            "event_threshold": int(context.config["windowing"]["event_threshold"]),
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        "reports/stages/20_events_extract_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="20_events_extract",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "delta_threshold": delta_threshold,
            "slope_source": slope_source,
            "ema_alpha": ema_alpha,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=input_path, dataframe=raw_df, row_count=raw_count),
            "parameter_datatype_profile": build_artifact_manifest(
                path=datatype_profile_path,
                dataframe=datatype_profile_df,
                row_count=datatype_profile_count,
            ),
        },
        output_artifacts={
            "events": build_artifact_manifest(path=output_path, dataframe=events_df, row_count=events_count),
        },
        replayable_from=["raw_telemetry", "parameter_datatype_profile"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/20_events_extract_manifest.json")
    LOGGER.info(
        "pipeline=events_extract format=%s write_mode=%s event_threshold=%s delta_threshold=%s slope_source=%s ema_alpha=%s input=%s datatype_profile=%s output=%s",
        table_format,
        write_mode,
        context.config["windowing"]["event_threshold"],
        delta_threshold,
        slope_source,
        ema_alpha,
        input_path,
        datatype_profile_path,
        output_path,
    )


if __name__ == "__main__":
    run()
