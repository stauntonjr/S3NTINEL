# File: pipelines/20_events_extract.py
"""Extract event stream from mixed-rate sensor channels."""

from libs.events import EventDetectionPlan, EventsTable
from libs.events.categorical import CategoricalDetectorConfig, CategoricalEventDetector
from libs.events.continuous import ContinuousDetectorConfig, ContinuousEventDetector
from libs.io.delta import get_spark, read_table
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
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="20_events_extract", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="20_events_extract")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("20_events_extract")
    context = runtime.context
    settings = runtime.settings
    input_path = runtime.artifacts.raw_table
    datatype_profile_path = runtime.artifacts.parameter_datatype_profile
    event_profile_path = runtime.artifacts.parameter_event_profile
    output_path = runtime.artifacts.events
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode
    delta_threshold = settings.events.delta_threshold
    slope_source = settings.events.slope_source
    ema_alpha = settings.events.ema_alpha
    slope_threshold_mode = settings.events.slope_threshold_mode
    slope_threshold_quantile = settings.events.slope_threshold_quantile
    slope_threshold_scale = settings.events.slope_threshold_scale
    slope_threshold_min = settings.events.slope_threshold_min
    slope_abs_threshold = settings.events.slope_abs_threshold
    slope_min_persistence_samples = settings.events.slope_min_persistence_samples
    slope_reemit_ratio = settings.events.slope_reemit_ratio
    warmup_points = settings.events.warmup_points

    spark = get_spark("s3ntinel.events_extract")
    raw_df = read_table(spark, input_path, fmt=table_format)
    datatype_profile_df = read_table(spark, datatype_profile_path, fmt=table_format)
    event_profile_df = read_table(spark, event_profile_path, fmt=table_format)

    events = (
        EventDetectionPlan(
            continuous_detector=ContinuousEventDetector(
                config=ContinuousDetectorConfig(
                    delta_threshold=delta_threshold,
                    slope_source=slope_source,
                    ema_alpha=ema_alpha,
                    slope_threshold_mode=slope_threshold_mode,
                    slope_threshold_quantile=slope_threshold_quantile,
                    slope_threshold_scale=slope_threshold_scale,
                    slope_threshold_min=slope_threshold_min,
                    slope_abs_threshold=slope_abs_threshold,
                    slope_min_persistence_samples=slope_min_persistence_samples,
                    slope_reemit_ratio=slope_reemit_ratio,
                    warmup_points=warmup_points,
                    emit_oscillation_events=False,
                )
            ),
            categorical_detector=CategoricalEventDetector(
                config=CategoricalDetectorConfig(
                    emit_state_enter=False,
                    emit_state_exit=False,
                    emit_dwell_bucket=False,
                )
            ),
        )
        .build(raw_df, datatype_profile_df=datatype_profile_df, event_profile_df=event_profile_df)
        .events.bind(
            path=output_path,
            format=table_format,
            partition_by=tuple(context.config["output"]["partition_by"]),
        )
    )
    events.write(mode=write_mode)
    raw_count = int(raw_df.count())
    datatype_profile_count = int(datatype_profile_df.count())
    event_profile_count = int(event_profile_df.count())
    events_count = int(events.to_dataframe().count())

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
            "parameter_event_profile_path": event_profile_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "delta_threshold": delta_threshold,
            "slope_source": slope_source,
            "ema_alpha": ema_alpha,
            "slope_threshold_mode": slope_threshold_mode,
            "slope_threshold_quantile": slope_threshold_quantile,
            "slope_threshold_scale": slope_threshold_scale,
            "slope_threshold_min": slope_threshold_min,
            "slope_abs_threshold": slope_abs_threshold,
            "slope_min_persistence_samples": slope_min_persistence_samples,
            "slope_reemit_ratio": slope_reemit_ratio,
            "warmup_points": warmup_points,
            "event_threshold": int(context.config["windowing"]["event_threshold"]),
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="20_events_extract",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "delta_threshold": delta_threshold,
            "slope_source": slope_source,
            "ema_alpha": ema_alpha,
            "slope_threshold_mode": slope_threshold_mode,
            "slope_threshold_quantile": slope_threshold_quantile,
            "slope_threshold_scale": slope_threshold_scale,
            "slope_threshold_min": slope_threshold_min,
            "slope_abs_threshold": slope_abs_threshold,
            "slope_min_persistence_samples": slope_min_persistence_samples,
            "slope_reemit_ratio": slope_reemit_ratio,
            "warmup_points": warmup_points,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=input_path, dataframe=raw_df, row_count=raw_count),
            "parameter_datatype_profile": build_artifact_manifest(
                path=datatype_profile_path,
                dataframe=datatype_profile_df,
                row_count=datatype_profile_count,
            ),
            "parameter_event_profile": build_artifact_manifest(
                path=event_profile_path,
                dataframe=event_profile_df,
                row_count=event_profile_count,
            ),
        },
        output_artifacts={
            "events": build_artifact_manifest(path=output_path, dataframe=events.to_dataframe(), row_count=events_count),
        },
        replayable_from=["raw_telemetry", "parameter_datatype_profile", "parameter_event_profile"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=events_extract format=%s write_mode=%s event_threshold=%s delta_threshold=%s slope_source=%s ema_alpha=%s slope_threshold_mode=%s slope_threshold_quantile=%s slope_threshold_scale=%s slope_threshold_min=%s slope_abs_threshold=%s slope_min_persistence_samples=%s slope_reemit_ratio=%s warmup_points=%s input=%s datatype_profile=%s event_profile=%s output=%s",
        table_format,
        write_mode,
        context.config["windowing"]["event_threshold"],
        delta_threshold,
        slope_source,
        ema_alpha,
        slope_threshold_mode,
        slope_threshold_quantile,
        slope_threshold_scale,
        slope_threshold_min,
        slope_abs_threshold,
        slope_min_persistence_samples,
        slope_reemit_ratio,
        warmup_points,
        input_path,
        datatype_profile_path,
        event_profile_path,
        output_path,
    )


if __name__ == "__main__":
    run()
