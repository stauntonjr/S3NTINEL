"""Fit parameter-level event detector policy profiles from raw telemetry."""

from __future__ import annotations

from libs.events import EventProfileConfig, ParameterEventProfile
from libs.io.delta import get_spark, read_table, write_table
from libs.perf import (
    build_artifact_manifest,
    build_stage_manifest,
    get_logger,
    log_dict_artifact_if_active,
    log_memory_usage,
    log_params_if_active,
    log_stage_manifest_if_active,
    log_wall_time,
    track_mlflow_run,
)
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


def _build_event_profile_config(event_settings) -> EventProfileConfig:
    return EventProfileConfig(
        slope_source=event_settings.slope_source,
        slope_threshold_mode=event_settings.slope_threshold_mode,
        slope_threshold_quantile=event_settings.slope_threshold_quantile,
        slope_threshold_scale=event_settings.slope_threshold_scale,
        slope_threshold_min=event_settings.slope_threshold_min,
        slope_abs_threshold=event_settings.slope_abs_threshold,
        slope_min_persistence_samples=event_settings.slope_min_persistence_samples,
        slope_reemit_ratio=event_settings.slope_reemit_ratio,
        warmup_points=event_settings.warmup_points,
        low_scale_responsiveness=event_settings.low_scale_responsiveness,
        repeatability_aggressiveness=event_settings.repeatability_aggressiveness,
        drift_conservatism=event_settings.drift_conservatism,
        chatter_suppression=event_settings.chatter_suppression,
    ).resolved()


@track_mlflow_run(stage_name="15_event_profiles_fit", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="15_event_profiles_fit")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("15_event_profiles_fit")
    raw_path = runtime.artifacts.raw_table
    datatype_profile_path = runtime.artifacts.parameter_datatype_profile
    output_path = runtime.artifacts.parameter_event_profile
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.fit_write_mode
    event_settings = runtime.settings.events
    profile_config = _build_event_profile_config(event_settings)
    profile_config_payload = profile_config.to_payload()

    spark = get_spark("s3ntinel.event_profiles_fit")
    raw_df = read_table(spark, raw_path, fmt=table_format)
    datatype_profile_df = read_table(spark, datatype_profile_path, fmt=table_format)

    profile_df = ParameterEventProfile.from_raw_input(
        raw_df,
        datatype_profile_df=datatype_profile_df,
        config=profile_config,
    ).to_dataframe()
    write_table(profile_df, path=output_path, mode=write_mode, fmt=table_format)

    raw_count = int(raw_df.count())
    datatype_profile_count = int(datatype_profile_df.count())
    profile_count = int(profile_df.count())

    log_params_if_active(
        {
            "table_format": table_format,
            **profile_config_payload,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "15_event_profiles_fit",
            "raw_path": raw_path,
            "parameter_datatype_profile_path": datatype_profile_path,
            "parameter_event_profile_path": output_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "raw_count": raw_count,
            "datatype_profile_count": datatype_profile_count,
            "parameter_event_profile_count": profile_count,
        },
        runtime.report_paths.summary_artifact_path,
    )
    stage_manifest = build_stage_manifest(
        stage_name="15_event_profiles_fit",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            **profile_config_payload,
        },
        input_artifacts={
            "raw_telemetry": build_artifact_manifest(path=raw_path, dataframe=raw_df, row_count=raw_count),
            "parameter_datatype_profile": build_artifact_manifest(
                path=datatype_profile_path,
                dataframe=datatype_profile_df,
                row_count=datatype_profile_count,
                artifact_version="PARAMETER_DATATYPE_PROFILE_V2",
            ),
        },
        output_artifacts={
            "parameter_event_profile": build_artifact_manifest(
                path=output_path,
                dataframe=profile_df,
                row_count=profile_count,
                artifact_version="PARAMETER_EVENT_PROFILE_V4",
            ),
        },
        replayable_from=["raw_telemetry", "parameter_datatype_profile"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=event_profiles_fit format=%s write_mode=%s slope_source=%s slope_threshold_mode=%s slope_threshold_quantile=%s slope_threshold_scale=%s slope_threshold_min=%s slope_abs_threshold=%s slope_min_persistence_samples=%s slope_reemit_ratio=%s warmup_points=%s low_scale_responsiveness=%s repeatability_aggressiveness=%s drift_conservatism=%s chatter_suppression=%s raw=%s datatype_profile=%s event_profile=%s",
        table_format,
        write_mode,
        profile_config.slope_source,
        profile_config.slope_threshold_mode,
        profile_config.slope_threshold_quantile,
        profile_config.slope_threshold_scale,
        profile_config.slope_threshold_min,
        profile_config.slope_abs_threshold,
        profile_config.slope_min_persistence_samples,
        profile_config.slope_reemit_ratio,
        profile_config.warmup_points,
        profile_config.low_scale_responsiveness,
        profile_config.repeatability_aggressiveness,
        profile_config.drift_conservatism,
        profile_config.chatter_suppression,
        raw_path,
        datatype_profile_path,
        output_path,
    )


if __name__ == "__main__":
    run()
