# File: pipelines/30_windows_adaptive.py
"""Build adaptive windows from event thresholds and max duration."""

from pathlib import Path

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
from libs.windows import WindowPolicy, WindowPolicyProfile, WindowPolicyProfileSpec, WindowsTable
from libs.windows.window import DEFAULT_MIN_SAMPLING_RATE_HZ
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="30_windows_adaptive", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="30_windows_adaptive")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("30_windows_adaptive")
    context = runtime.context
    settings = runtime.settings
    input_path = runtime.artifacts.events
    window_policy_profile_path = runtime.artifacts.window_policy_profile
    output_path = runtime.artifacts.windows
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.write_mode

    min_sampling_rate_hz = float(settings.windowing.min_sampling_rate_hz or DEFAULT_MIN_SAMPLING_RATE_HZ)
    derived_max_ms = WindowPolicy.max_ms_from_min_sampling_rate(min_sampling_rate_hz)
    configured_max_ms = int(settings.windowing.max_ms)
    max_ms = int(configured_max_ms if configured_max_ms > 0 else derived_max_ms)
    min_ms = settings.windowing.min_ms
    event_threshold = settings.windowing.event_threshold
    inactivity_timeout_ms = settings.windowing.inactivity_timeout_ms
    window_strategy = settings.windowing.strategy

    spark = get_spark("s3ntinel.windows_adaptive")
    events_df = read_table(spark, input_path, fmt=table_format)
    fallback_policy = WindowPolicyProfileSpec(
        min_sampling_rate_hz=min_sampling_rate_hz,
        configured_max_ms=max_ms,
        configured_event_threshold=int(event_threshold),
        min_ms=int(min_ms),
        inactivity_timeout_ms=int(inactivity_timeout_ms),
        strategy=str(window_strategy),
    ).fallback_policy
    policy_source = "configured"
    profile_df = None
    profile_path_obj = Path(str(window_policy_profile_path).strip()) if str(window_policy_profile_path).strip() else None
    if profile_path_obj is not None and profile_path_obj.exists():
        profile_df = read_table(spark, str(profile_path_obj), fmt=table_format)
    selected_policy, policy_source = WindowPolicyProfile.resolve_selected_policy(
        profile_df,
        fallback_policy=fallback_policy,
    )
    windows_df = WindowsTable.from_events(
        events_df,
        max_ms=selected_policy.max_ms,
        event_threshold=selected_policy.event_threshold,
        min_ms=selected_policy.min_ms,
        inactivity_timeout_ms=selected_policy.inactivity_timeout_ms,
        strategy=window_strategy,
    ).to_dataframe()
    write_table(
        windows_df,
        path=output_path,
        mode=write_mode,
        fmt=table_format,
        partition_by=context.config["output"]["partition_by"],
    )
    events_count = int(events_df.count())
    windows_count = int(windows_df.count())

    log_params_if_active(
        {
            "max_ms": selected_policy.max_ms,
            "min_sampling_rate_hz": min_sampling_rate_hz,
            "min_ms": selected_policy.min_ms,
            "event_threshold": selected_policy.event_threshold,
            "window_strategy": window_strategy,
            "window_inactivity_timeout_ms": selected_policy.inactivity_timeout_ms,
            "window_policy_source": policy_source,
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "30_windows_adaptive",
            "input_path": input_path,
            "output_path": output_path,
            "window_policy_profile_path": window_policy_profile_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "window_strategy": window_strategy,
            "max_ms": selected_policy.max_ms,
            "min_sampling_rate_hz": min_sampling_rate_hz,
            "min_ms": selected_policy.min_ms,
            "event_threshold": selected_policy.event_threshold,
            "inactivity_timeout_ms": selected_policy.inactivity_timeout_ms,
            "policy_source": policy_source,
            "partition_by": list(context.config["output"]["partition_by"]),
        },
        runtime.report_paths.summary_artifact_path,
    )
    input_artifacts = {
        "events": build_artifact_manifest(path=input_path, dataframe=events_df, row_count=events_count),
    }
    if profile_df is not None:
        input_artifacts["window_policy_profile"] = build_artifact_manifest(
            path=str(profile_path_obj),
            dataframe=profile_df,
            row_count=int(profile_df.count()),
            artifact_version="WINDOW_POLICY_PROFILE_V1",
        )

    stage_manifest = build_stage_manifest(
        stage_name="30_windows_adaptive",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "window_strategy": window_strategy,
            "max_ms": selected_policy.max_ms,
            "min_sampling_rate_hz": min_sampling_rate_hz,
            "min_ms": selected_policy.min_ms,
            "event_threshold": selected_policy.event_threshold,
            "inactivity_timeout_ms": selected_policy.inactivity_timeout_ms,
            "policy_source": policy_source,
        },
        input_artifacts=input_artifacts,
        output_artifacts={
            "windows": build_artifact_manifest(path=output_path, dataframe=windows_df, row_count=windows_count),
        },
        replayable_from=["events"],
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
    LOGGER.info(
        "pipeline=windows_adaptive format=%s write_mode=%s strategy=%s max_ms=%s event_threshold=%s inactivity_timeout_ms=%s input=%s output=%s",
        table_format,
        write_mode,
        window_strategy,
        selected_policy.max_ms,
        selected_policy.event_threshold,
        selected_policy.inactivity_timeout_ms,
        input_path,
        output_path,
    )


if __name__ == "__main__":
    run()
