"""Fit a data-driven window policy profile from detected events."""

from __future__ import annotations

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
from libs.windows import WindowPolicyProfileSpec, build_window_policy_profile_table
from libs.windows.window import DEFAULT_MIN_SAMPLING_RATE_HZ, WindowPolicy
from pipelines.common import build_context, context_artifacts, context_execution, context_settings


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="25_window_policy_profile", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="25_window_policy_profile")
@log_wall_time(logger=LOGGER)
def run() -> None:
    context = build_context()
    artifacts = context_artifacts(context)
    execution = context_execution(context)
    settings = context_settings(context)
    events_path = artifacts.events
    output_path = artifacts.window_policy_profile
    table_format = execution.table_format
    write_mode = execution.fit_write_mode

    min_sampling_rate_hz = float(settings.windowing.min_sampling_rate_hz or DEFAULT_MIN_SAMPLING_RATE_HZ)
    configured_max_ms = int(settings.windowing.max_ms)
    derived_max_ms = WindowPolicy.max_ms_from_min_sampling_rate(min_sampling_rate_hz)
    resolved_max_ms = int(configured_max_ms if configured_max_ms > 0 else derived_max_ms)

    spark = get_spark("s3ntinel.window_policy_profile")
    events_df = read_table(spark, events_path, fmt=table_format)
    profile_df = build_window_policy_profile_table(
        events_df,
        spec=WindowPolicyProfileSpec(
            min_sampling_rate_hz=min_sampling_rate_hz,
            configured_max_ms=resolved_max_ms,
            configured_event_threshold=int(settings.windowing.event_threshold),
            min_ms=int(settings.windowing.min_ms),
            inactivity_timeout_ms=int(settings.windowing.inactivity_timeout_ms),
            strategy=str(settings.windowing.strategy),
        ),
    )
    write_table(profile_df, path=output_path, mode=write_mode, fmt=table_format)

    events_count = int(events_df.count())
    profile_count = int(profile_df.count())
    selected_row = profile_df.where("is_selected").orderBy("candidate_rank").limit(1).first()
    selected_payload = selected_row.asDict() if selected_row is not None else {}

    log_params_if_active(
        {
            "table_format": table_format,
            "resolved_max_ms": resolved_max_ms,
            "configured_event_threshold": int(settings.windowing.event_threshold),
            "min_ms": int(settings.windowing.min_ms),
            "window_strategy": str(settings.windowing.strategy),
            "window_inactivity_timeout_ms": int(settings.windowing.inactivity_timeout_ms),
        }
    )
    log_dict_artifact_if_active(
        {
            "stage": "25_window_policy_profile",
            "events_path": events_path,
            "window_policy_profile_path": output_path,
            "table_format": table_format,
            "write_mode": write_mode,
            "events_count": events_count,
            "candidate_count": profile_count,
            "selected_policy": selected_payload,
        },
        "reports/stages/25_window_policy_profile_summary.json",
    )
    stage_manifest = build_stage_manifest(
        stage_name="25_window_policy_profile",
        config={
            "table_format": table_format,
            "write_mode": write_mode,
            "resolved_max_ms": resolved_max_ms,
            "event_threshold": int(settings.windowing.event_threshold),
            "min_ms": int(settings.windowing.min_ms),
            "inactivity_timeout_ms": int(settings.windowing.inactivity_timeout_ms),
            "window_strategy": str(settings.windowing.strategy),
        },
        input_artifacts={
            "events": build_artifact_manifest(path=events_path, dataframe=events_df, row_count=events_count),
        },
        output_artifacts={
            "window_policy_profile": build_artifact_manifest(
                path=output_path,
                dataframe=profile_df,
                row_count=profile_count,
                artifact_version="WINDOW_POLICY_PROFILE_V1",
            ),
        },
        replayable_from=["events"],
    )
    log_stage_manifest_if_active(stage_manifest, "reports/stages/25_window_policy_profile_manifest.json")
    LOGGER.info(
        "pipeline=window_policy_profile format=%s write_mode=%s events=%s candidates=%s selected_max_ms=%s selected_event_threshold=%s output=%s",
        table_format,
        write_mode,
        events_count,
        profile_count,
        selected_payload.get("max_ms"),
        selected_payload.get("event_threshold"),
        output_path,
    )


if __name__ == "__main__":
    run()
