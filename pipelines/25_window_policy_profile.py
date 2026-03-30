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
from libs.windows import (
    WindowPolicyEvaluationSpec,
    WindowPolicyProfileTable,
    WindowPolicyProfileSpec,
    build_window_policy_profile_evaluation_report_spark,
)
from libs.windows.window import DEFAULT_MIN_SAMPLING_RATE_HZ, WindowPolicy
from pipelines.common import build_stage_runtime


LOGGER = get_logger(__name__)


@track_mlflow_run(stage_name="25_window_policy_profile", logger=LOGGER)
@log_memory_usage(logger=LOGGER, label="25_window_policy_profile")
@log_wall_time(logger=LOGGER)
def run() -> None:
    runtime = build_stage_runtime("25_window_policy_profile")
    settings = runtime.settings
    events_path = runtime.artifacts.events
    output_path = runtime.artifacts.window_policy_profile
    table_format = runtime.execution.table_format
    write_mode = runtime.execution.fit_write_mode
    evaluation_report_path = "reports/stages/25_window_policy_profile_evaluation.json"

    min_sampling_rate_hz = float(settings.windowing.min_sampling_rate_hz or DEFAULT_MIN_SAMPLING_RATE_HZ)
    configured_max_ms = int(settings.windowing.max_ms)
    derived_max_ms = WindowPolicy.max_ms_from_min_sampling_rate(min_sampling_rate_hz)
    resolved_max_ms = int(configured_max_ms if configured_max_ms > 0 else derived_max_ms)
    profile_spec = WindowPolicyProfileSpec(
        min_sampling_rate_hz=min_sampling_rate_hz,
        configured_max_ms=resolved_max_ms,
        configured_event_threshold=int(settings.windowing.event_threshold),
        min_ms=int(settings.windowing.min_ms),
        inactivity_timeout_ms=int(settings.windowing.inactivity_timeout_ms),
        strategy=str(settings.windowing.strategy),
    )

    spark = get_spark("s3ntinel.window_policy_profile")
    events_df = read_table(spark, events_path, fmt=table_format)
    profile_df = WindowPolicyProfileTable.from_events(
        events_df,
        spec=profile_spec,
    ).to_dataframe()
    write_table(profile_df, path=output_path, mode=write_mode, fmt=table_format)
    evaluation_report = build_window_policy_profile_evaluation_report_spark(
        events_df,
        profile_df=profile_df,
        profile_spec=profile_spec,
        evaluation_spec=WindowPolicyEvaluationSpec(
            max_stability_flights=int(profile_spec.max_profile_flights),
        ),
    )

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
            "window_policy_profile_evaluation_path": evaluation_report_path,
            "selected_max_ms": selected_payload.get("max_ms"),
            "selected_event_threshold": selected_payload.get("event_threshold"),
            "selected_candidate_rank": selected_payload.get("candidate_rank"),
            "selected_objective_score": selected_payload.get("objective_score"),
            "selected_balance_penalty": selected_payload.get("balance_penalty"),
            "evaluation_status": evaluation_report.get("status"),
        },
        runtime.report_paths.summary_artifact_path,
    )
    log_dict_artifact_if_active(
        evaluation_report,
        evaluation_report_path,
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
        cache_artifacts={
            "window_policy_profile_cache": {
                "window_policy_profile_evaluation_path": evaluation_report_path,
            }
        },
    )
    log_stage_manifest_if_active(stage_manifest, runtime.report_paths.manifest_artifact_path)
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
