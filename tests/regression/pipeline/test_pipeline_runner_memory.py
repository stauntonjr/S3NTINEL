import contextlib

from pipelines import _pipeline_runner


def test_run_stage_group_summary_includes_memory_snapshot(monkeypatch) -> None:
    captured_artifacts: list[tuple[dict, str]] = []
    captured_metrics: list[tuple[str, float]] = []

    monkeypatch.setattr(_pipeline_runner, "pipeline_run_context", lambda **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(_pipeline_runner, "active_run_id", lambda: "parent-123")
    monkeypatch.setattr(_pipeline_runner.runpy, "run_path", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_pipeline_runner, "capture_memory_snapshot", lambda **kwargs: {"label": kwargs["label"], "status": kwargs["status"]})
    monkeypatch.setattr(
        _pipeline_runner,
        "log_metric_if_active",
        lambda name, value: captured_metrics.append((name, float(value))),
    )
    monkeypatch.setattr(
        _pipeline_runner,
        "log_dict_artifact_if_active",
        lambda payload, artifact_file: captured_artifacts.append((payload, artifact_file)),
    )

    _pipeline_runner.run_stage_group(
        run_name="s3ntinel.test_group",
        pipeline_mode="test",
        stage_scripts=["00_ingest_raw.py", "20_events_extract.py"],
        summary_artifact_path="reports/test_summary.json",
        logger_name="test.logger",
    )

    metric_names = {name for name, _ in captured_metrics}
    assert "pipeline_total_elapsed_ms" in metric_names
    assert "pipeline_completed_stage_count" in metric_names
    assert "pipeline_failed_stage_count" in metric_names
    summary, artifact_file = captured_artifacts[0]
    assert artifact_file == "reports/test_summary.json"
    assert summary["status"] == "success"
    assert summary["memory_snapshot_end"] == {"label": "s3ntinel.test_group", "status": "success"}
    assert [stage["status"] for stage in summary["stages"]] == ["success", "success"]
