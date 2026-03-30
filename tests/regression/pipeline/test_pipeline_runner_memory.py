import contextlib
import json
from pathlib import Path

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


def test_run_stage_group_supports_stage_range_and_replay_validation(monkeypatch, tmp_path) -> None:
    captured_stage_paths: list[str] = []

    replay_dir = tmp_path / "replay"
    manifest_dir = replay_dir / "reports" / "stages"
    input_dir = replay_dir / "delta"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    events_path = input_dir / "events"
    events_path.write_text("ok", encoding="utf-8")
    (manifest_dir / "20_events_extract_manifest.json").write_text(
        json.dumps(
            {
                "replayable_from": ["events"],
                "input_artifacts": {
                    "events": {"path": str(events_path)},
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(_pipeline_runner, "pipeline_run_context", lambda **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(_pipeline_runner, "active_run_id", lambda: "parent-123")
    monkeypatch.setattr(
        _pipeline_runner.runpy,
        "run_path",
        lambda path, **_kwargs: captured_stage_paths.append(Path(path).name),
    )
    monkeypatch.setattr(_pipeline_runner, "capture_memory_snapshot", lambda **kwargs: {"label": kwargs["label"], "status": kwargs["status"]})
    monkeypatch.setattr(_pipeline_runner, "log_metric_if_active", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_pipeline_runner, "log_dict_artifact_if_active", lambda *_args, **_kwargs: None)

    summary = _pipeline_runner.run_stage_group(
        run_name="s3ntinel.test_group",
        pipeline_mode="test",
        stage_scripts=["00_ingest_raw.py", "10_parameter_profiles_fit.py", "20_events_extract.py"],
        summary_artifact_path="reports/test_summary.json",
        logger_name="test.logger",
        start_stage_script="20_events_extract.py",
        replay_run_dir=str(replay_dir),
    )

    assert captured_stage_paths == ["20_events_extract.py"]
    assert summary.selected_stage_count == 1
    assert summary.start_stage_script == "20_events_extract.py"
    assert summary.replay_run_dir == str(replay_dir)


def test_run_stage_group_emits_group_manifest_from_child_manifest(monkeypatch, tmp_path) -> None:
    captured_stage_manifests: list[tuple[dict, str]] = []
    captured_summaries: list[tuple[dict, str]] = []
    artifact_base_dir = tmp_path / "artifacts"
    manifest_dir = artifact_base_dir / "reports" / "stages"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    def fake_run_path(path, **_kwargs):
        stage_name = Path(path).name
        (manifest_dir / f"{stage_name.removesuffix('.py')}_manifest.json").write_text(
            json.dumps(
                {
                    "replayable_from": ["raw_telemetry"],
                    "input_artifacts": {
                        "raw_telemetry": {"path": str(artifact_base_dir / "delta" / "raw_telemetry")},
                    },
                    "output_artifacts": {},
                }
            ),
            encoding="utf-8",
        )

    monkeypatch.setenv("S3NTINEL_LOCAL_ARTIFACT_BASE_DIR", str(artifact_base_dir))
    monkeypatch.setattr(_pipeline_runner, "pipeline_run_context", lambda **_kwargs: contextlib.nullcontext())
    monkeypatch.setattr(_pipeline_runner, "active_run_id", lambda: "parent-123")
    monkeypatch.setattr(_pipeline_runner.runpy, "run_path", fake_run_path)
    monkeypatch.setattr(_pipeline_runner, "capture_memory_snapshot", lambda **kwargs: {"label": kwargs["label"], "status": kwargs["status"]})
    monkeypatch.setattr(_pipeline_runner, "log_metric_if_active", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _pipeline_runner,
        "log_dict_artifact_if_active",
        lambda payload, artifact_file: captured_summaries.append((payload, artifact_file)),
    )
    monkeypatch.setattr(
        _pipeline_runner,
        "log_stage_manifest_if_active",
        lambda payload, artifact_file: captured_stage_manifests.append((payload, artifact_file)),
    )

    _pipeline_runner.run_stage_group(
        spec=_pipeline_runner.StageGroupSpec(
            run_name="s3ntinel.test_group",
            pipeline_mode="test",
            stage_scripts=("00_ingest_raw.py",),
            summary_artifact_path="reports/test_summary.json",
            manifest_artifact_path="reports/stages/test_group_manifest.json",
            logger_name="test.logger",
        )
    )

    manifest_payload, artifact_file = captured_stage_manifests[0]
    assert artifact_file == "reports/stages/test_group_manifest.json"
    assert manifest_payload["replayable_from"] == ["raw_telemetry"]
    assert "stage_group_summary" in manifest_payload["output_artifacts"]
    assert "00_ingest_raw_manifest" in manifest_payload["output_artifacts"]
