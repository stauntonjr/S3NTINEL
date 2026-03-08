import importlib


def test_fitting_runner_contains_expected_stage_grouping():
    module = importlib.import_module("pipelines.91_run_fitting_pipeline")
    assert module.FITTING_STAGE_SCRIPTS == ["00_ingest_raw.py", "05_parameter_profiles_fit.py", "10_backbone_fit.py", "11_graph_fit.py"]


def test_inference_runner_contains_expected_stage_grouping():
    module = importlib.import_module("pipelines.92_run_inference_pipeline")
    assert module.INFERENCE_STAGE_SCRIPTS == [
        "20_events_extract.py",
        "30_windows_adaptive.py",
        "50_phase_fit.py",
        "60_window_scores_raw.py",
        "70_window_scores_calibrate.py",
        "80_anomaly_attribution.py",
    ]


def test_full_pipeline_runs_grouped_runners(monkeypatch):
    module = importlib.import_module("pipelines.90_run_full_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run()

    assert captured["run_name"] == "s3ntinel.full_pipeline"
    assert captured["pipeline_mode"] == "full"
    assert captured["stage_scripts"] == ["91_run_fitting_pipeline.py", "92_run_inference_pipeline.py"]
    assert captured["summary_artifact_path"] == "reports/pipeline_run_summary.json"
