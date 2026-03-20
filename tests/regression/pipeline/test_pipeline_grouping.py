import importlib


def test_fitting_runner_contains_expected_stage_grouping():
    module = importlib.import_module("pipelines.97_run_fitting_pipeline")
    assert module.FITTING_STAGE_SCRIPTS == [
        "00_ingest_raw.py",
        "10_parameter_profiles_fit.py",
        "20_events_extract.py",
        "25_window_policy_profile.py",
        "30_windows_adaptive.py",
        "40_backbone_fit.py",
        "50_build_graph.py",
        "60_fit_hierarchy.py",
    ]


def test_inference_runner_contains_expected_stage_grouping():
    module = importlib.import_module("pipelines.98_run_inference_pipeline")
    assert module.INFERENCE_STAGE_SCRIPTS == [
        "70_phase_fit.py",
        "80_window_scores_raw.py",
        "85_window_scores_calibrate.py",
        "90_anomaly_attribution.py",
        "95_emit_explorer_bundle.py",
    ]


def test_full_pipeline_runs_grouped_runners(monkeypatch):
    module = importlib.import_module("pipelines.99_run_full_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run()

    assert captured["run_name"] == "s3ntinel.full_pipeline"
    assert captured["pipeline_mode"] == "full"
    assert captured["stage_scripts"] == ["97_run_fitting_pipeline.py", "98_run_inference_pipeline.py"]
    assert captured["summary_artifact_path"] == "reports/pipeline_run_summary.json"
