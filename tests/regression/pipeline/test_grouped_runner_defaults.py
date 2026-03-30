import importlib


def test_fitting_runner_sets_overwrite_defaults(monkeypatch):
    module = importlib.import_module("pipelines.97_run_fitting_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.delenv("S3NTINEL_WRITE_MODE", raising=False)
    monkeypatch.delenv("S3NTINEL_FIT_WRITE_MODE", raising=False)
    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run()

    assert module.os.environ["S3NTINEL_WRITE_MODE"] == "overwrite"
    assert module.os.environ["S3NTINEL_FIT_WRITE_MODE"] == "overwrite"
    assert captured["spec"] == module.FITTING_STAGE_GROUP


def test_fitting_runner_passes_stage_range_and_replay(monkeypatch):
    module = importlib.import_module("pipelines.97_run_fitting_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run(
        start_stage_script="20_events_extract.py",
        end_stage_script="30_windows_adaptive.py",
        replay_run_dir="/tmp/replay",
    )

    assert captured["spec"] == module.FITTING_STAGE_GROUP
    assert captured["start_stage_script"] == "20_events_extract.py"
    assert captured["end_stage_script"] == "30_windows_adaptive.py"
    assert captured["replay_run_dir"] == "/tmp/replay"


def test_inference_runner_sets_overwrite_default(monkeypatch):
    module = importlib.import_module("pipelines.98_run_inference_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.delenv("S3NTINEL_WRITE_MODE", raising=False)
    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run()

    assert module.os.environ["S3NTINEL_WRITE_MODE"] == "overwrite"
    assert captured["spec"] == module.INFERENCE_STAGE_GROUP


def test_inference_runner_passes_stage_range_and_replay(monkeypatch):
    module = importlib.import_module("pipelines.98_run_inference_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run(
        start_stage_script="80_window_scores_raw.py",
        end_stage_script="90_anomaly_attribution.py",
        replay_run_dir="/tmp/replay",
    )

    assert captured["spec"] == module.INFERENCE_STAGE_GROUP
    assert captured["start_stage_script"] == "80_window_scores_raw.py"
    assert captured["end_stage_script"] == "90_anomaly_attribution.py"
    assert captured["replay_run_dir"] == "/tmp/replay"
