import importlib


def test_fitting_runner_sets_overwrite_defaults(monkeypatch):
    module = importlib.import_module("pipelines.91_run_fitting_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.delenv("S3NTINEL_WRITE_MODE", raising=False)
    monkeypatch.delenv("S3NTINEL_FIT_WRITE_MODE", raising=False)
    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run()

    assert module.os.environ["S3NTINEL_WRITE_MODE"] == "overwrite"
    assert module.os.environ["S3NTINEL_FIT_WRITE_MODE"] == "overwrite"
    assert captured["run_name"] == "s3ntinel.fitting_pipeline"


def test_inference_runner_sets_overwrite_default(monkeypatch):
    module = importlib.import_module("pipelines.92_run_inference_pipeline")

    captured = {}

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.delenv("S3NTINEL_WRITE_MODE", raising=False)
    monkeypatch.setattr(module, "run_stage_group", fake_run_stage_group)

    module.run()

    assert module.os.environ["S3NTINEL_WRITE_MODE"] == "overwrite"
    assert captured["run_name"] == "s3ntinel.inference_pipeline"
