import importlib
from types import SimpleNamespace


class _FakeFrame:
    def count(self) -> int:
        return 0


def _patch_common(monkeypatch, module):
    monkeypatch.setattr(module, "build_context", lambda: SimpleNamespace(config={"output": {"anomalies_merge_key": ["tail_id", "flight_id", "win_id"], "partition_by": ["tail_id", "flight_id", "date_utc"]}}))
    monkeypatch.setattr(module, "get_spark", lambda _app: object())

    def fake_read_table(_spark, path, fmt="delta"):
        return _FakeFrame()

    monkeypatch.setattr(module, "read_table", fake_read_table)
    monkeypatch.setattr(module, "build_anomaly_window_attribution_table", lambda **_kwargs: _FakeFrame())
    monkeypatch.setattr(module, "build_anomaly_telemetry_attribution_table", lambda **_kwargs: _FakeFrame())
    monkeypatch.setattr(module, "build_anomaly_event_attribution_table", lambda **_kwargs: _FakeFrame())
    monkeypatch.setattr(module, "log_params_if_active", lambda *_args, **_kwargs: None)


def test_emit_anomalies_uses_upsert_when_write_mode_merge(monkeypatch):
    module = importlib.import_module("pipelines.90_anomaly_attribution")
    _patch_common(monkeypatch, module)

    calls = {"upsert": 0, "write": 0}
    artifact_calls: list[tuple[dict, str]] = []

    monkeypatch.setenv("S3NTINEL_WRITE_MODE", "merge")
    monkeypatch.setattr(module, "upsert_table", lambda *args, **kwargs: calls.__setitem__("upsert", calls["upsert"] + 1))
    monkeypatch.setattr(module, "write_table", lambda *args, **kwargs: calls.__setitem__("write", calls["write"] + 1))
    monkeypatch.setattr(module, "log_dict_artifact_if_active", lambda payload, artifact_file: artifact_calls.append((payload, artifact_file)))

    module.run()

    assert calls["upsert"] == 3
    assert calls["write"] == 0
    assert len(artifact_calls) == 1
    payload, artifact_file = artifact_calls[0]
    assert artifact_file == "reports/stages/90_anomaly_attribution_summary.json"
    assert payload["write_mode"] == "merge"
    assert payload["merge_key"] == ["tail_id", "flight_id", "win_id"]


def test_emit_anomalies_uses_write_when_non_merge_mode(monkeypatch):
    module = importlib.import_module("pipelines.90_anomaly_attribution")
    _patch_common(monkeypatch, module)

    calls = {"upsert": 0, "write": 0}
    artifact_calls: list[tuple[dict, str]] = []

    monkeypatch.setenv("S3NTINEL_WRITE_MODE", "append")
    monkeypatch.setattr(module, "upsert_table", lambda *args, **kwargs: calls.__setitem__("upsert", calls["upsert"] + 1))
    monkeypatch.setattr(module, "write_table", lambda *args, **kwargs: calls.__setitem__("write", calls["write"] + 1))
    monkeypatch.setattr(module, "log_dict_artifact_if_active", lambda payload, artifact_file: artifact_calls.append((payload, artifact_file)))

    module.run()

    assert calls["upsert"] == 0
    assert calls["write"] == 3
    assert len(artifact_calls) == 1
    payload, artifact_file = artifact_calls[0]
    assert artifact_file == "reports/stages/90_anomaly_attribution_summary.json"
    assert payload["write_mode"] == "append"
