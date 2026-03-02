import importlib
from types import SimpleNamespace


def _patch_common(monkeypatch, module):
    monkeypatch.setattr(module, "build_context", lambda: SimpleNamespace(config={"output": {"anomalies_merge_key": ["tail_id", "flight_id", "win_id"], "partition_by": ["tail_id", "flight_id", "date_utc"]}}))
    monkeypatch.setattr(module, "get_spark", lambda _app: object())

    def fake_read_table(_spark, path, fmt="delta"):
        if path.endswith("events") or path.endswith("sensor_subsystem_map") or path.endswith("raw_telemetry"):
            raise RuntimeError("optional table unavailable")
        return object()

    monkeypatch.setattr(module, "read_table", fake_read_table)
    monkeypatch.setattr(module, "build_anomalies_df", lambda **_kwargs: object())
    monkeypatch.setattr(module, "log_params_if_active", lambda *_args, **_kwargs: None)


def test_emit_anomalies_uses_upsert_when_write_mode_merge(monkeypatch):
    module = importlib.import_module("pipelines.80_emit_anomalies")
    _patch_common(monkeypatch, module)

    calls = {"upsert": 0, "write": 0}

    monkeypatch.setenv("S3NTINEL_WRITE_MODE", "merge")
    monkeypatch.setattr(module, "upsert_table", lambda *args, **kwargs: calls.__setitem__("upsert", calls["upsert"] + 1))
    monkeypatch.setattr(module, "write_table", lambda *args, **kwargs: calls.__setitem__("write", calls["write"] + 1))

    module.run()

    assert calls["upsert"] == 1
    assert calls["write"] == 0


def test_emit_anomalies_uses_write_when_non_merge_mode(monkeypatch):
    module = importlib.import_module("pipelines.80_emit_anomalies")
    _patch_common(monkeypatch, module)

    calls = {"upsert": 0, "write": 0}

    monkeypatch.setenv("S3NTINEL_WRITE_MODE", "append")
    monkeypatch.setattr(module, "upsert_table", lambda *args, **kwargs: calls.__setitem__("upsert", calls["upsert"] + 1))
    monkeypatch.setattr(module, "write_table", lambda *args, **kwargs: calls.__setitem__("write", calls["write"] + 1))

    module.run()

    assert calls["upsert"] == 0
    assert calls["write"] == 1
