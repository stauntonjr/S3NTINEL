import importlib
import os
from types import SimpleNamespace


class _FakeFrame:
    def count(self) -> int:
        return 0


class _FakeArtifact:
    def __init__(self, calls: dict[str, int]) -> None:
        self._frame = _FakeFrame()
        self._calls = calls

    def bind(self, **_kwargs):
        return self

    def to_dataframe(self):
        return self._frame

    def upsert(self, **_kwargs) -> None:
        self._calls["upsert"] += 1
        return None

    def write(self, **_kwargs) -> None:
        self._calls["write"] += 1
        return None


def _patch_common(monkeypatch, module, calls: dict[str, int]):
    def fake_runtime(*_args, **_kwargs):
        return SimpleNamespace(
            context=SimpleNamespace(
                config={
                    "output": {
                        "anomalies_merge_key": ["tail_id", "flight_id", "win_id"],
                        "partition_by": ["tail_id", "flight_id", "date_utc"],
                    }
                }
            ),
            artifacts=SimpleNamespace(
                window_scores_calibrated="delta/window_scores_calibrated",
                phase_windows="delta/phase_windows",
                windows="delta/windows",
                events="delta/events",
                hierarchy_sensor_map="delta/hierarchy_sensor_map",
                parameter_behavior_profile="delta/parameter_behavior_profile",
                raw_table="delta/raw_telemetry",
                anomaly_window_attribution="delta/anomaly_window_attribution",
                anomaly_telemetry_attribution="delta/anomaly_telemetry_attribution",
                anomaly_event_attribution="delta/anomaly_event_attribution",
                anomaly_parameter_candidate_evidence="delta/anomaly_parameter_candidate_evidence",
            ),
            execution=SimpleNamespace(
                table_format="delta",
                write_mode=os.environ.get("S3NTINEL_WRITE_MODE", "merge"),
            ),
            settings=SimpleNamespace(
                anomaly=SimpleNamespace(subsystem_top_sensors_k=5),
            ),
            report_paths=SimpleNamespace(
                summary_artifact_path="reports/stages/90_anomaly_attribution_summary.json",
                manifest_artifact_path="reports/stages/90_anomaly_attribution_manifest.json",
            ),
        )

    monkeypatch.setattr(module, "build_stage_runtime", fake_runtime)
    monkeypatch.setattr(module, "get_spark", lambda _app: object())

    def fake_read_table(_spark, path, fmt="delta"):
        return _FakeFrame()

    monkeypatch.setattr(module, "read_table", fake_read_table)
    monkeypatch.setattr(
        module.ParameterBehaviorProfile,
        "read",
        classmethod(lambda cls, *_args, **_kwargs: SimpleNamespace(to_dataframe=lambda: _FakeFrame())),
    )
    monkeypatch.setattr(
        module.AnomalyAttributionPlan,
        "build",
        lambda self, **_kwargs: SimpleNamespace(
            window_attribution=_FakeArtifact(calls),
            telemetry_attribution=_FakeArtifact(calls),
            event_attribution=_FakeArtifact(calls),
            parameter_candidate_evidence=_FakeArtifact(calls),
        ),
    )
    monkeypatch.setattr(module, "log_params_if_active", lambda *_args, **_kwargs: None)


def test_emit_anomalies_uses_upsert_when_write_mode_merge(monkeypatch):
    module = importlib.import_module("pipelines.90_anomaly_attribution")
    calls = {"upsert": 0, "write": 0}
    _patch_common(monkeypatch, module, calls)
    artifact_calls: list[tuple[dict, str]] = []

    monkeypatch.setenv("S3NTINEL_WRITE_MODE", "merge")
    monkeypatch.setattr(module, "log_dict_artifact_if_active", lambda payload, artifact_file: artifact_calls.append((payload, artifact_file)))

    module.run()

    assert calls["upsert"] == 4
    assert calls["write"] == 0
    assert len(artifact_calls) == 1
    payload, artifact_file = artifact_calls[0]
    assert artifact_file == "reports/stages/90_anomaly_attribution_summary.json"
    assert payload["write_mode"] == "merge"
    assert payload["merge_key"] == ["tail_id", "flight_id", "win_id"]


def test_emit_anomalies_uses_write_when_non_merge_mode(monkeypatch):
    module = importlib.import_module("pipelines.90_anomaly_attribution")
    calls = {"upsert": 0, "write": 0}
    _patch_common(monkeypatch, module, calls)
    artifact_calls: list[tuple[dict, str]] = []

    monkeypatch.setenv("S3NTINEL_WRITE_MODE", "append")
    monkeypatch.setattr(module, "log_dict_artifact_if_active", lambda payload, artifact_file: artifact_calls.append((payload, artifact_file)))

    module.run()

    assert calls["upsert"] == 0
    assert calls["write"] == 4
    assert len(artifact_calls) == 1
    payload, artifact_file = artifact_calls[0]
    assert artifact_file == "reports/stages/90_anomaly_attribution_summary.json"
    assert payload["write_mode"] == "append"
