from __future__ import annotations

import json

import pandas as pd
import pytest

from libs.io.delta import get_spark
from scripts import run_sim_pipeline as runner


def _runner_config(tmp_path):
    return runner.PipelineRunConfig(
        flight_name="power_chain",
        tail_id="TSEED",
        flight_id="FSEED",
        n_steps=6,
        dt_seconds=1.0,
        base_dir=str(tmp_path),
        mode="full",
        table_format="parquet",
        write_mode="overwrite",
        min_warm=1,
        delta_threshold=0.0,
        slope_source="ema",
        ema_alpha=0.2,
        window_max_ms=10000,
        window_event_threshold=2,
        window_min_ms=50,
        window_inactivity_timeout_ms=0,
        window_strategy="bucketed",
        phase_count=3,
        backbone_parameter_count=4,
        backbone_ridge_lambda=1.0,
    )


def test_sim_runner_uses_grouped_full_stage_scripts(monkeypatch, tmp_path):
    captured: dict[str, object] = {}

    monkeypatch.setattr(runner, "resolve_flight", lambda _flight_name: object())
    monkeypatch.setattr(runner, "get_spark", lambda _app_name: object())
    monkeypatch.setattr(
        runner,
        "_write_seed_tables",
        lambda **_kwargs: {"raw_input_rows": 1, "phase_label_rows": 1, "hierarchy_label_rows": 1},
    )
    monkeypatch.setattr(runner, "_write_validation_reports", lambda **_kwargs: {})

    def fake_run_stage_group(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(runner, "run_stage_group", fake_run_stage_group)
    result = runner.run_pipeline(_runner_config(tmp_path))

    assert captured["run_name"] == "s3ntinel.sim_full"
    assert captured["pipeline_mode"] == "sim_full:v2"
    assert captured["stage_scripts"] == [
        "00_ingest_raw.py",
        "05_parameter_profiles_fit.py",
        "20_events_extract.py",
        "30_windows_adaptive.py",
        "10_backbone_fit.py",
        "11_graph_fit.py",
        "50_phase_fit.py",
        "60_window_scores_raw.py",
        "70_window_scores_calibrate.py",
        "80_anomaly_attribution.py",
    ]
    assert captured["summary_artifact_path"] == "reports/pipeline_run_summary.json"
    assert result.status == "success"


def test_sim_runner_writes_seed_tables(tmp_path, monkeypatch):
    config = _runner_config(tmp_path)
    run_dir = tmp_path / "sim_seed"
    paths = runner.RunPaths(run_dir=run_dir)
    previous_env = runner._set_run_env(paths, config)
    written_tables: dict[str, dict[str, object]] = {}

    class FakeSparkFrame:
        def __init__(self, records):
            self.records = list(records)
            self.columns = list(self.records[0].keys()) if self.records else []
            self.write = self

        def mode(self, _mode):
            return self

        def parquet(self, path):
            written_tables["raw_input"] = {"path": path, "records": self.records, "columns": self.columns}

    class FakeSpark:
        def createDataFrame(self, records, schema=None):
            return FakeSparkFrame(records)

    def fake_write_table(df, path, mode, fmt, partition_by=None):
        written_tables[str(path)] = {
            "path": path,
            "mode": mode,
            "fmt": fmt,
            "partition_by": list(partition_by or []),
            "records": list(df.records),
            "columns": list(df.columns),
        }

    monkeypatch.setattr(runner, "write_table", fake_write_table)
    try:
        flight = runner.resolve_flight("power_chain")
        counts = runner._write_seed_tables(spark=FakeSpark(), paths=paths, config=config, flight=flight)

        assert counts["raw_input_rows"] > 0
        assert counts["phase_label_rows"] > 0
        assert counts["hierarchy_label_rows"] > 0

        raw_input = written_tables["raw_input"]
        phase_labels = written_tables[str(run_dir / "delta" / "phase_labels")]
        hierarchy_labels = written_tables[str(run_dir / "delta" / "hierarchy_sensor_map_label")]

        assert raw_input["path"] == str(run_dir / "input" / "raw_telemetry")
        assert len(raw_input["records"]) == counts["raw_input_rows"]
        assert {"tail_id", "flight_id", "timestamp_utc", "parameter_name", "parameter_value", "date_utc"}.issubset(raw_input["columns"])
        assert {"tail_id", "flight_id", "step_index", "timestamp_utc", "phase_label", "date_utc"}.issubset(phase_labels["columns"])
        assert {"parameter_name", "system_id", "subsystem_id", "module_id"}.issubset(hierarchy_labels["columns"])
    finally:
        runner._restore_env(previous_env)


def test_sim_runner_full_smoke_emits_bundle(monkeypatch, tmp_path):
    try:
        spark = get_spark("s3ntinel.test_sim_full_smoke_preflight")
    except Exception as exc:
        pytest.skip(f"local Spark unavailable in this environment: {exc.__class__.__name__}")
    else:
        spark.stop()

    config = runner.PipelineRunConfig(
        flight_name="power_chain",
        tail_id="T_SIM",
        flight_id="F_SIM",
        n_steps=12,
        dt_seconds=1.0,
        base_dir=str(tmp_path),
        mode="full",
        table_format="parquet",
        write_mode="overwrite",
        min_warm=1,
        delta_threshold=0.0,
        slope_source="ema",
        ema_alpha=0.2,
        window_max_ms=10000,
        window_event_threshold=2,
        window_min_ms=50,
        window_inactivity_timeout_ms=0,
        window_strategy="bucketed",
        phase_count=3,
        backbone_parameter_count=4,
        backbone_ridge_lambda=1.0,
    )
    result = runner.run_pipeline(config)
    run_dir = result.paths.run_dir

    manifest = json.loads((run_dir / "reports" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "success"
    assert (run_dir / "logs" / "run.log").exists()
    assert (run_dir / "reports" / "pipeline_run_summary.json").exists()
    assert (run_dir / "reports" / "stages" / "05_parameter_profiles_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "10_backbone_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "11_graph_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "50_phase_fit_manifest.json").exists()
    assert (run_dir / "reports" / "stages" / "60_window_scores_raw_manifest.json").exists()
    assert (run_dir / "reports" / "phase_validation_summary.json").exists()
    assert (run_dir / "reports" / "hierarchy_validation_summary.json").exists()
    assert (run_dir / "reports" / "score_validation_summary.json").exists()
    assert (run_dir / "reports" / "fault_window_validation_summary.json").exists()
    assert (run_dir / "reports" / "attribution_validation_summary.json").exists()

    assert len(pd.read_parquet(run_dir / "delta" / "raw_telemetry")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "events")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "windows")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "backbone")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "precision_graph")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "hierarchy_sensor_map")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "phase_windows")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "window_scores_calibrated")) > 0
    assert len(pd.read_parquet(run_dir / "delta" / "anomaly_window_attribution")) > 0


def test_sim_runner_uses_library_validation_reports(monkeypatch, tmp_path):
    config = _runner_config(tmp_path)
    monkeypatch.setattr(runner, "resolve_flight", lambda _flight_name: object())
    monkeypatch.setattr(runner, "get_spark", lambda _app_name: object())
    monkeypatch.setattr(
        runner,
        "_write_seed_tables",
        lambda **_kwargs: {"raw_input_rows": 1, "phase_label_rows": 1, "hierarchy_label_rows": 1},
    )
    monkeypatch.setattr(runner, "run_stage_group", lambda **_kwargs: None)
    called: dict[str, object] = {}

    def fake_write_validation_reports(**kwargs):
        called.update(kwargs)
        return {}

    monkeypatch.setattr(runner, "_write_validation_reports", fake_write_validation_reports)

    result = runner.run_pipeline(config)

    assert result.status == "success"
    assert called["table_format"] == config.table_format
