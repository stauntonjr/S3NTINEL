from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from libs.plotting import (
    SensorExplorerState,
    artifact_availability_table,
    build_sensor_explorer_dataset,
    build_sensor_explorer_figure,
    build_sensor_explorer_filter_options,
    discover_latest_run_dir,
    extract_log_records,
    filter_sensor_explorer_data,
    load_artifact_table,
    load_simulation_run_bundle,
    validation_summary_table,
)


def _write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _build_run_bundle(tmp_path: Path, run_name: str, *, include_validations: bool = True) -> Path:
    run_dir = tmp_path / run_name
    reports_dir = run_dir / "reports"
    logs_dir = run_dir / "logs"
    delta_dir = run_dir / "delta"
    reports_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    delta_dir.mkdir(parents=True, exist_ok=True)

    hierarchy_df = pd.DataFrame(
        [
            {"parameter_name": "sensor_a", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_A"},
            {"parameter_name": "sensor_b", "system_id": "SYS_A", "subsystem_id": "SUB_A", "module_id": "MOD_B"},
        ]
    )
    scores_df = pd.DataFrame(
        [
            {"tail_id": "T1", "flight_id": "F1", "win_id": "W1", "global_score": 0.25, "p_value": 0.8},
            {"tail_id": "T1", "flight_id": "F1", "win_id": "W2", "global_score": 1.2, "p_value": 0.1},
        ]
    )
    telemetry_df = pd.DataFrame(
        [
            {
                "timestamp_utc": "2025-01-01T00:00:00Z",
                "parameter_name": "sensor_a",
                "parameter_value": "1.0",
                "parameter_value_clean": "1.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_A",
                "phase_label": "climb",
            },
            {
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_a",
                "parameter_value": "2.0",
                "parameter_value_clean": "2.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_A",
                "phase_label": "cruise",
            },
            {
                "timestamp_utc": "2025-01-01T00:00:00Z",
                "parameter_name": "sensor_b",
                "parameter_value": "10.0",
                "parameter_value_clean": "10.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
                "phase_label": "climb",
            },
            {
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_b",
                "parameter_value": "11.0",
                "parameter_value_clean": "11.0",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
                "phase_label": "cruise",
            },
        ]
    )
    events_df = pd.DataFrame(
        [
            {"timestamp_utc": "2025-01-01T00:00:01Z", "parameter_name": "sensor_a", "event_type_detected": "slope_pos"},
            {"timestamp_utc": "2025-01-01T00:00:01Z", "parameter_name": "sensor_b", "event_type_detected": "transition"},
        ]
    )
    anomaly_telemetry_df = pd.DataFrame(
        [
            {
                "timestamp_utc": "2025-01-01T00:00:01Z",
                "parameter_name": "sensor_b",
                "severity": "high",
                "system_id": "SYS_A",
                "subsystem_id": "SUB_A",
                "module_id": "MOD_B",
            }
        ]
    )
    anomaly_window_df = pd.DataFrame(
        [
            {"timestamp_utc": "2025-01-01T00:00:01Z", "severity": "high"},
        ]
    )
    phase_windows_df = pd.DataFrame(
        [
            {
                "t_start": "2025-01-01T00:00:00Z",
                "t_end": "2025-01-01T00:00:01Z",
                "phase_state_detected": "climb",
            }
        ]
    )
    hierarchy_df.to_parquet(delta_dir / "hierarchy_sensor_map")
    scores_df.to_parquet(delta_dir / "window_scores_calibrated")
    telemetry_df.to_parquet(delta_dir / "raw_telemetry")
    events_df.to_parquet(delta_dir / "events")
    anomaly_telemetry_df.to_parquet(delta_dir / "anomaly_telemetry_attribution")
    anomaly_window_df.to_parquet(delta_dir / "anomaly_window_attribution")
    phase_windows_df.to_parquet(delta_dir / "phase_windows")

    manifest = {
        "status": "success",
        "run_dir": str(run_dir),
        "artifacts": {
            "raw_telemetry": {"path": str(delta_dir / "raw_telemetry"), "exists": True},
            "hierarchy_sensor_map": {"path": str(delta_dir / "hierarchy_sensor_map"), "exists": True},
            "window_scores_calibrated": {"path": str(delta_dir / "window_scores_calibrated"), "exists": True},
            "events": {"path": str(delta_dir / "events"), "exists": True},
            "anomaly_telemetry_attribution": {"path": str(delta_dir / "anomaly_telemetry_attribution"), "exists": True},
            "anomaly_window_attribution": {"path": str(delta_dir / "anomaly_window_attribution"), "exists": True},
            "phase_windows": {"path": str(delta_dir / "phase_windows"), "exists": True},
        },
    }
    summary = {
        "status": "success",
        "pipeline_mode": "sim_full:v2",
        "completed_stage_count": 10,
        "failed_stage_count": 0,
        "stages": [
            {"stage_script": "00_ingest_raw.py", "status": "success", "elapsed_ms": 1200.0},
            {"stage_script": "11_build_graph.py", "status": "success", "elapsed_ms": 3500.0},
        ],
    }
    _write_json(reports_dir / "run_manifest.json", manifest)
    _write_json(reports_dir / "pipeline_run_summary.json", summary)
    if include_validations:
        _write_json(reports_dir / "profile_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "event_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "label_contract_summary.json", {"status": "ok"})
        _write_json(reports_dir / "phase_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "hierarchy_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "coupling_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "score_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "misbehavior_score_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "misbehavior_window_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "misbehavior_attribution_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "fault_window_validation_summary.json", {"status": "ok"})
        _write_json(reports_dir / "attribution_validation_summary.json", {"status": "ok"})
    (logs_dir / "run.log").write_text(
        "\n".join(
            [
                "2026-03-14T10:00:00 | INFO | s3ntinel.run_sim_pipeline | sim_run_start",
                "2026-03-14T10:00:05 | WARN | __main__ | something_slow",
                "2026-03-14T10:00:10 | INFO | s3ntinel.run_sim_pipeline | sim_run_complete",
            ]
        ),
        encoding="utf-8",
    )
    return run_dir


def test_discover_latest_run_dir_finds_latest_manifest_bundle(tmp_path):
    older = _build_run_bundle(tmp_path, "20260314T100000Z_power_chain")
    newer = _build_run_bundle(tmp_path, "20260314T110000Z_power_chain")

    latest = discover_latest_run_dir(tmp_path)

    assert latest == newer
    assert latest != older


def test_load_simulation_run_bundle_reads_reports_and_inventory(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T120000Z_power_chain")

    bundle = load_simulation_run_bundle(run_dir)

    assert bundle.manifest is not None
    assert bundle.pipeline_summary is not None
    assert bundle.validation_reports["phase_validation_summary.json"] == {"status": "ok"}
    assert bundle.validation_reports["profile_validation_summary.json"] == {"status": "ok"}
    assert bundle.artifact_inventory["hierarchy_sensor_map"]["exists"] is True
    assert bundle.log_text is not None


def test_load_simulation_run_bundle_handles_missing_validations(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T130000Z_power_chain", include_validations=False)

    bundle = load_simulation_run_bundle(run_dir)
    validation_df = validation_summary_table(bundle)

    assert "phase_validation_summary" in set(validation_df["report_name"])
    assert "missing" in set(validation_df["status"])
    assert any("phase_validation_summary.json" in path for path in bundle.missing_files)


def test_artifact_availability_and_table_loading(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T140000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)

    inventory_df = artifact_availability_table(bundle)
    hierarchy_df = load_artifact_table(bundle, "hierarchy_sensor_map")
    scores_df = load_artifact_table(bundle, "window_scores_calibrated", columns=["global_score"], limit=1)

    assert {"artifact_name", "exists", "path"} == set(inventory_df.columns)
    assert set(hierarchy_df.columns) >= {"parameter_name", "system_id", "subsystem_id", "module_id"}
    assert list(scores_df.columns) == ["global_score"]
    assert len(scores_df) == 1


def test_extract_log_records_parses_log_lines():
    df = extract_log_records(
        "\n".join(
            [
                "2026-03-14T10:00:00 | INFO | logger.name | start",
                "2026-03-14T10:00:01 | ERROR | logger.name | failed",
            ]
        )
    )

    assert list(df["level"]) == ["INFO", "ERROR"]
    assert list(df["message"]) == ["start", "failed"]


def test_sensor_explorer_dataset_and_filter_options(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T150000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)

    dataset = build_sensor_explorer_dataset(bundle)
    options = build_sensor_explorer_filter_options(dataset["telemetry"])

    assert "plot_value" in dataset["telemetry"].columns
    assert options["system_ids"] == ["SYS_A"]
    assert options["module_ids"] == ["MOD_A", "MOD_B"]
    assert options["parameter_names"] == ["sensor_a", "sensor_b"]


def test_sensor_explorer_filtering_and_figure_build(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T160000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)
    dataset = build_sensor_explorer_dataset(bundle)
    state = SensorExplorerState(module_id="MOD_B", scale_mode="normalized", show_events=True, show_anomaly_markers=True)

    filtered = filter_sensor_explorer_data(dataset, state)
    figure = build_sensor_explorer_figure(dataset, state)

    assert set(filtered["telemetry"]["parameter_name"]) == {"sensor_b"}
    assert len(figure.data) >= 2
    assert any(trace.name == "sensor_b" for trace in figure.data)
    assert "normalized" in (figure.layout.title.text or "")


def test_sensor_explorer_dual_axis_mode_builds_plotly_figure(tmp_path):
    run_dir = _build_run_bundle(tmp_path, "20260314T170000Z_power_chain")
    bundle = load_simulation_run_bundle(run_dir)
    dataset = build_sensor_explorer_dataset(bundle)
    state = SensorExplorerState(scale_mode="dual_axis", parameter_names=("sensor_a", "sensor_b"))

    figure = build_sensor_explorer_figure(dataset, state)

    assert len(figure.data) >= 2
    assert "raw" in (figure.layout.title.text or "")
